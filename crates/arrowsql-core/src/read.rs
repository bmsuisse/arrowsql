//! Partitioned parallel Arrow reads.
//!
//! One TDS session delivers a result at whatever rate SQL Server serializes it
//! (about 1.4M rows/s for a 20-column table on a dev box), well below what the
//! decoder can take. [`read_arrow_stream`] splits the query on a column into
//! ranges and reads them over separate connections at once.
//!
//! The range is cut into more slices than workers; each worker holds one
//! connection and pulls slices from a queue, so one slow connection does not
//! set the wall time. The MIN/MAX probe runs on the first worker's connection,
//! which also runs the first slice (its result gives the schema).
//!
//! The workers' batches are merged into one bounded, unordered stream
//! ([`MergeCore`]), so a consumer overlaps with the read.

use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use parking_lot::Mutex;

use crate::api::{ConnectOptions, Connection, Param, Statement, Value};
use crate::arrow::stream::{MergeCore, MergeReader, StreamCore};
use crate::error::Error;
use crate::statement::StmtShared;

/// Slices per worker, and the fewest rows worth a slice (when the row count is known).
const SLICES_PER_WORKER: usize = 2;
const MIN_SLICE_ROWS: i64 = 8 * 1024;
/// A result of at most this many rows (catalog estimate) is read over the probe
/// connection alone (measured: parallel wins from about 20k rows up).
const MIN_PARALLEL_ROWS: i64 = 20_000;
/// How long [`ParallelReader::close`] waits for the workers to finish.
const STOP_BUDGET: Duration = Duration::from_secs(30);

const NULLABLE_SQL: &str =
    "(SELECT TOP 1 is_nullable FROM sys.dm_exec_describe_first_result_set(?, NULL, 0))";
const ROWS_SQL: &str = "(SELECT SUM(row_count) FROM sys.dm_db_partition_stats \
     WHERE object_id = OBJECT_ID(?) AND index_id IN (0, 1))";

/// How a read is split across connections.
#[derive(Clone, Debug)]
pub enum Partitioning {
    /// One statement on one connection.
    None,
    /// Cut the [min, max] range of an integer column of the result into
    /// equal-width slices (about twice `partitions`, never more than the key
    /// values in the range), read over `partitions` connections. Rows whose
    /// value is NULL are a slice of their own. A result known to be small
    /// (catalog estimate, when the query is a plain `SELECT ... FROM table`)
    /// is read over one connection.
    Range { column: String, partitions: usize },
    /// Caller-supplied ascending boundaries of `column` (any comparable
    /// type, one type): `k` values give the half-open slices `< b1`,
    /// `[b1, b2)`, ..., `>= bk` (plus `IS NULL` when the column is nullable;
    /// `nullable: None` asks the server). No MIN/MAX or size probe runs. The
    /// bounds must ascend in the column's own ordering (for strings: its
    /// collation), else slices overlap or leave gaps.
    Bounds {
        column: String,
        bounds: Vec<Param>,
        nullable: Option<bool>,
        partitions: usize,
    },
}

/// Options of [`read_arrow_stream`].
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Rows per record batch.
    pub batch_rows: usize,
    /// Batches buffered between the workers and the consumer (default
    /// `2 * workers`, at least 4).
    pub max_buffered: Option<usize>,
    /// Transfer nvarchar/nchar/ntext as UTF-8 (see [`crate::utf8::utf8_query`]).
    pub utf8_strings: bool,
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        ReadOptions {
            batch_rows: crate::arrow::DEFAULT_BATCH_ROWS,
            max_buffered: None,
            utf8_strings: true,
        }
    }
}

/// Opens the connections of a read: the probe connection first, then one per
/// extra worker (from the worker's thread).
pub trait Connector: Send + Sync {
    fn connect(&self) -> Result<Connection, Error>;
}

impl Connector for ConnectOptions {
    fn connect(&self) -> Result<Connection, Error> {
        Connection::connect(self)
    }
}

/// One statement of a read: SQL with `?` placeholders and its parameters.
#[derive(Clone, Debug)]
pub struct Slice {
    pub sql: String,
    pub params: Vec<Param>,
}

impl Slice {
    fn new(sql: String, params: Vec<Param>) -> Slice {
        Slice { sql, params }
    }
}

/// `[name]`, with `]` doubled.
pub fn quote_identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// The slices of `query` cut at `edges` (ascending): `IS NULL` first when
/// `nullable`, then the open-ended first range, the closed ranges between
/// consecutive edges and the open-ended last range, so rows outside the
/// edges cannot be lost. `edges` must not be empty.
pub fn range_slices<T: Clone>(
    query: &str,
    column: &str,
    edges: &[T],
    nullable: bool,
) -> Vec<(String, Vec<T>)> {
    let base = format!("SELECT * FROM ({query}) AS _q WHERE ");
    let mut work = Vec::with_capacity(edges.len() + 2);
    if nullable {
        work.push((format!("{base}{column} IS NULL"), Vec::new()));
    }
    let (Some(first), Some(last)) = (edges.first(), edges.last()) else {
        return work;
    };
    work.push((format!("{base}{column} < ?"), vec![first.clone()]));
    for pair in edges.windows(2) {
        work.push((
            format!("{base}{column} >= ? AND {column} < ?"),
            vec![pair[0].clone(), pair[1].clone()],
        ));
    }
    work.push((format!("{base}{column} >= ?"), vec![last.clone()]));
    work
}

/// `(estimated row count, column may hold NULL)` in one round trip.
///
/// The row count comes from the catalog and only when `plain` (the caller's
/// own query text; `query` when `None`) is a plain `SELECT cols FROM table`:
/// a count scan is not worth it just to size the plan. Filters, joins, views
/// or missing permissions give `None`. A failed nullability check counts as
/// nullable.
pub fn probe_meta(
    conn: &Connection,
    query: &str,
    column: &str,
    plain: Option<&str>,
    with_rows: bool,
) -> (Option<i64>, bool) {
    let describe = Param::Str(format!("SELECT {column} FROM ({query}) AS _q"));
    let table = if with_rows {
        plain_table(plain.unwrap_or(query))
    } else {
        None
    };
    if let Some(table) = table {
        let sql = format!("SELECT {ROWS_SQL}, {NULLABLE_SQL}");
        match first_row(
            conn,
            &sql,
            &[Param::Str(table.to_string()), describe.clone()],
        ) {
            Ok(row) => {
                let rows = row.first().and_then(Value::as_int);
                let nullable = row.get(1).is_none_or(|v| *v == Value::Null || v.truthy());
                return (rows, nullable);
            }
            Err(err) => log::debug!(
                "parallel read: catalog probe failed, falling back: {}",
                err.sqlstate
            ),
        }
    }
    match first_row(conn, &format!("SELECT {NULLABLE_SQL}"), &[describe]) {
        Ok(row) => (
            None,
            row.first().is_none_or(|v| *v == Value::Null || v.truthy()),
        ),
        Err(err) => {
            log::debug!(
                "parallel read: nullability probe failed, assuming nullable: {}",
                err.sqlstate
            );
            (None, true)
        }
    }
}

/// The first row of `sql` (empty when it has none).
fn first_row(conn: &Connection, sql: &str, params: &[Param]) -> Result<Vec<Value>, Error> {
    Ok(conn
        .execute(sql, params)?
        .scalar_rows(1)?
        .into_iter()
        .next()
        .unwrap_or_default())
}

/// Probe [min, max] of the integer `column` and cut it into slices; empty
/// to read in one stream. `name` is the column as the caller named it (for
/// the error message); `plain` the caller's query text before any rewrite.
pub fn range_plan(
    conn: &Connection,
    query: &str,
    column: &str,
    name: &str,
    partitions: usize,
    plain: Option<&str>,
) -> Result<Vec<Slice>, Error> {
    let row = first_row(
        conn,
        &format!("SELECT MIN({column}), MAX({column}) FROM ({query}) AS _q"),
        &[],
    )?;
    let (low, high) = (
        row.first().cloned().unwrap_or(Value::Null),
        row.get(1).cloned().unwrap_or(Value::Null),
    );
    if low == Value::Null {
        return Ok(Vec::new());
    }
    let (Some(low), Some(high)) = (low.as_int(), high.as_int()) else {
        return Err(Error::type_error(format!(
            "partition_on column {} must be an integer column",
            py_repr(name)
        )));
    };
    let (low, high) = (i128::from(low), i128::from(high));
    let span = high - low + 1;
    let mut count = (partitions * SLICES_PER_WORKER) as i128;
    let (rows, has_null) = probe_meta(conn, query, column, plain, true);
    if let Some(rows) = rows {
        if rows <= MIN_PARALLEL_ROWS {
            return Ok(Vec::new());
        }
        count = count.min(i128::from((rows / MIN_SLICE_ROWS).max(2)));
    }
    // Cannot cut finer than one key value per slice. The size of the key
    // range says nothing about the row count (many rows can share a key),
    // so it never makes the read single-stream except for a single value.
    count = count.min(span);
    if count < 2 {
        return Ok(Vec::new());
    }
    let step = ceil_of_true_division(span, count);
    let edges: Vec<i64> = (1..count)
        .map(|i| low + i * step)
        .filter(|edge| *edge <= high)
        .map(|edge| i64::try_from(edge).expect("edge between MIN and MAX"))
        .collect();
    if edges.is_empty() {
        return Ok(Vec::new());
    }
    Ok(range_slices(query, column, &edges, has_null)
        .into_iter()
        .map(|(sql, params)| Slice::new(sql, params.into_iter().map(Param::I64).collect()))
        .collect())
}

/// Slices for caller-supplied boundaries (no data probe; with `nullable:
/// None` one metadata round trip). Bounds of the `I64`, `F64` and `Str`
/// kinds must strictly ascend; others are taken as given.
pub fn bounds_plan(
    conn: &Connection,
    query: &str,
    column: &str,
    bounds: &[Param],
    nullable: Option<bool>,
) -> Result<Vec<Slice>, Error> {
    if bounds.is_empty() {
        return Ok(Vec::new());
    }
    check_ascending(bounds)?;
    let nullable = match nullable {
        Some(nullable) => nullable,
        None => probe_meta(conn, query, column, None, false).1,
    };
    Ok(range_slices(query, column, bounds, nullable)
        .into_iter()
        .map(|(sql, params)| Slice::new(sql, params))
        .collect())
}

fn check_ascending(bounds: &[Param]) -> Result<(), Error> {
    for pair in bounds.windows(2) {
        let ascending = match (&pair[0], &pair[1]) {
            (Param::I64(a), Param::I64(b)) => Some(a < b),
            (Param::F64(a), Param::F64(b)) => Some(a < b),
            (Param::Str(a), Param::Str(b)) => Some(a < b),
            (Param::Null, _) | (_, Param::Null) => {
                return Err(Error::type_error(
                    "partition bounds must be non-NULL values of one type",
                ));
            }
            _ => None,
        };
        if ascending == Some(false) {
            return Err(Error::value_error(format!(
                "partition_bounds must be strictly ascending ({:?} !< {:?})",
                pair[0], pair[1]
            )));
        }
    }
    Ok(())
}

/// `math.ceil(n / d)` as Python computes it for positive ints: the quotient
/// correctly rounded to a double (ties to even), then its ceiling. Matches
/// the slice width the Python implementation used, also past 2**53.
fn ceil_of_true_division(n: i128, d: i128) -> i128 {
    debug_assert!(n >= d && d > 0);
    let (n, d) = (n as u128, d as u128);
    let q = n / d;
    let bits = 128 - q.leading_zeros() as i32;
    let e = bits - 53;
    let (num, den) = if e >= 0 { (n, d << e) } else { (n << -e, d) };
    let mut m = num / den;
    let rem = num % den;
    if 2 * rem > den || (2 * rem == den && m % 2 == 1) {
        m += 1;
    }
    if e >= 0 {
        (m << e) as i128
    } else {
        m.div_ceil(1u128 << -e) as i128
    }
}

/// Python's `repr()` of a str, for messages that used to come from Python.
fn py_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_space(c: char) -> bool {
    c.is_whitespace() || ('\x1c'..='\x1f').contains(&c)
}

/// The table of a plain `SELECT <*|columns> FROM <table>` query (one to
/// three name parts, `[quoted]` or bare), or `None`. Same language as the
/// Python implementation's regex: `^\s*SELECT\s+(?:\*|[\w\s,.\[\]]+?)\s+FROM\s+
/// ((?:\[[^\]]+\]|\w+)(?:\.(?:\[[^\]]+\]|\w+)){0,2})\s*;?\s*$`, ignoring case.
pub fn plain_table(query: &str) -> Option<&str> {
    let chars: Vec<(usize, char)> = query.char_indices().collect();
    let at = |i: usize| chars.get(i).map(|&(_, c)| c);
    let offset = |i: usize| chars.get(i).map_or(query.len(), |&(o, _)| o);
    let skip_space = |mut i: usize| {
        while at(i).is_some_and(is_space) {
            i += 1;
        }
        i
    };
    let keyword = |i: usize, word: &str| {
        word.chars()
            .enumerate()
            .all(|(k, w)| at(i + k).is_some_and(|c| c.eq_ignore_ascii_case(&w)))
    };
    // One name part: `[...]` (no `]` inside, at least one char) or `\w+`.
    let part = |i: usize| -> Option<usize> {
        match at(i)? {
            '[' => {
                let mut j = i + 1;
                while at(j).is_some_and(|c| c != ']') {
                    j += 1;
                }
                (j > i + 1 && at(j) == Some(']')).then_some(j + 1)
            }
            c if is_word(c) => {
                let mut j = i;
                while at(j).is_some_and(is_word) {
                    j += 1;
                }
                Some(j)
            }
            _ => None,
        }
    };
    // `\s+FROM\s+NAME\s*;?\s*$` at `i`: the table's char range.
    let tail = |i: usize| -> Option<(usize, usize)> {
        let j = skip_space(i);
        if j == i || !keyword(j, "FROM") {
            return None;
        }
        let k = skip_space(j + 4);
        if k == j + 4 {
            return None;
        }
        let mut end = part(k)?;
        for _ in 0..2 {
            match at(end).filter(|c| *c == '.').and_then(|_| part(end + 1)) {
                Some(next) => end = next,
                None => break,
            }
        }
        let mut rest = skip_space(end);
        if at(rest) == Some(';') {
            rest = skip_space(rest + 1);
        }
        (rest == chars.len()).then_some((k, end))
    };
    let start = skip_space(0);
    if !keyword(start, "SELECT") {
        return None;
    }
    let list = skip_space(start + 6);
    if list == start + 6 {
        return None;
    }
    let in_list = |c: char| is_word(c) || is_space(c) || matches!(c, ',' | '.' | '[' | ']');
    let found = if at(list) == Some('*') {
        tail(list + 1)
    } else {
        None
    };
    let found = found.or_else(|| {
        let mut end = list;
        while at(end).is_some_and(in_list) {
            end += 1;
            if let Some(found) = tail(end) {
                return Some(found);
            }
        }
        None
    });
    found.map(|(a, b)| &query[offset(a)..offset(b)])
}

/// A planned read: the probe connection and the statements to run.
pub struct Planned {
    pub conn: Connection,
    pub work: Vec<Slice>,
    pub partitions: usize,
}

/// Open the probe connection and plan the slices of `query` (rewritten for
/// UTF-8 transfer first when `utf8_strings`). A read that is not split gets
/// one slice, the query itself.
pub fn plan_read(
    connector: &dyn Connector,
    query: &str,
    partitioning: &Partitioning,
    utf8_strings: bool,
) -> Result<Planned, Error> {
    let conn = connector.connect()?;
    let rewritten = if utf8_strings {
        crate::utf8::utf8_query(&conn, query)
    } else {
        query.to_string()
    };
    let (work, partitions) = match partitioning {
        Partitioning::Range { column, partitions } if *partitions > 1 => {
            let quoted = quote_identifier(column);
            (
                range_plan(&conn, &rewritten, &quoted, column, *partitions, Some(query))?,
                *partitions,
            )
        }
        Partitioning::Bounds {
            column,
            bounds,
            nullable,
            partitions,
        } => (
            bounds_plan(
                &conn,
                &rewritten,
                &quote_identifier(column),
                bounds,
                *nullable,
            )?,
            *partitions,
        ),
        _ => (Vec::new(), 1),
    };
    let work = if work.is_empty() {
        vec![Slice::new(rewritten, Vec::new())]
    } else {
        work
    };
    Ok(Planned {
        conn,
        work,
        partitions,
    })
}

/// Read a single-slice plan into batches on the calling thread, then close
/// the connection.
pub fn fetch_planned(
    planned: Planned,
    batch_rows: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>), Error> {
    let Planned { conn, work, .. } = planned;
    let slice = work
        .into_iter()
        .next()
        .expect("a plan has at least one slice");
    let mut stmt = conn.execute(&slice.sql, &slice.params)?;
    stmt.read_all(batch_rows)
}

/// Statements of the workers of one read (so a stop can cancel them), the
/// first worker error, and the queue of slices not started yet.
struct Workers {
    merge: Arc<MergeCore>,
    connector: Arc<dyn Connector>,
    pending: Mutex<VecDeque<Slice>>,
    active: Mutex<Vec<Weak<StmtShared>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    error: Mutex<Option<Error>>,
    batch_rows: usize,
}

impl Workers {
    fn cancel_all(&self) {
        let active: Vec<Arc<StmtShared>> = self
            .active
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for stmt in active {
            stmt.cancel_execution();
        }
    }

    /// Stop the merge and cancel the statements (repeatedly: a cancel can
    /// precede its execute); with `wait`, until every worker is gone.
    fn stop(&self, wait: bool) {
        self.merge.shutdown();
        let started = Instant::now();
        loop {
            self.cancel_all();
            let current = std::thread::current().id();
            let alive = self
                .threads
                .lock()
                .iter()
                .any(|t| !t.is_finished() && t.thread().id() != current);
            if !alive || !wait || started.elapsed() >= STOP_BUDGET {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn track(&self, stmt: &Arc<StmtShared>) {
        let mut active = self.active.lock();
        active.retain(|s| s.strong_count() > 0);
        active.push(Arc::downgrade(stmt));
    }

    fn untrack(&self, stmt: &Arc<StmtShared>) {
        self.active
            .lock()
            .retain(|s| !std::ptr::eq(s.as_ptr(), Arc::as_ptr(stmt)));
    }

    /// Stream the statement's current result into the merge.
    fn pump(&self, stmt: &Statement) -> Result<(), Error> {
        let (plan, generation, execution) =
            crate::arrow::current_plan(stmt.shared_stmt()).map_err(Error::state_error)?;
        let stream = StreamCore::start(
            stmt.shared_stmt().clone(),
            plan,
            generation,
            execution,
            self.batch_rows,
            1,
            None,
        );
        let pumped = self.merge.pump(&stream);
        drop(stream);
        pumped.map_err(|_| Error::state_error("the slice's stream was already read"))
    }

    /// Run queued slices on `conn` until the queue is empty or the read stops.
    fn drain(&self, conn: &Connection) -> Result<(), Error> {
        while !self.merge.stopped() {
            let Some(slice) = self.pending.lock().pop_front() else {
                return Ok(());
            };
            let handle = conn.new_statement();
            self.track(&handle);
            let outcome = conn
                .execute_on(handle.clone(), &slice.sql, &slice.params)
                .and_then(|stmt| self.pump(&stmt));
            self.untrack(&handle);
            outcome?;
        }
        Ok(())
    }

    fn run(&self, first: Option<Statement>, conn: Option<Connection>) {
        let outcome = (|| -> Result<(), Error> {
            if let Some(stmt) = first {
                self.track(stmt.shared_stmt());
                let pumped = self.pump(&stmt);
                self.untrack(stmt.shared_stmt());
                drop(stmt);
                pumped?;
                self.drain(
                    conn.as_ref()
                        .expect("the first worker has the probe connection"),
                )?;
            } else if !(self.merge.stopped() || self.pending.lock().is_empty()) {
                let own = self.connector.connect()?;
                self.drain(&own)?;
            }
            Ok(())
        })();
        if let Err(err) = outcome
            && !self.merge.stopped()
        {
            let message = err.to_string();
            self.error.lock().get_or_insert(err);
            self.merge.fail(message);
        }
        drop(conn);
        self.merge.worker_done();
    }
}

/// Run the first slice on the probe connection (for the schema), then start
/// the workers (at most `partitions`, at most one per slice).
pub fn start(
    planned: Planned,
    connector: Arc<dyn Connector>,
    batch_rows: usize,
    max_buffered: Option<usize>,
) -> Result<ParallelReader, Error> {
    let Planned {
        conn,
        work,
        partitions,
    } = planned;
    let mut work: VecDeque<Slice> = work.into();
    let first = work.pop_front().expect("a plan has at least one slice");
    let stmt = conn.execute(&first.sql, &first.params)?;
    let (plan, ..) = crate::arrow::current_plan(stmt.shared_stmt()).map_err(Error::state_error)?;
    let count = partitions.min(work.len() + 1).max(1);
    let depth = max_buffered
        .filter(|d| *d > 0)
        .unwrap_or((2 * count).max(4));
    let merge = MergeCore::new(plan.schema.clone(), count, depth);
    let workers = Arc::new(Workers {
        merge: merge.clone(),
        connector,
        pending: Mutex::new(work),
        active: Mutex::new(Vec::new()),
        threads: Mutex::new(Vec::new()),
        error: Mutex::new(None),
        batch_rows: batch_rows.max(1),
    });
    let mut jobs = vec![(Some(stmt), Some(conn))];
    jobs.extend((1..count).map(|_| (None, None)));
    for (first, conn) in jobs {
        let w = workers.clone();
        let thread = std::thread::Builder::new()
            .name("mssql-read-arrow".into())
            .spawn(move || w.run(first, conn))
            .map_err(|e| Error::driver("HY001", format!("failed to start a read worker: {e}")));
        match thread {
            Ok(thread) => workers.threads.lock().push(thread),
            Err(err) => {
                workers.stop(false);
                return Err(err);
            }
        }
    }
    Ok(ParallelReader {
        reader: None,
        merge,
        workers,
    })
}

/// Batches of a (partitioned) read, arriving while the slices are read.
/// One-shot and unordered, with a bounded buffer. Dropping it, or
/// [`close`](Self::close), cancels the running statements, stops the
/// workers and closes their connections. The first worker error ends the
/// stream (and is kept, see [`take_error`](Self::take_error)).
pub struct ParallelReader {
    reader: Option<Result<MergeReader, ()>>,
    merge: Arc<MergeCore>,
    workers: Arc<Workers>,
}

impl ParallelReader {
    pub fn schema(&self) -> SchemaRef {
        self.merge.schema().clone()
    }

    /// Cancel running statements, stop the workers and close their
    /// connections (waits up to 30 s for the workers to end).
    pub fn close(&self) {
        self.workers.stop(true);
    }

    pub fn is_closed(&self) -> bool {
        self.merge.closed()
    }

    /// The first error a worker met (it also ended the stream).
    pub fn take_error(&self) -> Option<Error> {
        self.workers.error.lock().take()
    }

    /// Stops the read from another thread (as [`close`](Self::close) does,
    /// without waiting).
    pub fn cancel_handle(&self) -> crate::CancelHandle {
        let workers = Arc::downgrade(&self.workers);
        crate::CancelHandle::new(move || {
            if let Some(workers) = workers.upgrade() {
                workers.stop(false);
            }
        })
    }

    /// The merged stream, for hosts that export or poll it themselves.
    #[doc(hidden)]
    pub fn merge(&self) -> &Arc<MergeCore> {
        &self.merge
    }
}

impl Iterator for ParallelReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self
            .reader
            .get_or_insert_with(|| self.merge.reader().map_err(|_| ()));
        match reader {
            Ok(reader) => reader.next(),
            Err(()) => {
                self.reader = Some(Err(()));
                None
            }
        }
    }
}

impl RecordBatchReader for ParallelReader {
    fn schema(&self) -> SchemaRef {
        self.merge.schema().clone()
    }
}

impl Drop for ParallelReader {
    fn drop(&mut self) {
        // A stream handed to a consumer stops itself when that is done.
        if !self.merge.in_use() {
            self.workers.stop(false);
        }
    }
}

/// Run `query` and stream its result as Arrow record batches, split across
/// connections as `partitioning` says (see [`Partitioning`]). Returns once the
/// first statement runs; the batches arrive unordered.
pub fn read_arrow_stream(
    options: &ConnectOptions,
    query: &str,
    partitioning: Partitioning,
    read: ReadOptions,
) -> Result<ParallelReader, Error> {
    read_arrow_stream_with(Arc::new(options.clone()), query, &partitioning, &read)
}

/// [`read_arrow_stream`] with connections from `connector`.
pub fn read_arrow_stream_with(
    connector: Arc<dyn Connector>,
    query: &str,
    partitioning: &Partitioning,
    read: &ReadOptions,
) -> Result<ParallelReader, Error> {
    let planned = plan_read(connector.as_ref(), query, partitioning, read.utf8_strings)?;
    start(planned, connector, read.batch_rows, read.max_buffered)
}

/// [`read_arrow_stream`] read to the end: the schema and every batch. A read
/// planned as one slice runs on the calling thread.
pub fn read_arrow(
    options: &ConnectOptions,
    query: &str,
    partitioning: Partitioning,
    read: ReadOptions,
) -> Result<(SchemaRef, Vec<RecordBatch>), Error> {
    let connector: Arc<dyn Connector> = Arc::new(options.clone());
    let planned = plan_read(connector.as_ref(), query, &partitioning, read.utf8_strings)?;
    if planned.work.len() == 1 {
        return fetch_planned(planned, read.batch_rows);
    }
    let mut reader = start(planned, connector, read.batch_rows, read.max_buffered)?;
    let schema = reader.schema();
    let batches: Result<Vec<RecordBatch>, ArrowError> = reader.by_ref().collect();
    let first_error = reader.take_error();
    reader.close();
    match (batches, first_error) {
        (Ok(batches), _) => Ok((schema, batches)),
        (Err(_), Some(err)) => Err(err),
        (Err(err), None) => Err(stream_error(&err.to_string())),
    }
}

/// The error of a failed stream, from its text (`[SQLSTATE] message; ...`).
fn stream_error(text: &str) -> Error {
    let text = text.strip_prefix("External error: ").unwrap_or(text);
    match text.find('[').map(|i| &text[i..]) {
        Some(rest)
            if rest.len() > 8 && rest.as_bytes()[6] == b']' && rest.as_bytes()[7] == b' ' =>
        {
            Error::from_records(vec![crate::diag::DiagRecord::new(
                &rest[1..6],
                0,
                rest[8..].to_string(),
            )])
        }
        _ => Error::driver("HY000", text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceil_division_matches_python() {
        // math.ceil(n / d) in Python
        assert_eq!(ceil_of_true_division(10, 3), 4);
        assert_eq!(ceil_of_true_division(9, 3), 3);
        assert_eq!(ceil_of_true_division(360_000, 8), 45_000);
        assert_eq!(ceil_of_true_division(12_000_000_001, 8), 1_500_000_001);
        // Past 2**53 the float quotient rounds: math.ceil((2**60 + 1) / 3) == 384307168202282304
        assert_eq!(
            ceil_of_true_division((1 << 60) + 1, 3),
            384_307_168_202_282_304
        );
        // math.ceil((2**64) / 8) == 2**61
        assert_eq!(ceil_of_true_division(1 << 64, 8), 1 << 61);
        // math.ceil((2**63 + 3) / 7) == 1317624576693539328
        assert_eq!(
            ceil_of_true_division((1 << 63) + 3, 7),
            1_317_624_576_693_539_328
        );
        // math.ceil(2**53 // 1 + 1) == 9007199254740992 (2**53 + 1 rounds to even)
        assert_eq!(ceil_of_true_division((1 << 53) + 1, 1), 1 << 53);
    }

    #[test]
    fn plain_table_matches_the_regex() {
        assert_eq!(plain_table("SELECT * FROM dbo.t"), Some("dbo.t"));
        assert_eq!(
            plain_table("  select id, v from [dbo].[my table] ; "),
            Some("[dbo].[my table]")
        );
        assert_eq!(plain_table("SELECT a FROM b FROM c"), Some("c"));
        assert_eq!(
            plain_table("SELECT [a], b.c FROM db.dbo.t"),
            Some("db.dbo.t")
        );
        assert_eq!(plain_table("SELECT * FROM a.b.c.d"), None);
        assert_eq!(plain_table("SELECT * FROM t WHERE x = 1"), None);
        assert_eq!(plain_table("SELECT x + 1 FROM t"), None);
        assert_eq!(plain_table("SELECT * FROM []"), None);
        assert_eq!(plain_table("SELECT *FROM t"), None);
        assert_eq!(plain_table("SELECT * FROM t\n"), Some("t"));
        assert_eq!(
            plain_table("SELECT id FROM ünïcode_tbl"),
            Some("ünïcode_tbl")
        );
    }

    #[test]
    fn slices_are_open_ended() {
        let work = range_slices("SELECT 1", "[id]", &[10, 20, 30], true);
        let sql: Vec<&str> = work.iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(
            sql,
            [
                "SELECT * FROM (SELECT 1) AS _q WHERE [id] IS NULL",
                "SELECT * FROM (SELECT 1) AS _q WHERE [id] < ?",
                "SELECT * FROM (SELECT 1) AS _q WHERE [id] >= ? AND [id] < ?",
                "SELECT * FROM (SELECT 1) AS _q WHERE [id] >= ? AND [id] < ?",
                "SELECT * FROM (SELECT 1) AS _q WHERE [id] >= ?",
            ]
        );
        let params: Vec<Vec<i32>> = work.into_iter().map(|(_, p)| p).collect();
        assert_eq!(
            params,
            vec![vec![], vec![10], vec![10, 20], vec![20, 30], vec![30]]
        );
    }

    #[test]
    fn repr_like_python() {
        assert_eq!(py_repr("id"), "'id'");
        assert_eq!(py_repr("it's"), "\"it's\"");
        assert_eq!(py_repr("a\\b"), "'a\\\\b'");
    }

    #[test]
    fn bounds_must_ascend() {
        assert!(check_ascending(&[Param::I64(1), Param::I64(2)]).is_ok());
        assert_eq!(
            check_ascending(&[Param::I64(2), Param::I64(2)])
                .unwrap_err()
                .kind,
            crate::ErrorKind::Value
        );
        assert_eq!(
            check_ascending(&[Param::Null, Param::I64(2)])
                .unwrap_err()
                .kind,
            crate::ErrorKind::Type
        );
    }

    #[test]
    fn stream_error_text() {
        let err = stream_error("External error: [22012] [Microsoft]Divide by zero");
        assert_eq!(err.sqlstate, "22012");
        assert_eq!(err.message, "[Microsoft]Divide by zero");
    }
}
