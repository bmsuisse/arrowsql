//! The async session state machine: one `TdsClient` plus the per-cursor result
//! state, driven from tokio tasks. Nothing here touches Python.
//!
//! Mirrors `statement.rs` (the sync path) in async form: a cursor that needs
//! the wire while another cursor still has rows on it first drains those rows
//! into the other cursor's buffer, so several cursors per connection work.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use mssql_tds::connection::tds_client::{ExecuteOptions, PreparedStatement, ResultSet, StatementResult, TdsClient};
use mssql_tds::core::CancelHandle;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::transaction_management::TransactionIsolationLevel;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use mssql_tds::query::metadata::ColumnMetadata;

use crate::arrow::plan::Plan;
use crate::arrow::sink::ArrowSink;
use crate::decode::RowBuffer;
use crate::diag::{self, DiagRecord};
use crate::params::ParamSet;
use crate::statement::BatchSink;

/// Rows (and bytes) `execute` reads ahead so small results complete in one hop.
pub const PREFETCH_BYTES: usize = 4 << 20;
/// Rows per TDS request in executemany (one round trip each).
const EXECUTEMANY_CHUNK: usize = 2000;
const PARAM_TYPE_CACHE_SIZE: usize = 512;

pub type Diag = Vec<DiagRecord>;

/// A cancellable operation: the mssql-tds handle (attention on the wire) plus a
/// flag checked between rows and before the operation starts.
pub struct OpCancel {
    pub handle: Arc<CancelHandle>,
    flag: AtomicBool,
    wake: tokio::sync::Notify,
}

impl OpCancel {
    pub fn new() -> Self {
        Self { handle: Arc::new(CancelHandle::new()), flag: AtomicBool::new(false), wake: tokio::sync::Notify::new() }
    }

    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
        self.handle.cancel();
        self.wake.notify_one();
    }

    /// Resolves once `cancel` was called (for operations still queued).
    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            self.wake.notified().await;
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

pub fn cancelled() -> Diag {
    vec![DiagRecord::driver("HY008", "Operation canceled")]
}

fn closed() -> Diag {
    vec![DiagRecord::driver("08003", "Connection is closed")]
}

pub enum Command {
    Batch(String),
    Rpc(String, ParamSet),
}

/// Result of `execute` / `nextset`.
pub struct ExecOut {
    pub columns: Option<Arc<Vec<ColumnMetadata>>>,
    pub rows: RowBuffer,
    /// The current result has no more rows (no further `fetch` needed).
    pub done: bool,
    pub rowcount: i64,
    pub info: Diag,
}

pub struct RowsOut {
    pub columns: Option<Arc<Vec<ColumnMetadata>>>,
    pub rows: RowBuffer,
    pub ncols: usize,
    pub done: bool,
}

/// A result drained off the wire because another cursor needed it.
struct Buffered {
    columns: Option<Arc<Vec<ColumnMetadata>>>,
    rows: RowBuffer,
    rowcount: i64,
    info: Diag,
    error: Option<Diag>,
}

#[derive(Default)]
struct CursorState {
    columns: Option<Arc<Vec<ColumnMetadata>>>,
    /// Rows of the current result drained off the wire (MARS emulation).
    drained: Option<RowBuffer>,
    drained_pos: usize,
    drained_error: Option<Diag>,
    /// Results after the current one, drained off the wire.
    queued: VecDeque<Buffered>,
}

impl CursorState {
    fn ncols(&self) -> usize {
        self.columns.as_ref().map_or(0, |c| c.len())
    }
}

pub struct Session {
    client: Option<TdsClient>,
    pub autocommit: bool,
    pub timeout: u32,
    /// Cursor whose batch is live on the wire.
    owner: Option<u64>,
    /// Owner still has rows of its current result on the wire.
    owner_rows: bool,
    /// Cancellation of the execution that started the owner's batch: reads of
    /// that batch obey it, so cancelling a drain means cancelling this.
    owner_handle: Option<Arc<CancelHandle>>,
    cursors: HashMap<u64, CursorState>,
    param_types: HashMap<String, Vec<(String, String)>>,
    /// When the TDS login finished (pool `max_lifetime`).
    born: Instant,
}

async fn read_rows<W: mssql_tds::datatypes::row_writer::RowWriter + Send>(
    client: &mut TdsClient,
    sink: &mut W,
    rows: impl Fn(&W) -> usize,
    bytes: impl Fn(&W) -> usize,
    max_rows: usize,
    max_bytes: usize,
    cancel: Option<&OpCancel>,
) -> mssql_tds::core::TdsResult<bool> {
    let start = rows(sink);
    while rows(sink) - start < max_rows && bytes(sink) < max_bytes {
        if cancel.is_some_and(OpCancel::is_cancelled) {
            return Err(mssql_tds::error::Error::OperationCancelledError(
                "Operation canceled".into(),
            ));
        }
        if !client.next_row_into(sink).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Session {
    pub fn new(client: TdsClient, autocommit: bool, timeout: u32) -> Self {
        Self {
            client: Some(client),
            autocommit,
            timeout,
            owner: None,
            owner_rows: false,
            owner_handle: None,
            cursors: HashMap::new(),
            param_types: HashMap::new(),
            born: Instant::now(),
        }
    }

    pub fn age(&self) -> Duration {
        self.born.elapsed()
    }

    /// Whether the server still answers on this session (pool checkout
    /// validation); the reset flag, if pending, rides on this request.
    ///
    /// Bounded by `limit` and by `cancel`: a half-open socket never answers,
    /// and the caller holds a pool permit while it waits.
    pub async fn ping(&mut self, cancel: &OpCancel, limit: std::time::Duration) -> bool {
        let probe = self.query(Command::Batch("SELECT 1".into()), 1, cancel);
        matches!(tokio::time::timeout(limit, probe).await, Ok(Ok(_))) && self.is_open()
    }

    pub fn is_open(&self) -> bool {
        self.client.as_ref().is_some_and(|c| !c.is_connection_dead())
    }

    /// Hand the client back for pooling: no live batch, and (when `reset`)
    /// session state reset on the next request.
    pub async fn into_reusable(mut self, reset: bool) -> Option<Session> {
        self.discard_wire().await;
        let client = self.client.as_mut()?;
        if client.is_connection_dead() || client.has_open_batch() {
            return None;
        }
        if client.has_active_transaction() {
            client.rollback_transaction(None, None).await.ok()?;
        }
        if reset {
            // RESETCONNECTION rides on the next request: no extra round trip.
            client.prepare_reset_connection(false);
        }
        self.cursors.clear();
        Some(self)
    }

    pub async fn close(&mut self) {
        self.discard_wire().await;
        if let Some(mut client) = self.client.take() {
            if client.has_active_transaction() {
                let _ = client.rollback_transaction(None, None).await;
            }
            let _ = client.close_connection().await;
        }
    }

    pub fn close_cursor(&mut self, cursor: u64) {
        self.cursors.remove(&cursor);
    }

    /// Free the wire for `requester` (0 = the connection): drain another
    /// cursor's live rows into its buffer, or discard our own.
    async fn release_wire(&mut self, requester: u64, cancel: Option<&OpCancel>) {
        let Some(client) = self.client.as_mut() else { return };
        if let Some(owner) = self.owner.take() {
            let keep = owner != requester && self.cursors.contains_key(&owner);
            if keep {
                let state = self.cursors.entry(owner).or_default();
                let drain = drain_into(client, state, self.owner_rows, cancel);
                match (cancel, self.owner_handle.as_ref()) {
                    (Some(cancel), Some(handle)) => {
                        tokio::pin!(drain);
                        tokio::select! {
                            () = &mut drain => {}
                            // The drain may be blocked in a read of the owner's
                            // batch (e.g. its WAITFOR): cancel that read too.
                            () = cancel.cancelled() => {
                                handle.cancel();
                                drain.await;
                            }
                        }
                    }
                    _ => drain.await,
                }
            }
        }
        self.owner_rows = false;
        if client.has_open_batch() {
            let _ = client.close_query().await;
            let _ = client.take_info_messages();
        }
    }

    /// Throw away whatever is still on the wire (close, pool checkin, end of a
    /// shortcut query): an attention ends the response without buffering it.
    async fn discard_wire(&mut self) {
        self.owner = None;
        self.owner_rows = false;
        let Some(client) = self.client.as_mut() else { return };
        if client.has_open_batch() {
            if client.cancel_query(5).await.is_err() {
                let _ = client.close_query().await;
            }
            let _ = client.take_info_messages();
        }
    }

    /// Manual-commit mode opens the transaction before the statement, like msodbcsql.
    async fn prepare_session(&mut self) -> Result<(), Diag> {
        let client = self.client.as_mut().ok_or_else(closed)?;
        if self.autocommit || client.has_active_transaction() {
            return Ok(());
        }
        let result = client
            .begin_transaction_with_options(
                TransactionIsolationLevel::NoChange,
                None,
                ExecuteOptions::new().timeout_secs(self.timeout),
            )
            .await;
        let _ = client.take_info_messages();
        result.map_err(|e| diag::records_for_error(&e, "HY000"))
    }

    /// Type untyped NULL parameters the way the server infers them (cached).
    async fn resolve_null_types(&mut self, sql: &str, params: &mut ParamSet) {
        if params.untyped_nulls.is_empty() {
            return;
        }
        let described = match self.param_types.get(sql) {
            Some(types) => types.clone(),
            None => {
                let Some(client) = self.client.as_mut() else { return };
                let query = format!(
                    "EXEC sp_describe_undeclared_parameters @tsql = N'{}'",
                    sql.replace('\'', "''")
                );
                let mut rows = RowBuffer::new();
                let ok = async {
                    if client.execute(query, ExecuteOptions::new().timeout_secs(self.timeout)).await?
                        == StatementResult::Rows
                    {
                        while client.next_row_into(&mut rows).await? {}
                    }
                    client.close_query().await
                }
                .await;
                let _ = client.take_info_messages();
                if ok.is_err() {
                    let _ = client.close_query().await;
                    return;
                }
                let ncols = rows.cells.len().checked_div(rows.rows).unwrap_or(0).max(1);
                let types: Vec<(String, String)> = rows
                    .cells
                    .chunks(ncols)
                    .filter_map(|row| {
                        Some((rows.text(row.get(1)?)?, rows.text(row.get(3)?)?))
                    })
                    .collect();
                if self.param_types.len() >= PARAM_TYPE_CACHE_SIZE {
                    self.param_types.clear();
                }
                self.param_types.insert(sql.to_string(), types.clone());
                types
            }
        };
        for &i in &params.untyped_nulls {
            let name = format!("@P{}", i + 1);
            if let Some((_, type_name)) = described.iter().find(|(n, _)| n.eq_ignore_ascii_case(&name))
                && let Some(sql_type) = crate::params::null_types::null_of(type_name)
            {
                params.types[i] = sql_type;
            }
        }
    }

    /// Execute `command` for `cursor` and read up to `prefetch` rows of the
    /// first result.
    pub async fn execute(
        &mut self,
        cursor: u64,
        command: Command,
        prefetch: usize,
        prefetch_bytes: usize,
        cancel: &OpCancel,
    ) -> Result<ExecOut, Diag> {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        self.release_wire(cursor, Some(cancel)).await;
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        self.cursors.insert(cursor, CursorState::default());
        self.prepare_session().await?;
        let (sql, params) = match command {
            Command::Rpc(sql, mut params) => {
                self.resolve_null_types(&sql, &mut params).await;
                (sql, Some(params.into_rpc()))
            }
            Command::Batch(sql) => (sql, None),
        };
        self.run(cursor, sql, params, prefetch, prefetch_bytes, cancel).await
    }

    async fn run(
        &mut self,
        cursor: u64,
        sql: String,
        params: Option<Vec<RpcParameter>>,
        prefetch: usize,
        prefetch_bytes: usize,
        cancel: &OpCancel,
    ) -> Result<ExecOut, Diag> {
        let timeout = self.timeout;
        let client = self.client.as_mut().ok_or_else(closed)?;
        self.owner_handle = Some(cancel.handle.clone());
        let options = ExecuteOptions::new().timeout_secs(timeout).cancel(&cancel.handle);
        let result = match params {
            None => client.execute(sql, options).await,
            Some(params) => client.execute_sp_executesql(sql, params, options).await,
        };
        let mut info: Diag = diag::info_records(&client.take_info_messages()).collect();
        let mut out = ExecOut { columns: None, rows: RowBuffer::new(), done: true, rowcount: -1, info: Vec::new() };
        match result {
            Ok(StatementResult::Rows) => {
                let mut failed = false;
                let columns = Arc::new(client.get_metadata().clone());
                let read = read_rows(
                    client,
                    &mut out.rows,
                    |b| b.rows,
                    RowBuffer::byte_size,
                    prefetch.max(1),
                    prefetch_bytes,
                    Some(cancel),
                )
                .await;
                info.extend(diag::info_records(&client.take_info_messages()));
                match read {
                    Ok(done) => out.done = done,
                    // Like msodbcsql: an error before the first row fails execute.
                    Err(err) if out.rows.rows == 0 => {
                        let mut records = diag::records_for_error(&err, "HY000");
                        records.extend(info);
                        self.after_error(cursor);
                        return Err(records);
                    }
                    // Rows came first: hand them out, and raise the error from the
                    // next fetch (done stays false so the caller asks for it).
                    Err(err) => {
                        failed = true;
                        out.done = false;
                        self.cursors.entry(cursor).or_default().drained_error =
                            Some(diag::records_for_error(&err, "HY000"));
                    }
                }
                self.cursors.entry(cursor).or_default().columns = Some(columns.clone());
                out.columns = Some(columns);
                self.owner_rows = !out.done && !failed;
            }
            Ok(StatementResult::NoRows { rows_affected }) => {
                out.rowcount = rows_affected.map_or(-1, |n| n as i64);
            }
            Ok(StatementResult::End) => {}
            Err(err) => {
                let mut records = diag::records_for_error(&err, "HY000");
                records.extend(info);
                self.after_error(cursor);
                return Err(records);
            }
        }
        let client = self.client.as_mut().ok_or_else(closed)?;
        if client.has_open_batch() {
            self.owner = Some(cursor);
        }
        out.info = info;
        Ok(out)
    }

    fn after_error(&mut self, cursor: u64) {
        if self.client.as_ref().is_some_and(TdsClient::has_open_batch) {
            self.owner = Some(cursor);
        }
        self.owner_rows = false;
    }

    /// Up to `n` more rows of the cursor's current result.
    pub async fn fetch(&mut self, cursor: u64, n: usize, cancel: &OpCancel) -> Result<RowsOut, Diag> {
        let state = self.cursors.get_mut(&cursor).ok_or_else(invalid_cursor)?;
        let ncols = state.ncols();
        let columns = state.columns.clone();
        if let Some(buf) = state.drained.as_ref() {
            let available = buf.rows - state.drained_pos;
            let take = n.min(available);
            let rows = if state.drained_pos == 0 && take == available {
                state.drained.take().unwrap_or_default()
            } else {
                let out = buf.slice_rows(ncols, state.drained_pos, take);
                state.drained_pos += take;
                out
            };
            let mut done = state.drained.as_ref().is_none_or(|b| state.drained_pos >= b.rows);
            if done {
                state.drained = None;
                state.drained_pos = 0;
                if state.drained_error.is_some() {
                    // The error follows the last buffered rows: raise it now, or
                    // from the next fetch when there are rows to hand out first.
                    if rows.rows == 0 {
                        return Err(state.drained_error.take().unwrap_or_else(cancelled));
                    }
                    done = false;
                }
            }
            return Ok(RowsOut { columns, rows, ncols, done });
        }
        if self.owner != Some(cursor) || !self.owner_rows {
            if let Some(err) = state.drained_error.take() {
                return Err(err);
            }
            return Ok(RowsOut { columns, rows: RowBuffer::new(), ncols, done: true });
        }
        let client = self.client.as_mut().ok_or_else(closed)?;
        let mut rows = RowBuffer::new();
        let read = read_rows(client, &mut rows, |b| b.rows, RowBuffer::byte_size, n, usize::MAX, Some(cancel)).await;
        match read {
            Ok(done) => {
                self.owner_rows = !done;
                Ok(RowsOut { columns, rows, ncols, done })
            }
            Err(err) => {
                self.owner_rows = false;
                if cancel.is_cancelled() {
                    let _ = client.cancel_query(5).await;
                }
                if rows.rows > 0 {
                    state.drained_error = Some(diag::records_for_error(&err, "HY000"));
                    Ok(RowsOut { columns, rows, ncols, done: false })
                } else {
                    Err(diag::records_for_error(&err, "HY000"))
                }
            }
        }
    }

    /// Advance to the next result; `None` when there is none.
    pub async fn nextset(&mut self, cursor: u64, prefetch: usize, cancel: &OpCancel) -> Result<Option<ExecOut>, Diag> {
        let state = self.cursors.get_mut(&cursor).ok_or_else(invalid_cursor)?;
        state.drained = None;
        state.drained_pos = 0;
        let pending_error = state.drained_error.take();
        state.columns = None;
        if let Some(err) = pending_error {
            return Err(err);
        }
        if let Some(next) = state.queued.pop_front() {
            if let Some(err) = next.error.clone().filter(|_| next.columns.is_none()) {
                return Err(err);
            }
            // A buffered error follows its rows: done stays false until it is raised.
            let done = next.error.is_none();
            state.columns = next.columns.clone();
            state.drained_error = next.error;
            return Ok(Some(ExecOut { columns: next.columns, rows: next.rows, done, rowcount: next.rowcount, info: next.info }));
        }
        if self.owner != Some(cursor) {
            return Ok(None);
        }
        let client = self.client.as_mut().ok_or_else(closed)?;
        let advanced = client.advance().await;
        let mut info: Diag = diag::info_records(&client.take_info_messages()).collect();
        let mut out = ExecOut { columns: None, rows: RowBuffer::new(), done: true, rowcount: -1, info: Vec::new() };
        let result = match advanced {
            Ok(StatementResult::Rows) => {
                let mut failed = false;
                let columns = Arc::new(client.get_metadata().clone());
                let read = read_rows(client, &mut out.rows, |b| b.rows, RowBuffer::byte_size, prefetch.max(1), PREFETCH_BYTES, Some(cancel)).await;
                info.extend(diag::info_records(&client.take_info_messages()));
                match read {
                    Ok(done) => out.done = done,
                    Err(err) if out.rows.rows == 0 => {
                        self.owner_rows = false;
                        if !client.has_open_batch() {
                            self.owner = None;
                        }
                        let mut records = diag::records_for_error(&err, "HY000");
                        records.extend(info);
                        return Err(records);
                    }
                    // Rows came first: hand them out, and raise the error from the
                    // next fetch (done stays false so the caller asks for it).
                    Err(err) => {
                        failed = true;
                        out.done = false;
                        self.cursors.entry(cursor).or_default().drained_error =
                            Some(diag::records_for_error(&err, "HY000"));
                    }
                }
                self.cursors.entry(cursor).or_default().columns = Some(columns.clone());
                out.columns = Some(columns);
                self.owner_rows = !out.done && !failed;
                Ok(Some(out))
            }
            Ok(StatementResult::NoRows { rows_affected }) => {
                self.owner_rows = false;
                out.rowcount = rows_affected.map_or(-1, |n| n as i64);
                Ok(Some(out))
            }
            Ok(StatementResult::End) => {
                self.owner_rows = false;
                Ok(None)
            }
            Err(err) => {
                self.owner_rows = false;
                let mut records = diag::records_for_error(&err, "HY000");
                records.extend(info.drain(..));
                Err(records)
            }
        };
        let client = self.client.as_mut().ok_or_else(closed)?;
        if !client.has_open_batch() {
            self.owner = None;
        }
        result.map(|r| {
            r.map(|mut o| {
                o.info = info;
                o
            })
        })
    }

    /// Every remaining row of the current result as Arrow record batches.
    /// `head` holds rows already read (the execute prefetch) and goes first.
    pub async fn fetch_arrow(
        &mut self,
        cursor: u64,
        head: Option<(RowBuffer, usize)>,
        batch_rows: usize,
        cancel: &OpCancel,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), Diag> {
        let state = self.cursors.get_mut(&cursor).ok_or_else(invalid_cursor)?;
        let columns = state.columns.clone().ok_or_else(|| {
            vec![DiagRecord::driver("24000", "No active result set. Cannot fetch Arrow batch.")]
        })?;
        let plan = Plan::new(&columns).map_err(|m| vec![DiagRecord::driver("HY000", m)])?;
        let batch_rows = batch_rows.max(1);
        let mut batches = Vec::new();
        let mut sink = ArrowSink::new(&plan, batch_rows);
        if let Some((rows, ncols)) = head {
            sink.push_buffered(&rows, ncols);
        }
        if let Some(rows) = state.drained.take() {
            let rest = rows.slice_rows(columns.len(), state.drained_pos, rows.rows - state.drained_pos);
            sink.push_buffered(&rest, columns.len());
            state.drained_pos = 0;
        }
        let finish = |sink: ArrowSink| sink.finish().map_err(|m| vec![DiagRecord::driver("HY000", m)]);
        if self.owner == Some(cursor) && self.owner_rows {
            let client = self.client.as_mut().ok_or_else(closed)?;
            loop {
                let want = batch_rows.saturating_sub(BatchSink::rows(&sink)).max(1);
                let read = read_rows(client, &mut sink, |s| BatchSink::rows(s), |s| BatchSink::byte_size(s), want, usize::MAX, Some(cancel)).await;
                match read {
                    Ok(done) => {
                        if BatchSink::rows(&sink) >= batch_rows || done {
                            let full = std::mem::replace(&mut sink, ArrowSink::new(&plan, batch_rows));
                            batches.push(finish(full)?);
                        }
                        if done {
                            break;
                        }
                    }
                    Err(err) => {
                        self.owner_rows = false;
                        if cancel.is_cancelled() {
                            let _ = client.cancel_query(5).await;
                        }
                        return Err(diag::records_for_error(&err, "HY000"));
                    }
                }
            }
            self.owner_rows = false;
        } else if BatchSink::rows(&sink) > 0 {
            batches.push(finish(sink)?);
        }
        Ok((plan.schema.clone(), batches))
    }

    pub async fn execute_many(
        &mut self,
        cursor: u64,
        sql: String,
        mut rows: Vec<Vec<SqlType>>,
        all_null: Vec<usize>,
        cancel: &OpCancel,
    ) -> Result<i64, Diag> {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        self.release_wire(cursor, Some(cancel)).await;
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        self.cursors.insert(cursor, CursorState::default());
        self.prepare_session().await?;
        if !all_null.is_empty() && !rows.is_empty() {
            let mut probe = ParamSet { types: rows[0].clone(), untyped_nulls: all_null.clone() };
            self.resolve_null_types(&sql, &mut probe).await;
            for row in rows.iter_mut() {
                for &c in &all_null {
                    row[c] = probe.types[c].clone();
                }
            }
        }
        let timeout = self.timeout;
        let client = self.client.as_mut().ok_or_else(closed)?;
        let mut prepared = PreparedStatement::new(sql);
        let mut orphaned = None;
        let mut total: Option<i64> = None;
        let mut failure = None;
        let mut rows = rows.into_iter().enumerate().peekable();
        while rows.peek().is_some() {
            let chunk: Vec<(usize, Vec<SqlType>)> = rows.by_ref().take(EXECUTEMANY_CHUNK).collect();
            let iter = chunk.into_iter().map(|(i, types)| {
                let params = types
                    .into_iter()
                    .enumerate()
                    .map(|(j, t)| RpcParameter::new(Some(format!("@P{}", j + 1)), StatusFlags::NONE, t))
                    .collect();
                Ok((i, params))
            });
            let options = ExecuteOptions::new().timeout_secs(timeout).cancel(&cancel.handle);
            match client.execute_prepared_batch(&mut prepared, iter, &mut orphaned, options).await {
                Ok(batch) => {
                    if let Some(n) = batch.total_rows_affected() {
                        total = Some(total.unwrap_or(0) + n);
                    }
                    if let Some(failed) = batch.rows.iter().find(|r| !r.errors.is_empty()) {
                        failure = Some(
                            failed
                                .errors
                                .iter()
                                .map(|e| {
                                    DiagRecord::new(
                                        diag::sqlstate_for_server_error(e.number, e.class),
                                        e.number as i32,
                                        format!("{}[SQL Server]{}", diag::DRIVER_PREFIX, e.message),
                                    )
                                })
                                .collect(),
                        );
                        break;
                    }
                }
                Err(err) => {
                    failure = Some(diag::records_for_error(&err, "HY000"));
                    break;
                }
            }
        }
        if client.has_open_batch() {
            let _ = client.close_query().await;
        }
        if let Some(id) = prepared.take_id() {
            let _ = client.unprepare(id, ()).await;
        }
        let _ = client.take_info_messages();
        match failure {
            Some(records) => Err(records),
            None => Ok(total.unwrap_or(-1)),
        }
    }

    pub async fn commit(&mut self) -> Result<(), Diag> {
        self.release_wire(0, None).await;
        let client = self.client.as_mut().ok_or_else(closed)?;
        if client.has_active_transaction() {
            client.commit_transaction(None, None).await.map_err(|e| diag::records_for_error(&e, "HY000"))?;
        }
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), Diag> {
        self.release_wire(0, None).await;
        let client = self.client.as_mut().ok_or_else(closed)?;
        if client.has_active_transaction() {
            client.rollback_transaction(None, None).await.map_err(|e| diag::records_for_error(&e, "HY000"))?;
        }
        Ok(())
    }

    /// Execute and read every row of the first result; later results are
    /// discarded. Used by the `fetch`/`fetchrow`/`fetchval` shortcuts.
    pub async fn query(&mut self, command: Command, max_rows: usize, cancel: &OpCancel) -> Result<(ExecOut, usize), Diag> {
        const SHORTCUT: u64 = 0;
        let out = self.execute(SHORTCUT, command, max_rows, usize::MAX, cancel).await?;
        let ncols = out.columns.as_ref().map_or(0, |c| c.len());
        let pending_error = self.cursors.get_mut(&SHORTCUT).and_then(|s| s.drained_error.take());
        self.discard_wire().await;
        self.cursors.remove(&SHORTCUT);
        match pending_error {
            Some(records) => Err(records),
            None => Ok((out, ncols)),
        }
    }
}

fn invalid_cursor() -> Diag {
    vec![DiagRecord::new("24000", 0, "[Microsoft][ODBC Driver Manager] Invalid cursor state")]
}

/// Move everything still on the wire for `state`'s cursor into its buffers.
async fn drain_into(client: &mut TdsClient, state: &mut CursorState, rows_on_wire: bool, cancel: Option<&OpCancel>) {
    if rows_on_wire {
        let mut rows = state.drained.take().unwrap_or_default();
        if let Err(err) = read_rows(client, &mut rows, |b| b.rows, RowBuffer::byte_size, usize::MAX, usize::MAX, cancel).await {
            state.drained_error = Some(diag::records_for_error(&err, "HY000"));
        }
        state.drained = Some(rows);
    }
    while client.has_open_batch() {
        if cancel.is_some_and(OpCancel::is_cancelled) {
            // Give up on the rest of the previous cursor's results.
            let _ = client.cancel_query(5).await;
            let _ = client.take_info_messages();
            state.drained_error.get_or_insert_with(cancelled);
            break;
        }
        let advanced = client.advance().await;
        let info: Diag = diag::info_records(&client.take_info_messages()).collect();
        match advanced {
            Ok(StatementResult::End) => break,
            Ok(StatementResult::NoRows { rows_affected }) => state.queued.push_back(Buffered {
                columns: None,
                rows: RowBuffer::new(),
                rowcount: rows_affected.map_or(-1, |n| n as i64),
                info,
                error: None,
            }),
            Ok(StatementResult::Rows) => {
                let columns = Some(Arc::new(client.get_metadata().clone()));
                let mut rows = RowBuffer::new();
                let error = read_rows(client, &mut rows, |b| b.rows, RowBuffer::byte_size, usize::MAX, usize::MAX, cancel)
                    .await
                    .err()
                    .map(|e| diag::records_for_error(&e, "HY000"));
                state.queued.push_back(Buffered { columns, rows, rowcount: -1, info, error });
            }
            Err(err) => state.queued.push_back(Buffered {
                columns: None,
                rows: RowBuffer::new(),
                rowcount: -1,
                info,
                error: Some(diag::records_for_error(&err, "HY000")),
            }),
        }
    }
}
