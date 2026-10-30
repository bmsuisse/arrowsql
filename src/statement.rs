//! Statement handles (`SqlHandle`) and the execute / fetch / more-results
//! state machine on top of one shared `TdsClient`.
//!
//! Concurrency model (see docs/ARCHITECTURE.md):
//! * Lock order is always `ConnShared::slot` -> `StmtShared::state`.
//! * Both locks are only ever taken with the GIL released (`py.detach`), and
//!   never held while re-acquiring the GIL. Python objects are built from a
//!   detached `RowBuffer` after the locks are dropped.
//! * Exactly one statement "owns the wire" (has a live batch on the TDS
//!   connection). When another statement needs the connection, the owner's
//!   remaining results are drained into its own buffers first
//!   (`drain_into_buffer`), so both cursors keep working (MARS-like).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mssql_tds::connection::tds_client::{
    ExecuteOptions, PreparedStatement, ResultSet, StatementResult, TdsClient,
};
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::core::CancelHandle;
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use mssql_tds::query::metadata::ColumnMetadata;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::connection::{ConnShared, Slot};
use crate::consts::*;
use crate::decode::{RowBuffer, describe, python as pyconv};
use crate::diag::{self, DiagRecord};
use crate::params::ParamSet;
use crate::params::py_types::py_to_sql_type;
use crate::runtime::block_on;

static NEXT_STMT_ID: AtomicU64 = AtomicU64::new(1);

/// Rows read ahead by `fetchone` so per-row calls don't each hit the wire.
const FETCHONE_READAHEAD: usize = 256;
/// Batch caps for fetchall: the GIL is re-taken between batches to convert.
const FETCHALL_BATCH_ROWS: usize = 8192;
const FETCH_BATCH_BYTES: usize = 16 << 20;

/// A result set that was drained off the wire because another statement on
/// the same connection needed it.
pub struct BufferedResult {
    columns: Option<Arc<Vec<ColumnMetadata>>>,
    rows: RowBuffer,
    rowcount: i64,
    info: Vec<DiagRecord>,
    error: Option<Vec<DiagRecord>>,
}

#[derive(Default)]
pub struct StmtState {
    pub diag: Vec<DiagRecord>,
    columns: Option<Arc<Vec<ColumnMetadata>>>,
    rowcount: i64,
    /// This statement owns the live batch on the connection.
    on_wire: bool,
    /// The current result has no more rows to read from the wire.
    result_done: bool,
    /// Rows already decoded but not yet handed to Python.
    pending: RowBuffer,
    pending_pos: usize,
    /// Error met while reading ahead; reported once `pending` is drained.
    pending_error: Option<Vec<DiagRecord>>,
    /// Results after the current one, drained from the wire (MARS emulation).
    queued: VecDeque<BufferedResult>,
    pub query_timeout: u32,
    freed: bool,
}

impl StmtState {
    fn ncols(&self) -> usize {
        self.columns.as_ref().map_or(0, |c| c.len())
    }

    fn pending_rows(&self) -> usize {
        self.pending.rows - self.pending_pos
    }

    fn reset_results(&mut self) {
        self.columns = None;
        self.rowcount = -1;
        self.on_wire = false;
        self.result_done = true;
        self.pending.clear();
        self.pending_pos = 0;
        self.pending_error = None;
        self.queued.clear();
    }

    /// Take up to `n` undelivered rows.
    fn take_pending(&mut self, n: usize) -> RowBuffer {
        let available = self.pending_rows();
        let n = n.min(available);
        if n == 0 {
            return RowBuffer::new();
        }
        if self.pending_pos == 0 && n == available {
            self.pending_pos = 0;
            return std::mem::take(&mut self.pending);
        }
        let out = self.pending.slice_rows(self.ncols(), self.pending_pos, n);
        self.pending_pos += n;
        if self.pending_pos == self.pending.rows {
            self.pending.clear();
            self.pending_pos = 0;
        }
        out
    }

    fn install_buffered(&mut self, result: BufferedResult) {
        self.columns = result.columns;
        self.rowcount = result.rowcount;
        self.pending = result.rows;
        self.pending_pos = 0;
        self.pending_error = result.error;
        self.result_done = true;
        self.diag.extend(result.info);
    }
}

pub struct StmtShared {
    pub id: u64,
    pub conn: Arc<ConnShared>,
    pub state: Mutex<StmtState>,
    /// Separate lock so `_cancel` from another thread never waits on a fetch.
    cancel: Mutex<Option<Arc<CancelHandle>>>,
}

impl StmtShared {
    pub fn new(conn: Arc<ConnShared>) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
            conn,
            state: Mutex::new(StmtState { rowcount: -1, result_done: true, ..Default::default() }),
            cancel: Mutex::new(None),
        })
    }
}

/// Read rows of the current wire result into `sink` until `max_rows` rows
/// were written, `max_bytes` is exceeded, or the result ends. This is the
/// generic decode seam: any `RowWriter` (Python `RowBuffer`, Arrow builders)
/// can be filled here. Returns `Ok(true)` when the result is exhausted.
pub fn read_rows_into<W: RowWriter + Send>(
    client: &mut TdsClient,
    sink: &mut W,
    rows_written: impl Fn(&W) -> usize,
    bytes_written: impl Fn(&W) -> usize,
    max_rows: usize,
    max_bytes: usize,
) -> mssql_tds::core::TdsResult<bool> {
    block_on(async {
        let start = rows_written(sink);
        while rows_written(sink) - start < max_rows && bytes_written(sink) < max_bytes {
            if !client.next_row_into(sink).await? {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

/// Pull up to `want` more rows of the current wire result into `state.pending`.
fn fill_pending(client: &mut TdsClient, state: &mut StmtState, want: usize, max_bytes: usize) {
    if state.result_done || state.pending_error.is_some() || !state.on_wire {
        return;
    }
    if !client.on_rows() {
        state.result_done = true;
        return;
    }
    let result = read_rows_into(
        client,
        &mut state.pending,
        |b| b.rows,
        RowBuffer::byte_size,
        want,
        max_bytes,
    );
    match result {
        Ok(done) => state.result_done = done,
        Err(err) => {
            state.result_done = true;
            state.pending_error = Some(diag::records_for_error(&err, "HY000"));
            if !client.has_open_batch() {
                state.on_wire = false;
            }
        }
    }
    state.diag.extend(diag::info_records(&client.take_info_messages()));
}

/// Move everything `other` still has on the wire into its own buffers.
/// Called with the slot locked by a statement that needs the connection.
fn drain_into_buffer(other: &StmtShared, client: &mut TdsClient) {
    let mut state = other.state.lock();
    if !state.on_wire {
        return;
    }
    fill_pending(client, &mut state, usize::MAX, usize::MAX);
    loop {
        if !client.has_open_batch() {
            break;
        }
        let advanced = block_on(client.advance());
        let info: Vec<DiagRecord> = diag::info_records(&client.take_info_messages()).collect();
        match advanced {
            Ok(StatementResult::End) => break,
            Ok(StatementResult::NoRows { rows_affected }) => state.queued.push_back(BufferedResult {
                columns: None,
                rows: RowBuffer::new(),
                rowcount: rows_affected.map_or(-1, |n| n as i64),
                info,
                error: None,
            }),
            Ok(StatementResult::Rows) => {
                let columns = Some(Arc::new(client.get_metadata().clone()));
                let mut rows = RowBuffer::new();
                let error = read_rows_into(
                    client,
                    &mut rows,
                    |b| b.rows,
                    RowBuffer::byte_size,
                    usize::MAX,
                    usize::MAX,
                )
                .err()
                .map(|e| diag::records_for_error(&e, "HY000"));
                state.queued.push_back(BufferedResult { columns, rows, rowcount: -1, info, error });
            }
            Err(err) => {
                state.queued.push_back(BufferedResult {
                    columns: None,
                    rows: RowBuffer::new(),
                    rowcount: -1,
                    info,
                    error: Some(diag::records_for_error(&err, "HY000")),
                });
            }
        }
    }
    state.on_wire = false;
}

/// Free the wire for statement `requester` (0 = the connection itself):
/// buffer another live statement's results, or discard our own.
pub fn release_wire(slot: &mut Slot, requester: u64) {
    let Some(client) = slot.client.as_mut() else { return };
    if let Some((id, weak)) = slot.owner.take() {
        if id != requester
            && let Some(other) = weak.upgrade()
        {
            drain_into_buffer(&other, client);
        } else if id == requester
            && let Some(me) = weak.upgrade()
        {
            me.state.lock().on_wire = false;
        }
    }
    if client.has_open_batch() {
        let _ = block_on(client.close_query());
        let _ = client.take_info_messages();
    }
}

fn not_connected() -> DiagRecord {
    DiagRecord::driver("08003", "Connection is closed")
}

enum Command {
    Batch(String),
    Rpc(String, ParamSet),
    /// Pre-built RPC parameters (catalog procedures).
    RawRpc(String, Vec<RpcParameter>),
}

/// Give untyped NULL parameters the type SQL Server infers for them.
fn resolve_null_types(conn: &ConnShared, client: &mut TdsClient, sql: &str, params: &mut ParamSet) {
    if params.untyped_nulls.is_empty() {
        return;
    }
    let described = match conn.cached_param_types(sql) {
        Some(types) => types,
        None => {
            let escaped = sql.replace('\'', "''");
            let query = format!("EXEC sp_describe_undeclared_parameters @tsql = N'{escaped}'");
            let mut rows = RowBuffer::new();
            let ok = block_on(async {
                let result = client.execute(query, ()).await?;
                if result == StatementResult::Rows {
                    while client.next_row_into(&mut rows).await? {}
                }
                client.close_query().await
            });
            let _ = client.take_info_messages();
            if ok.is_err() {
                let _ = block_on(client.close_query());
                Vec::new()
            } else {
                let ncols = rows.cells.len().checked_div(rows.rows).unwrap_or(0);
                let mut types = Vec::new();
                for row in rows.cells.chunks(ncols.max(1)) {
                    // Columns: parameter_ordinal, name, suggested_system_type_id,
                    // suggested_system_type_name, ...
                    if let (Some(name), Some(type_name)) =
                        (row.get(1).and_then(|c| rows.text(c)), row.get(3).and_then(|c| rows.text(c)))
                    {
                        types.push((name, type_name));
                    }
                }
                conn.cache_param_types(sql, types.clone());
                types
            }
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

/// Apply deferred session settings and, in manual-commit mode, make sure a
/// transaction is open before the next statement.
fn prepare_session(
    conn: &ConnShared,
    client: &mut TdsClient,
    timeout: u32,
    handle: &CancelHandle,
) -> Result<(), Vec<DiagRecord>> {
    // Manual-commit mode uses SET IMPLICIT_TRANSACTIONS so the server opens
    // a transaction with the next statement: no BEGIN round trip per
    // transaction. The SET is sent once per mode change.
    let mut session = conn.take_pending_session_sql();
    let want_implicit = !conn.autocommit();
    if conn.implicit_transactions() != want_implicit {
        let set = if want_implicit { "SET IMPLICIT_TRANSACTIONS ON" } else { "SET IMPLICIT_TRANSACTIONS OFF" };
        session = Some(match session {
            Some(sql) => format!("{set}; {sql}"),
            None => set.to_string(),
        });
    }
    if let Some(sql) = session {
        let result = block_on(async {
            client.execute(sql, ExecuteOptions::new().timeout_secs(timeout)).await?;
            client.close_query().await
        });
        let _ = client.take_info_messages();
        if let Err(err) = result {
            return Err(diag::records_for_error(&err, "HY000"));
        }
        conn.set_implicit_transactions(want_implicit);
    }
    let _ = handle;
    Ok(())
}

fn execute_command(stmt: &Arc<StmtShared>, command: Command) -> SqlReturn {
    let conn = &stmt.conn;
    let mut slot = conn.slot.lock();
    release_wire(&mut slot, stmt.id);
    let mut state = stmt.state.lock();
    state.diag.clear();
    state.reset_results();
    if state.freed {
        return SQL_INVALID_HANDLE;
    }
    let Some(client) = slot.client.as_mut() else {
        state.diag.push(not_connected());
        return SQL_ERROR;
    };

    let handle = Arc::new(CancelHandle::new());
    *stmt.cancel.lock() = Some(handle.clone());
    let timeout = state.query_timeout;

    if let Err(records) = prepare_session(conn, client, timeout, &handle) {
        state.diag.extend(records);
        return SQL_ERROR;
    }

    let command = match command {
        Command::Rpc(sql, mut params) => {
            resolve_null_types(conn, client, &sql, &mut params);
            Command::RawRpc(sql, params.into_rpc())
        }
        other => other,
    };
    let options = ExecuteOptions::new().timeout_secs(timeout).cancel(&handle);
    let result = block_on(async {
        match command {
            Command::Batch(sql) => client.execute(sql, options).await,
            Command::RawRpc(sql, params) => client.execute_sp_executesql(sql, params, options).await,
            Command::Rpc(..) => unreachable!("converted above"),
        }
    });
    let ret = match result {
        Ok(StatementResult::Rows) => {
            state.columns = Some(Arc::new(client.get_metadata().clone()));
            state.result_done = false;
            state.on_wire = true;
            SQL_SUCCESS
        }
        Ok(StatementResult::NoRows { rows_affected }) => {
            state.rowcount = rows_affected.map_or(-1, |n| n as i64);
            state.on_wire = client.has_open_batch();
            SQL_SUCCESS
        }
        Ok(StatementResult::End) => SQL_SUCCESS,
        Err(err) => {
            state.diag.extend(diag::records_for_error(&err, "HY000"));
            state.on_wire = client.has_open_batch();
            SQL_ERROR
        }
    };
    state.diag.extend(diag::info_records(&client.take_info_messages()));
    // Like msodbcsql, read up to the first row so errors raised before any
    // row is produced (lock timeout, divide by zero, ...) fail the execute.
    let ret = if ret == SQL_SUCCESS && state.columns.is_some() {
        fill_pending(client, &mut state, 1, FETCH_BATCH_BYTES);
        if state.pending.rows == 0 && let Some(err) = state.pending_error.take() {
            state.columns = None;
            let info = std::mem::take(&mut state.diag);
            state.diag = err;
            state.diag.extend(info);
            SQL_ERROR
        } else {
            ret
        }
    } else {
        ret
    };
    if state.on_wire {
        slot.owner = Some((stmt.id, Arc::downgrade(stmt)));
    }
    if ret == SQL_SUCCESS && !state.diag.is_empty() { SQL_SUCCESS_WITH_INFO } else { ret }
}

/// Fetch outcome handed back to the GIL-holding side.
struct Fetched {
    rows: RowBuffer,
    ncols: usize,
    /// Info records produced by this fetch (for the `messages` list).
    info: Vec<DiagRecord>,
    ret: SqlReturn,
}

/// Deliver up to `n` rows. `readahead` controls how many rows to pull from
/// the wire when nothing is pending (>= n).
fn fetch(stmt: &StmtShared, n: usize, readahead: usize) -> Fetched {
    let mut slot = stmt.conn.slot.lock();
    let mut state = stmt.state.lock();
    state.diag.clear();
    if state.freed {
        return Fetched { rows: RowBuffer::new(), ncols: 0, info: vec![], ret: SQL_INVALID_HANDLE };
    }
    if state.columns.is_none() {
        state.diag.push(DiagRecord::new(
            "24000",
            0,
            "[Microsoft][ODBC Driver Manager] Invalid cursor state",
        ));
        return Fetched { rows: RowBuffer::new(), ncols: 0, info: vec![], ret: SQL_ERROR };
    }
    if state.pending_rows() < n
        && let Some(client) = slot.client.as_mut()
    {
        let want = readahead.max(n) - state.pending_rows();
        fill_pending(client, &mut state, want, FETCH_BATCH_BYTES);
    }
    let rows = state.take_pending(n);
    let ncols = state.ncols();
    let info = state.diag.clone();
    let ret = if rows.rows > 0 {
        if info.is_empty() { SQL_SUCCESS } else { SQL_SUCCESS_WITH_INFO }
    } else if let Some(err) = state.pending_error.take() {
        state.diag = err;
        SQL_ERROR
    } else {
        SQL_NO_DATA
    };
    drop(state);
    drop(slot);
    Fetched { rows, ncols, info, ret }
}

fn append_messages(py: Python<'_>, messages: Option<&Bound<'_, PyAny>>, info: &[DiagRecord]) -> PyResult<()> {
    let Some(messages) = messages else { return Ok(()) };
    if messages.is_none() || info.is_empty() {
        return Ok(());
    }
    let list = messages.cast::<PyList>()?;
    for record in info {
        list.append(PyTuple::new(py, [diag::state_label(record), record.message.clone()])?)?;
    }
    Ok(())
}

fn stmt_of(handle: &Bound<'_, PyAny>) -> PyResult<Arc<StmtShared>> {
    Ok(handle.cast::<SqlHandle>()?.get().inner.clone())
}

#[pyclass(name = "SqlHandle", module = "ddbc_bindings", frozen)]
pub struct SqlHandle {
    pub inner: Arc<StmtShared>,
}

fn close_cursor(py: Python<'_>, stmt: &Arc<StmtShared>, free: bool) {
    py.detach(|| {
        let mut slot = stmt.conn.slot.lock();
        if slot.owner.as_ref().is_some_and(|(id, _)| *id == stmt.id) {
            release_wire(&mut slot, stmt.id);
        }
        let mut state = stmt.state.lock();
        state.reset_results();
        if free {
            state.freed = true;
        }
    });
}

#[pymethods]
impl SqlHandle {
    fn free(&self, py: Python<'_>) {
        close_cursor(py, &self.inner, true);
    }

    fn _close_cursor(&self, py: Python<'_>) {
        close_cursor(py, &self.inner, false);
    }

    fn _cancel(&self) {
        if let Some(handle) = self.inner.cancel.lock().as_ref() {
            handle.cancel();
        }
    }
}

// ---------------------------------------------------------------------------
// Module-level functions (the DDBCSQL* surface)
// ---------------------------------------------------------------------------

#[pyfunction]
#[pyo3(name = "DDBCSQLExecDirect")]
pub fn exec_direct(py: Python<'_>, statement_handle: &Bound<'_, PyAny>, query: String) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    crate::logging::debug(py, "SQLExecDirect: executing batch");
    let query = crate::params::placeholders::translate_call_escape(&query).unwrap_or(query);
    Ok(py.detach(|| execute_command(&stmt, Command::Batch(query))))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLExecute", signature = (statementHandle, query, params, inputSizes, isStmtPrepared, usePrepare, encodingSettings))]
#[allow(non_snake_case, unused_variables)]
pub fn execute(
    py: Python<'_>,
    statementHandle: &Bound<'_, PyAny>,
    query: String,
    params: &Bound<'_, PyAny>,
    inputSizes: &Bound<'_, PyAny>,
    isStmtPrepared: &Bound<'_, PyAny>,
    usePrepare: bool,
    encodingSettings: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    let stmt = stmt_of(statementHandle)?;
    let rpc_params = crate::params::build_rpc_params(params, Some(inputSizes))?;
    let (sql, markers) = crate::params::placeholders::rewrite_qmarks(&query);
    if markers != rpc_params.types.len() {
        let mut state = stmt.state.lock();
        state.diag.clear();
        state.diag.push(DiagRecord::driver(
            "07002",
            format!(
                "COUNT field incorrect or syntax error: the SQL contains {markers} parameter markers, but {} parameters were supplied",
                rpc_params.types.len()
            ),
        ));
        return Ok(SQL_ERROR);
    }
    if usePrepare
        && let Ok(list) = isStmtPrepared.cast::<PyList>()
        && !list.is_empty()
    {
        list.set_item(0, true)?;
    }
    Ok(py.detach(|| execute_command(&stmt, Command::Rpc(sql, rpc_params))))
}

/// Rows per TDS request in executemany.
const EXECUTEMANY_CHUNK: usize = 2000;

/// executemany: all parameter rows go to the server as `sp_execute` RPCs
/// against one prepared handle, packed `EXECUTEMANY_CHUNK` rows per TDS
/// request (one round trip per chunk). Rows affected are summed.
fn execute_batch(
    stmt: &Arc<StmtShared>,
    sql: String,
    mut rows: Vec<Vec<SqlType>>,
    all_null: Vec<usize>,
) -> SqlReturn {
    let conn = &stmt.conn;
    let mut slot = conn.slot.lock();
    release_wire(&mut slot, stmt.id);
    let mut state = stmt.state.lock();
    state.diag.clear();
    state.reset_results();
    let Some(client) = slot.client.as_mut() else {
        state.diag.push(not_connected());
        return SQL_ERROR;
    };
    let handle = Arc::new(CancelHandle::new());
    *stmt.cancel.lock() = Some(handle.clone());
    let timeout = state.query_timeout;
    if let Err(records) = prepare_session(conn, client, timeout, &handle) {
        state.diag.extend(records);
        return SQL_ERROR;
    }
    if !all_null.is_empty() && !rows.is_empty() {
        let mut probe = ParamSet { types: rows[0].clone(), untyped_nulls: all_null.clone() };
        resolve_null_types(conn, client, &sql, &mut probe);
        for row in rows.iter_mut() {
            for &c in &all_null {
                row[c] = probe.types[c].clone();
            }
        }
    }
    let mut prepared = PreparedStatement::new(sql);
    let mut orphaned = None;
    let mut total: Option<i64> = None;
    let mut ret = SQL_SUCCESS;
    let mut offset = 0usize;
    while !rows.is_empty() {
        let take = rows.len().min(EXECUTEMANY_CHUNK);
        let chunk: Vec<Vec<SqlType>> = rows.drain(..take).collect();
        let base = offset;
        offset += take;
        let iter = chunk.into_iter().enumerate().map(move |(i, types)| {
            let params = types
                .into_iter()
                .enumerate()
                .map(|(j, t)| RpcParameter::new(Some(format!("@P{}", j + 1)), StatusFlags::NONE, t))
                .collect();
            Ok((base + i, params))
        });
        let options = ExecuteOptions::new().timeout_secs(timeout).cancel(&handle);
        let result = block_on(client.execute_prepared_batch(&mut prepared, iter, &mut orphaned, options));
        state.diag.extend(diag::info_records(&client.take_info_messages()));
        match result {
            Ok(batch) => {
                if let Some(n) = batch.total_rows_affected() {
                    total = Some(total.unwrap_or(0) + n);
                }
                if let Some(failed) = batch.rows.iter().find(|r| !r.errors.is_empty()) {
                    for e in &failed.errors {
                        state.diag.insert(
                            0,
                            DiagRecord::new(
                                diag::sqlstate_for_server_error(e.number, e.class),
                                e.number as i32,
                                format!("{}[SQL Server]{}", diag::DRIVER_PREFIX, e.message),
                            ),
                        );
                    }
                    ret = SQL_ERROR;
                    break;
                }
            }
            Err(err) => {
                let mut records = diag::records_for_error(&err, "HY000");
                records.append(&mut state.diag);
                state.diag = records;
                ret = SQL_ERROR;
                break;
            }
        }
    }
    if client.has_open_batch() {
        let _ = block_on(client.close_query());
    }
    if let Some(id) = prepared.take_id() {
        let _ = block_on(client.unprepare(id, ()));
    }
    let _ = client.take_info_messages();
    state.rowcount = total.unwrap_or(-1);
    if ret == SQL_SUCCESS && !state.diag.is_empty() { SQL_SUCCESS_WITH_INFO } else { ret }
}

#[pyfunction]
#[pyo3(name = "SQLExecuteMany", signature = (statementHandle, query, columnwise_params, paramInfos, paramSetSize, encodingSettings))]
#[allow(non_snake_case, unused_variables)]
pub fn execute_many(
    py: Python<'_>,
    statementHandle: &Bound<'_, PyAny>,
    query: String,
    columnwise_params: &Bound<'_, PyAny>,
    paramInfos: &Bound<'_, PyAny>,
    paramSetSize: usize,
    encodingSettings: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    let stmt = stmt_of(statementHandle)?;
    let columns: Vec<Bound<'_, PyAny>> = columnwise_params.cast::<PyList>()?.iter().collect();
    let (sql, markers) = crate::params::placeholders::rewrite_qmarks(&query);
    if markers != columns.len() {
        let mut state = stmt.state.lock();
        state.diag.clear();
        state.diag.push(DiagRecord::driver(
            "07002",
            format!("COUNT field incorrect: the SQL contains {markers} parameter markers, but {} were supplied", columns.len()),
        ));
        return Ok(SQL_ERROR);
    }
    let mut rows: Vec<Vec<SqlType>> = Vec::with_capacity(paramSetSize);
    let mut nulls: Vec<Vec<bool>> = Vec::with_capacity(paramSetSize);
    for row in 0..paramSetSize {
        let mut types = Vec::with_capacity(columns.len());
        let mut null_row = Vec::with_capacity(columns.len());
        for column in columns.iter() {
            let value = column.get_item(row)?;
            null_row.push(value.is_none());
            types.push(py_to_sql_type(&value).map_err(|e| crate::params::conversion_error(&e))?);
        }
        rows.push(types);
        nulls.push(null_row);
    }
    let all_null = crate::params::batch::normalize(&mut rows, &nulls);
    Ok(py.detach(|| execute_batch(&stmt, sql, rows, all_null)))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLRowCount")]
pub fn row_count(statement_handle: &Bound<'_, PyAny>) -> PyResult<i64> {
    Ok(stmt_of(statement_handle)?.state.lock().rowcount)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLNumResultCols", signature = (statementHandle, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn num_result_cols(statementHandle: &Bound<'_, PyAny>, messages: Option<&Bound<'_, PyAny>>) -> PyResult<i64> {
    Ok(stmt_of(statementHandle)?.state.lock().ncols() as i64)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLDescribeCol", signature = (StatementHandle, ColumnMetadata, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn describe_col(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    ColumnMetadata: &Bound<'_, PyList>,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    let columns = stmt.state.lock().columns.clone();
    let Some(columns) = columns else { return Ok(SQL_SUCCESS) };
    for meta in columns.iter() {
        let d = PyDict::new(py);
        d.set_item("ColumnName", &meta.column_name)?;
        d.set_item("DataType", describe::odbc_sql_type(meta))?;
        d.set_item("ColumnSize", describe::column_size(meta))?;
        d.set_item("DecimalDigits", describe::decimal_digits(meta))?;
        d.set_item("Nullable", describe::nullable(meta))?;
        ColumnMetadata.append(d)?;
    }
    Ok(SQL_SUCCESS)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchOne", signature = (StatementHandle, row, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_one(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    row: &Bound<'_, PyList>,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    let fetched = py.detach(|| fetch(&stmt, 1, FETCHONE_READAHEAD));
    append_messages(py, messages, &fetched.info)?;
    if fetched.rows.rows > 0 {
        pyconv::extend_with_row(py, &fetched.rows, &fetched.rows.cells[..fetched.ncols], row)?;
    }
    Ok(fetched.ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchMany", signature = (StatementHandle, rows, fetchSize, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_many(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    rows: &Bound<'_, PyList>,
    fetchSize: usize,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    let fetched = py.detach(|| fetch(&stmt, fetchSize, fetchSize));
    append_messages(py, messages, &fetched.info)?;
    pyconv::append_rows(py, &fetched.rows, fetched.ncols, rows)?;
    // An empty fetchmany at end-of-data is a success with no rows upstream.
    Ok(if fetched.ret == SQL_NO_DATA { SQL_SUCCESS } else { fetched.ret })
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchAll", signature = (StatementHandle, rows, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_all(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    rows: &Bound<'_, PyList>,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    crate::logging::debug(py, "FetchAll: fetching remaining rows");
    let mut ret = SQL_SUCCESS;
    loop {
        let fetched = py.detach(|| fetch(&stmt, FETCHALL_BATCH_ROWS, FETCHALL_BATCH_ROWS));
        append_messages(py, messages, &fetched.info)?;
        pyconv::append_rows(py, &fetched.rows, fetched.ncols, rows)?;
        match fetched.ret {
            SQL_NO_DATA => break,
            SQL_SUCCESS_WITH_INFO => ret = SQL_SUCCESS_WITH_INFO,
            SQL_SUCCESS => {}
            other => return Ok(other),
        }
    }
    Ok(ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchScroll")]
#[allow(non_snake_case)]
pub fn fetch_scroll(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    FetchOrientation: i16,
    FetchOffset: i64,
    row_data: &Bound<'_, PyList>,
) -> PyResult<i16> {
    const SQL_FETCH_NEXT: i16 = 1;
    let stmt = stmt_of(StatementHandle)?;
    if FetchOrientation != SQL_FETCH_NEXT {
        let mut state = stmt.state.lock();
        state.diag.clear();
        state.diag.push(DiagRecord::driver("HY106", "Fetch type out of range"));
        return Ok(SQL_ERROR);
    }
    let _ = FetchOffset;
    let fetched = py.detach(|| fetch(&stmt, 1, FETCHONE_READAHEAD));
    if fetched.rows.rows > 0 && row_data.is_empty() {
        pyconv::extend_with_row(py, &fetched.rows, &fetched.rows.cells[..fetched.ncols], row_data)?;
    }
    Ok(fetched.ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLMoreResults")]
pub fn more_results(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    Ok(py.detach(|| {
        let mut slot = stmt.conn.slot.lock();
        let mut state = stmt.state.lock();
        state.diag.clear();
        state.pending.clear();
        state.pending_pos = 0;
        state.pending_error = None;
        state.columns = None;
        state.rowcount = -1;
        if let Some(next) = state.queued.pop_front() {
            let error = next.error.is_some() && next.columns.is_none();
            state.install_buffered(next);
            if error {
                state.diag = state.pending_error.take().unwrap_or_default();
                return SQL_ERROR;
            }
            return if state.diag.is_empty() { SQL_SUCCESS } else { SQL_SUCCESS_WITH_INFO };
        }
        if !state.on_wire {
            return SQL_NO_DATA;
        }
        let Some(client) = slot.client.as_mut() else { return SQL_NO_DATA };
        let advanced = block_on(client.advance());
        let ret = match advanced {
            Ok(StatementResult::Rows) => {
                state.columns = Some(Arc::new(client.get_metadata().clone()));
                state.result_done = false;
                SQL_SUCCESS
            }
            Ok(StatementResult::NoRows { rows_affected }) => {
                state.result_done = true;
                state.rowcount = rows_affected.map_or(-1, |n| n as i64);
                SQL_SUCCESS
            }
            Ok(StatementResult::End) => SQL_NO_DATA,
            Err(err) => {
                state.diag.extend(diag::records_for_error(&err, "HY000"));
                SQL_ERROR
            }
        };
        state.diag.extend(diag::info_records(&client.take_info_messages()));
        if !client.has_open_batch() && ret != SQL_SUCCESS {
            state.on_wire = false;
            slot.owner = None;
        }
        if ret == SQL_SUCCESS && !state.diag.is_empty() {
            SQL_SUCCESS_WITH_INFO
        } else if ret == SQL_NO_DATA && !state.diag.is_empty() {
            SQL_NO_DATA
        } else {
            ret
        }
    }))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLResetStmt")]
pub fn reset_stmt(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    if stmt.state.lock().freed {
        return Ok(SQL_INVALID_HANDLE);
    }
    close_cursor(py, &stmt, false);
    stmt.state.lock().diag.clear();
    Ok(SQL_SUCCESS)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFreeHandle")]
pub fn free_handle(py: Python<'_>, handle_type: i16, handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    if handle_type != SQL_HANDLE_STMT {
        return Ok(SQL_INVALID_HANDLE);
    }
    let Ok(stmt) = stmt_of(handle) else { return Ok(SQL_INVALID_HANDLE) };
    close_cursor(py, &stmt, true);
    Ok(SQL_SUCCESS)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLSetStmtAttr")]
pub fn set_stmt_attr(stmt: &Bound<'_, PyAny>, attr: i32, value: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(stmt)?;
    if attr == SQL_ATTR_QUERY_TIMEOUT {
        let timeout = value.extract::<i64>().unwrap_or(0).clamp(0, u32::MAX as i64) as u32;
        stmt.state.lock().query_timeout = timeout;
    }
    Ok(SQL_SUCCESS)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLGetAllDiagRecords")]
pub fn get_all_diag_records<'py>(py: Python<'py>, handle: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    if let Ok(stmt) = stmt_of(handle) {
        for record in stmt.state.lock().diag.iter() {
            out.append(PyTuple::new(py, [diag::state_label(record), record.message.clone()])?)?;
        }
    }
    Ok(out)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLCheckError")]
pub fn check_error(handle_type: i16, handle: &Bound<'_, PyAny>, retcode: i16) -> PyResult<crate::types::ErrorInfo> {
    let _ = handle_type;
    let mut info = crate::types::ErrorInfo::default();
    if retcode == SQL_INVALID_HANDLE || handle.is_none() {
        info.ddbcErrorMsg = "Invalid handle!".to_string();
        return Ok(info);
    }
    if retcode >= 0 {
        return Ok(info);
    }
    let Ok(stmt) = stmt_of(handle) else {
        info.ddbcErrorMsg = "Invalid handle!".to_string();
        return Ok(info);
    };
    if let Some(record) = stmt.state.lock().diag.first() {
        info.sqlState = record.sqlstate.clone();
        info.ddbcErrorMsg = record.message.clone();
        info.nativeError = record.native_error;
    }
    Ok(info)
}

/// Record a "not implemented" diagnostic and return SQL_ERROR.
pub fn not_implemented(handle: &Bound<'_, PyAny>, function: &str) -> i16 {
    if let Ok(stmt) = stmt_of(handle) {
        let mut state = stmt.state.lock();
        state.diag.clear();
        state.diag.push(DiagRecord::not_implemented(function));
    }
    SQL_ERROR
}

/// Run `sql` as a metadata query on this statement (catalog functions).
pub fn exec_catalog_query(py: Python<'_>, handle: &Bound<'_, PyAny>, sql: String) -> PyResult<i16> {
    let stmt = stmt_of(handle)?;
    Ok(py.detach(|| execute_command(&stmt, Command::Batch(sql))))
}

/// Run an RPC-parameterized metadata query on this statement.
pub fn exec_catalog_rpc(
    py: Python<'_>,
    handle: &Bound<'_, PyAny>,
    sql: String,
    params: Vec<RpcParameter>,
) -> PyResult<i16> {
    let stmt = stmt_of(handle)?;
    Ok(py.detach(|| execute_command(&stmt, Command::RawRpc(sql, params))))
}

/// Replace the current result's column metadata (catalog column renames).
pub fn map_columns(
    stmt: &StmtShared,
    f: impl FnOnce(&Arc<Vec<ColumnMetadata>>) -> Arc<Vec<ColumnMetadata>>,
) {
    let mut state = stmt.state.lock();
    if let Some(cols) = state.columns.as_ref() {
        let mapped = f(cols);
        state.columns = Some(mapped);
    }
}
