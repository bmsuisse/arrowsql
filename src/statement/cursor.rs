//! The result cursor of one statement: rows already decoded, results queued
//! behind the current one, and the fetch / next-result operations on them.
//!
//! Rows are pulled from the wire into `StmtState::pending` with the GIL
//! released; the caller converts them to Python objects after dropping the
//! locks.

use std::collections::VecDeque;
use std::sync::Arc;

use mssql_tds::connection::tds_client::{ResultSet, StatementResult, TdsClient};
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::query::metadata::ColumnMetadata;

use super::StmtShared;
use crate::consts::*;
use crate::decode::RowBuffer;
use crate::diag::{self, DiagRecord};
use crate::runtime::block_on;

/// Rows read ahead by `fetchone` so per-row calls don't each hit the wire.
pub const FETCHONE_READAHEAD: usize = 256;
/// Batch caps for fetchall: the GIL is re-taken between batches to convert.
pub const FETCHALL_BATCH_ROWS: usize = 8192;
const FETCH_BATCH_BYTES: usize = 16 << 20;

/// A result that was moved off the wire because the connection was needed
/// for something else.
pub struct BufferedResult {
    pub columns: Option<Arc<Vec<ColumnMetadata>>>,
    pub rows: RowBuffer,
    pub rowcount: i64,
    pub info: Vec<DiagRecord>,
    pub error: Option<Vec<DiagRecord>>,
}

impl BufferedResult {
    pub fn without_rows(rowcount: i64, info: Vec<DiagRecord>) -> Self {
        Self { columns: None, rows: RowBuffer::new(), rowcount, info, error: None }
    }

    pub fn failed(error: Vec<DiagRecord>, info: Vec<DiagRecord>) -> Self {
        Self { columns: None, rows: RowBuffer::new(), rowcount: -1, info, error: Some(error) }
    }
}

#[derive(Default)]
pub struct StmtState {
    pub diag: Vec<DiagRecord>,
    pub(super) columns: Option<Arc<Vec<ColumnMetadata>>>,
    pub(super) rowcount: i64,
    /// This statement owns the live batch on the connection.
    pub(super) on_wire: bool,
    /// The current result has no more rows to read from the wire.
    pub(super) result_done: bool,
    /// Rows already decoded but not yet handed to Python.
    pub(super) pending: RowBuffer,
    pending_pos: usize,
    /// Error met while reading ahead; reported once `pending` is drained.
    pub(super) pending_error: Option<Vec<DiagRecord>>,
    /// Results after the current one, drained from the wire (MARS emulation).
    pub(super) queued: VecDeque<BufferedResult>,
    pub(super) freed: bool,
}

impl StmtState {
    pub fn new() -> Self {
        Self { rowcount: -1, result_done: true, ..Default::default() }
    }

    pub fn ncols(&self) -> usize {
        self.columns.as_ref().map_or(0, |c| c.len())
    }

    pub fn rowcount(&self) -> i64 {
        self.rowcount
    }

    pub fn columns(&self) -> Option<Arc<Vec<ColumnMetadata>>> {
        self.columns.clone()
    }

    pub fn is_freed(&self) -> bool {
        self.freed
    }

    pub(super) fn mark_freed(&mut self) {
        self.freed = true;
    }

    /// Replace the current result's column metadata (catalog column renames).
    pub(super) fn map_columns(
        &mut self,
        f: impl FnOnce(&Arc<Vec<ColumnMetadata>>) -> Arc<Vec<ColumnMetadata>>,
    ) {
        if let Some(cols) = self.columns.as_ref() {
            self.columns = Some(f(cols));
        }
    }

    fn pending_rows(&self) -> usize {
        self.pending.rows - self.pending_pos
    }

    /// Forget the current result (columns and undelivered rows).
    fn clear_current(&mut self) {
        self.columns = None;
        self.rowcount = -1;
        self.pending.clear();
        self.pending_pos = 0;
        self.pending_error = None;
    }

    /// Forget every result of this statement, including queued ones.
    pub(super) fn reset_results(&mut self) {
        self.clear_current();
        self.on_wire = false;
        self.result_done = true;
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

    pub(super) fn install_buffered(&mut self, result: BufferedResult) {
        self.columns = result.columns;
        self.rowcount = result.rowcount;
        self.pending = result.rows;
        self.pending_pos = 0;
        self.pending_error = result.error;
        self.result_done = true;
        self.diag.extend(result.info);
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

/// Read rows of the current result into a Python-bound buffer.
pub(super) fn read_rows(
    client: &mut TdsClient,
    sink: &mut RowBuffer,
    max_rows: usize,
    max_bytes: usize,
) -> mssql_tds::core::TdsResult<bool> {
    read_rows_into(client, sink, |b| b.rows, RowBuffer::byte_size, max_rows, max_bytes)
}

/// A server error as a diagnostic record, in msodbcsql's message shape.
pub(super) fn server_error_record(e: &mssql_tds::error::SqlErrorInfo) -> DiagRecord {
    DiagRecord::new(
        diag::sqlstate_for_server_error(e.number, e.class),
        i32::try_from(e.number).unwrap_or(i32::MAX),
        format!("{}[SQL Server]{}", diag::DRIVER_PREFIX, e.message),
    )
}

/// Errors the server reported for individual parameter sets of a prepared
/// batch (executemany). They are only visible once the set's results were
/// read, so they are collected whenever a batch ends.
pub(super) fn prepared_batch_errors(client: &mut TdsClient) -> Vec<DiagRecord> {
    client
        .take_prepared_batch_results()
        .map(|batch| {
            batch
                .rows
                .iter()
                .flat_map(|row| row.errors.iter())
                .map(server_error_record)
                .collect()
        })
        .unwrap_or_default()
}

/// Pull up to `want` more rows of the current wire result into `state.pending`.
pub(super) fn fill_pending(
    client: &mut TdsClient,
    state: &mut StmtState,
    want: usize,
    max_bytes: usize,
) {
    if state.result_done || state.pending_error.is_some() || !state.on_wire {
        return;
    }
    if !client.on_rows() {
        state.result_done = true;
        return;
    }
    match read_rows(client, &mut state.pending, want, max_bytes) {
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

/// Like msodbcsql, read up to the first row of a freshly positioned result so
/// errors raised before any row is produced (lock timeout, divide by zero,
/// ...) fail the call that moved onto the result (`execute`, `nextset`).
pub(super) fn surface_first_row_error(client: &mut TdsClient, state: &mut StmtState) -> SqlReturn {
    fill_pending(client, state, 1, FETCH_BATCH_BYTES);
    if state.pending.rows == 0
        && let Some(err) = state.pending_error.take()
    {
        state.columns = None;
        let info = std::mem::take(&mut state.diag);
        state.diag = err;
        state.diag.extend(info);
        return SQL_ERROR;
    }
    SQL_SUCCESS
}

/// Fetch outcome handed back to the GIL-holding side.
pub struct Fetched {
    pub rows: RowBuffer,
    pub ncols: usize,
    /// Info records produced by this fetch (for the `messages` list).
    pub info: Vec<DiagRecord>,
    pub ret: SqlReturn,
}

impl Fetched {
    fn without_rows(ret: SqlReturn) -> Self {
        Self { rows: RowBuffer::new(), ncols: 0, info: vec![], ret }
    }
}

/// How much to read from the wire when too few rows are pending.
#[derive(Clone, Copy)]
pub enum FetchPlan {
    /// Deliver exactly `n` rows unless the result ends first, reading ahead
    /// up to `readahead` rows (within the byte cap) to save round trips.
    Exact { readahead: usize },
    /// Deliver whatever one capped read yields (at most `n` rows); the caller
    /// loops until `SQL_NO_DATA`.
    Batch,
}

/// Deliver up to `n` rows of the statement's current result.
pub fn fetch(stmt: &StmtShared, n: usize, plan: FetchPlan) -> Fetched {
    let mut slot = stmt.conn.slot.lock();
    let mut state = stmt.state.lock();
    state.diag.clear();
    if state.freed {
        return Fetched::without_rows(SQL_INVALID_HANDLE);
    }
    if state.columns.is_none() {
        state
            .diag
            .push(DiagRecord::new("24000", 0, "[Microsoft][ODBC Driver Manager] Invalid cursor state"));
        return Fetched::without_rows(SQL_ERROR);
    }
    if state.pending_rows() < n
        && let Some(client) = slot.client.as_mut()
    {
        read_for_fetch(client, &mut state, n, plan);
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
    Fetched { rows, ncols, info, ret }
}

fn read_for_fetch(client: &mut TdsClient, state: &mut StmtState, n: usize, plan: FetchPlan) {
    let readahead = match plan {
        FetchPlan::Exact { readahead } => readahead.max(n),
        FetchPlan::Batch => n,
    };
    fill_pending(client, state, readahead - state.pending_rows(), FETCH_BATCH_BYTES);
    // The byte cap may have stopped the read early; a caller that asked for
    // exactly `n` rows still gets them, bounded by `n` instead of the cap.
    if matches!(plan, FetchPlan::Exact { .. }) && state.pending_rows() < n {
        fill_pending(client, state, n - state.pending_rows(), usize::MAX);
    }
}

/// Move to the statement's next result (`SQLMoreResults`).
pub fn next_result(stmt: &StmtShared) -> SqlReturn {
    let mut slot = stmt.conn.slot.lock();
    let mut state = stmt.state.lock();
    state.diag.clear();
    // An error met while reading ahead belongs to the result being left.
    if let Some(err) = state.pending_error.take() {
        state.clear_current();
        state.diag = err;
        return SQL_ERROR;
    }
    state.clear_current();
    if let Some(next) = state.queued.pop_front() {
        return install_queued(&mut state, next);
    }
    if !state.on_wire {
        return SQL_NO_DATA;
    }
    let Some(client) = slot.client.as_mut() else { return SQL_NO_DATA };
    let ret = advance_on_wire(client, &mut state);
    if !state.on_wire && slot.owner.as_ref().is_some_and(|(id, _)| *id == stmt.id) {
        slot.owner = None;
    }
    ret
}

fn install_queued(state: &mut StmtState, next: BufferedResult) -> SqlReturn {
    let failed = next.error.is_some() && next.columns.is_none();
    state.install_buffered(next);
    if failed {
        state.diag = state.pending_error.take().unwrap_or_default();
        return SQL_ERROR;
    }
    if state.diag.is_empty() { SQL_SUCCESS } else { SQL_SUCCESS_WITH_INFO }
}

fn advance_on_wire(client: &mut TdsClient, state: &mut StmtState) -> SqlReturn {
    let mut ret = match block_on(client.advance()) {
        Ok(StatementResult::Rows) => {
            state.columns = Some(Arc::new(client.get_metadata().clone()));
            state.result_done = false;
            state.diag.extend(diag::info_records(&client.take_info_messages()));
            surface_first_row_error(client, state)
        }
        Ok(StatementResult::NoRows { rows_affected }) => {
            state.result_done = true;
            state.rowcount = rows_affected.map_or(-1, |n| n as i64);
            SQL_SUCCESS
        }
        Ok(StatementResult::End) => {
            let errors = prepared_batch_errors(client);
            if errors.is_empty() {
                SQL_NO_DATA
            } else {
                state.diag.extend(errors);
                SQL_ERROR
            }
        }
        Err(err) => {
            state.diag.extend(diag::records_for_error(&err, "HY000"));
            SQL_ERROR
        }
    };
    state.diag.extend(diag::info_records(&client.take_info_messages()));
    if !client.has_open_batch() && ret != SQL_SUCCESS {
        state.on_wire = false;
    }
    if ret == SQL_SUCCESS && !state.diag.is_empty() {
        ret = SQL_SUCCESS_WITH_INFO;
    }
    ret
}
