//! The result cursor of one statement: rows already decoded, results queued
//! behind the current one, and the fetch / next-result operations on them.
//!
//! Rows are pulled from the wire into `StmtState::pending` with the GIL
//! released; the caller converts them to Python objects after dropping the
//! locks.

use crate::runtime::Lockable;
use std::collections::VecDeque;
use std::sync::Arc;

use mssql_tds::connection::tds_client::{MetadataHandle, ResultSet, StatementResult};
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::query::metadata::ColumnMetadata;

use super::StmtShared;
use super::cancel::StmtCancel;
use super::wire::Slot;
use crate::consts::*;
use crate::decode::{RowBuffer, describe, hidden};
use crate::diag::{self, DiagRecord};
use crate::runtime::SyncClient;

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
    /// Bumped whenever the current results are discarded (execute, close,
    /// next result). Background Arrow producers stop when it no longer
    /// matches theirs.
    pub generation: u64,
    /// Cancellation of the execution that produced the current results; its
    /// reads of the wire stop with it. Kept so that closing an old Arrow
    /// stream cancels that execution only, never a newer one.
    pub(super) execution: Option<Arc<StmtCancel>>,
    /// What `DDBCSQLDescribeCol` last returned.
    pub(super) describe_cache: Option<describe::DescribeCache>,
    /// The columns last taken from the client and the metadata they came from.
    last_columns: Option<(MetadataHandle, Arc<Vec<ColumnMetadata>>)>,
    /// Fixed text widths and LOB flag of `columns`, valid while the Arc is the same.
    text_shape: Option<TextShape>,
}

type TextShape = (Arc<Vec<ColumnMetadata>>, Arc<[(usize, usize)]>, bool);

impl StmtState {
    pub fn new() -> Self {
        Self { rowcount: -1, result_done: true, ..Default::default() }
    }

    /// Fixed text widths and LOB flag of the current columns, cached per column set.
    fn text_shape(&mut self) -> (Arc<[(usize, usize)]>, bool) {
        let Some(columns) = self.columns.as_ref() else {
            return (Arc::from([]), false);
        };
        if let Some((cached, widths, has_lob)) = &self.text_shape
            && Arc::ptr_eq(cached, columns)
        {
            return (widths.clone(), *has_lob);
        }
        let widths: Arc<[(usize, usize)]> = describe::fixed_text_widths(columns).into();
        let has_lob = columns.iter().any(describe::is_lob);
        self.text_shape = Some((columns.clone(), widths.clone(), has_lob));
        (widths, has_lob)
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

    /// The column list of the client's current result, shared with the previous
    /// one when the server sent the same metadata (a repeated query), so
    /// neither the copy nor the per-column comparison in `DDBCSQLDescribeCol`
    /// is repeated.
    pub(super) fn columns_of_current(&mut self, client: &SyncClient) -> Arc<Vec<ColumnMetadata>> {
        let Some(handle) = client.metadata_handle() else {
            return Arc::new(hidden::visible_columns(client.get_metadata()));
        };
        if let Some((last, columns)) = &self.last_columns
            && last.same_as(&handle)
        {
            return Arc::clone(columns);
        }
        let columns = Arc::new(hidden::visible_columns(handle.columns()));
        self.last_columns = Some((handle, Arc::clone(&columns)));
        columns
    }

    /// Nothing of this statement is on the wire or queued behind the current
    /// result, so closing it needs no connection access.
    pub(super) fn is_off_wire(&self) -> bool {
        !self.freed && !self.on_wire && self.queued.is_empty()
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
        self.text_shape = None;
        self.rowcount = -1;
        self.pending.clear();
        self.pending_pos = 0;
        self.pending_error = None;
    }

    /// Forget every result of this statement, including queued ones.
    pub(super) fn reset_results(&mut self) {
        self.generation += 1;
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

/// SQL Server error 266: transaction count after EXECUTE indicates a mismatch.
const TRANSACTION_COUNT_MISMATCH: u32 = 266;

/// Read rows of the current wire result into `sink` until `max_rows` rows
/// were written, `max_bytes` is exceeded, or the result ends. This is the
/// generic decode seam: any `RowWriter` (Python `RowBuffer`, Arrow builders)
/// can be filled here. Returns `Ok(true)` when the result is exhausted.
pub fn read_rows_into<W: RowWriter + Send>(
    client: &mut SyncClient,
    sink: &mut W,
    rows_written: impl Fn(&W) -> usize,
    bytes_written: impl Fn(&W) -> usize,
    max_rows: usize,
    max_bytes: usize,
) -> mssql_tds::core::TdsResult<bool> {
    match hidden::visible_map(client.get_metadata()) {
        None => read_loop(client, sink, rows_written, bytes_written, max_rows, max_bytes),
        Some(map) => {
            let mut filter = hidden::HiddenFilter::new(sink, &map);
            read_loop(
                client,
                &mut filter,
                |f| rows_written(&*f.inner),
                |f| bytes_written(&*f.inner),
                max_rows,
                max_bytes,
            )
        }
    }
}

fn read_loop<W: RowWriter + Send>(
    client: &mut SyncClient,
    sink: &mut W,
    rows_written: impl Fn(&W) -> usize,
    bytes_written: impl Fn(&W) -> usize,
    max_rows: usize,
    max_bytes: usize,
) -> mssql_tds::core::TdsResult<bool> {
    client.block_on(|c| async move {
        let start = rows_written(sink);
        while rows_written(sink) - start < max_rows && (max_bytes == usize::MAX || bytes_written(sink) < max_bytes) {
            if !c.next_row_into(sink).await? {
                // Like msodbcsql, error 266 (BEGIN/COMMIT mismatch at procedure
                // exit) fails the fetch of the procedure's last row set; other
                // errors wait for nextset.
                c.complete_current_result().await?;
                if c.parked_error_number() == Some(TRANSACTION_COUNT_MISMATCH) {
                    c.advance().await?;
                }
                return Ok(true);
            }
        }
        Ok(false)
    })
}/// Read rows of the current result into a Python-bound buffer.
pub(super) fn read_rows(
    client: &mut SyncClient,
    sink: &mut RowBuffer,
    max_rows: usize,
    max_bytes: usize,
) -> mssql_tds::core::TdsResult<bool> {
    sink.row_hint = sink.rows.saturating_add(max_rows);
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
pub(super) fn prepared_batch_errors(client: &mut SyncClient) -> Vec<DiagRecord> {
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
    client: &mut SyncClient,
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
pub(super) fn surface_first_row_error(client: &mut SyncClient, state: &mut StmtState) -> SqlReturn {
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
    /// The result set has a LOB column, so upstream streams it row by row.
    pub has_lob: bool,
}

impl Fetched {
    fn without_rows(ret: SqlReturn) -> Self {
        Self { rows: RowBuffer::new(), ncols: 0, info: vec![], ret, has_lob: false }
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
    let mut slot = stmt.conn.slot.lock_checked();
    let mut state = stmt.state.lock_checked();
    deliver(&mut state, slot.client.as_mut(), n, plan)
}

/// [`fetch`] for callers that hold the GIL: serves the rows already buffered
/// (or the end of a finished result) without touching the connection, and
/// returns `None` when the wire would be needed or a lock is busy.
pub fn fetch_buffered(stmt: &StmtShared, n: usize, plan: FetchPlan) -> Option<Fetched> {
    let mut state = stmt.state.try_lock()?;
    if state.pending_rows() < n && state.on_wire {
        return None;
    }
    Some(deliver(&mut state, None, n, plan))
}

fn deliver(state: &mut StmtState, client: Option<&mut SyncClient>, n: usize, plan: FetchPlan) -> Fetched {
    state.diag.clear();
    if state.freed {
        return Fetched::without_rows(SQL_INVALID_HANDLE);
    }
    if state.columns.is_none() {
        // No result set is positioned (the statement never produced one, or
        // was closed): msodbcsql's HY010, not the 24000 of an open cursor.
        state
            .diag
            .push(DiagRecord::new("HY010", 0, "[Microsoft][ODBC Driver Manager] Function sequence error"));
        return Fetched::without_rows(SQL_ERROR);
    }
    if state.pending_rows() < n
        && let Some(client) = client
    {
        read_for_fetch(client, state, n, plan);
    }
    let mut rows = state.take_pending(n);
    let ncols = state.ncols();
    let (widths, has_lob) = state.text_shape();
    if rows.rows > 0 && !widths.is_empty() {
        rows.pad_fixed_text(ncols, &widths);
    }
    let info = state.diag.clone();
    let ret = if rows.rows > 0 {
        if info.is_empty() { SQL_SUCCESS } else { SQL_SUCCESS_WITH_INFO }
    } else if let Some(err) = state.pending_error.take() {
        state.diag = err;
        SQL_ERROR
    } else {
        SQL_NO_DATA
    };
    Fetched { rows, ncols, info, ret, has_lob }
}

fn read_for_fetch(client: &mut SyncClient, state: &mut StmtState, n: usize, plan: FetchPlan) {
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
    let mut slot = stmt.conn.slot.lock_checked();
    let mut state = stmt.state.lock_checked();
    state.diag.clear();
    state.generation += 1;
    // An error met while reading ahead belongs to the result being left.
    if let Some(err) = state.pending_error.take() {
        state.clear_current();
        state.diag = err;
        return SQL_ERROR;
    }
    let leaving_rows = state.columns.is_some();
    state.clear_current();
    if leaving_rows {
        return skip_to_result_set(stmt, &mut slot, &mut state);
    }
    let ret = step_result(stmt, &mut slot, &mut state);
    with_info(&state, ret)
}

/// Leaving a result set: the results without columns that follow it (a PRINT,
/// an informational RAISERROR, a DML count) are passed over when a result set
/// comes after them, their messages kept in `diag`. When none does, they are
/// reported one at a time, as `nextset()` does for any other no-column result.
fn skip_to_result_set(stmt: &StmtShared, slot: &mut Slot, state: &mut StmtState) -> SqlReturn {
    let mut held: Vec<BufferedResult> = Vec::new();
    loop {
        let ret = step_result(stmt, slot, state);
        let info = std::mem::take(&mut state.diag);
        if matches!(ret, SQL_SUCCESS | SQL_SUCCESS_WITH_INFO) && state.columns.is_none() {
            held.push(BufferedResult::without_rows(state.rowcount, info));
            continue;
        }
        if ret == SQL_NO_DATA && !held.is_empty() {
            let first = held.remove(0);
            for rest in held.into_iter().rev() {
                state.queued.push_front(rest);
            }
            state.install_buffered(first);
            return with_info(state, SQL_SUCCESS);
        }
        // A result set, or an error, follows: the passed-over results are gone.
        state.diag = held.into_iter().flat_map(|r| r.info).chain(info).collect();
        return with_info(state, ret);
    }
}

/// `SQL_SUCCESS` becomes `SQL_SUCCESS_WITH_INFO` when messages are pending.
fn with_info(state: &StmtState, ret: SqlReturn) -> SqlReturn {
    if ret == SQL_SUCCESS && !state.diag.is_empty() { SQL_SUCCESS_WITH_INFO } else { ret }
}

/// Install the result after the current one: a queued result, else the next
/// one read off the wire.
fn step_result(stmt: &StmtShared, slot: &mut Slot, state: &mut StmtState) -> SqlReturn {
    if let Some(next) = state.queued.pop_front() {
        return install_queued(state, next);
    }
    if !state.on_wire {
        return SQL_NO_DATA;
    }
    let Some(client) = slot.client.as_mut().filter(|c| !c.is_foreign()) else { return SQL_NO_DATA };
    let ret = advance_on_wire(client, state);
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

fn advance_on_wire(client: &mut SyncClient, state: &mut StmtState) -> SqlReturn {
    let mut ret = match client.block_on(|c| c.advance()) {
        Ok(StatementResult::Rows) => {
            state.columns = Some(state.columns_of_current(client));
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

/// A sink that whole batches are decoded into (e.g. Arrow builders). It is
/// filled from rows already buffered as `RowBuffer` cells (read ahead by
/// `fetchone`, or drained off the wire for MARS emulation), then straight
/// from the wire through `read_rows_into`.
pub trait BatchSink: RowWriter + Send {
    /// Complete rows written so far.
    fn rows(&self) -> usize;
    /// Approximate bytes held.
    fn byte_size(&self) -> usize;
    /// Append the rows of `rows`, an `ncols`-wide buffer.
    fn push_buffered(&mut self, rows: &RowBuffer, ncols: usize);
    /// Drop the values of a row that was only partly decoded when an error hit.
    fn truncate_to_rows(&mut self);
}

/// Outcome of `fetch_into`.
pub struct SinkFetch {
    pub ret: SqlReturn,
    /// Info records produced by this fetch (for the `messages` list).
    pub info: Vec<DiagRecord>,
    /// The statement moved on to other results since `generation` was taken.
    pub stale: bool,
}

/// Current result columns and result generation; `None` without a result.
pub fn result_columns(stmt: &StmtShared) -> Option<(Arc<Vec<ColumnMetadata>>, u64, Option<Arc<StmtCancel>>)> {
    let state = stmt.state.lock_checked();
    state.columns.clone().map(|c| (c, state.generation, state.execution.clone()))
}

/// Error records of the statement (after a failed `fetch_into`).
pub fn diag_records(stmt: &StmtShared) -> Vec<DiagRecord> {
    stmt.state.lock_checked().diag.clone()
}

/// Deliver up to `n` rows of the current result into `sink`: buffered rows
/// first, then rows decoded straight off the wire. With `generation`, does
/// nothing (and reports `stale`) if the statement moved on to other results.
pub fn fetch_into<S: BatchSink>(
    stmt: &StmtShared,
    sink: &mut S,
    n: usize,
    generation: Option<u64>,
) -> SinkFetch {
    let mut slot = stmt.conn.slot.lock_checked();
    let mut state = stmt.state.lock_checked();
    if state.freed || generation.is_some_and(|g| g != state.generation) {
        return SinkFetch { ret: SQL_NO_DATA, info: vec![], stale: true };
    }
    state.diag.clear();
    if state.columns.is_none() {
        state
            .diag
            .push(DiagRecord::new("24000", 0, "[Microsoft][ODBC Driver Manager] Invalid cursor state"));
        return SinkFetch { ret: SQL_ERROR, info: vec![], stale: false };
    }
    let ncols = state.ncols();
    let start = sink.rows();
    if n > 0 && state.pending_rows() > 0 {
        let buffered = state.take_pending(n);
        sink.push_buffered(&buffered, ncols);
    }
    let want = n - (sink.rows() - start);
    let mut error = None;
    if want > 0
        && state.pending_rows() == 0
        && state.pending_error.is_none()
        && state.on_wire
        && !state.result_done
        && let Some(client) = slot.client.as_mut()
    {
        if client.on_rows() {
            match read_rows_into(client, sink, S::rows, S::byte_size, want, usize::MAX) {
                Ok(done) => state.result_done = done,
                Err(err) => {
                    sink.truncate_to_rows();
                    state.result_done = true;
                    error = Some(diag::records_for_error(&err, "HY000"));
                    if !client.has_open_batch() {
                        state.on_wire = false;
                    }
                }
            }
        } else {
            state.result_done = true;
        }
        state.diag.extend(diag::info_records(&client.take_info_messages()));
    }
    let delivered = sink.rows() - start;
    if let Some(err) = error {
        // Rows decoded before the error are delivered; the error comes next.
        if delivered == 0 {
            state.diag.extend(err);
            return SinkFetch { ret: SQL_ERROR, info: vec![], stale: false };
        }
        state.pending_error = Some(err);
    }
    let info = state.diag.clone();
    let ret = if delivered > 0 || n == 0 {
        if info.is_empty() { SQL_SUCCESS } else { SQL_SUCCESS_WITH_INFO }
    } else if let Some(err) = state.pending_error.take() {
        state.diag = err;
        SQL_ERROR
    } else {
        SQL_NO_DATA
    };
    SinkFetch { ret, info, stale: false }
}
