//! Running SQL on a statement: `execute` (batch or `sp_executesql`) and
//! `executemany` (`sp_execute` RPCs against one prepared handle).
//!
//! Both start from [`with_exclusive_wire`], which owns the lock order, frees
//! the wire, applies deferred session SQL and registers the statement as the
//! wire's owner when its results are still pending afterwards.

use std::sync::Arc;

use mssql_tds::connection::tds_client::{
    BatchErrorMode, ExecuteOptions, PreparedBatchResult, PreparedStatement, ResultSet, StatementResult,
    TdsClient,
};
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::error::Error as TdsError;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};

use super::StmtShared;
use super::cancel::StmtCancel;
use super::cursor::{
    BufferedResult, StmtState, prepared_batch_errors, server_error_record, surface_first_row_error,
};
use super::wire::{self, DRAIN_BUDGET_BYTES, Slot};
use crate::connection::ConnShared;
use crate::consts::*;
use crate::decode::RowBuffer;
use crate::diag::{self, DiagRecord};
use crate::params::ParamSet;
use crate::runtime::block_on;

/// Rows per TDS request in executemany.
const EXECUTEMANY_CHUNK: usize = 2000;
/// How long to wait for the server to acknowledge an abandoned request.
const RECOVER_SETTLE_SECS: u32 = 5;

/// What to run.
pub enum Command {
    /// A SQL batch.
    Batch(String),
    /// A parameterized statement from Python values; untyped NULLs are typed
    /// against the server first.
    Parameterized(String, ParamSet),
    /// A statement with parameters that are already fully typed.
    Typed(String, Vec<RpcParameter>),
}

/// Everything a command needs while it holds the wire.
pub struct Exclusive<'a> {
    pub conn: &'a ConnShared,
    pub client: &'a mut TdsClient,
    pub state: &'a mut StmtState,
    pub cancel: &'a Arc<StmtCancel>,
    pub timeout: u32,
}

fn options(timeout: u32, cancel: &StmtCancel) -> ExecuteOptions<'_> {
    ExecuteOptions::new().timeout_secs(timeout).cancel(cancel.handle())
}

/// Take the connection for `stmt`, run `body` on it, and leave the statement
/// registered as the wire's owner if results are still pending.
pub(super) fn with_exclusive_wire(
    stmt: &Arc<StmtShared>,
    body: impl FnOnce(&mut Exclusive<'_>) -> SqlReturn,
) -> SqlReturn {
    // Published before any lock is taken so `_cancel` reaches an execution
    // that is still waiting for the wire.
    let cancel = StmtCancel::new();
    let _executing = ExecutingGuard::enter(stmt);
    *stmt.cancel.lock() = Some(cancel.clone());
    let timeout = stmt.query_timeout();
    let conn = &stmt.conn;

    let mut slot = conn.slot.lock();
    wire::free_for_statement(&mut slot, stmt, &cancel, timeout);
    let mut state = stmt.state.lock();
    state.diag.clear();
    state.reset_results();
    if state.is_freed() {
        return SQL_INVALID_HANDLE;
    }
    if let Some(record) = cancel.interrupt_record() {
        state.diag.push(record);
        return SQL_ERROR;
    }
    let Slot { client, owner } = &mut *slot;
    let Some(client) = client.as_mut() else {
        state.diag.push(DiagRecord::driver("08003", "Connection is closed"));
        return SQL_ERROR;
    };
    if let Err(records) = prepare_session(conn, client, timeout, &cancel) {
        state.diag.extend(records);
        return SQL_ERROR;
    }

    let ret = body(&mut Exclusive { conn, client, state: &mut state, cancel: &cancel, timeout });
    if state.on_wire {
        *owner = Some((stmt.id, Arc::downgrade(stmt)));
    }
    if ret == SQL_SUCCESS && !state.diag.is_empty() { SQL_SUCCESS_WITH_INFO } else { ret }
}

/// Marks `stmt` as executing until dropped (see `StmtShared::executing`).
struct ExecutingGuard<'a>(&'a StmtShared);

impl<'a> ExecutingGuard<'a> {
    fn enter(stmt: &'a StmtShared) -> Self {
        stmt.executing.store(true, std::sync::atomic::Ordering::Release);
        Self(stmt)
    }
}

impl Drop for ExecutingGuard<'_> {
    fn drop(&mut self) {
        self.0.executing.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Restore the client to a usable state after a failed request.
fn recover(client: &mut TdsClient) {
    if client.has_open_batch() {
        let _ = block_on(client.cancel_query(RECOVER_SETTLE_SECS));
    }
    let _ = client.take_info_messages();
}

/// Apply deferred session settings and, in manual-commit mode, make sure a
/// transaction is open before the next statement.
fn prepare_session(
    conn: &ConnShared,
    client: &mut TdsClient,
    timeout: u32,
    cancel: &StmtCancel,
) -> Result<(), Vec<DiagRecord>> {
    // Manual-commit mode uses SET IMPLICIT_TRANSACTIONS so the server opens
    // a transaction with the next statement: no BEGIN round trip per
    // transaction. The SET is sent once per mode change.
    let pending = conn.pending_session();
    let mut statements: Vec<String> = pending.sql();
    let want_implicit = !conn.autocommit();
    if conn.implicit_transactions() != want_implicit {
        let set = if want_implicit { "SET IMPLICIT_TRANSACTIONS ON" } else { "SET IMPLICIT_TRANSACTIONS OFF" };
        statements.insert(0, set.to_string());
    }
    if statements.is_empty() {
        return Ok(());
    }
    let result = block_on(async {
        client.execute(statements.join("; "), options(timeout, cancel)).await?;
        client.close_query().await
    });
    let _ = client.take_info_messages();
    if let Err(err) = result {
        recover(client);
        return Err(diag::records_for_error(&err, "HY000"));
    }
    // Cleared only now: a failed attempt must be retried by the next statement.
    conn.clear_applied_session(&pending);
    conn.set_implicit_transactions(want_implicit);
    Ok(())
}

/// Give untyped NULL parameters the type SQL Server infers for them.
///
/// A failure of the describe query itself is ignored (the statement's own
/// execution reports it), but a timeout, cancellation or broken connection
/// is the requester's error.
fn resolve_null_types(ex: &mut Exclusive<'_>, sql: &str, params: &mut ParamSet) -> Result<(), Vec<DiagRecord>> {
    if params.untyped_nulls.is_empty() {
        return Ok(());
    }
    let described = match ex.conn.cached_param_types(sql) {
        Some(types) => types,
        None => {
            let types = describe_parameters(ex, sql)?;
            ex.conn.cache_param_types(sql, types.clone());
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
    Ok(())
}

/// `(parameter name, suggested type name)` pairs from
/// `sp_describe_undeclared_parameters`; empty if the server rejects the query.
fn describe_parameters(ex: &mut Exclusive<'_>, sql: &str) -> Result<Vec<(String, String)>, Vec<DiagRecord>> {
    let escaped = sql.replace('\'', "''");
    let query = format!("EXEC sp_describe_undeclared_parameters @tsql = N'{escaped}'");
    let mut rows = RowBuffer::new();
    let opts = options(ex.timeout, ex.cancel);
    let client = &mut *ex.client;
    let outcome = block_on(async {
        if client.execute(query, opts).await? == StatementResult::Rows {
            while client.next_row_into(&mut rows).await? {}
        }
        client.close_query().await
    });
    let _ = client.take_info_messages();
    match outcome {
        Ok(()) => Ok(parameter_types(&rows)),
        Err(TdsError::SqlServerError { .. }) => {
            recover(client);
            Ok(Vec::new())
        }
        Err(other) => {
            recover(client);
            Err(diag::records_for_error(&other, "HY000"))
        }
    }
}

fn parameter_types(rows: &RowBuffer) -> Vec<(String, String)> {
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
    types
}

/// Run one command and leave its first result current.
pub fn execute_command(stmt: &Arc<StmtShared>, command: Command) -> SqlReturn {
    with_exclusive_wire(stmt, |ex| run_command(ex, command))
}

fn run_command(ex: &mut Exclusive<'_>, command: Command) -> SqlReturn {
    let batch_mode = BatchErrorMode::Continue;
    let result = match command {
        Command::Batch(sql) => {
            let opts = options(ex.timeout, ex.cancel).on_error(batch_mode);
            block_on(ex.client.execute(sql, opts))
        }
        Command::Parameterized(sql, mut params) => {
            if let Err(records) = resolve_null_types(ex, &sql, &mut params) {
                ex.state.diag.extend(records);
                return SQL_ERROR;
            }
            let opts = options(ex.timeout, ex.cancel);
            block_on(ex.client.execute_sp_executesql(sql, params.into_rpc(), opts))
        }
        Command::Typed(sql, params) => {
            let opts = options(ex.timeout, ex.cancel);
            block_on(ex.client.execute_sp_executesql(sql, params, opts))
        }
    };
    let ret = apply_result(ex, result);
    ex.state.diag.extend(diag::info_records(&ex.client.take_info_messages()));
    if ret == SQL_SUCCESS && ex.state.columns.is_some() {
        surface_first_row_error(ex.client, ex.state)
    } else {
        ret
    }
}

/// Record where the client is positioned after an execute call.
fn apply_result(ex: &mut Exclusive<'_>, result: Result<StatementResult, TdsError>) -> SqlReturn {
    let Exclusive { client, state, .. } = ex;
    match result {
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
    }
}

/// executemany: all parameter rows go to the server as `sp_execute` RPCs
/// against one prepared handle, packed `EXECUTEMANY_CHUNK` rows per TDS
/// request (one round trip per chunk). Rows affected are summed. Result sets
/// the statement produces (`INSERT ... OUTPUT`) stay readable: those of all
/// but the last chunk are buffered, the last chunk's stream stays on the wire.
pub fn execute_batch(stmt: &Arc<StmtShared>, sql: String, rows: Vec<Vec<SqlType>>, all_null: Vec<usize>) -> SqlReturn {
    with_exclusive_wire(stmt, |ex| run_batch(ex, sql, rows, all_null))
}

fn run_batch(ex: &mut Exclusive<'_>, sql: String, mut rows: Vec<Vec<SqlType>>, all_null: Vec<usize>) -> SqlReturn {
    if !all_null.is_empty() && !rows.is_empty() {
        let mut probe = ParamSet { types: rows[0].clone(), untyped_nulls: all_null.clone() };
        if let Err(records) = resolve_null_types(ex, &sql, &mut probe) {
            ex.state.diag.extend(records);
            return SQL_ERROR;
        }
        for row in rows.iter_mut() {
            for &c in &all_null {
                row[c] = probe.types[c].clone();
            }
        }
    }
    let mut prepared = PreparedStatement::new(sql);
    let mut orphaned = None;
    let mut progress = BatchProgress::default();
    let mut remaining = rows.into_iter();
    let mut offset = 0usize;
    let mut ret = SQL_SUCCESS;
    loop {
        let chunk: Vec<Vec<SqlType>> = remaining.by_ref().take(EXECUTEMANY_CHUNK).collect();
        if chunk.is_empty() {
            break;
        }
        let base = offset;
        offset += chunk.len();
        let is_last = remaining.len() == 0;
        let params = chunk.into_iter().enumerate().map(move |(i, types)| Ok((base + i, name_positionally(types))));
        let opts = options(ex.timeout, ex.cancel);
        let result = block_on(ex.client.begin_execute_prepared_batch(&mut prepared, params, &mut orphaned, opts));
        ex.state.diag.extend(diag::info_records(&ex.client.take_info_messages()));
        ret = progress.absorb_chunk(ex, result, is_last);
        if ret != SQL_SUCCESS {
            break;
        }
    }
    if ex.client.has_open_batch() && ret != SQL_SUCCESS {
        recover(ex.client);
    }
    if let Some(id) = prepared.take_id() {
        // The server-side handle can only be released once the stream is
        // clear; a stream left open for OUTPUT rows keeps it until the
        // session is reset.
        if !ex.client.has_open_batch() {
            let _ = block_on(ex.client.unprepare(id, ()));
        }
    }
    let _ = ex.client.take_info_messages();
    ex.state.rowcount = progress.total_rows_affected.unwrap_or(-1);
    ret
}

fn name_positionally(types: Vec<SqlType>) -> Vec<RpcParameter> {
    types
        .into_iter()
        .enumerate()
        .map(|(j, t)| RpcParameter::new(Some(format!("@P{}", j + 1)), StatusFlags::NONE, t))
        .collect()
}

#[derive(Default)]
struct BatchProgress {
    total_rows_affected: Option<i64>,
    /// A result of an earlier chunk is already the statement's current result.
    has_current_result: bool,
}

impl BatchProgress {
    /// Fold one chunk's outcome into the statement; returns the ODBC code.
    fn absorb_chunk(
        &mut self,
        ex: &mut Exclusive<'_>,
        result: Result<PreparedBatchResult, TdsError>,
        is_last: bool,
    ) -> SqlReturn {
        let batch = match result {
            Ok(batch) => batch,
            Err(err) => {
                let mut records = diag::records_for_error(&err, "HY000");
                records.append(&mut ex.state.diag);
                ex.state.diag = records;
                return SQL_ERROR;
            }
        };
        self.add_rows_affected(batch.total_rows_affected());
        if self.reject_failed_rows(ex, &batch) {
            return SQL_ERROR;
        }
        if !ex.client.on_rows() {
            return SQL_SUCCESS;
        }
        self.keep_result_sets(ex, is_last)
    }

    fn add_rows_affected(&mut self, n: Option<i64>) {
        if let Some(n) = n {
            self.total_rows_affected = Some(self.total_rows_affected.unwrap_or(0) + n);
        }
    }

    /// Server errors of a parameter set fail the whole call; the first
    /// failing set's errors are reported in server order.
    fn reject_failed_rows(
        &self,
        ex: &mut Exclusive<'_>,
        batch: &PreparedBatchResult,
    ) -> bool {
        let Some(failed) = batch.rows.iter().find(|r| !r.errors.is_empty()) else { return false };
        let errors: Vec<DiagRecord> = failed.errors.iter().map(server_error_record).collect();
        ex.state.diag.splice(0..0, errors);
        true
    }

    /// The chunk produced a result set that is waiting on the wire.
    fn keep_result_sets(&mut self, ex: &mut Exclusive<'_>, is_last: bool) -> SqlReturn {
        let Exclusive { client, state, .. } = ex;
        let mut chunk_results = StmtState::new();
        chunk_results.columns = Some(Arc::new(client.get_metadata().clone()));
        chunk_results.result_done = false;
        chunk_results.on_wire = true;

        if is_last && !self.has_current_result {
            // The only results so far: stream them straight from the wire.
            state.columns = chunk_results.columns;
            state.result_done = false;
            state.on_wire = true;
            self.has_current_result = true;
            return SQL_SUCCESS;
        }
        wire::buffer_all(client, &mut chunk_results, DRAIN_BUDGET_BYTES);
        self.absorb_buffered(state, chunk_results);
        let errors = prepared_batch_errors(client);
        if !errors.is_empty() {
            state.diag.extend(errors);
            return SQL_ERROR;
        }
        SQL_SUCCESS
    }

    /// Append a buffered chunk's results after the statement's own.
    fn absorb_buffered(&mut self, state: &mut StmtState, mut chunk: StmtState) {
        let first = BufferedResult {
            columns: chunk.columns.take(),
            rows: std::mem::take(&mut chunk.pending),
            rowcount: -1,
            info: Vec::new(),
            error: chunk.pending_error.take(),
        };
        if self.has_current_result {
            state.queued.push_back(first);
        } else {
            state.install_buffered(first);
            self.has_current_result = true;
        }
        state.queued.append(&mut chunk.queued);
    }
}
