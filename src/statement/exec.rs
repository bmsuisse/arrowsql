//! Running SQL on a statement: `execute` (batch or `sp_executesql`) and
//! `executemany` (`sp_execute` RPCs against one prepared handle).
//!
//! Both start from [`with_exclusive_wire`], which owns the lock order, frees
//! the wire, applies deferred session SQL and registers the statement as the
//! wire's owner when its results are still pending afterwards.

use std::sync::Arc;

use mssql_tds::connection::tds_client::{
    BatchErrorMode, ExecuteOptions, PreparedBatchResult, PreparedStatement, ResultSet, StatementResult,
};
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::error::Error as TdsError;
use mssql_tds::message::transaction_management::TransactionIsolationLevel;
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
use crate::decode::buffer::Cell;
use crate::diag::{self, DiagRecord};
use crate::params::ParamSet;
use crate::params::udt::{self, DescribedParam};
use crate::runtime::SyncClient;

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
    pub client: &'a mut SyncClient,
    pub state: &'a mut StmtState,
    pub cancel: &'a Arc<StmtCancel>,
    pub timeout: u32,
    /// The open transaction was begun by the driver for this statement and
    /// holds nothing of the caller's yet.
    pub fresh_txn: bool,
}

fn options(timeout: u32, cancel: &StmtCancel) -> ExecuteOptions<'_> {
    ExecuteOptions::new().timeout_secs(timeout).cancel(cancel.handle())
}

/// Take the connection for `stmt`, run `body` on it, and leave the statement
/// registered as the wire's owner if results are still pending.
pub(crate) fn with_exclusive_wire(
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
    if !conn.autocommit()
        && conn.take_failed()
        && !client.has_active_transaction()
        && xact_abort_is_on(client, timeout, &cancel)
    {
        conn.mark_txn_aborted();
    }
    if let Err(records) = prepare_session(conn, client, timeout, &cancel) {
        state.diag.extend(records);
        return SQL_ERROR;
    }

    let ret = body(&mut Exclusive {
        conn,
        client,
        state: &mut state,
        cancel: &cancel,
        timeout,
        fresh_txn: conn.take_txn_untouched(),
    });
    // The server may still be sending the rollback; the next statement checks.
    if ret == SQL_ERROR && !conn.autocommit() {
        conn.mark_failed();
    }
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
fn recover(client: &mut SyncClient) {
    if client.has_open_batch() {
        let _ = client.block_on(|c| c.cancel_query(RECOVER_SETTLE_SECS));
    }
    let _ = client.take_info_messages();
}

/// Whether the session has XACT_ABORT on. Asked only after a failed statement
/// took the transaction with it: with XACT_ABORT the caller asked for
/// abort-everything, so writes must not silently run outside their
/// transaction. Other failures (e.g. a conversion error) roll back too, but
/// msodbcsql silently starts a new transaction.
fn xact_abort_is_on(client: &mut SyncClient, timeout: u32, cancel: &StmtCancel) -> bool {
    // @@OPTIONS bit 16384 is XACT_ABORT; a server error answers "yes".
    const PROBE: &str = "IF (@@OPTIONS & 16384) = 16384 RAISERROR('xact_abort', 16, 1)";
    let opts = options(timeout, cancel);
    let result = client.block_on(|c| async move {
        c.execute(PROBE.to_string(), opts).await?;
        c.close_query().await
    });
    let _ = client.take_info_messages();
    match result {
        Err(TdsError::SqlServerError { .. }) => {
            recover(client);
            true
        }
        _ => false,
    }
}

/// Apply deferred session settings and, in manual-commit mode, make sure a
/// transaction is open before the next statement.
fn prepare_session(
    conn: &ConnShared,
    client: &mut SyncClient,
    timeout: u32,
    cancel: &StmtCancel,
) -> Result<(), Vec<DiagRecord>> {
    let pending = conn.pending_session();
    let statements: Vec<String> = pending.sql();
    // Manual-commit mode opens the transaction with a TDS transaction-manager
    // request before the statement, like msodbcsql, so @@TRANCOUNT is already
    // 1 inside the first statement (SET IMPLICIT_TRANSACTIONS would not
    // open one for a bare SELECT) and a user COMMIT leaves a fresh one behind.
    let begin = !conn.autocommit() && !client.has_active_transaction();
    if statements.is_empty() && !begin {
        return Ok(());
    }
    let (exec_opts, begin_opts) = (options(timeout, cancel), options(timeout, cancel));
    let result = client.block_on(|c| async move {
        if !statements.is_empty() {
            c.execute(statements.join("; "), exec_opts).await?;
            c.close_query().await?;
        }
        if begin {
            c.begin_transaction_with_options(TransactionIsolationLevel::NoChange, None, begin_opts).await?;
        }
        Ok(())
    });
    let _ = client.take_info_messages();
    if let Err(err) = result {
        recover(client);
        return Err(diag::records_for_error(&err, "HY000"));
    }
    // Cleared only now: a failed attempt must be retried by the next statement.
    conn.clear_applied_session(&pending);
    conn.set_txn_untouched(begin);
    Ok(())
}

/// Why describing a statement's parameters failed.
enum DescribeFailure {
    /// The server rejected the describe query (e.g. it cannot infer a type).
    Rejected(Vec<DiagRecord>),
    /// Timeout, cancellation or a broken connection: the requester's error.
    Interrupted(Vec<DiagRecord>),
}

/// Give untyped NULL parameters the type SQL Server infers for them.
///
/// A rejected describe query is ignored (the statement's own execution
/// reports the problem), but a timeout, cancellation or broken connection
/// is the requester's error.
fn resolve_null_types(ex: &mut Exclusive<'_>, sql: &str, params: &mut ParamSet) -> Result<(), Vec<DiagRecord>> {
    if params.untyped_nulls.is_empty() {
        return Ok(());
    }
    let described = match described_params(ex, sql) {
        Ok(described) => described,
        Err(DescribeFailure::Rejected(_)) => return Ok(()),
        Err(DescribeFailure::Interrupted(records)) => return Err(records),
    };
    for &i in &params.untyped_nulls {
        let name = format!("@P{}", i + 1);
        if let Some(sql_type) = described
            .iter()
            .find(|param| param.name.eq_ignore_ascii_case(&name))
            .and_then(|param| crate::params::null_types::null_of(&param.system_type))
        {
            params.types[i] = sql_type;
        }
    }
    Ok(())
}

/// Name the CLR UDT behind each SQL_SS_UDT parameter. A failed describe fails
/// the execute with the server's error, as the UDT name cannot be guessed.
fn resolve_udt_names(ex: &mut Exclusive<'_>, sql: &str, params: &mut ParamSet) -> Result<(), Vec<DiagRecord>> {
    if !udt::has_unresolved_udt(params) {
        return Ok(());
    }
    let described = described_params(ex, sql).map_err(|failure| match failure {
        DescribeFailure::Rejected(records) | DescribeFailure::Interrupted(records) => records,
    })?;
    udt::resolve_udt_names(params, &described).map_err(|message| vec![DiagRecord::driver("HY004", message)])
}

/// Parameter metadata of `sql` from `sp_describe_undeclared_parameters`,
/// cached per SQL text. Failures are not cached.
fn described_params(ex: &mut Exclusive<'_>, sql: &str) -> Result<Vec<DescribedParam>, DescribeFailure> {
    if let Some(described) = ex.conn.cached_param_types(sql) {
        return Ok(described);
    }
    let escaped = sql.replace('\'', "''");
    let query = format!("EXEC sp_describe_undeclared_parameters @tsql = N'{escaped}'");
    let mut rows = RowBuffer::new();
    let opts = options(ex.timeout, ex.cancel);
    let client = &mut *ex.client;
    let sink = &mut rows;
    let outcome = client.block_on(|c| async move {
        if c.execute(query, opts).await? == StatementResult::Rows {
            while c.next_row_into(sink).await? {}
        }
        c.close_query().await
    });
    let _ = client.take_info_messages();
    match outcome {
        Ok(()) => {
            let described = described_rows(&rows);
            ex.conn.cache_param_types(sql, described.clone());
            Ok(described)
        }
        Err(err @ TdsError::SqlServerError { .. }) => {
            recover(client);
            Err(DescribeFailure::Rejected(diag::records_for_error(&err, "HY000")))
        }
        Err(other) => {
            recover(client);
            Err(DescribeFailure::Interrupted(diag::records_for_error(&other, "HY000")))
        }
    }
}

fn described_rows(rows: &RowBuffer) -> Vec<DescribedParam> {
    let ncols = rows.cells.len().checked_div(rows.rows).unwrap_or(0);
    rows.cells.chunks(ncols.max(1)).filter_map(|row| describe_row(rows, row)).collect()
}

/// One describe row. Columns: parameter_ordinal, name, suggested_system_type_id,
/// suggested_system_type_name, ..., suggested_user_type_database,
/// suggested_user_type_schema, suggested_user_type_name.
fn describe_row(rows: &RowBuffer, row: &[Cell]) -> Option<DescribedParam> {
    let text = |index: usize| row.get(index).and_then(|c| rows.text(c));
    let system_type_id = match row.get(2)? {
        Cell::Int(id) => Some(*id),
        _ => None,
    };
    Some(DescribedParam {
        name: text(1)?,
        system_type: text(3)?,
        udt: udt::udt_name(system_type_id, text(8), text(9), text(10)),
    })
}

/// Statements that change data or schema, judged by their first keyword.
fn is_write(sql: &str) -> bool {
    let word = sql.trim_start().split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("");
    ["INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "CREATE", "ALTER", "DROP", "EXEC", "EXECUTE", "BULK"]
        .iter()
        .any(|w| word.eq_ignore_ascii_case(w))
}

/// Rejects a write issued after the server aborted the caller's transaction
/// (XACT_ABORT), like go-mssqldb: it would run outside that transaction and a
/// later commit() would persist it. The caller's rollback() clears the state.
fn reject_write_after_abort(ex: &mut Exclusive<'_>, sql: &str) -> bool {
    if !is_write(sql) || !ex.conn.take_txn_aborted() {
        return false;
    }
    ex.state.diag.push(DiagRecord::driver(
        "25000",
        "The server rolled back the transaction after an error; the statement was not run. Call rollback() to continue.",
    ));
    true
}

/// Run one command and leave its first result current.
pub fn execute_command(stmt: &Arc<StmtShared>, command: Command) -> SqlReturn {
    with_exclusive_wire(stmt, |ex| run_command(ex, command))
}

fn run_command(ex: &mut Exclusive<'_>, command: Command) -> SqlReturn {
    let (Command::Batch(sql) | Command::Parameterized(sql, _) | Command::Typed(sql, _)) = &command;
    if reject_write_after_abort(ex, sql) {
        return SQL_ERROR;
    }
    let batch_mode = BatchErrorMode::Continue;
    let result = match command {
        Command::Batch(sql) => {
            let opts = options(ex.timeout, ex.cancel).on_error(batch_mode);
            ex.client.block_on(|c| c.execute(sql, opts))
        }
        Command::Parameterized(sql, mut params) => {
            if let Err(records) = resolve_null_types(ex, &sql, &mut params) {
                ex.state.diag.extend(records);
                return SQL_ERROR;
            }
            if let Err(records) = resolve_udt_names(ex, &sql, &mut params) {
                ex.state.diag.extend(records);
                return SQL_ERROR;
            }
            let opts = options(ex.timeout, ex.cancel);
            let rp = params.into_unnamed_rpc();
            ex.client.block_on(|c| c.execute_sp_executesql_positional(sql, rp, opts))
        }
        Command::Typed(sql, params) => {
            let opts = options(ex.timeout, ex.cancel);
            ex.client.block_on(|c| c.execute_sp_executesql(sql, params, opts))
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
            state.columns = Some(state.columns_of_current(client));
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
    if reject_write_after_abort(ex, &sql) {
        return SQL_ERROR;
    }
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
        let result = ex.client.block_on(|c| c.begin_execute_prepared_batch(&mut prepared, params, &mut orphaned, opts));
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
            let _ = ex.client.block_on(|c| c.unprepare(id, ()));
        }
    }
    let _ = ex.client.take_info_messages();
    ex.state.rowcount = progress.total_rows_affected.unwrap_or(-1);
    ret
}

fn name_positionally(types: Vec<SqlType>) -> Vec<RpcParameter> {
    // sp_execute takes the values in the order sp_prepare declared them, so no
    // per-row `@Pn` name is built or sent.
    types.into_iter().map(|t| RpcParameter::new(None, StatusFlags::NONE, t)).collect()
}

/// Error 2628 (verbose truncation) is data-related (22001) when a batch row
/// hits it; msodbcsql reports it as 42000 for a single statement.
const VERBOSE_TRUNCATION_ERROR: u32 = 2628;

fn batch_error_record(error: &mssql_tds::error::SqlErrorInfo) -> DiagRecord {
    let mut record = server_error_record(error);
    if error.number == VERBOSE_TRUNCATION_ERROR {
        record.sqlstate = "22001".to_string();
    }
    record
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
        let errors: Vec<DiagRecord> = failed.errors.iter().map(batch_error_record).collect();
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
            let raised = client.current_parameter_set_errors();
            if !raised.is_empty() {
                // An error raised ahead of the first row (RAISERROR) fails the call.
                state.columns = None;
                state.diag.splice(0..0, raised.iter().map(server_error_record));
                return SQL_ERROR;
            }
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
