//! `Connection`: one TDS session, shared by all statement handles it allocates.
//!
//! Errors raised to Python use the C++ binding's format so the vendored
//! `_raise_connection_error` keeps mapping them:
//! `RuntimeError("SQLSTATE:XXXXX:NATIVE:<n>:<message>")`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::runtime::SyncClient;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyTuple};

use crate::consts::*;
use crate::diag::{self, DiagRecord};
use crate::pooling::{self, Acquired, IdleClient, Lease};
use crate::params::udt::DescribedParam;
use crate::statement::{SqlHandle, StmtShared};

pub use crate::statement::Slot;

const PARAM_TYPE_CACHE_SIZE: usize = 512;
/// A pooled session whose token expires within this many seconds is
/// re-validated against the token factory on checkout.
const TOKEN_REFRESH_THRESHOLD_SECS: i64 = 300;
const READ_COMMITTED_SQL: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";

/// Session settings requested before the connection could apply them. They
/// are sent ahead of the next statement and cleared only once that succeeded.
#[derive(Default, Clone, PartialEq)]
pub struct PendingSession {
    isolation: Option<&'static str>,
    catalog: Option<String>,
}

impl PendingSession {
    pub fn sql(&self) -> Vec<String> {
        let mut statements: Vec<String> = self.isolation.map(str::to_string).into_iter().collect();
        statements.extend(self.catalog.as_deref().map(|db| format!("USE {}", quote_ident(db))));
        statements
    }
}

pub struct ConnShared {
    pub slot: Mutex<Slot>,
    autocommit: AtomicBool,
    /// The last statement failed; if it also took the transaction with it the
    /// next statement may have to be rejected (see `exec::rejects_after_abort`).
    last_failed: AtomicBool,
    /// XACT_ABORT rolled the caller's transaction back and no statement told them yet.
    txn_aborted: AtomicBool,
    connection_timeout: AtomicU32,
    pending_session: Mutex<PendingSession>,
    /// sp_describe_undeclared_parameters results per SQL text.
    param_types: Mutex<HashMap<String, Vec<DescribedParam>>>,
}

impl ConnShared {
    fn new(client: SyncClient, attrs: &ConnAttrs) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(Slot { client: Some(client), owner: None }),
            autocommit: AtomicBool::new(attrs.autocommit),
            last_failed: AtomicBool::new(false),
            txn_aborted: AtomicBool::new(false),
            connection_timeout: AtomicU32::new(attrs.connection_timeout),
            pending_session: Mutex::new(attrs.pending.clone()),
            param_types: Mutex::new(HashMap::new()),
        })
    }

    pub fn autocommit(&self) -> bool {
        self.autocommit.load(Ordering::Acquire)
    }

    pub fn mark_failed(&self) {
        self.last_failed.store(true, Ordering::Release);
    }

    pub fn mark_txn_aborted(&self) {
        self.txn_aborted.store(true, Ordering::Release);
    }

    pub fn take_txn_aborted(&self) -> bool {
        self.txn_aborted.swap(false, Ordering::AcqRel)
    }

    pub fn take_failed(&self) -> bool {
        self.last_failed.swap(false, Ordering::AcqRel)
    }

    pub fn cached_param_types(&self, sql: &str) -> Option<Vec<DescribedParam>> {
        self.param_types.lock().get(sql).cloned()
    }

    pub fn cache_param_types(&self, sql: &str, types: Vec<DescribedParam>) {
        let mut cache = self.param_types.lock();
        if cache.len() >= PARAM_TYPE_CACHE_SIZE {
            cache.clear();
        }
        cache.insert(sql.to_string(), types);
    }

    /// Settings still waiting to be sent. The caller clears them with
    /// [`clear_applied_session`](Self::clear_applied_session) once they ran.
    pub fn pending_session(&self) -> PendingSession {
        self.pending_session.lock().clone()
    }

    /// Forget the settings in `applied`, unless a newer value replaced them.
    pub fn clear_applied_session(&self, applied: &PendingSession) {
        let mut pending = self.pending_session.lock();
        if pending.isolation == applied.isolation {
            pending.isolation = None;
        }
        if pending.catalog == applied.catalog {
            pending.catalog = None;
        }
    }

    /// An explicit `set_attr` supersedes a queued value of the same kind.
    fn supersede_isolation(&self) {
        self.pending_session.lock().isolation = None;
    }

    fn supersede_catalog(&self) {
        self.pending_session.lock().catalog = None;
    }
}

fn error_text(record: &DiagRecord) -> String {
    format!("SQLSTATE:{}:NATIVE:{}:{}", record.sqlstate, record.native_error, record.message)
}

fn runtime_error(records: &[DiagRecord]) -> PyErr {
    match records.first() {
        Some(record) => PyRuntimeError::new_err(error_text(record)),
        None => PyRuntimeError::new_err("SQLSTATE:HY000:NATIVE:0:Unknown error"),
    }
}

fn driver_error(sqlstate: &str, message: impl AsRef<str>) -> PyErr {
    runtime_error(&[DiagRecord::driver(sqlstate, message)])
}

fn tds_error(err: &mssql_tds::error::Error, default_state: &str) -> PyErr {
    runtime_error(&diag::records_for_error(err, default_state))
}

fn connection_closed() -> PyErr {
    driver_error("08003", "Connection is closed")
}

fn isolation_sql(level: i64) -> Option<&'static str> {
    Some(match level {
        SQL_TXN_READ_UNCOMMITTED => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        SQL_TXN_READ_COMMITTED => READ_COMMITTED_SQL,
        SQL_TXN_REPEATABLE_READ => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        SQL_TXN_SERIALIZABLE => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        SQL_TXN_SS_SNAPSHOT => "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        _ => return None,
    })
}

fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Connection attributes handed to the constructor (`attrs_before`).
struct ConnAttrs {
    autocommit: bool,
    token: Option<String>,
    login_timeout: Option<u32>,
    connection_timeout: u32,
    pending: PendingSession,
}

impl ConnAttrs {
    fn parse(attrs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut parsed = Self {
            autocommit: false,
            token: None,
            login_timeout: None,
            connection_timeout: 0,
            pending: PendingSession::default(),
        };
        let Some(attrs) = attrs else { return Ok(parsed) };
        for (key, value) in attrs.iter() {
            let Ok(key) = key.extract::<i32>() else { continue };
            match key {
                SQL_ATTR_AUTOCOMMIT => parsed.autocommit = value.extract::<i64>().unwrap_or(1) != 0,
                SQL_COPT_SS_ACCESS_TOKEN => parsed.token = Some(access_token_from(&value)?),
                SQL_ATTR_LOGIN_TIMEOUT => parsed.login_timeout = value.extract::<u32>().ok(),
                SQL_ATTR_CONNECTION_TIMEOUT => {
                    parsed.connection_timeout = value.extract::<u32>().unwrap_or(0)
                }
                SQL_ATTR_TXN_ISOLATION => {
                    parsed.pending.isolation = value.extract::<i64>().ok().and_then(isolation_sql)
                }
                SQL_ATTR_CURRENT_CATALOG => parsed.pending.catalog = value.extract::<String>().ok(),
                _ => {}
            }
        }
        Ok(parsed)
    }
}

/// The token of an `SQL_COPT_SS_ACCESS_TOKEN` value. A value that is not a
/// valid token struct is an error: silently dropping it would log in with
/// whatever other credentials the connection string carries.
fn access_token_from(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let raw: Vec<u8> = if let Ok(bytes) = value.cast::<PyBytes>() {
        bytes.as_bytes().to_vec()
    } else if let Ok(bytes) = value.cast::<PyByteArray>() {
        bytes.to_vec()
    } else {
        return Err(driver_error("HY024", "SQL_COPT_SS_ACCESS_TOKEN must be bytes"));
    };
    crate::connstr::decode_access_token(&raw).map_err(|msg| driver_error("HY024", msg))
}

/// What a `token_factory` call produced.
struct FactoryToken {
    token: Option<String>,
    /// Unix seconds; 0 when the factory reported no expiry.
    expiry: i64,
}

/// Call the factory (GIL held). It returns the connect attributes, or a
/// `(attributes, expires_on)` tuple.
fn call_token_factory(py: Python<'_>, factory: &Py<PyAny>) -> PyResult<FactoryToken> {
    let result = factory.bind(py).call0()?;
    let (attrs, expiry) = match result.cast::<PyTuple>() {
        Ok(parts) if !parts.is_empty() => {
            let expiry = match parts.get_item(1) {
                Ok(v) if !v.is_none() => {
                    v.extract::<i64>().or_else(|_| v.extract::<f64>().map(|f| f as i64))?
                }
                _ => 0,
            };
            (parts.get_item(0)?, expiry)
        }
        _ => (result, 0),
    };
    let attrs = attrs
        .cast::<PyDict>()
        .map_err(|_| driver_error("HY024", "token_factory must return a dict of connection attributes"))?;
    let token = match attrs.get_item(SQL_COPT_SS_ACCESS_TOKEN)? {
        Some(value) => Some(access_token_from(&value)?),
        None => None,
    };
    Ok(FactoryToken { token, expiry })
}

/// The session a new `Connection` runs on.
struct Session {
    client: SyncClient,
    token: Option<String>,
    token_expiry: i64,
    /// Came out of the pool: server-side state is reset by the next request.
    reused: bool,
}

impl Session {
    fn reused(entry: IdleClient) -> Self {
        let IdleClient { mut client, token, token_expiry } = entry;
        client.prepare_reset_connection(false);
        Self { client, token, token_expiry, reused: true }
    }
}

/// Pool membership of an open connection.
struct PoolMembership {
    lease: Lease,
    token: Option<String>,
    token_expiry: i64,
}

#[pyclass(name = "Connection", module = "ddbc_bindings", frozen)]
pub struct Connection {
    shared: Arc<ConnShared>,
    closed: AtomicBool,
    pool: Mutex<Option<PoolMembership>>,
}

impl Connection {
    fn client_op<T>(
        &self,
        py: Python<'_>,
        op: impl FnOnce(&mut SyncClient) -> Result<T, mssql_tds::error::Error> + Send,
    ) -> PyResult<T>
    where
        T: Send,
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(connection_closed());
        }
        let shared = self.shared.clone();
        py.detach(move || {
            let mut slot = shared.slot.lock();
            // Transaction / session operations need the wire: buffer any
            // statement that still has results pending.
            crate::statement::free_for_connection(&mut slot);
            let Some(client) = slot.client.as_mut() else { return Err(connection_closed()) };
            op(client).map_err(|e| tds_error(&e, "HY000"))
        })
    }

    /// Commit or roll back. The next transaction begins with the next
    /// statement, not here: system-versioned tables stamp rows with the
    /// transaction's start time (upstream starts it at commit and gets that wrong).
    fn end_transaction(&self, py: Python<'_>, commit: bool) -> PyResult<()> {
        self.shared.take_failed();
        self.shared.take_txn_aborted();
        self.client_op(py, move |client| {
            if !client.has_active_transaction() {
                return Ok(());
            }
            client.block_on(|c| async move {
                if commit { c.commit_transaction(None, None).await } else { c.rollback_transaction(None, None).await }
            })
        })
    }

    fn run_session_sql(&self, py: Python<'_>, sql: String) -> PyResult<()> {
        self.client_op(py, move |client| {
            client.block_on(|c| async move {
                c.execute(sql, ()).await?;
                c.close_query().await
            })
        })
    }

    /// Take the client out of the slot, ready to be retired. `None` when the
    /// connection was already closed.
    fn take_client(&self, transaction_already_rolled_back: bool, pooled: bool) -> PyResult<Option<SyncClient>> {
        let mut slot = self.shared.slot.lock();
        let Some(client) = slot.client.as_ref() else {
            self.closed.store(true, Ordering::Release);
            return Ok(None);
        };
        // Like SQLDisconnect: a plain disconnect refuses to drop an open
        // transaction. The connection stays usable so the caller can end it.
        if !pooled && !transaction_already_rolled_back && client.has_active_transaction() {
            return Err(driver_error("25000", "Invalid transaction state"));
        }
        crate::statement::discard_all(&mut slot);
        self.closed.store(true, Ordering::Release);
        Ok(slot.client.take())
    }

    /// Close a non-pooled session, or sanitise a pooled one and return it.
    fn retire(&self, mut client: SyncClient, transaction_already_rolled_back: bool) -> PyResult<()> {
        let Some(membership) = self.pool.lock().take() else {
            let _ = client.block_on(|c| c.close_connection());
            return Ok(());
        };
        if let Err(err) = sanitise(&mut client, transaction_already_rolled_back) {
            // Never keep a session whose state could not be cleaned up.
            let _ = client.block_on(|c| c.close_connection());
            return Err(err);
        }
        let entry = IdleClient { client, token: membership.token, token_expiry: membership.token_expiry };
        if let Some(rejected) = pooling::checkin(membership.lease, entry) {
            pooling::close_all(vec![rejected]);
        }
        Ok(())
    }
}

/// End the caller's work on a session that is about to be pooled.
fn sanitise(client: &mut SyncClient, transaction_already_rolled_back: bool) -> PyResult<()> {
    if client.is_connection_dead() {
        return Err(driver_error("08S01", "Communication link failure"));
    }
    if !transaction_already_rolled_back && client.has_active_transaction() {
        client.block_on(|c| c.rollback_transaction(None, None)).map_err(|e| tds_error(&e, "HY000"))?;
    }
    Ok(())
}

impl Drop for Connection {
    /// An unclosed connection takes its session down with it, which ends any
    /// open transaction on the server; the pool lease frees its slot.
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        if let Some(mut slot) = self.shared.slot.try_lock() {
            slot.client.take();
        }
    }
}

/// Open the session for a new `Connection`: reuse a pooled one when it is
/// still good, otherwise connect (invoking the token factory first).
fn open_session(
    py: Python<'_>,
    conn_str: &str,
    attrs: &ConnAttrs,
    factory: Option<&Py<PyAny>>,
    lease: Option<&Lease>,
    pooled: Option<IdleClient>,
) -> PyResult<Session> {
    let mut refreshed = None;
    if let Some(entry) = pooled {
        match factory {
            Some(factory) if entry.token_near_expiry(TOKEN_REFRESH_THRESHOLD_SECS) => {
                let fresh = call_token_factory(py, factory)?;
                if fresh.token.is_some() && fresh.token == entry.token && expiry_is_safe(fresh.expiry) {
                    // The provider still hands out the same token: the session is healthy.
                    return Ok(Session::reused(IdleClient { token_expiry: fresh.expiry, ..entry }));
                }
                if let (Some(stale), Some(lease)) = (entry.token.as_deref(), lease) {
                    // Siblings logged in with the same stale token.
                    py.detach(|| pooling::evict_with_token(lease, stale));
                }
                py.detach(|| pooling::close_all(vec![entry.client]));
                refreshed = Some(fresh);
            }
            _ => return Ok(Session::reused(entry)),
        }
    }
    let FactoryToken { token, expiry } = match (refreshed, factory) {
        (Some(fresh), _) => fresh,
        (None, Some(factory)) => call_token_factory(py, factory)?,
        (None, None) => FactoryToken { token: attrs.token.clone(), expiry: 0 },
    };
    connect(py, conn_str, attrs, token, expiry)
}

fn expiry_is_safe(expiry: i64) -> bool {
    expiry > 0 && pooling::unix_now() + TOKEN_REFRESH_THRESHOLD_SECS < expiry
}

fn connect(py: Python<'_>, conn_str: &str, attrs: &ConnAttrs, token: Option<String>, expiry: i64) -> PyResult<Session> {
    let (mut context, opts) = crate::connstr::build_context(conn_str, token.clone())
        .map_err(|msg| driver_error("HY000", msg))?;
    if let Some(timeout) = attrs.login_timeout.or(opts.login_timeout) {
        context.connect_timeout = timeout;
    }
    let datasource = context.data_source.clone();
    let client = py
        .detach(|| SyncClient::connect(context, &datasource))
        .map_err(|e| tds_error(&e, "08001"))?;
    Ok(Session { client, token, token_expiry: expiry, reused: false })
}

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (conn_str, use_pool, attrs_before=None, pool_key=String::new(), token_factory=None))]
    fn new(
        py: Python<'_>,
        conn_str: String,
        use_pool: bool,
        attrs_before: Option<&Bound<'_, PyDict>>,
        pool_key: String,
        token_factory: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut attrs = ConnAttrs::parse(attrs_before)?;
        let factory = token_factory.filter(|f| !f.bind(py).is_none());
        let pool_id = use_pool.then(|| pooling::key(&conn_str, &pool_key));
        let acquired = match &pool_id {
            Some(id) => py
                .detach(|| pooling::acquire(id))
                .map_err(|_| driver_error("HY000", "ConnectionPool::acquire: pool size limit reached"))?,
            None => Acquired::Declined,
        };
        let (lease, pooled) = match acquired {
            Acquired::Declined => (None, None),
            Acquired::Idle(lease, entry) => (Some(lease), Some(*entry)),
            Acquired::Fresh(lease) => (Some(lease), None),
        };
        // On any early return the lease drops and frees the pool slot.
        let session = open_session(py, &conn_str, &attrs, factory.as_ref(), lease.as_ref(), pooled)?;
        if session.reused && attrs.pending.isolation.is_none() {
            // RESETCONNECTION leaves the isolation level of the previous user.
            attrs.pending.isolation = Some(READ_COMMITTED_SQL);
        }
        let pool = lease.map(|lease| PoolMembership {
            lease,
            token: session.token.clone(),
            token_expiry: session.token_expiry,
        });
        Ok(Self {
            shared: ConnShared::new(session.client, &attrs),
            closed: AtomicBool::new(false),
            pool: Mutex::new(pool),
        })
    }

    #[pyo3(signature = (transaction_already_rolled_back=false))]
    fn close(&self, py: Python<'_>, transaction_already_rolled_back: bool) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let pooled = self.pool.lock().is_some();
        py.detach(|| {
            match self.take_client(transaction_already_rolled_back, pooled)? {
                Some(client) => self.retire(client, transaction_already_rolled_back),
                None => Ok(()),
            }
        })
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        self.end_transaction(py, true)
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        self.end_transaction(py, false)
    }

    fn set_autocommit(&self, py: Python<'_>, enable: bool) -> PyResult<()> {
        if enable && !self.shared.autocommit() {
            // ODBC: switching to autocommit commits the open transaction.
            self.end_transaction(py, true)?;
        }
        self.shared.autocommit.store(enable, Ordering::Release);
        Ok(())
    }

    fn get_autocommit(&self) -> bool {
        self.shared.autocommit()
    }

    fn _get_connection_timeout(&self) -> u32 {
        self.shared.connection_timeout.load(Ordering::Relaxed)
    }

    #[pyo3(signature = (attribute, value))]
    fn set_attr(&self, py: Python<'_>, attribute: i32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let fail = |msg: &str| {
            PyRuntimeError::new_err(format!("Failed to set connection attribute {attribute}: {msg}"))
        };
        match attribute {
            SQL_ATTR_AUTOCOMMIT => {
                let enable = value.extract::<i64>().map_err(|_| fail("Invalid attribute value"))? != 0;
                self.set_autocommit(py, enable)
            }
            SQL_ATTR_TXN_ISOLATION => {
                let level = value.extract::<i64>().map_err(|_| fail("Invalid attribute value"))?;
                // Upstream's message carries no "invalid", which would make it an InterfaceError.
                let sql = isolation_sql(level)
                    .ok_or_else(|| PyRuntimeError::new_err(format!("Failed to set connection attribute {attribute}")))?;
                self.run_session_sql(py, sql.to_string())?;
                self.shared.supersede_isolation();
                Ok(())
            }
            SQL_ATTR_CURRENT_CATALOG => {
                let db = value.extract::<String>().map_err(|_| fail("Invalid attribute value"))?;
                self.run_session_sql(py, format!("USE {}", quote_ident(&db)))?;
                self.shared.supersede_catalog();
                Ok(())
            }
            SQL_ATTR_CONNECTION_TIMEOUT => {
                let timeout = value.extract::<u32>().map_err(|_| fail("Invalid attribute value"))?;
                self.shared.connection_timeout.store(timeout, Ordering::Relaxed);
                Ok(())
            }
            SQL_ATTR_LOGIN_TIMEOUT | SQL_ATTR_PACKET_SIZE => {
                Err(fail("[Microsoft][ODBC Driver 18 for SQL Server]Attribute cannot be set now"))
            }
            a if ACCEPTED_NOOP_ATTRS.contains(&a) => Ok(()),
            _ => Err(fail("[Microsoft][ODBC Driver Manager]Invalid attribute/option identifier")),
        }
    }

    fn alloc_statement_handle(&self) -> PyResult<SqlHandle> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("Connection object is not initialized"));
        }
        Ok(SqlHandle { inner: StmtShared::new(self.shared.clone()) })
    }

    #[pyo3(signature = (info_type))]
    fn get_info(&self, py: Python<'_>, info_type: u16) -> PyResult<Py<PyAny>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(connection_closed());
        }
        crate::getinfo::get_info(py, &self.shared, info_type)
    }
}

/// Attribute ids accepted for compatibility; they have no TDS-level effect.
const ACCEPTED_NOOP_ATTRS: &[i32] = &[101, 104, 105, 106, 107, 110, 111, 4, 117, 119, 1207, 1208];
