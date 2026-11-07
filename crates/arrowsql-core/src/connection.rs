//! The session state shared by a connection and its statements.
//!
//! `ConnShared` holds the TDS client (`Slot`) and the transaction and session
//! flags every statement consults. The Python `Connection` pyclass and the
//! Rust [`crate::Connection`] are both handles over one of these.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::FedAuthProvider;
use crate::consts::*;
use crate::diag::DiagRecord;
use crate::params::udt::DescribedParam;
use crate::runtime::SyncClient;
use crate::statement::Slot;

const PARAM_TYPE_CACHE_SIZE: usize = 512;
pub const READ_COMMITTED_SQL: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";

/// Session settings requested before the connection could apply them. They
/// are sent ahead of the next statement and cleared only once that succeeded.
#[derive(Default, Clone, PartialEq)]
pub struct PendingSession {
    pub isolation: Option<&'static str>,
    pub catalog: Option<String>,
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
    pub autocommit: AtomicBool,
    /// The last statement failed; if it also took the transaction with it the
    /// next statement may have to be rejected (see `exec::rejects_after_abort`).
    /// The open transaction was begun by the driver and ran nothing yet.
    txn_untouched: AtomicBool,
    last_failed: AtomicBool,
    /// XACT_ABORT rolled the caller's transaction back and no statement told them yet.
    txn_aborted: AtomicBool,
    pub connection_timeout: AtomicU32,
    pending_session: Mutex<PendingSession>,
    /// sp_describe_undeclared_parameters results per SQL text.
    param_types: Mutex<HashMap<String, Arc<[DescribedParam]>>>,
    /// Whether the server accepts UTF-8 collations (`crate::utf8::SUPPORT_*`),
    /// learned from the first UTF-8 rewrite describe.
    pub utf8_support: AtomicU8,
}

impl ConnShared {
    pub fn new(client: SyncClient, autocommit: bool, connection_timeout: u32, pending: PendingSession) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(Slot { client: Some(client), owner: None }),
            autocommit: AtomicBool::new(autocommit),
            txn_untouched: AtomicBool::new(false),
            last_failed: AtomicBool::new(false),
            txn_aborted: AtomicBool::new(false),
            connection_timeout: AtomicU32::new(connection_timeout),
            pending_session: Mutex::new(pending),
            param_types: Mutex::new(HashMap::new()),
            utf8_support: AtomicU8::new(0),
        })
    }

    pub fn autocommit(&self) -> bool {
        self.autocommit.load(Ordering::Acquire)
    }

    pub fn mark_failed(&self) {
        self.last_failed.store(true, Ordering::Release);
    }

    pub fn set_txn_untouched(&self, untouched: bool) {
        self.txn_untouched.store(untouched, Ordering::Release);
    }

    /// Whether the open transaction is still the driver's empty one; the
    /// caller is about to use it.
    pub fn take_txn_untouched(&self) -> bool {
        self.txn_untouched.swap(false, Ordering::AcqRel)
    }

    pub fn mark_txn_aborted(&self) {
        self.txn_aborted.store(true, Ordering::Release);
    }

    pub fn txn_aborted(&self) -> bool {
        self.txn_aborted.load(Ordering::Acquire)
    }

    /// Ends the aborted state (commit or rollback).
    pub fn clear_txn_aborted(&self) {
        self.txn_aborted.store(false, Ordering::Release);
    }

    pub fn take_failed(&self) -> bool {
        self.last_failed.swap(false, Ordering::AcqRel)
    }

    pub fn cached_param_types(&self, sql: &str) -> Option<Arc<[DescribedParam]>> {
        self.param_types.lock().get(sql).cloned()
    }

    pub fn cache_param_types(&self, sql: &str, types: Arc<[DescribedParam]>) {
        let mut cache = self.param_types.lock();
        // Drop one entry rather than all of them, so the hot statements stay cached.
        if cache.len() >= PARAM_TYPE_CACHE_SIZE && !cache.contains_key(sql) {
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
            }
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
    pub fn supersede_isolation(&self) {
        self.pending_session.lock().isolation = None;
    }

    pub fn supersede_catalog(&self) {
        self.pending_session.lock().catalog = None;
    }
}

/// `SQLSTATE:<state>:NATIVE:<number>:<message>`, the RuntimeError text the Python
/// layer maps to a DB-API exception class.
pub fn error_text(record: &DiagRecord) -> String {
    format!("SQLSTATE:{}:NATIVE:{}:{}", record.sqlstate, record.native_error, record.message)
}

pub fn isolation_sql(level: i64) -> Option<&'static str> {
    Some(match level {
        SQL_TXN_READ_UNCOMMITTED => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        SQL_TXN_READ_COMMITTED => READ_COMMITTED_SQL,
        SQL_TXN_REPEATABLE_READ => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        SQL_TXN_SERIALIZABLE => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        SQL_TXN_SS_SNAPSHOT => "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        _ => return None,
    })
}

pub fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Bridges a [`FedAuthProvider`] to mssql-tds' federated-auth hook. The
/// provider may block (network I/O, an interpreter lock): it runs on a
/// blocking thread of the shared runtime, not on the connection's reactor,
/// and not on the connection's own runtime, which is dropped when the login
/// times out (dropping it would wait for the blocking task).
#[derive(Clone)]
struct FedAuthBridge(Arc<dyn FedAuthProvider>);

#[async_trait::async_trait]
impl mssql_tds::connection::client_context::EntraIdTokenFactory for FedAuthBridge {
    async fn create_token(
        &self,
        spn: String,
        sts_url: String,
        auth_method: mssql_tds::connection::client_context::TdsAuthenticationMethod,
    ) -> mssql_tds::core::TdsResult<Vec<u8>> {
        let provider = self.0.clone();
        let runtime = crate::runtime::try_runtime().map_err(|e| {
            mssql_tds::error::Error::ConnectionError(format!("failed to start the mssql-rs I/O runtime: {e}"))
        })?;
        let outcome =
            runtime.spawn_blocking(move || provider.token(&spn, &sts_url, &format!("{auth_method:?}"))).await;
        match outcome {
            Ok(Ok(token)) => Ok(token),
            Ok(Err(e)) => {
                Err(mssql_tds::error::Error::ConnectionError(format!("Entra ID token acquisition failed: {e}")))
            }
            Err(e) => Err(mssql_tds::error::Error::ConnectionError(format!("Entra ID token acquisition aborted: {e}"))),
        }
    }
}

/// Log in: build the client context from `conn_str` (plus an access token or
/// a federated-auth provider) and connect. Blocks on the network; the error
/// is the login's diagnostic records.
pub fn open_client(
    conn_str: &str,
    token: Option<String>,
    fedauth: Option<Arc<dyn FedAuthProvider>>,
    login_timeout: Option<u32>,
) -> Result<SyncClient, Vec<DiagRecord>> {
    let federated = fedauth.map(|provider| {
        Box::new(FedAuthBridge(provider)) as Box<dyn mssql_tds::connection::client_context::CloneableEntraIdTokenFactory>
    });
    let (mut context, opts) = crate::connstr::build_context_with(conn_str, token, federated).map_err(|msg| {
        let state = if msg == crate::connstr::MISSING_SERVER { "08001" } else { "HY000" };
        vec![DiagRecord::driver(state, msg)]
    })?;
    if let Some(timeout) = login_timeout.or(opts.login_timeout) {
        context.connect_timeout = timeout;
    }
    let datasource = context.data_source.clone();
    SyncClient::connect(context, &datasource).map_err(|e| crate::diag::login_records_for_error(&e))
}

/// Close the (not pooled) session of `shared`: abandon any batch on the
/// wire, roll back an open transaction and log out. A session inherited
/// across fork is left alone (it is the parent's).
pub fn close_session(shared: &ConnShared) {
    let client = {
        let mut slot = shared.slot.lock();
        if slot.client.as_ref().is_some_and(SyncClient::is_foreign) {
            std::mem::forget(slot.client.take());
            return;
        }
        crate::statement::discard_all(&mut slot);
        slot.client.take()
    };
    if let Some(mut client) = client {
        if client.has_active_transaction() && !client.is_connection_dead() {
            let _ = client.block_on(|c| c.rollback_transaction(None, None));
        }
        client.close();
    }
}


#[cfg(test)]
mod tests {
    /// Every second login to a server hung when the cached rustls config
    /// resumed the previous login's TLS session (VENDOR.md patch 16).
    #[test]
    fn rustls_configs_do_not_resume_tls_sessions() {
        assert!(!mssql_tds::connection::rustls_session_resumption_enabled().unwrap());
    }
}
