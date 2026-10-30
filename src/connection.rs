//! `Connection`: one TDS session, shared by all statement handles it allocates.
//!
//! Errors raised to Python use the C++ binding's format so the vendored
//! `_raise_connection_error` keeps mapping them:
//! `RuntimeError("SQLSTATE:XXXXX:NATIVE:<n>:<message>")`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::consts::*;
use crate::diag::{self, DiagRecord};
use crate::params::udt::DescribedParam;
use crate::runtime::block_on;
use crate::statement::{SqlHandle, StmtShared, release_wire};

/// The TDS client plus which statement currently owns its live batch.
pub struct Slot {
    pub client: Option<TdsClient>,
    pub owner: Option<(u64, Weak<StmtShared>)>,
}

const PARAM_TYPE_CACHE_SIZE: usize = 512;

pub struct ConnShared {
    pub slot: Mutex<Slot>,
    autocommit: AtomicBool,
    /// Server-side IMPLICIT_TRANSACTIONS state last sent on this session.
    implicit_txn: AtomicBool,
    connection_timeout: AtomicU32,
    /// Session-level SQL (isolation level, USE db) applied before the next
    /// statement when it could not be sent immediately.
    pending_session_sql: Mutex<Vec<String>>,
    /// sp_describe_undeclared_parameters results per SQL text.
    param_types: Mutex<HashMap<String, Vec<DescribedParam>>>,
}

impl ConnShared {
    pub fn autocommit(&self) -> bool {
        self.autocommit.load(Ordering::Acquire)
    }

    pub fn implicit_transactions(&self) -> bool {
        self.implicit_txn.load(Ordering::Acquire)
    }

    pub fn set_implicit_transactions(&self, on: bool) {
        self.implicit_txn.store(on, Ordering::Release);
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

    pub fn take_pending_session_sql(&self) -> Option<String> {
        let mut pending = self.pending_session_sql.lock();
        if pending.is_empty() { None } else { Some(std::mem::take(&mut *pending).join("; ")) }
    }
}

fn error_text(record: &DiagRecord) -> String {
    format!(
        "SQLSTATE:{}:NATIVE:{}:{}",
        record.sqlstate, record.native_error, record.message
    )
}

fn runtime_error(records: &[DiagRecord]) -> PyErr {
    match records.first() {
        Some(record) => PyRuntimeError::new_err(error_text(record)),
        None => PyRuntimeError::new_err("SQLSTATE:HY000:NATIVE:0:Unknown error"),
    }
}

fn tds_error(err: &mssql_tds::error::Error, default_state: &str) -> PyErr {
    runtime_error(&diag::records_for_error(err, default_state))
}

fn isolation_sql(level: i64) -> Option<&'static str> {
    Some(match level {
        SQL_TXN_READ_UNCOMMITTED => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        SQL_TXN_READ_COMMITTED => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        SQL_TXN_REPEATABLE_READ => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        SQL_TXN_SERIALIZABLE => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        SQL_TXN_SS_SNAPSHOT => "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        _ => return None,
    })
}

fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

#[pyclass(name = "Connection", module = "ddbc_bindings")]
pub struct Connection {
    shared: Arc<ConnShared>,
    closed: bool,
    /// Set when this connection returns its session to the pool on close.
    pool_key: Option<String>,
}

impl Connection {
    fn client_op<T>(
        &self,
        py: Python<'_>,
        op: impl FnOnce(&mut TdsClient) -> Result<T, mssql_tds::error::Error> + Send,
    ) -> PyResult<T>
    where
        T: Send,
    {
        if self.closed {
            return Err(runtime_error(&[DiagRecord::driver("08003", "Connection is closed")]));
        }
        let shared = self.shared.clone();
        py.detach(move || {
            let mut slot = shared.slot.lock();
            // Transaction / session operations need the wire: buffer any
            // statement that still has results pending.
            release_wire(&mut slot, 0);
            let Some(client) = slot.client.as_mut() else {
                return Err(runtime_error(&[DiagRecord::driver("08003", "Connection is closed")]));
            };
            op(client).map_err(|e| tds_error(&e, "HY000"))
        })
    }

    fn run_session_sql(&self, py: Python<'_>, sql: String) -> PyResult<()> {
        self.client_op(py, move |client| {
            block_on(async {
                client.execute(sql, ()).await?;
                client.close_query().await
            })
        })
    }
}

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (conn_str, use_pool, attrs_before=None, pool_key=String::new(), token_factory=None))]
    #[allow(unused_variables)]
    fn new(
        py: Python<'_>,
        conn_str: String,
        use_pool: bool,
        attrs_before: Option<&Bound<'_, PyDict>>,
        pool_key: String,
        token_factory: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut autocommit = false;
        let mut access_token = None;
        let mut login_timeout = None;
        let mut connection_timeout = 0u32;
        let mut session_sql = Vec::new();
        if let Some(attrs) = attrs_before {
            for (key, value) in attrs.iter() {
                let Ok(key) = key.extract::<i32>() else { continue };
                match key {
                    SQL_ATTR_AUTOCOMMIT => autocommit = value.extract::<i64>().unwrap_or(1) != 0,
                    SQL_COPT_SS_ACCESS_TOKEN => {
                        if let Ok(bytes) = value.cast::<PyBytes>() {
                            access_token = crate::connstr::decode_access_token(bytes.as_bytes());
                        }
                    }
                    SQL_ATTR_LOGIN_TIMEOUT => login_timeout = value.extract::<u32>().ok(),
                    SQL_ATTR_CONNECTION_TIMEOUT => {
                        connection_timeout = value.extract::<u32>().unwrap_or(0)
                    }
                    SQL_ATTR_TXN_ISOLATION => {
                        if let Some(sql) = value.extract::<i64>().ok().and_then(isolation_sql) {
                            session_sql.push(sql.to_string());
                        }
                    }
                    SQL_ATTR_CURRENT_CATALOG => {
                        if let Ok(db) = value.extract::<String>() {
                            session_sql.push(format!("USE {}", quote_ident(&db)));
                        }
                    }
                    _ => {}
                }
            }
        }
        // TODO(connection): Entra ID via `token_factory` (ActiveDirectory* auth).
        let (mut context, opts) = crate::connstr::build_context(&conn_str, access_token)
            .map_err(|msg| runtime_error(&[DiagRecord::driver("HY000", msg)]))?;
        if let Some(timeout) = login_timeout.or(opts.login_timeout) {
            context.connect_timeout = timeout;
        }
        let datasource = context.data_source.clone();
        let pool_key = use_pool.then(|| crate::pooling::key(&conn_str, &pool_key));
        let pooled = pool_key.as_deref().and_then(crate::pooling::checkout);
        let client = match pooled {
            Some(client) => client,
            None => py
                .detach(|| {
                    block_on(TdsConnectionProvider {}.create_client(context, &datasource, None))
                })
                .map_err(|e| tds_error(&e, "08001"))?,
        };
        let shared = Arc::new(ConnShared {
            slot: Mutex::new(Slot { client: Some(client), owner: None }),
            autocommit: AtomicBool::new(autocommit),
            implicit_txn: AtomicBool::new(false),
            connection_timeout: AtomicU32::new(connection_timeout),
            pending_session_sql: Mutex::new(session_sql),
            param_types: Mutex::new(HashMap::new()),
        });
        Ok(Self { shared, closed: false, pool_key })
    }

    #[pyo3(signature = (transaction_already_rolled_back=false))]
    fn close(&mut self, py: Python<'_>, transaction_already_rolled_back: bool) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let shared = self.shared.clone();
        let pool_key = self.pool_key.take();
        py.detach(move || {
            let mut slot = shared.slot.lock();
            release_wire(&mut slot, 0);
            let Some(mut client) = slot.client.take() else { return };
            if !transaction_already_rolled_back && client.has_active_transaction() {
                let _ = block_on(client.rollback_transaction(None, None));
            }
            let client = match pool_key {
                Some(key) => crate::pooling::checkin(key, client),
                None => Some(client),
            };
            if let Some(mut client) = client {
                let _ = block_on(client.close_connection());
            }
        });
        Ok(())
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        self.client_op(py, |client| {
            if client.has_active_transaction() {
                block_on(client.commit_transaction(None, None))
            } else {
                Ok(())
            }
        })
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        self.client_op(py, |client| {
            if client.has_active_transaction() {
                block_on(client.rollback_transaction(None, None))
            } else {
                Ok(())
            }
        })
    }

    fn set_autocommit(&self, py: Python<'_>, enable: bool) -> PyResult<()> {
        if enable && !self.shared.autocommit() {
            // ODBC: switching to autocommit commits the open transaction.
            self.commit(py)?;
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
                let sql = isolation_sql(level).ok_or_else(|| fail("Invalid attribute value"))?;
                self.run_session_sql(py, sql.to_string())
            }
            SQL_ATTR_CURRENT_CATALOG => {
                let db = value.extract::<String>().map_err(|_| fail("Invalid attribute value"))?;
                self.run_session_sql(py, format!("USE {}", quote_ident(&db)))
            }
            SQL_ATTR_CONNECTION_TIMEOUT => {
                let timeout = value.extract::<u32>().map_err(|_| fail("Invalid attribute value"))?;
                self.shared.connection_timeout.store(timeout, Ordering::Relaxed);
                Ok(())
            }
            SQL_ATTR_LOGIN_TIMEOUT | SQL_ATTR_PACKET_SIZE => {
                Err(fail("[Microsoft][ODBC Driver 18 for SQL Server]Attribute cannot be set now"))
            }
            // Accepted for compatibility; no TDS-level effect.
            101 | 104 | 105 | 106 | 107 | 110 | 111 | 4 | 117 | 119 | 1207 | 1208 => Ok(()),
            _ => Err(fail("[Microsoft][ODBC Driver Manager]Invalid attribute/option identifier")),
        }
    }

    fn alloc_statement_handle(&self) -> PyResult<SqlHandle> {
        if self.closed {
            return Err(runtime_error(&[DiagRecord::driver("08003", "Connection is closed")]));
        }
        Ok(SqlHandle { inner: StmtShared::new(self.shared.clone()) })
    }

    #[pyo3(signature = (info_type))]
    fn get_info(&self, py: Python<'_>, info_type: u16) -> PyResult<Py<PyAny>> {
        if self.closed {
            return Err(runtime_error(&[DiagRecord::driver("08003", "Connection is closed")]));
        }
        crate::getinfo::get_info(py, &self.shared, info_type)
    }
}
