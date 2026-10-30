//! `AioConnection`: one TDS session driven by tokio tasks.
//!
//! Every method converts its Python arguments on the calling (loop) thread,
//! spawns one task and returns `(future, request_id)`. The task locks the
//! session (a tokio mutex: operations on one connection queue up without
//! blocking a thread), runs, and completes the future through the hub.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::datatypes::sqltypes::SqlType;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PySequence;
use tokio::sync::OwnedSemaphorePermit;

use crate::diag::DiagRecord;
use crate::params::py_types::py_to_sql_type;

use super::convert::row_batch_of;
use super::hub::{Hub, Outcome, QueryMode, to_exception};
use super::pool::PoolInner;
use super::session::{Command, Diag, OpCancel, Session};

/// Rows `execute` reads ahead by default.
pub const DEFAULT_PREFETCH: usize = 512;

pub struct ConnCore {
    session: tokio::sync::Mutex<Option<Session>>,
    cancels: Mutex<HashMap<u64, Arc<OpCancel>>>,
    next_cursor: AtomicU64,
    autocommit: AtomicBool,
    timeout: AtomicU32,
    as_tuple: bool,
    /// Set for connections checked out of a pool: returned there on close.
    pool: Option<(Arc<PoolInner>, Mutex<Option<OwnedSemaphorePermit>>)>,
}

impl ConnCore {
    pub fn new(session: Session, as_tuple: bool, pool: Option<(Arc<PoolInner>, OwnedSemaphorePermit)>) -> Arc<Self> {
        let autocommit = session.autocommit;
        let timeout = session.timeout;
        Arc::new(Self {
            session: tokio::sync::Mutex::new(Some(session)),
            cancels: Mutex::new(HashMap::new()),
            next_cursor: AtomicU64::new(1),
            autocommit: AtomicBool::new(autocommit),
            timeout: AtomicU32::new(timeout),
            as_tuple,
            pool: pool.map(|(inner, permit)| (inner, Mutex::new(Some(permit)))),
        })
    }

    /// Run `op` on the open session; error if the connection is closed.
    pub async fn with_session<T>(
        &self,
        op: impl AsyncFnOnce(&mut Session) -> Result<T, Diag>,
    ) -> Result<T, Diag> {
        let mut guard = self.session.lock().await;
        let Some(session) = guard.as_mut() else {
            return Err(vec![DiagRecord::driver("08003", "Connection is closed")]);
        };
        session.timeout = self.timeout.load(Ordering::Relaxed);
        session.autocommit = self.autocommit.load(Ordering::Acquire);
        op(session).await
    }

    async fn close(&self) {
        let Some(mut session) = self.session.lock().await.take() else { return };
        match &self.pool {
            Some((pool, permit)) => {
                pool.checkin(session).await;
                drop(permit.lock().take());
            }
            None => session.close().await,
        }
    }
}

/// Register a future on `hub`, run `make(core, cancel)` on the runtime and
/// complete the future with its outcome.
pub fn spawn_op<'py, Fut>(
    py: Python<'py>,
    hub: &Bound<'py, Hub>,
    as_tuple: bool,
    cancels: Option<&Arc<ConnCore>>,
    task: impl FnOnce(Arc<OpCancel>) -> Fut,
) -> PyResult<(Bound<'py, PyAny>, u64)>
where
    Fut: Future<Output = Outcome> + Send + 'static,
{
    let hub = hub.get();
    let (id, future) = hub.register(py, as_tuple)?;
    let cancel = Arc::new(OpCancel::new());
    if let Some(core) = cancels {
        core.cancels.lock().insert(id, cancel.clone());
    }
    let inner = hub.inner.clone();
    let tracker = cancels.cloned();
    let fut = task(cancel);
    crate::runtime::runtime().spawn(async move {
        let outcome = fut.await;
        if let Some(core) = tracker {
            core.cancels.lock().remove(&id);
        }
        inner.complete(id, outcome);
    });
    Ok((future, id))
}

fn outcome<T>(result: Result<T, Diag>, ok: impl FnOnce(T) -> Outcome) -> Outcome {
    match result {
        Ok(value) => ok(value),
        Err(records) => Outcome::Error(records),
    }
}

/// SQL + optional parameters -> what the session executes.
pub fn build_command(py: Python<'_>, sql: String, params: Option<&Bound<'_, PyAny>>) -> PyResult<Command> {
    let sql = crate::params::placeholders::translate_call_escape(&sql).unwrap_or(sql);
    let Some(params) = params.filter(|p| !p.is_none()) else {
        return Ok(Command::Batch(sql));
    };
    if params.cast::<PySequence>()?.len()? == 0 {
        return Ok(Command::Batch(sql));
    }
    let set = crate::params::build_rpc_params(params, None)?;
    let (rewritten, markers) = crate::params::placeholders::rewrite_qmarks(&sql);
    if markers != set.types.len() {
        return Err(to_exception(
            py,
            &[DiagRecord::driver(
                "07002",
                format!(
                    "COUNT field incorrect or syntax error: the SQL contains {markers} parameter markers, but {} parameters were supplied",
                    set.types.len()
                ),
            )],
        ));
    }
    Ok(Command::Rpc(rewritten, set))
}

pub fn query_mode(mode: u8) -> QueryMode {
    match mode {
        1 => QueryMode::One,
        2 => QueryMode::Value,
        _ => QueryMode::All,
    }
}

#[pyclass(name = "AioConnection", module = "ddbc_bindings", frozen)]
pub struct AioConnection {
    core: Arc<ConnCore>,
}

impl AioConnection {
    pub fn from_core(core: Arc<ConnCore>) -> Self {
        Self { core }
    }
}

type Op<'py> = PyResult<(Bound<'py, PyAny>, u64)>;

#[pymethods]
impl AioConnection {
    /// A new cursor id on this connection.
    fn cursor(&self) -> u64 {
        self.core.next_cursor.fetch_add(1, Ordering::Relaxed)
    }

    #[pyo3(signature = (hub, cursor, sql, params=None, prefetch=DEFAULT_PREFETCH))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        hub: &Bound<'py, Hub>,
        cursor: u64,
        sql: String,
        params: Option<&Bound<'py, PyAny>>,
        prefetch: usize,
    ) -> Op<'py> {
        let command = build_command(py, sql, params)?;
        let core = self.core.clone();
        spawn_op(py, hub, core.as_tuple, Some(&self.core), move |cancel| async move {
            let result = core
                .with_session(async |s| {
                    s.execute(cursor, command, prefetch, super::session::PREFETCH_BYTES, &cancel).await
                })
                .await;
            outcome(result, Outcome::Exec)
        })
    }

    fn executemany<'py>(
        &self,
        py: Python<'py>,
        hub: &Bound<'py, Hub>,
        cursor: u64,
        sql: String,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> Op<'py> {
        let (sql, _) = crate::params::placeholders::rewrite_qmarks(&sql);
        let mut rows: Vec<Vec<SqlType>> = Vec::new();
        let mut nulls: Vec<Vec<bool>> = Vec::new();
        for row in seq_of_params.try_iter()? {
            let row = row?;
            let row = row.cast::<PySequence>()?;
            let n = row.len()?;
            let mut types = Vec::with_capacity(n);
            let mut null_row = Vec::with_capacity(n);
            for i in 0..n {
                let value = row.get_item(i)?;
                null_row.push(value.is_none());
                types.push(py_to_sql_type(&value).map_err(|e| crate::params::conversion_error(&e))?);
            }
            rows.push(types);
            nulls.push(null_row);
        }
        let all_null = crate::params::batch::normalize(&mut rows, &nulls);
        let core = self.core.clone();
        spawn_op(py, hub, false, Some(&self.core), move |cancel| async move {
            let result = core
                .with_session(async |s| s.execute_many(cursor, sql, rows, all_null, &cancel).await)
                .await;
            outcome(result, Outcome::Int)
        })
    }

    fn fetch<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>, cursor: u64, n: usize) -> Op<'py> {
        let core = self.core.clone();
        spawn_op(py, hub, core.as_tuple, Some(&self.core), move |cancel| async move {
            let result = core.with_session(async |s| s.fetch(cursor, n, &cancel).await).await;
            outcome(result, Outcome::Rows)
        })
    }

    #[pyo3(signature = (hub, cursor, prefetch=DEFAULT_PREFETCH))]
    fn nextset<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>, cursor: u64, prefetch: usize) -> Op<'py> {
        let core = self.core.clone();
        spawn_op(py, hub, core.as_tuple, Some(&self.core), move |cancel| async move {
            let result = core.with_session(async |s| s.nextset(cursor, prefetch, &cancel).await).await;
            outcome(result, |next| next.map_or(Outcome::NoMoreResults, Outcome::Exec))
        })
    }

    #[pyo3(signature = (hub, cursor, head=None, batch_rows=crate::arrow::DEFAULT_BATCH_ROWS))]
    fn fetch_arrow<'py>(
        &self,
        py: Python<'py>,
        hub: &Bound<'py, Hub>,
        cursor: u64,
        head: Option<&Bound<'py, PyAny>>,
        batch_rows: usize,
    ) -> Op<'py> {
        let head = match head {
            Some(obj) => row_batch_of(obj)?.and_then(|b| b.get().take_rest()),
            None => None,
        };
        let core = self.core.clone();
        spawn_op(py, hub, false, Some(&self.core), move |cancel| async move {
            let result = core
                .with_session(async |s| s.fetch_arrow(cursor, head, batch_rows, &cancel).await)
                .await;
            outcome(result, |(schema, batches)| Outcome::Arrow(schema, batches))
        })
    }

    /// Execute and return every row of the first result (mode 0), the first
    /// row (1) or the first value (2) in one round trip.
    #[pyo3(signature = (hub, sql, params=None, mode=0))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        hub: &Bound<'py, Hub>,
        sql: String,
        params: Option<&Bound<'py, PyAny>>,
        mode: u8,
    ) -> Op<'py> {
        let command = build_command(py, sql, params)?;
        let mode = query_mode(mode);
        let max_rows = if matches!(mode, QueryMode::All) { usize::MAX } else { 1 };
        let core = self.core.clone();
        spawn_op(py, hub, core.as_tuple, Some(&self.core), move |cancel| async move {
            let result = core.with_session(async |s| s.query(command, max_rows, &cancel).await).await;
            outcome(result, |(out, ncols)| Outcome::Query { out, ncols, mode })
        })
    }

    fn commit<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>) -> Op<'py> {
        let core = self.core.clone();
        spawn_op(py, hub, false, Some(&self.core), move |_| async move {
            outcome(core.with_session(async |s| s.commit().await).await, |_| Outcome::None)
        })
    }

    fn rollback<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>) -> Op<'py> {
        let core = self.core.clone();
        spawn_op(py, hub, false, Some(&self.core), move |_| async move {
            outcome(core.with_session(async |s| s.rollback().await).await, |_| Outcome::None)
        })
    }

    /// Switching to autocommit commits the open transaction (ODBC semantics).
    fn set_autocommit<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>, enable: bool) -> Op<'py> {
        let core = self.core.clone();
        spawn_op(py, hub, false, Some(&self.core), move |_| async move {
            let result = core
                .with_session(async |s| {
                    if enable && !s.autocommit {
                        s.commit().await?;
                    }
                    Ok(())
                })
                .await;
            if result.is_ok() {
                core.autocommit.store(enable, Ordering::Release);
            }
            outcome(result, |_| Outcome::None)
        })
    }

    fn close<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>) -> Op<'py> {
        let core = self.core.clone();
        spawn_op(py, hub, false, None, move |_| async move {
            core.close().await;
            Outcome::None
        })
    }

    /// Forget a cursor's results (fire and forget).
    fn close_cursor(&self, cursor: u64) {
        let core = self.core.clone();
        crate::runtime::runtime().spawn(async move {
            if let Some(session) = core.session.lock().await.as_mut() {
                session.close_cursor(cursor);
            }
        });
    }

    /// Cancel request `id` (a no-op once it finished): sends a TDS attention
    /// if it is running, or makes it fail fast if it is still queued.
    fn cancel(&self, id: u64) {
        if let Some(cancel) = self.core.cancels.lock().get(&id) {
            cancel.cancel();
        }
    }

    #[getter]
    fn autocommit(&self) -> bool {
        self.core.autocommit.load(Ordering::Acquire)
    }

    #[getter]
    fn get_timeout(&self) -> u32 {
        self.core.timeout.load(Ordering::Relaxed)
    }

    #[setter]
    fn set_timeout(&self, value: u32) {
        self.core.timeout.store(value, Ordering::Relaxed);
    }
}

/// Open a session from an ODBC-style connection string.
pub async fn open_session(conn_str: &str, autocommit: bool, timeout: u32) -> Result<Session, Diag> {
    let (mut context, opts) = crate::connstr::build_context(conn_str, None)
        .map_err(|msg| vec![DiagRecord::driver("HY000", msg)])?;
    if let Some(t) = opts.login_timeout {
        context.connect_timeout = t;
    }
    let datasource = context.data_source.clone();
    let client = TdsConnectionProvider {}
        .create_client(context, &datasource, None)
        .await
        .map_err(|e| crate::diag::records_for_error(&e, "08001"))?;
    Ok(Session::new(client, autocommit, timeout))
}

#[pyfunction]
#[pyo3(name = "aio_connect", signature = (hub, conn_str, autocommit=false, timeout=0, tuple_rows=false))]
pub fn connect<'py>(
    py: Python<'py>,
    hub: &Bound<'py, Hub>,
    conn_str: String,
    autocommit: bool,
    timeout: u32,
    tuple_rows: bool,
) -> PyResult<Bound<'py, PyAny>> {
    // Validate the connection string now so syntax errors raise at the call.
    crate::connstr::build_context(&conn_str, None)
        .map_err(|msg| to_exception(py, &[DiagRecord::driver("HY000", msg)]))?;
    let (future, _) = spawn_op(py, hub, false, None, move |_| async move {
        match open_session(&conn_str, autocommit, timeout).await {
            Ok(session) => Outcome::Connection(ConnCore::new(session, tuple_rows, None)),
            Err(records) => Outcome::Error(records),
        }
    })?;
    Ok(future)
}

