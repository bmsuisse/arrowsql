//! `AioPool`: a tokio-native pool of async sessions.
//!
//! A semaphore bounds the number of sessions in use; idle sessions wait in a
//! stack (most recently used first, so the warm ones are reused). Sessions
//! are reset with the TDS RESETCONNECTION flag on their next request, so a
//! reused session costs no extra round trip. The `query`/`execute` shortcuts
//! check a session out, run, and check it back in inside one task: one hop
//! between Python and Rust per call.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use pyo3::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::diag::DiagRecord;

use super::connection::{AioOpening, CancelMap, ConnCore, build_command, open_session, query_mode, spawn_op};
use super::hub::{Hub, Outcome, QueryMode, to_exception};
use super::session::{Command, Diag, OpCancel, Session, cancelled};

/// Longest a checkout waits for an idle session to answer its validation ping.
const VALIDATE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct PoolInner {
    conn_str: String,
    autocommit: bool,
    timeout: AtomicU32,
    as_tuple: bool,
    /// Idle sessions with the time they were returned, oldest first.
    idle: Mutex<Vec<(Session, Instant)>>,
    slots: Arc<Semaphore>,
    max_size: usize,
    /// Reset session state (temp tables, SET options, ...) between users.
    reset: bool,
    closed: AtomicBool,
    /// Idle sessions older than this are closed (asyncpg's
    /// `max_inactive_connection_lifetime`).
    max_idle: Option<Duration>,
    /// Sessions older than this are closed on return and never handed out.
    max_lifetime: Option<Duration>,
    /// A session idle for at least this long is pinged before it is reused.
    validate_after: Option<Duration>,
    /// Default wait for a checkout, in milliseconds (0 = unbounded).
    acquire_timeout_ms: AtomicU64,
    /// In-flight pool requests, so `AioPool.cancel` can reach them.
    cancels: Arc<CancelMap>,
}

fn pool_closed() -> Diag {
    vec![DiagRecord::driver("08003", "Pool is closed")]
}

fn acquire_timed_out() -> Diag {
    vec![DiagRecord::driver("HYT00", "Timed out waiting for a connection from the pool")]
}

impl PoolInner {
    /// Idle sessions that waited too long, oldest first (they sit at the front).
    fn take_idle_expired(&self) -> Vec<Session> {
        let Some(max_idle) = self.max_idle else { return Vec::new() };
        let mut idle = self.idle.lock();
        let keep_from = idle.partition_point(|(_, since)| since.elapsed() >= max_idle);
        idle.drain(..keep_from).map(|(session, _)| session).collect()
    }

    fn lifetime_over(&self, session: &Session) -> bool {
        self.max_lifetime.is_some_and(|max| session.age() >= max)
    }

    /// `timeout`: seconds to wait (also for opening a session); `None` uses
    /// the pool's default.
    async fn checkout(
        &self,
        cancel: &OpCancel,
        timeout: Option<Duration>,
    ) -> Result<(Session, OwnedSemaphorePermit), Diag> {
        let timeout = timeout.or_else(|| match self.acquire_timeout_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        });
        match timeout {
            None => self.checkout_unbounded(cancel).await,
            Some(limit) => tokio::time::timeout(limit, self.checkout_unbounded(cancel))
                .await
                .unwrap_or_else(|_| Err(acquire_timed_out())),
        }
    }

    async fn checkout_unbounded(&self, cancel: &OpCancel) -> Result<(Session, OwnedSemaphorePermit), Diag> {
        let permit = tokio::select! {
            permit = self.slots.clone().acquire_owned() => permit.map_err(|_| pool_closed())?,
            () = cancel.cancelled() => return Err(cancelled()),
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(pool_closed());
        }
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        for mut stale in self.take_idle_expired() {
            stale.close().await;
        }
        loop {
            // Under the idle lock, so a concurrent `close` either sees nothing
            // left to hand out or has not taken the idle list yet.
            let popped = {
                let mut idle = self.idle.lock();
                if self.closed.load(Ordering::Acquire) {
                    return Err(pool_closed());
                }
                idle.pop()
            };
            let Some((mut session, since)) = popped else { break };
            if !session.is_open() {
                continue;
            }
            if self.lifetime_over(&session) {
                session.close().await;
                continue;
            }
            // The borrower may have changed these on the native side. Restored
            // before the ping: in manual-commit mode it would open a
            // transaction that checkin rolls back with the next borrower's work.
            session.autocommit = self.autocommit;
            session.timeout = self.timeout.load(Ordering::Relaxed);
            if self.validate_after.is_some_and(|after| since.elapsed() >= after)
                && !session.ping(cancel, VALIDATE_TIMEOUT).await
            {
                // Unanswered: a polite close could block on the same socket.
                drop(session);
                if cancel.is_cancelled() {
                    return Err(cancelled());
                }
                continue;
            }
            return self.hand_over(session, permit).await;
        }
        let timeout = self.timeout.load(Ordering::Relaxed);
        // Dropping the login abandons it and frees the permit with it.
        let session = tokio::select! {
            session = open_session(&self.conn_str, self.autocommit, timeout) => session?,
            () = cancel.cancelled() => return Err(cancelled()),
        };
        self.hand_over(session, permit).await
    }

    /// `close` may have completed while the ping or login above was pending:
    /// a session handed out now would outlive the pool.
    async fn hand_over(
        &self,
        mut session: Session,
        permit: OwnedSemaphorePermit,
    ) -> Result<(Session, OwnedSemaphorePermit), Diag> {
        if self.closed.load(Ordering::Acquire) {
            session.close().await;
            return Err(pool_closed());
        }
        Ok((session, permit))
    }

    pub async fn checkin(&self, session: Session) {
        if self.closed.load(Ordering::Acquire) {
            let mut session = session;
            session.close().await;
            return;
        }
        if self.lifetime_over(&session) {
            let mut session = session;
            session.close().await;
            return;
        }
        if let Some(mut session) = session.into_reusable(self.reset).await {
            // `close` may have run during the rollback above: decide under the
            // idle lock, close a rejected session outside it.
            let rejected = {
                let mut idle = self.idle.lock();
                if self.closed.load(Ordering::Acquire) {
                    Some(session)
                } else {
                    idle.push((session, Instant::now()));
                    None
                }
            };
            if let Some(mut session) = rejected {
                session.close().await;
            }
        }
        for mut stale in self.take_idle_expired() {
            stale.close().await;
        }
    }

    async fn run(&self, command: Command, max_rows: usize, mode: QueryMode, cancel: &OpCancel) -> Outcome {
        let (mut session, permit) = match self.checkout(cancel, None).await {
            Ok(checked_out) => checked_out,
            Err(records) => return Outcome::Error(records),
        };
        session.timeout = self.timeout.load(Ordering::Relaxed);
        let result = session.query(command, max_rows, cancel).await;
        self.checkin(session).await;
        drop(permit);
        match result {
            Ok((out, ncols)) => match mode {
                QueryMode::All | QueryMode::One | QueryMode::Value if out.columns.is_some() => {
                    Outcome::Query { out, ncols, mode }
                }
                _ => Outcome::Int(out.rowcount),
            },
            Err(records) => Outcome::Error(records),
        }
    }
}

#[pyclass(name = "AioPool", module = "ddbc_bindings", frozen)]
pub struct AioPool {
    inner: Arc<PoolInner>,
}

impl AioPool {
    pub fn from_inner(inner: Arc<PoolInner>) -> Self {
        Self { inner }
    }
}

type Op<'py> = PyResult<(Bound<'py, PyAny>, u64)>;

#[pymethods]
impl AioPool {
    /// Check out a connection; closing it returns the session to the pool.
    #[pyo3(signature = (hub, timeout=None))]
    fn acquire<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>, timeout: Option<f64>) -> Op<'py> {
        let timeout = timeout.and_then(|secs| Duration::try_from_secs_f64(secs).ok());
        let inner = self.inner.clone();
        let cancels = inner.cancels.clone();
        spawn_op(py, hub, false, Some(&cancels), move |cancel| async move {
            match inner.checkout(&cancel, timeout).await {
                Ok((session, permit)) => {
                    let as_tuple = inner.as_tuple;
                    Outcome::Connection(ConnCore::new(session, as_tuple, Some((inner, permit))))
                }
                Err(records) => Outcome::Error(records),
            }
        })
    }

    /// Run one statement on a pooled session: all rows (mode 0), first row
    /// (1), first value (2), or the rowcount when it returns no rows.
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
        let inner = self.inner.clone();
        let cancels = inner.cancels.clone();
        spawn_op(py, hub, inner.as_tuple, Some(&cancels), move |cancel| async move {
            inner.run(command, max_rows, mode, &cancel).await
        })
    }

    /// Close idle sessions and refuse new checkouts. Sessions in use are
    /// closed when they are returned.
    fn close<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>) -> Op<'py> {
        let inner = self.inner.clone();
        spawn_op(py, hub, false, None, move |_| async move {
            inner.closed.store(true, Ordering::Release);
            inner.slots.close();
            let idle = std::mem::take(&mut *inner.idle.lock());
            for (mut session, _) in idle {
                session.close().await;
            }
            Outcome::None
        })
    }

    /// Cancel request `id` (a no-op once it finished): a queued acquire gives
    /// up its place, a running query gets a TDS attention.
    fn cancel(&self, id: u64) {
        if let Some(cancel) = self.inner.cancels.lock().get(&id) {
            cancel.cancel();
        }
    }

    #[getter]
    fn max_size(&self) -> usize {
        self.inner.max_size
    }

    /// Sessions currently idle in the pool.
    #[getter]
    fn idle(&self) -> usize {
        self.inner.idle.lock().len()
    }

    /// Sessions currently checked out.
    #[getter]
    fn in_use(&self) -> usize {
        self.inner.max_size - self.inner.slots.available_permits()
    }

    #[getter]
    fn get_timeout(&self) -> u32 {
        self.inner.timeout.load(Ordering::Relaxed)
    }

    #[setter]
    fn set_timeout(&self, value: u32) {
        self.inner.timeout.store(value, Ordering::Relaxed);
    }

    /// Default seconds `acquire` and the shortcuts wait for a connection (0 = unbounded).
    #[getter]
    fn get_acquire_timeout(&self) -> f64 {
        self.inner.acquire_timeout_ms.load(Ordering::Relaxed) as f64 / 1000.0
    }

    #[setter]
    fn set_acquire_timeout(&self, seconds: f64) -> PyResult<()> {
        self.inner.acquire_timeout_ms.store(millis(seconds, "acquire_timeout")?, Ordering::Relaxed);
        Ok(())
    }
}

/// A non-negative number of seconds as milliseconds.
fn millis(seconds: f64, name: &str) -> PyResult<u64> {
    if seconds.is_nan() || seconds < 0.0 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!("{name} must be >= 0 seconds")));
    }
    Ok((seconds * 1000.0).min(u64::MAX as f64) as u64)
}

/// 0 means "off".
fn duration_option(seconds: f64, name: &str) -> PyResult<Option<Duration>> {
    Ok(match millis(seconds, name)? {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    })
}

#[pyfunction]
#[pyo3(name = "aio_create_pool", signature = (hub, conn_str, min_size=1, max_size=10, autocommit=true, timeout=0, tuple_rows=false, reset=true, max_inactive_connection_lifetime=300.0, max_lifetime=0.0, acquire_timeout=0.0, validate_after=30.0))]
#[allow(clippy::too_many_arguments)]
pub fn create_pool<'py>(
    py: Python<'py>,
    hub: &Bound<'py, Hub>,
    conn_str: String,
    min_size: usize,
    max_size: usize,
    autocommit: bool,
    timeout: u32,
    tuple_rows: bool,
    reset: bool,
    max_inactive_connection_lifetime: f64,
    max_lifetime: f64,
    acquire_timeout: f64,
    validate_after: f64,
) -> PyResult<(Bound<'py, PyAny>, u64, AioOpening)> {
    if max_size == 0 || min_size > max_size {
        return Err(pyo3::exceptions::PyValueError::new_err("need 0 <= min_size <= max_size and max_size > 0"));
    }
    crate::connstr::build_context(&conn_str, None)
        .map_err(|msg| to_exception(py, &[DiagRecord::driver("HY000", msg)]))?;
    let inner = Arc::new(PoolInner {
        conn_str,
        autocommit,
        timeout: AtomicU32::new(timeout),
        as_tuple: tuple_rows,
        idle: Mutex::new(Vec::with_capacity(max_size)),
        slots: Arc::new(Semaphore::new(max_size)),
        max_size,
        reset,
        closed: AtomicBool::new(false),
        max_idle: duration_option(max_inactive_connection_lifetime, "max_inactive_connection_lifetime")?,
        max_lifetime: duration_option(max_lifetime, "max_lifetime")?,
        validate_after: duration_option(validate_after, "validate_after")?,
        acquire_timeout_ms: AtomicU64::new(millis(acquire_timeout, "acquire_timeout")?),
        cancels: Arc::new(Mutex::new(std::collections::HashMap::new())),
    });
    let opening_requests = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let (future, id) = spawn_op(py, hub, false, Some(&opening_requests), move |cancel| async move {
        // Open the minimum number of sessions concurrently. Dropping the set
        // (an error, or a cancelled request) abandons the logins still running.
        let mut opening = tokio::task::JoinSet::new();
        for _ in 0..min_size {
            let conn_str = inner.conn_str.clone();
            opening.spawn(async move { open_session(&conn_str, autocommit, timeout).await });
        }
        loop {
            let opened = tokio::select! {
                opened = opening.join_next() => opened,
                () = cancel.cancelled() => return Outcome::Error(cancelled()),
            };
            match opened {
                None => break,
                Some(Ok(Ok(session))) => inner.idle.lock().push((session, Instant::now())),
                Some(Ok(Err(records))) => return Outcome::Error(records),
                Some(Err(err)) => return Outcome::Error(vec![DiagRecord::driver("HY000", err.to_string())]),
            }
        }
        Outcome::Pool(inner)
    })?;
    Ok((future, id, AioOpening { cancels: opening_requests }))
}
