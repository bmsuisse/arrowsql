//! `AioPool`: a tokio-native pool of async sessions.
//!
//! A semaphore bounds the number of sessions in use; idle sessions wait in a
//! stack (most recently used first, so the warm ones are reused). Sessions
//! are reset with the TDS RESETCONNECTION flag on their next request, so a
//! reused session costs no extra round trip. The `query`/`execute` shortcuts
//! check a session out, run, and check it back in inside one task: one hop
//! between Python and Rust per call.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::Mutex;
use pyo3::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::diag::DiagRecord;

use super::connection::{ConnCore, build_command, open_session, query_mode, spawn_op};
use super::hub::{Hub, Outcome, QueryMode, to_exception};
use super::session::{Command, Diag, OpCancel, Session};

pub struct PoolInner {
    conn_str: String,
    autocommit: bool,
    timeout: AtomicU32,
    as_tuple: bool,
    idle: Mutex<Vec<Session>>,
    slots: Arc<Semaphore>,
    max_size: usize,
    /// Reset session state (temp tables, SET options, ...) between users.
    reset: bool,
    closed: AtomicBool,
}

fn pool_closed() -> Diag {
    vec![DiagRecord::driver("08003", "Pool is closed")]
}

impl PoolInner {
    async fn checkout(&self) -> Result<(Session, OwnedSemaphorePermit), Diag> {
        let permit = self.slots.clone().acquire_owned().await.map_err(|_| pool_closed())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(pool_closed());
        }
        loop {
            let session = self.idle.lock().pop();
            match session {
                Some(session) if session.is_open() => return Ok((session, permit)),
                Some(_) => continue,
                None => break,
            }
        }
        let timeout = self.timeout.load(Ordering::Relaxed);
        let session = open_session(&self.conn_str, self.autocommit, timeout).await?;
        Ok((session, permit))
    }

    pub async fn checkin(&self, session: Session) {
        if self.closed.load(Ordering::Acquire) {
            let mut session = session;
            session.close().await;
            return;
        }
        if let Some(session) = session.into_reusable(self.reset).await {
            self.idle.lock().push(session);
        }
    }

    async fn run(&self, command: Command, max_rows: usize, mode: QueryMode, cancel: &OpCancel) -> Outcome {
        let (mut session, permit) = match self.checkout().await {
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
    fn acquire<'py>(&self, py: Python<'py>, hub: &Bound<'py, Hub>) -> Op<'py> {
        let inner = self.inner.clone();
        spawn_op(py, hub, false, None, move |_| async move {
            match inner.checkout().await {
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
        spawn_op(py, hub, inner.as_tuple, None, move |cancel| async move {
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
            for mut session in idle {
                session.close().await;
            }
            Outcome::None
        })
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
}

#[pyfunction]
#[pyo3(name = "aio_create_pool", signature = (hub, conn_str, min_size=1, max_size=10, autocommit=true, timeout=0, tuple_rows=false, reset=true))]
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
) -> PyResult<Bound<'py, PyAny>> {
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
    });
    let (future, _) = spawn_op(py, hub, false, None, move |_| async move {
        // Open the minimum number of sessions concurrently.
        let mut opening = tokio::task::JoinSet::new();
        for _ in 0..min_size {
            let conn_str = inner.conn_str.clone();
            opening.spawn(async move { open_session(&conn_str, autocommit, timeout).await });
        }
        while let Some(opened) = opening.join_next().await {
            match opened {
                Ok(Ok(session)) => inner.idle.lock().push(session),
                Ok(Err(records)) => return Outcome::Error(records),
                Err(err) => return Outcome::Error(vec![DiagRecord::driver("HY000", err.to_string())]),
            }
        }
        Outcome::Pool(inner)
    })?;
    Ok(future)
}
