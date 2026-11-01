//! Cancellation and timeout state of one statement execution.
//!
//! `_cancel()` may arrive from another thread while the execution is still
//! waiting for the wire. When the execution has to drain another statement's
//! batch first, the drain runs on that batch's own `CancelHandle`; the
//! requester therefore *forwards* its cancellation (and its timeout) to the
//! handle of the batch being drained.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mssql_tds::core::CancelHandle;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::diag::DiagRecord;

const NOT_INTERRUPTED: u8 = 0;
const CANCELLED: u8 = 1;
const TIMED_OUT: u8 = 2;

pub struct StmtCancel {
    handle: Arc<CancelHandle>,
    interrupt: AtomicU8,
    forwards: Mutex<Vec<Arc<CancelHandle>>>,
}

impl StmtCancel {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handle: Arc::new(CancelHandle::new()),
            interrupt: AtomicU8::new(NOT_INTERRUPTED),
            forwards: Mutex::new(Vec::new()),
        })
    }

    pub fn handle(&self) -> &CancelHandle {
        &self.handle
    }

    pub fn shared_handle(&self) -> Arc<CancelHandle> {
        self.handle.clone()
    }

    /// The user asked to cancel (`cursor.cancel()`).
    pub fn cancel(&self) {
        self.trigger(CANCELLED);
    }

    fn expire(&self) {
        self.trigger(TIMED_OUT);
    }

    fn trigger(&self, reason: u8) {
        let _ = self
            .interrupt
            .compare_exchange(NOT_INTERRUPTED, reason, Ordering::AcqRel, Ordering::Acquire);
        self.handle.cancel();
        for target in self.forwards.lock().iter() {
            target.cancel();
        }
    }

    /// Also cancel `target` when this execution is cancelled or times out.
    pub fn forward_to(&self, target: Arc<CancelHandle>) {
        let mut forwards = self.forwards.lock();
        // Checked under the lock: `trigger` sets the flag before it takes the
        // lock, so either we see the flag here or `trigger` sees our target.
        if self.interrupt.load(Ordering::Acquire) != NOT_INTERRUPTED {
            target.cancel();
        }
        forwards.push(target);
    }

    pub fn stop_forwarding(&self) {
        self.forwards.lock().clear();
    }

    /// Diagnostic for a cancellation or timeout that already happened.
    pub fn interrupt_record(&self) -> Option<DiagRecord> {
        match self.interrupt.load(Ordering::Acquire) {
            CANCELLED => Some(DiagRecord::driver("HY008", "Operation canceled")),
            TIMED_OUT => Some(DiagRecord::driver("HYT00", "Query timeout expired")),
            _ => None,
        }
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupt.load(Ordering::Acquire) != NOT_INTERRUPTED
    }

    /// Expire this execution after `timeout_secs` (0 = never) until the
    /// returned guard is dropped.
    pub fn arm_deadline(self: &Arc<Self>, timeout_secs: u32) -> Deadline {
        if timeout_secs == 0 {
            return Deadline(None);
        }
        let this = self.clone();
        // No runtime (thread limit reached) means no timer: the statement runs without a deadline.
        let Ok(runtime) = crate::runtime::try_runtime() else { return Deadline(None) };
        let task = runtime.spawn(async move {
            tokio::time::sleep(Duration::from_secs(u64::from(timeout_secs))).await;
            this.expire();
        });
        Deadline(Some(task))
    }
}

/// Disarms the timer started by [`StmtCancel::arm_deadline`] on drop.
pub struct Deadline(Option<JoinHandle<()>>);

impl Drop for Deadline {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}
