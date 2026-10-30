//! Native-to-Python log bridge.
//!
//! Upstream's C++ layer formats its own messages with a `[DDBC] ` prefix and
//! hands them to the Python logger, so they land in the same handlers and file
//! as the Python-side records. This module does the same through
//! `mssql_python.logging.logger.py_core_log`.
//!
//! The Python side pushes its level into `update_log_level`; `enabled` checks
//! that cached level before any message is formatted.
//!
//! TODO(logging): route `tracing` events from mssql-tds through `log` as well.

use std::sync::atomic::{AtomicI32, Ordering};

use pyo3::prelude::*;
use pyo3::types::PyModule;

pub const DEBUG: i32 = 10;
const WARNING: i32 = 30;

static LEVEL: AtomicI32 = AtomicI32::new(WARNING);

#[pyfunction]
pub fn update_log_level(level: i32) {
    LEVEL.store(level, Ordering::Relaxed);
}

fn cached_level() -> i32 {
    LEVEL.load(Ordering::Relaxed)
}

/// Whether a message at `level` would be emitted. Warnings and errors always
/// pass, matching `py_core_log`.
pub fn enabled(level: i32) -> bool {
    level >= WARNING || level >= cached_level()
}

/// Emit `msg` at `level` through the Python logger. The caller holds the GIL.
/// Failures are swallowed: logging must never fail a driver call.
pub fn log(py: Python<'_>, level: i32, msg: &str) {
    if !enabled(level) {
        return;
    }
    let _ = emit(py, level, msg);
}

/// Shorthand for `log(py, DEBUG, msg)`.
pub fn debug(py: Python<'_>, msg: &str) {
    log(py, DEBUG, msg);
}

fn emit(py: Python<'_>, level: i32, msg: &str) -> PyResult<()> {
    let module = PyModule::import(py, "mssql_python.logging")?;
    let logger = module.getattr("logger")?;
    logger.call_method1("py_core_log", (level, format!("[DDBC] {msg}")))?;
    Ok(())
}
