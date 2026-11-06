//! Native-to-Python log bridge.
//!
//! Upstream's C++ layer formats its own messages with a `[DDBC] ` prefix and
//! hands them to the Python logger, so they land in the same handlers and file
//! as the Python-side records. This module does the same through
//! `arrowsql.logging.logger.py_core_log`.
//!
//! The Python side pushes its level into `update_log_level`; `enabled` checks
//! that cached level before any message is formatted.
//!
//! TODO(logging): route `tracing` events from mssql-tds through `log` as well.

use std::sync::atomic::{AtomicI32, Ordering};

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;

pub const DEBUG: i32 = 10;
const WARNING: i32 = 30;

static LEVEL: AtomicI32 = AtomicI32::new(WARNING);
static LOGGER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

#[pyfunction]
pub fn update_log_level(level: i32) {
    LEVEL.store(level, Ordering::Relaxed);
    log::set_max_level(level_filter(level));
}

/// Forwards `log` records of `arrowsql_core` to the Python logger, as the
/// binding's own messages are. Other crates' records (mssql-tds' `tracing`
/// falls back to `log`) are not forwarded.
struct CoreLog;

const CORE_TARGET: &str = "arrowsql_core";

fn py_level(level: log::Level) -> i32 {
    match level {
        log::Level::Error => 40,
        log::Level::Warn => WARNING,
        log::Level::Info => 20,
        log::Level::Debug | log::Level::Trace => DEBUG,
    }
}

fn level_filter(level: i32) -> log::LevelFilter {
    if level <= DEBUG {
        log::LevelFilter::Debug
    } else if level <= 20 {
        log::LevelFilter::Info
    } else {
        log::LevelFilter::Warn
    }
}

impl log::Log for CoreLog {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target().starts_with(CORE_TARGET) && enabled(py_level(metadata.level()))
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            Python::attach(|py| log(py, py_level(record.level()), &record.args().to_string()));
        }
    }

    fn flush(&self) {}
}

/// Install the forwarder (module init).
pub fn install() {
    if log::set_logger(&CoreLog).is_ok() {
        log::set_max_level(level_filter(cached_level()));
    }
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
    let logger = LOGGER.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(PyModule::import(py, "arrowsql.logging")?.getattr("logger")?.unbind())
    })?;
    logger.bind(py).call_method1("py_core_log", (level, format!("[DDBC] {msg}")))?;
    Ok(())
}

/// Emit `msg` as the Python-side `logger.error(msg)` would: tagged `[Python]`
/// and dropped when the logger's level is above ERROR. For the lines that
/// `exceptions.py` and `connection.py` used to log.
pub fn python_error(py: Python<'_>, msg: &str) {
    let Ok(logger) = LOGGER.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(PyModule::import(py, "arrowsql.logging")?.getattr("logger")?.unbind())
    }) else {
        return;
    };
    let _ = logger.bind(py).call_method1("error", (msg,));
}
