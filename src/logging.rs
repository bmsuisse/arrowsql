//! Logger bridge. The C++ binding cached Python's log level so native code
//! could skip formatting; nothing native logs yet, so the level is stored.
//!
//! TODO(logging): route `tracing` events from mssql-tds into
//! `mssql_python.logging.driver_logger` when the cached level allows it.

use std::sync::atomic::{AtomicI32, Ordering};

use pyo3::prelude::*;

static LEVEL: AtomicI32 = AtomicI32::new(30);

#[pyfunction]
pub fn update_log_level(level: i32) {
    LEVEL.store(level, Ordering::Relaxed);
}

pub fn level() -> i32 {
    LEVEL.load(Ordering::Relaxed)
}
