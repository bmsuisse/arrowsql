//! Python entry points of the connection pool (`arrowsql_core::pooling`).

use pyo3::prelude::*;

pub use arrowsql_core::pooling::*;

#[pyfunction]
#[pyo3(name = "enable_pooling", signature = (max_size=100, idle_timeout=600))]
pub fn enable_pooling_py(max_size: i64, idle_timeout: i64) {
    enable_pooling(max_size, idle_timeout);
}

#[pyfunction]
#[pyo3(name = "close_pooling")]
pub fn close_pooling_py(py: Python<'_>) {
    py.detach(close_pooling);
}

#[pyfunction]
#[pyo3(name = "disable_pooling")]
pub fn disable_pooling_py(py: Python<'_>) {
    py.detach(disable_pooling);
}
