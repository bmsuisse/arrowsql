//! Nanoseconds per Python object created from Rust, by technique.
//!
//! `per_object(kind, n)` builds `n` objects, keeps them alive in a Vec (so the
//! allocator cannot recycle one slot) and returns ns/object excluding
//! deallocation. Raw CPython C API calls are used where PyO3's safe wrappers
//! would add overhead, which is exactly what the question is about.

use std::time::Instant;

use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyModule, PyTuple};

const SAMPLE_UTF8: &str = "N";
const SAMPLE_TEXT: &str = "2024-01-01 Manhattan / JFK Airport";

fn utf16(s: &str) -> Vec<u16> {
    s.encode_utf16().collect()
}

/// ASCII fast path: allocate a compact 1-byte-kind str and memcpy into it.
unsafe fn ascii_str(bytes: &[u8]) -> *mut ffi::PyObject {
    unsafe {
        let obj = ffi::PyUnicode_New(bytes.len() as ffi::Py_ssize_t, 127);
        let data = ffi::PyUnicode_DATA(obj) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), data, bytes.len());
        obj
    }
}

#[pyfunction]
fn per_object(py: Python<'_>, kind: &str, n: usize) -> PyResult<f64> {
    let decimal_mod = PyModule::import(py, "decimal")?;
    let decimal_cls = decimal_mod.getattr("Decimal")?;
    let wide = utf16(SAMPLE_TEXT);
    let mut keep: Vec<*mut ffi::PyObject> = Vec::with_capacity(n);
    let mut keep_bound: Vec<Bound<'_, PyAny>> = Vec::new();
    let start = Instant::now();
    unsafe {
        match kind {
            "int_small" => (0..n).for_each(|i| keep.push(ffi::PyLong_FromLongLong((i % 200) as i64))),
            "int_large" => (0..n).for_each(|i| keep.push(ffi::PyLong_FromLongLong(1_000_000 + i as i64))),
            "float" => (0..n).for_each(|i| keep.push(ffi::PyFloat_FromDouble(i as f64 * 0.5))),
            "str_utf8_1char" => (0..n).for_each(|_| {
                keep.push(ffi::PyUnicode_FromStringAndSize(SAMPLE_UTF8.as_ptr() as *const _, 1))
            }),
            "str_utf8_34" => (0..n).for_each(|_| {
                keep.push(ffi::PyUnicode_FromStringAndSize(
                    SAMPLE_TEXT.as_ptr() as *const _,
                    SAMPLE_TEXT.len() as ffi::Py_ssize_t,
                ))
            }),
            "str_utf16_decode_34" => (0..n).for_each(|_| {
                let mut order: i32 = -1;
                keep.push(ffi::PyUnicode_DecodeUTF16(
                    wide.as_ptr() as *const _,
                    (wide.len() * 2) as ffi::Py_ssize_t,
                    std::ptr::null(),
                    &mut order,
                ))
            }),
            "str_ascii_fastpath_34" => (0..n).for_each(|_| keep.push(ascii_str(SAMPLE_TEXT.as_bytes()))),
            "str_interned_1char" => {
                // One cached object per distinct value: a refcount bump per cell.
                let cached = ffi::PyUnicode_FromStringAndSize(SAMPLE_UTF8.as_ptr() as *const _, 1);
                (0..n).for_each(|_| {
                    ffi::Py_INCREF(cached);
                    keep.push(cached)
                });
                ffi::Py_DECREF(cached);
            }
            "decimal_from_str" => {
                for i in 0..n {
                    let s = format!("{}.{:02}", i % 10000, i % 100);
                    keep_bound.push(decimal_cls.call1((s,))?);
                }
            }
            "decimal_from_tuple" => {
                for i in 0..n {
                    let digits = PyTuple::new(py, [(i % 10) as i64, ((i / 10) % 10) as i64, 2, 5])?;
                    keep_bound.push(decimal_cls.call1(((0i64, digits, -2i64),))?);
                }
            }
            "decimal_from_int_scaleb" => {
                let scaleb = decimal_cls.getattr("scaleb")?;
                for i in 0..n {
                    let d = decimal_cls.call1((i as i64 * 7 + 25,))?;
                    keep_bound.push(scaleb.call1((d, -2i64))?);
                }
            }
            "datetime_capi" => {
                for i in 0..n {
                    let dt = PyDateTime::new(py, 2024, 1, 1 + (i % 28) as u8, (i % 24) as u8, (i % 60) as u8, 0, (i % 1_000_000) as u32, None)?;
                    keep_bound.push(dt.into_any());
                }
            }
            "tuple20_of_none" => (0..n).for_each(|_| {
                let t = ffi::PyTuple_New(20);
                for c in 0..20 {
                    ffi::Py_INCREF(ffi::Py_None());
                    ffi::PyTuple_SET_ITEM(t, c, ffi::Py_None());
                }
                keep.push(t)
            }),
            "list20_of_none" => (0..n).for_each(|_| {
                let l = ffi::PyList_New(20);
                for c in 0..20 {
                    ffi::Py_INCREF(ffi::Py_None());
                    ffi::PyList_SET_ITEM(l, c, ffi::Py_None());
                }
                keep.push(l)
            }),
            other => return Err(PyValueError::new_err(format!("unknown kind {other}"))),
        }
    }
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    for obj in keep {
        unsafe { ffi::Py_DECREF(obj) };
    }
    drop(keep_bound);
    Ok(ns)
}

#[pymodule]
fn pyobj_bench(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(per_object, m)?)?;
    Ok(())
}
