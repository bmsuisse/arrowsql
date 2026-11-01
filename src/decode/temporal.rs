//! date / time / datetime / smalldatetime / datetime2 / datetimeoffset.
//!
//! Inputs are (days since 0001-01-01, 100ns ticks since midnight); fractional
//! precision below 1µs is truncated like the C++ binding did.
//!
//! Values are built by the `datetime.date`, `datetime.time` and
//! `datetime.datetime` classes as they are when the module loads. While a class
//! is the builtin type the C-API constructor is used directly. A substituted
//! class is called with the same positional arguments instead, so it sees every
//! value.

use std::sync::Mutex;

use pyo3::PyTypeInfo;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDate, PyDateTime, PyDelta, PyTime, PyType, PyTzInfo};

/// How one temporal value is built.
enum Maker {
    /// The builtin type: use the C-API constructor.
    Native,
    /// A substituted class: call it with the positional arguments.
    Python(Py<PyAny>),
}

struct Makers {
    date: Maker,
    time: Maker,
    datetime: Maker,
}

static MAKERS: PyOnceLock<Makers> = PyOnceLock::new();

fn resolve(cls: Bound<'_, PyAny>, builtin: &Bound<'_, PyType>) -> Maker {
    if cls.as_ptr() == builtin.as_ptr() {
        Maker::Native
    } else {
        Maker::Python(cls.unbind())
    }
}

/// Resolve the temporal classes once. Called from module init, so a class
/// substituted before `mssql_python` is imported is the one every fetch uses.
pub fn init(py: Python<'_>) -> PyResult<()> {
    MAKERS.get_or_try_init(py, || -> PyResult<Makers> {
        let module = py.import("datetime")?;
        Ok(Makers {
            date: resolve(module.getattr("date")?, &PyDate::type_object(py)),
            time: resolve(module.getattr("time")?, &PyTime::type_object(py)),
            datetime: resolve(module.getattr("datetime")?, &PyDateTime::type_object(py)),
        })
    })?;
    Ok(())
}

fn makers(py: Python<'_>) -> PyResult<&'static Makers> {
    MAKERS
        .get(py)
        .ok_or_else(|| PyRuntimeError::new_err("temporal constructors are not initialised"))
}

/// (year, month, day) -> days since 0001-01-01, the inverse of [`civil`]
/// (Hinnant's days_from_civil). Negative before 0001-01-01.
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 306
}

/// Days since 0001-01-01 -> (year, month, day). Hinnant's civil_from_days.
pub fn civil(days: u32) -> (i32, u8, u8) {
    // Shift epoch from 0001-01-01 to 0000-03-01.
    let z = i64::from(days) + 306;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u8;
    ((y + i64::from(m <= 2)) as i32, m, d)
}

fn clock(ticks: u64) -> (u8, u8, u8, u32) {
    let micros = ticks / 10;
    let secs = micros / 1_000_000;
    (
        (secs / 3600) as u8,
        ((secs / 60) % 60) as u8,
        (secs % 60) as u8,
        (micros % 1_000_000) as u32,
    )
}

pub fn date(py: Python<'_>, days: u32) -> PyResult<Bound<'_, PyAny>> {
    let (y, m, d) = civil(days);
    match &makers(py)?.date {
        Maker::Native => Ok(PyDate::new(py, y, m, d)?.into_any()),
        Maker::Python(cls) => cls.bind(py).call1((y, m, d)),
    }
}

pub fn time(py: Python<'_>, ticks: u64) -> PyResult<Bound<'_, PyAny>> {
    let (h, mi, s, us) = clock(ticks);
    match &makers(py)?.time {
        Maker::Native => Ok(PyTime::new(py, h, mi, s, us, None)?.into_any()),
        Maker::Python(cls) => cls.bind(py).call1((h, mi, s, us)),
    }
}

pub fn datetime(py: Python<'_>, days: u32, ticks: u64) -> PyResult<Bound<'_, PyAny>> {
    // msodbcsql rounding can carry a datetime to 24:00:00.000 -> next day.
    let (days, ticks) = if ticks >= 864_000_000_000 {
        (days + 1, ticks - 864_000_000_000)
    } else {
        (days, ticks)
    };
    let (y, m, d) = civil(days);
    let (h, mi, s, us) = clock(ticks);
    match &makers(py)?.datetime {
        Maker::Native => Ok(PyDateTime::new(py, y, m, d, h, mi, s, us, None)?.into_any()),
        Maker::Python(cls) => cls.bind(py).call1((y, m, d, h, mi, s, us)),
    }
}

/// Upper bound on distinct offsets kept. Real offsets are a few dozen; the
/// cap only guards against a pathological server.
const FIXED_TZ_LIMIT: usize = 64;

/// Fixed-offset tzinfo per offset in minutes. Building one is a Python call
/// per cell, so a column of offsets shares a handful of objects instead.
static FIXED_TZ: Mutex<Vec<(i16, Py<PyTzInfo>)>> = Mutex::new(Vec::new());

fn fixed_tz(py: Python<'_>, offset_minutes: i16) -> PyResult<Bound<'_, PyTzInfo>> {
    // Poisoning only means another fetch panicked mid-insert; the cache holds
    // whole entries, so it is still usable.
    let mut cache = FIXED_TZ.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, tz)) = cache.iter().find(|(offset, _)| *offset == offset_minutes) {
        return Ok(tz.bind(py).clone());
    }
    let delta = PyDelta::new(py, 0, i32::from(offset_minutes) * 60, 0, true)?;
    let tz = PyTzInfo::fixed_offset(py, delta)?;
    if cache.len() < FIXED_TZ_LIMIT {
        cache.push((offset_minutes, tz.clone().unbind()));
    }
    Ok(tz)
}

/// datetimeoffset is stored as UTC on the wire; return local time with a
/// fixed-offset tzinfo.
pub fn datetimeoffset(
    py: Python<'_>,
    days: u32,
    ticks: u64,
    offset_minutes: i16,
) -> PyResult<Bound<'_, PyAny>> {
    let total = i64::from(days) * 864_000_000_000
        + ticks as i64
        + i64::from(offset_minutes) * 60 * 10_000_000;
    let local_days = total.div_euclid(864_000_000_000) as u32;
    let local_ticks = total.rem_euclid(864_000_000_000) as u64;
    let (y, m, d) = civil(local_days);
    let (h, mi, s, us) = clock(local_ticks);
    let tz = fixed_tz(py, offset_minutes)?;
    match &makers(py)?.datetime {
        Maker::Native => Ok(PyDateTime::new(py, y, m, d, h, mi, s, us, Some(&tz))?.into_any()),
        Maker::Python(cls) => cls.bind(py).call1((y, m, d, h, mi, s, us, &tz)),
    }
}

#[cfg(test)]
mod tests {
    use super::civil;

    #[test]
    fn civil_dates() {
        assert_eq!(civil(0), (1, 1, 1));
        assert_eq!(civil(693_595), (1900, 1, 1));
        assert_eq!(civil(3_652_058), (9999, 12, 31));
        assert_eq!(civil(730_119), (2000, 1, 1));
    }
}

#[cfg(test)]
mod civil_tests {
    use super::{civil, days_from_civil};

    #[test]
    fn days_from_civil_inverts_civil() {
        assert_eq!(days_from_civil(1, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 1), 719_162);
        assert_eq!(days_from_civil(9999, 12, 31), 3_652_058);
        for days in (0..=3_652_058u32).step_by(7) {
            let (y, m, d) = civil(days);
            assert_eq!(days_from_civil(i64::from(y), i64::from(m), i64::from(d)), i64::from(days));
        }
    }
}
