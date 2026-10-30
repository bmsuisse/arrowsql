//! date / time / datetime / smalldatetime / datetime2 / datetimeoffset.
//!
//! Inputs are (days since 0001-01-01, 100ns ticks since midnight); fractional
//! precision below 1µs is truncated like the C++ binding did.

use pyo3::prelude::*;
use pyo3::types::{PyDate, PyDateTime, PyDelta, PyTime, PyTzInfo};

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
    Ok(PyDate::new(py, y, m, d)?.into_any())
}

pub fn time(py: Python<'_>, ticks: u64) -> PyResult<Bound<'_, PyAny>> {
    let (h, mi, s, us) = clock(ticks);
    Ok(PyTime::new(py, h, mi, s, us, None)?.into_any())
}

pub fn datetime(py: Python<'_>, days: u32, ticks: u64) -> PyResult<Bound<'_, PyAny>> {
    // msodbcsql rounding can carry a datetime to 24:00:00.000 -> next day.
    let (days, ticks) = if ticks >= 864_000_000_000 { (days + 1, ticks - 864_000_000_000) } else { (days, ticks) };
    let (y, m, d) = civil(days);
    let (h, mi, s, us) = clock(ticks);
    Ok(PyDateTime::new(py, y, m, d, h, mi, s, us, None)?.into_any())
}

/// datetimeoffset is stored as UTC on the wire; return local time with a
/// fixed-offset tzinfo.
pub fn datetimeoffset(
    py: Python<'_>,
    days: u32,
    ticks: u64,
    offset_minutes: i16,
) -> PyResult<Bound<'_, PyAny>> {
    let total = i64::from(days) * 864_000_000_000 + ticks as i64
        + i64::from(offset_minutes) * 60 * 10_000_000;
    let local_days = total.div_euclid(864_000_000_000) as u32;
    let local_ticks = total.rem_euclid(864_000_000_000) as u64;
    let (y, m, d) = civil(local_days);
    let (h, mi, s, us) = clock(local_ticks);
    let delta = PyDelta::new(py, 0, i32::from(offset_minutes) * 60, 0, true)?;
    let tz = PyTzInfo::fixed_offset(py, delta)?;
    Ok(PyDateTime::new(py, y, m, d, h, mi, s, us, Some(&tz))?.into_any())
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
