//! Text forms of SQL Server values, shared by the agent renderer and the
//! Arrow sink (which uses them for `sql_variant` cells).

use std::fmt::Write;

/// Civil date of `days` since 1970-01-01 (proleptic Gregorian, Hinnant's algorithm).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

/// `YYYY-MM-DD` of `days` since 1970-01-01.
pub fn write_date(out: &mut String, days: i64) {
    let (y, m, d) = civil(days);
    let _ = write!(out, "{y:04}-{m:02}-{d:02}");
}

/// `HH:MM:SS` plus `digits` fractional digits (0..=9) of `nanos` since midnight.
pub fn write_time(out: &mut String, nanos: i64, digits: u32) {
    let secs = nanos.div_euclid(1_000_000_000);
    let frac = nanos.rem_euclid(1_000_000_000);
    let _ = write!(out, "{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    if digits > 0 {
        let digits = digits.min(9);
        let _ = write!(out, ".{:0width$}", frac / 10_i64.pow(9 - digits), width = digits as usize);
    }
}

/// Like [`write_time`] with the shortest fraction that is exact to microseconds.
pub fn write_time_trimmed(out: &mut String, nanos: i64) {
    let micros = nanos.rem_euclid(1_000_000_000) / 1000;
    let digits = if micros == 0 {
        0
    } else if micros % 1000 == 0 {
        3
    } else {
        6
    };
    write_time(out, nanos, digits);
}

/// `YYYY-MM-DD HH:MM:SS[.f]` of microseconds since the Unix epoch.
pub fn write_timestamp(out: &mut String, micros: i64, digits: u32) {
    write_date(out, micros.div_euclid(86_400_000_000));
    out.push(' ');
    write_time(out, micros.rem_euclid(86_400_000_000) * 1000, digits);
}

/// A scaled decimal such as `-12.3400`.
pub fn write_decimal(out: &mut String, value: i128, scale: u32) {
    if value < 0 {
        out.push('-');
    }
    let magnitude = value.unsigned_abs();
    if scale == 0 {
        let _ = write!(out, "{magnitude}");
        return;
    }
    let divisor = 10_u128.pow(scale);
    let _ = write!(out, "{}.{:0width$}", magnitude / divisor, magnitude % divisor, width = scale as usize);
}

/// `0x` followed by lowercase hex of `bytes`.
pub fn write_hex(out: &mut String, bytes: &[u8]) {
    out.push_str("0x");
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_around_the_epoch_and_leap_days() {
        let mut s = String::new();
        for (days, text) in [
            (0, "1970-01-01"),
            (-1, "1969-12-31"),
            (11_016, "2000-02-29"),
            (-719_162, "0001-01-01"),
            (2_932_896, "9999-12-31"),
        ] {
            s.clear();
            write_date(&mut s, days);
            assert_eq!(s, text);
        }
    }

    #[test]
    fn decimals_keep_scale_and_sign() {
        let mut s = String::new();
        write_decimal(&mut s, -5, 4);
        assert_eq!(s, "-0.0005");
        s.clear();
        write_decimal(&mut s, 1234, 0);
        assert_eq!(s, "1234");
    }

    #[test]
    fn times_truncate_to_the_requested_digits() {
        let mut s = String::new();
        write_time(&mut s, 3_723_456_789_000, 3);
        assert_eq!(s, "01:02:03.456");
    }
}
