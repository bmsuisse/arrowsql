//! Integer helpers shared by the bind and bulk paths.

/// `10^n` for `n` in `0..=38`, the powers of ten that fit a `u128`.
pub const POW10: [u128; 39] = {
    let mut table = [1u128; 39];
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// `10^exponent` from the table, `None` past `10^38`.
#[inline]
pub fn checked_pow10(exponent: u32) -> Option<u128> {
    POW10.get(exponent as usize).copied()
}

/// `magnitude / divisor` rounded half away from zero (the caller keeps the
/// sign). SQL Server rounds every narrowing numeric conversion this way.
#[inline]
pub fn div_round_half_away(magnitude: u128, divisor: u128) -> u128 {
    let (quotient, remainder) = (magnitude / divisor, magnitude % divisor);
    quotient + u128::from(remainder >= divisor - remainder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ties_round_away_from_zero() {
        assert_eq!(div_round_half_away(25, 10), 3);
        assert_eq!(div_round_half_away(35, 10), 4);
        assert_eq!(div_round_half_away(24, 10), 2);
        assert_eq!(div_round_half_away(0, 10), 0);
        assert_eq!(div_round_half_away(5, 10), 1);
    }
}
