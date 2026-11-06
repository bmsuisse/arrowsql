//! UTF-16LE text that is pure ASCII, narrowed to one byte per code unit.

use std::mem::MaybeUninit;

/// High bytes and bit 7 of the low byte of four UTF-16LE units.
const NON_ASCII_MASK: u64 = 0xFF80_FF80_FF80_FF80;

/// Narrow UTF-16LE `src` into `dst` (one byte per unit) while checking that
/// every unit is ASCII. Returns false at the first non-ASCII unit, leaving
/// `dst` partly written; on true all of `dst` is initialised. `src` must hold
/// exactly `2 * dst.len()` bytes.
pub fn narrow_ascii_utf16(src: &[u8], dst: &mut [MaybeUninit<u8>]) -> bool {
    debug_assert_eq!(src.len(), dst.len() * 2);
    let mut chunks = src.chunks_exact(8);
    let mut out = dst.chunks_exact_mut(4);
    for (chunk, o) in (&mut chunks).zip(&mut out) {
        let word = u64::from_le_bytes(chunk.try_into().unwrap());
        if word & NON_ASCII_MASK != 0 {
            return false;
        }
        o[0].write(chunk[0]);
        o[1].write(chunk[2]);
        o[2].write(chunk[4]);
        o[3].write(chunk[6]);
    }
    for (unit, o) in chunks.remainder().chunks_exact(2).zip(out.into_remainder()) {
        if unit[0] >= 0x80 || unit[1] != 0 {
            return false;
        }
        o.write(unit[0]);
    }
    true
}

/// True when `bytes` is UTF-16LE in which every code unit is ASCII.
#[cfg(test)]
fn is_ascii_utf16(bytes: &[u8]) -> bool {
    bytes.len() % 2 == 0
        && narrow_ascii_utf16(bytes, &mut vec![MaybeUninit::uninit(); bytes.len() / 2])
}

#[cfg(test)]
mod tests {
    use super::{is_ascii_utf16, narrow_ascii_utf16};

    #[test]
    fn ascii_utf16_detection() {
        assert!(is_ascii_utf16(&[]));
        assert!(is_ascii_utf16(&[b'a', 0, b'Z', 0]));
        assert!(!is_ascii_utf16(&[0xE9, 0]), "U+00E9 is not ASCII");
        assert!(!is_ascii_utf16(&[b'a', 1]), "U+0161 is not ASCII");
        assert!(!is_ascii_utf16(&[b'a']), "odd length is malformed");
    }

    fn utf16(s: &[u16]) -> Vec<u8> {
        s.iter().flat_map(|u| u.to_le_bytes()).collect()
    }

    #[test]
    fn ascii_utf16_word_loop_edges() {
        // Every length around the 4-unit word boundary, with the bad unit at
        // every position and with each kind of non-ASCII value.
        for len in 0..=13usize {
            let ok: Vec<u16> = (0..len).map(|i| 0x20 + i as u16).collect();
            let mut dst = vec![std::mem::MaybeUninit::<u8>::uninit(); len];
            assert!(narrow_ascii_utf16(&utf16(&ok), &mut dst));
            // SAFETY: a true result initialised all of `dst`.
            let dst: Vec<u8> = dst.iter().map(|b| unsafe { b.assume_init() }).collect();
            assert_eq!(dst, ok.iter().map(|u| *u as u8).collect::<Vec<_>>());
            for pos in 0..len {
                for bad in [0x80u16, 0xE9, 0x100, 0x161, 0xFF00, 0xD800, 0xDC00] {
                    let mut units = ok.clone();
                    units[pos] = bad;
                    assert!(
                        !is_ascii_utf16(&utf16(&units)),
                        "len {len} pos {pos} unit {bad:#x}"
                    );
                }
            }
        }
        assert!(
            is_ascii_utf16(&utf16(&[0, 0x7F, 0, 0x7F, 0x7F])),
            "NUL and DEL are ASCII"
        );
        assert!(
            !is_ascii_utf16(&[b'a', 0, b'b', 0, b'c']),
            "odd length with a word"
        );
    }
}
