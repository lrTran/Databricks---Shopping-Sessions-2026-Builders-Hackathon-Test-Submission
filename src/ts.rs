//! Timestamps, exactly per rules.md. Returns epoch milliseconds (UTC) or None.
//!
//! F1 `YYYY-MM-DDTHH:MM:SSZ`        (20 bytes)
//! F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`   (25 bytes; offset hours 00-23, minutes 00-59)
//! F3 one or more ASCII digits: epoch ms, leading zeros allowed.
//!
//! An F3 value too large for i64 is "outside the window", which drops the row
//! exactly like a malformed value, so both return None.

#[inline(always)]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() { Some((b - b'0') as i64) } else { None }
}

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    Some(dig(s[i])? * 10 + dig(s[i + 1])?)
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap(y) { 29 } else { 28 },
        _ => 0,
    }
}

/// Days since 1970-01-01 for a valid proleptic Gregorian date (H. Hinnant).
#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn parse_datetime(s: &[u8]) -> Option<i64> {
    // s.len() >= 19 is guaranteed by the caller.
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

#[inline]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        0 => None,
        20 if s[19] == b'Z' => parse_datetime(s),
        25 if s[19] == b'+' || s[19] == b'-' => {
            let base = parse_datetime(s)?;
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            Some(if s[19] == b'+' { base - off } else { base + off })
        }
        _ => {
            // F3: digits only.
            let mut v: i64 = 0;
            for &b in s {
                let d = dig(b)?;
                v = v.checked_mul(10)?.checked_add(d)?;
            }
            Some(v)
        }
    }
}

// ---------------------------------------------------------------------------
// Fast path. `Parser::parse` returns exactly what `parse` returns; `parse` stays
// as the plain reference (used for the manifest and to fuzz this one).

const DIGITS: u64 = 0x3030_3030_3030_3030;

/// 8 ASCII digits (first byte most significant) -> value, or None.
#[inline(always)]
fn parse8(b: [u8; 8]) -> Option<u64> {
    let v = u64::from_le_bytes(b);
    // Every byte in 0x30..=0x39: high nibble 3, and adding 6 keeps it 3.
    if (v & 0xF0F0_F0F0_F0F0_F0F0) != DIGITS || ((v + 0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) != DIGITS {
        return None;
    }
    let mut x = v - DIGITS;
    x = (x * 10 + (x >> 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x * 100 + (x >> 16)) & 0x0000_FFFF_0000_FFFF;
    x = (x * 10000 + (x >> 32)) & 0x0000_0000_FFFF_FFFF;
    Some(x)
}

/// F3: digits only.
#[inline(always)]
fn f3(s: &[u8]) -> Option<i64> {
    let len = s.len();
    if (8..=16).contains(&len) {
        let lo = parse8(s[len - 8..].try_into().unwrap())?;
        let lead = len - 8; // leading digits, 0..=8
        let hi = if lead == 0 {
            0
        } else {
            // The first `lead` bytes moved to the top of a word whose low bytes are '0'.
            let v = u64::from_le_bytes(s[..8].try_into().unwrap()) << ((8 - lead) * 8);
            let fill = if lead == 8 { 0 } else { DIGITS >> (lead * 8) };
            parse8((v | fill).to_le_bytes())?
        };
        Some((hi * 100_000_000 + lo) as i64)
    } else {
        let mut v: i64 = 0;
        for &b in s {
            v = v.checked_mul(10)?.checked_add(dig(b)?)?;
        }
        Some(v)
    }
}

const NO_DATE: i64 = i64::MIN;
const CACHE: usize = 64;

/// A timestamp parser with a small cache of `YYYY-MM-DD` -> days since the epoch.
/// A dataset holds only a few dozen distinct dates, so the calendar work is
/// done once per date and thread.
pub struct Parser {
    keys: [u128; CACHE], // the 10 date bytes; 0 = empty
    days: [i64; CACHE],  // days since the epoch, or NO_DATE for an impossible date
}

impl Default for Parser {
    fn default() -> Self {
        Parser { keys: [0; CACHE], days: [0; CACHE] }
    }
}

impl Parser {
    #[cold]
    fn date_slow(s: &[u8]) -> i64 {
        if s[4] != b'-' || s[7] != b'-' {
            return NO_DATE;
        }
        let (Some(y1), Some(y2), Some(mo), Some(d)) = (d2(s, 0), d2(s, 2), d2(s, 5), d2(s, 8)) else { return NO_DATE };
        let y = y1 * 100 + y2;
        if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
            return NO_DATE;
        }
        days_from_civil(y, mo, d)
    }

    /// `YYYY-MM-DDTHH:MM:SS` (the first 19 bytes) -> epoch ms, or None.
    #[inline(always)]
    fn datetime(&mut self, s: &[u8]) -> Option<i64> {
        let key = u64::from_le_bytes(s[..8].try_into().unwrap()) as u128 | (u16::from_le_bytes([s[8], s[9]]) as u128) << 64;
        let slot = ((key as u64 ^ (key >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
        let days = if self.keys[slot] == key {
            self.days[slot]
        } else {
            let d = Self::date_slow(s);
            self.keys[slot] = key;
            self.days[slot] = d;
            d
        };
        let t = |i: usize| s[i].wrapping_sub(b'0') as i64;
        let (h1, h2, m1, m2, s1, s2) = (t(11), t(12), t(14), t(15), t(17), t(18));
        let bad = (days == NO_DATE) | (s[10] != b'T') | (s[13] != b':') | (s[16] != b':')
            | (h1 as u64 > 9) | (h2 as u64 > 9) | (m1 as u64 > 9) | (m2 as u64 > 9) | (s1 as u64 > 9) | (s2 as u64 > 9);
        let (h, mi, se) = (h1 * 10 + h2, m1 * 10 + m2, s1 * 10 + s2);
        if bad | (h > 23) | (mi > 59) | (se > 59) {
            return None;
        }
        Some(((days * 24 + h) * 60 + mi) * 60_000 + se * 1000)
    }

    #[inline(always)]
    pub fn parse(&mut self, s: &[u8]) -> Option<i64> {
        match s.len() {
            0 => None,
            20 if s[19] == b'Z' => self.datetime(s),
            25 if s[19] == b'+' || s[19] == b'-' => {
                let base = self.datetime(s)?;
                if s[22] != b':' {
                    return None;
                }
                let oh = d2(s, 20)?;
                let om = d2(s, 23)?;
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh * 60 + om) * 60_000;
                Some(if s[19] == b'+' { base - off } else { base + off })
            }
            _ => f3(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1791071890000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"2026-10-03T10:00:00-00:00"), parse(b"2026-10-03T10:00:00Z"));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10", b"2026-10-03T23:58:10.000Z", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-02-30T00:00:00Z", b"2026-02-29T00:00:00Z",
            b"2026-10-03T24:00:00Z", b"2026-10-03T23:59:60Z", b"2026-10-03T10:00:00+24:00",
            b"2026-10-03T10:00:00+05:60", b"2026-10-03T10:00:00+0200", b"99999999999999999999999",
            "١٧٩١٠٧١٨٩٠٠٠٠".as_bytes(), "２026-10-03T10:00:00Z".as_bytes(), b"2026-10-03T10:00:00Z\r",
        ] {
            assert_eq!(parse(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse(b"86400000"), Some(86_400_000));
        assert_eq!(parse(b"2026-10-03T10:00:00+23:59").unwrap(), parse(b"2026-10-02T10:01:00Z").unwrap());
    }

    /// The cached parser agrees with the reference on mutated timestamps.
    #[test]
    fn fast_parser_matches_reference() {
        let seeds: &[&[u8]] = &[
            b"2026-10-03T23:58:10Z", b"2028-02-29T12:00:00Z", b"2027-02-28T00:00:00+05:30",
            b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000", b"9223372036854775807",
            b"2000-02-29T23:59:59Z", b"1900-02-28T23:59:59Z", b"86400000", b"1", b"123456789",
            b"1234567890123456", b"12345678901234567", b"0000000000000000",
        ];
        let alphabet = b"0123456789-+:TZtz .e\r\x00\xff";
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut p = Parser::default();
        for _ in 0..2_000_000 {
            let mut v = seeds[(rnd() as usize) % seeds.len()].to_vec();
            for _ in 0..(rnd() % 4) {
                let r = rnd();
                match r % 4 {
                    0 if !v.is_empty() => {
                        let i = (rnd() as usize) % v.len();
                        v[i] = alphabet[(rnd() as usize) % alphabet.len()];
                    }
                    1 if !v.is_empty() => {
                        let i = (rnd() as usize) % v.len();
                        v.remove(i);
                    }
                    2 => {
                        let i = (rnd() as usize) % (v.len() + 1);
                        v.insert(i, alphabet[(rnd() as usize) % alphabet.len()]);
                    }
                    _ if v.len() >= 19 => {
                        for i in [0usize, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
                            if rnd() % 3 == 0 {
                                v[i] = b'0' + (rnd() % 10) as u8;
                            }
                        }
                    }
                    _ => {}
                }
            }
            assert_eq!(p.parse(&v), parse(&v), "{:?}", String::from_utf8_lossy(&v));
        }
    }
}
