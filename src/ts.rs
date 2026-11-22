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
}
