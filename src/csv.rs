//! The CSV dialect from rules.md, implemented exactly.
//!
//! * Only `\n` ends a line. `\r` is an ordinary byte of whatever field it is in.
//! * Fields are split on every comma (no value ever holds a comma).
//! * A field may be wrapped in double quotes as a whole; the quotes are syntax.
//!   `"u1"` is `u1`, `""` is empty. A quote anywhere else makes the line malformed.
//! * A line with the wrong number of fields is malformed. An empty line is malformed.
//! * Spaces are never trimmed.

use memchr::memchr;

/// Split `line` (without its `\n`) into exactly `N` unquoted fields.
/// Returns false for a malformed line.
#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let len = line.len();
    let mut n = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u32;
    let mut i = 0usize;
    loop {
        let at_end = i == len;
        let c = if at_end { b',' } else { unsafe { *line.get_unchecked(i) } };
        if c == b',' {
            if n == N {
                return false; // too many fields
            }
            let f = &line[start..i];
            let v = if quotes == 0 {
                f
            } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                &f[1..f.len() - 1]
            } else {
                return false; // a quote that is not around the whole field
            };
            out[n] = v;
            n += 1;
            if at_end {
                break;
            }
            start = i + 1;
            quotes = 0;
        } else if c == b'"' {
            quotes += 1;
        }
        i += 1;
    }
    n == N
}

/// Iterate the lines of a chunk. A chunk holds whole lines; the final line of a
/// file may lack its `\n`. An empty line is yielded (and is then malformed).
#[inline(always)]
pub fn for_each_line<F: FnMut(&'static [u8])>(chunk: &'static [u8], mut f: F) {
    let mut p = 0usize;
    let n = chunk.len();
    while p < n {
        let e = match memchr(b'\n', &chunk[p..]) {
            Some(x) => p + x,
            None => n,
        };
        f(&chunk[p..e]);
        p = e + 1;
    }
}

/// Split a file's bytes (header excluded) into chunks of about `target` bytes,
/// each ending just after a `\n` (or at end of file).
pub fn chunks(file: &'static [u8], target: usize, out: &mut Vec<&'static [u8]>) {
    // Skip the header row: everything up to and including the first `\n`.
    let body_start = match memchr(b'\n', file) {
        Some(p) => p + 1,
        None => return, // header only, no newline
    };
    let mut s = body_start;
    let n = file.len();
    while s < n {
        let mut e = (s + target).min(n);
        if e < n {
            e = match memchr(b'\n', &file[e..]) {
                Some(x) => e + x + 1,
                None => n,
            };
        }
        out.push(&file[s..e]);
        s = e;
    }
}

/// Lines of a small file (header skipped), for products/campaigns.
pub fn body_lines(file: &'static [u8]) -> Vec<&'static [u8]> {
    let mut v = Vec::new();
    if let Some(p) = memchr(b'\n', file) {
        for_each_line(&file[p + 1..], |l| v.push(l));
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    fn s<const N: usize>(l: &'static [u8]) -> Option<Vec<&'static [u8]>> {
        let mut o = [&b""[..]; N];
        if split::<N>(l, &mut o) { Some(o.to_vec()) } else { None }
    }
    #[test]
    fn dialect() {
        assert_eq!(s::<2>(b"a,b"), Some(vec![&b"a"[..], b"b"]));
        assert_eq!(s::<2>(b"\"a\",\"\""), Some(vec![&b"a"[..], b""]));
        assert_eq!(s::<2>(b"a,"), Some(vec![&b"a"[..], b""]));
        assert_eq!(s::<2>(b"a,b,c"), None);
        assert_eq!(s::<2>(b"a"), None);
        assert_eq!(s::<2>(b""), None);
        assert_eq!(s::<2>(b"u1,u5\""), None);
        assert_eq!(s::<2>(b"\",b"), None);
        assert_eq!(s::<2>(b"a\"b\",c"), None);
        assert_eq!(s::<2>(b"\"a\"\"\",c"), None);
        assert_eq!(s::<2>(b" a ,b\r"), Some(vec![&b" a "[..], b"b\r"]));
        assert_eq!(s::<1>(b"\"A\tB\""), Some(vec![&b"A\tB"[..]]));
    }
}
