//! SIMD structural scanner for the CSV dialect: finds `,` `"` `\n` 64 bytes at a
//! time and walks only those positions. Same results as `csv::split` per line.

#[inline(always)]
fn masks_scalar(b: &[u8; 64]) -> (u64, u64, u64) {
    let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
    for (i, &x) in b.iter().enumerate() {
        c |= ((x == b',') as u64) << i;
        q |= ((x == b'"') as u64) << i;
        n |= ((x == b'\n') as u64) << i;
    }
    (c, q, n)
}

/// Bitmasks of commas, quotes and newlines in the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        let comma = _mm_set1_epi8(b',' as i8);
        let quote = _mm_set1_epi8(b'"' as i8);
        let nl = _mm_set1_epi8(b'\n' as i8);
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
            c |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, comma)) as u32 as u64) << (16 * k);
            q |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, quote)) as u32 as u64) << (16 * k);
            n |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, nl)) as u32 as u64) << (16 * k);
        }
        (c, q, n)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        masks_scalar(&*(p as *const [u8; 64]))
    }
}

/// Calls `f` with the `N` unquoted fields of every well-formed line in `chunk`
/// (malformed and empty lines are skipped). The final line may lack its `\n`.
#[inline(always)]
#[allow(unused_assignments)] // the macro's resets after the final field
pub fn for_each_record<const N: usize, F: FnMut(&[&'static [u8]; N])>(chunk: &'static [u8], mut f: F) {
    let n = chunk.len();
    let ptr = chunk.as_ptr();
    let mut fields: [&'static [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields closed on this line
    let mut fs = 0usize; // start of the open field
    let mut q = 0u32; // quotes in the open field
    let mut q0 = 0usize; // first quote
    let mut q1 = 0usize; // last quote
    let mut bad = false;
    let mut line_start = 0usize;
    // Close the open field ending at `i` (exclusive).
    macro_rules! close {
        ($i:expr) => {{
            let i = $i;
            if !bad {
                if nf == N || (q != 0 && !(q == 2 && q0 == fs && q1 + 1 == i)) {
                    bad = true;
                } else {
                    // SAFETY: fs <= i <= n, and when quoted fs < q1 = i - 1.
                    fields[nf] = unsafe {
                        if q == 0 { chunk.get_unchecked(fs..i) } else { chunk.get_unchecked(fs + 1..i - 1) }
                    };
                    nf += 1;
                }
            }
            q = 0;
            fs = i + 1;
        }};
    }
    let mut block = 0usize;
    while block < n {
        let (cm, qm, nm) = if block + 64 <= n {
            unsafe { masks(ptr.add(block)) }
        } else {
            let mut tail = [0u8; 64];
            tail[..n - block].copy_from_slice(&chunk[block..]);
            masks_scalar(&tail)
        };
        let mut m = cm | qm | nm;
        while m != 0 {
            let bit = m & m.wrapping_neg();
            let i = block + m.trailing_zeros() as usize;
            m &= m - 1;
            if qm & bit != 0 {
                q += 1;
                if q == 1 {
                    q0 = i;
                }
                q1 = i;
                continue;
            }
            close!(i);
            if nm & bit != 0 {
                if !bad && nf == N && i > line_start {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                line_start = i + 1;
            }
        }
        block += 64;
    }
    if line_start < n {
        close!(n);
        if !bad && nf == N {
            f(&fields);
        }
    }
}

