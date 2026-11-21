//! CSV structural scanner.
//!
//! Every 64-byte block of a chunk is turned into three bitmasks (comma, quote,
//! newline) with SIMD compares, the way simdjson finds structural characters.
//! Lines are then cut from those masks without touching the bytes again, and a
//! line's fields are validated against the dialect in rules.md:
//!   * exactly N fields, every comma separates
//!   * a quote is legal only as the first and last byte of a field
//!   * `\r` and every other byte is ordinary data

/// (comma, quote, newline) masks for the 64 bytes at `p`; bit i is byte i.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use core::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |c: u8| {
        let s = _mm256_set1_epi8(c as i8);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, s)) as u32 as u64;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, s)) as u32 as u64;
        lo | (hi << 32)
    };
    (m(b','), m(b'"'), m(b'\n'))
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use core::arch::x86_64::*;
    let v = [
        _mm_loadu_si128(p as *const __m128i),
        _mm_loadu_si128(p.add(16) as *const __m128i),
        _mm_loadu_si128(p.add(32) as *const __m128i),
        _mm_loadu_si128(p.add(48) as *const __m128i),
    ];
    let m = |c: u8| {
        let s = _mm_set1_epi8(c as i8);
        let mut r = 0u64;
        for (i, x) in v.iter().enumerate() {
            r |= (_mm_movemask_epi8(_mm_cmpeq_epi8(*x, s)) as u32 as u64) << (16 * i);
        }
        r
    };
    (m(b','), m(b'"'), m(b'\n'))
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use core::arch::aarch64::*;
    let v0 = vld1q_u8(p);
    let v1 = vld1q_u8(p.add(16));
    let v2 = vld1q_u8(p.add(32));
    let v3 = vld1q_u8(p.add(48));
    const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
    let bits = vld1q_u8(W.as_ptr());
    let m = |c: u8| {
        let s = vdupq_n_u8(c);
        let t0 = vandq_u8(vceqq_u8(v0, s), bits);
        let t1 = vandq_u8(vceqq_u8(v1, s), bits);
        let t2 = vandq_u8(vceqq_u8(v2, s), bits);
        let t3 = vandq_u8(vceqq_u8(v3, s), bits);
        let s0 = vpaddq_u8(t0, t1);
        let s1 = vpaddq_u8(t2, t3);
        let s2 = vpaddq_u8(s0, s1);
        let s3 = vpaddq_u8(s2, s2);
        vgetq_lane_u64(vreinterpretq_u64_u8(s3), 0)
    };
    (m(b','), m(b'"'), m(b'\n'))
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let mut r = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let b = *p.add(i);
        r.0 |= ((b == b',') as u64) << i;
        r.1 |= ((b == b'"') as u64) << i;
        r.2 |= ((b == b'\n') as u64) << i;
    }
    r
}

/// Validate one line given its comma and quote positions; return its N unquoted fields.
#[inline(always)]
fn fields<'a, const N: usize>(
    d: &'a [u8],
    s0: usize,
    e0: usize,
    commas: &[u32; 8],
    nc: usize,
    quotes: &[u32; 16],
    nq: usize,
) -> Option<[&'a [u8]; N]> {
    if nc != N - 1 || nq > 2 * N {
        return None;
    }
    let mut out: [&[u8]; N] = [&d[0..0]; N];
    let mut qi = 0usize;
    let mut s = s0;
    for k in 0..N {
        let e = if k < N - 1 { commas[k] as usize } else { e0 };
        if s < e && d[s] == b'"' {
            if e - s < 2 || qi + 1 >= nq || quotes[qi] as usize != s || quotes[qi + 1] as usize != e - 1 {
                return None;
            }
            qi += 2;
            out[k] = &d[s + 1..e - 1];
        } else {
            if qi < nq && (quotes[qi] as usize) < e {
                return None;
            }
            out[k] = &d[s..e];
        }
        s = e + 1;
    }
    if qi != nq {
        return None;
    }
    Some(out)
}

/// Call `f` with the fields of every well-formed line of `data` (which starts at
/// a line start). Malformed and empty lines are skipped. `data` must be < 4 GiB.
#[inline(always)]
pub fn for_each_record<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    debug_assert!(N >= 2 && N <= 8);
    let n = data.len();
    let p = data.as_ptr();
    let mut commas = [0u32; 8];
    let mut quotes = [0u32; 16];
    let (mut nc, mut nq) = (0usize, 0usize);
    let mut ls = 0usize;
    let mut base = 0usize;
    let mut tail = [0u8; 64];
    while base < n {
        let (mut mc, mut mq, mut mn) = if base + 64 <= n {
            unsafe { masks(p.add(base)) }
        } else {
            tail = [0u8; 64];
            tail[..n - base].copy_from_slice(&data[base..]);
            unsafe { masks(tail.as_ptr()) }
        };
        loop {
            let below = if mn != 0 { (mn & mn.wrapping_neg()) - 1 } else { u64::MAX };
            let mut c = mc & below;
            mc &= !below;
            while c != 0 {
                if nc < 8 {
                    commas[nc] = (base + c.trailing_zeros() as usize) as u32;
                }
                nc += 1;
                c &= c - 1;
            }
            let mut q = mq & below;
            mq &= !below;
            while q != 0 {
                if nq < 16 {
                    quotes[nq] = (base + q.trailing_zeros() as usize) as u32;
                }
                nq += 1;
                q &= q - 1;
            }
            if mn == 0 {
                break;
            }
            let e = base + mn.trailing_zeros() as usize;
            if let Some(fl) = fields::<N>(data, ls, e, &commas, nc, &quotes, nq) {
                f(&fl);
            }
            ls = e + 1;
            nc = 0;
            nq = 0;
            mn &= mn - 1;
        }
        base += 64;
    }
    if ls < n {
        if let Some(fl) = fields::<N>(data, ls, n, &commas, nc, &quotes, nq) {
            f(&fl);
        }
    }
}

/// Everything after a CSV file's header line.
pub fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &data[data.len()..],
    }
}
