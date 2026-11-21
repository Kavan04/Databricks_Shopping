//! CSV structural scanner and input reading.
//!
//! Every 64-byte block of a chunk is turned into three bitmasks (comma, quote,
//! newline) with SIMD compares, the way simdjson finds structural characters.
//! Lines and fields are cut from those masks, and a line's fields are validated
//! against the dialect in rules.md:
//!   * exactly N fields, every comma separates
//!   * a quote is legal only as the first and last byte of a field
//!   * `\r` and every other byte is ordinary data
//!
//! Every buffer handed to the scanner has at least PAD readable bytes after its
//! end, so loads may run past a field or the last block without bounds checks.

use std::fs::File;

pub const PAD: usize = 64;

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

/// Validate one line [s0, e0) given its comma positions and quote count, and
/// return its N unquoted fields. Branch-free per field: a field is quoted iff
/// it starts with a quote; it must then end with one and be >= 2 bytes long,
/// and the line's quote count must equal two per quoted field (so no quote
/// appears anywhere else).
#[inline(always)]
fn fields<'a, const N: usize>(d: &'a [u8], s0: usize, e0: usize, commas: &[u32; 8], nc: usize, nq: u32) -> Option<[&'a [u8]; N]> {
    if nc != N - 1 {
        return None;
    }
    let p = d.as_ptr();
    let mut st = [0usize; N];
    let mut ln = [0usize; N];
    let mut s = s0;
    let mut q2 = 0u32;
    let mut bad = false;
    for k in 0..N {
        let e = if k < N - 1 { commas[k] as usize } else { e0 };
        let len = e - s;
        let first = unsafe { *p.add(s) };
        let li = if len >= 2 { e - 1 } else { s };
        let last = unsafe { *p.add(li) };
        let qs = len >= 1 && first == b'"';
        let qe = len >= 2 && last == b'"';
        bad |= qs & !qe;
        q2 += 2 * qs as u32;
        let q = (qs & qe) as usize;
        st[k] = s + q;
        ln[k] = len - 2 * q;
        s = e + 1;
    }
    if bad || q2 != nq {
        return None;
    }
    Some(std::array::from_fn(|k| unsafe { std::slice::from_raw_parts(p.add(st[k]), ln[k]) }))
}

/// Call `f` with the fields of every well-formed line of `data` (which starts at
/// a line start, is < 4 GiB, and has PAD readable bytes after it). Malformed and
/// empty lines are skipped.
#[inline(always)]
pub fn for_each_record<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let n = data.len();
    let p = data.as_ptr();
    let mut commas = [0u32; 8];
    let mut nc = 0usize;
    let mut nq = 0u32;
    let mut ls = 0usize;
    let mut base = 0usize;
    while base < n {
        let (mut mc, mut mq, mut mn) = unsafe { masks(p.add(base)) };
        if n - base < 64 {
            let keep = (1u64 << (n - base)) - 1;
            mc &= keep;
            mq &= keep;
            mn &= keep;
        }
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
            nq += (mq & below).count_ones();
            mq &= !below;
            if mn == 0 {
                break;
            }
            let e = base + mn.trailing_zeros() as usize;
            if let Some(fl) = fields::<N>(data, ls, e, &commas, nc, nq) {
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
        if let Some(fl) = fields::<N>(data, ls, n, &commas, nc, nq) {
            f(&fl);
        }
    }
}

/// An input file mapped read-only, with zero pages after its end so the
/// scanner may read PAD bytes past the last line.
pub struct InFile {
    ptr: *const u8,
    pub size: usize,
}
unsafe impl Send for InFile {}
unsafe impl Sync for InFile {}

impl InFile {
    pub fn open(path: &std::path::Path) -> Option<InFile> {
        use std::os::fd::AsRawFd;
        let f = File::open(path).ok()?;
        let size = f.metadata().ok()?.len() as usize;
        if size == 0 {
            return None;
        }
        let page = 4096usize.max(unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize);
        let total = (size + PAD + page - 1) / page * page + page;
        unsafe {
            // reserve zero pages, then map the file over the front of the reservation
            let base = libc::mmap(std::ptr::null_mut(), total, libc::PROT_READ, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0);
            assert!(base != libc::MAP_FAILED, "mmap reserve failed");
            let m = libc::mmap(base, size, libc::PROT_READ, libc::MAP_PRIVATE | libc::MAP_FIXED, f.as_raw_fd(), 0);
            assert!(m == base, "mmap file failed");
            Some(InFile { ptr: base as *const u8, size })
        }
    }
    fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }
}

/// A byte range of an input file; its worker parses the lines that start inside it.
pub struct RTask {
    pub file: usize,
    pub off: usize,
    pub len: usize,
    pub kind: u8,
}

fn madvise(d: &[u8], advice: libc::c_int) {
    if d.is_empty() {
        return;
    }
    let a = d.as_ptr() as usize;
    let start = a & !4095;
    let len = (a + d.len() - start + 4095) & !4095;
    unsafe {
        libc::madvise(start as *mut libc::c_void, len, advice);
    }
}

/// Start reading a range into the page cache in the background.
pub fn prefetch_range(f: &InFile, off: usize, len: usize) {
    madvise(&f.bytes()[off..off + len], libc::MADV_WILLNEED);
}

/// Drop this process's page mappings of a parsed range (the pages stay in the
/// page cache; a later touch maps them again). Done by the worker right after
/// parsing, so the kernel's teardown work is spread over all workers instead
/// of running single-threaded at exit. Only whole pages inside the range.
pub fn release_range(f: &InFile, off: usize, len: usize) {
    let a = f.ptr as usize + off;
    let start = (a + 4095) & !4095;
    let end = (a + len) & !4095;
    if end > start {
        unsafe {
            libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_DONTNEED);
        }
    }
}

/// The complete lines that start in [off, off+len); the header line is skipped
/// when off == 0. PAD readable bytes always follow the returned slice. Its pages
/// are mapped in with one call, not one fault per page.
pub fn lines_in(file: &InFile, off: usize, len: usize) -> &[u8] {
    let d = file.bytes();
    let n = d.len();
    let from = off.saturating_sub(1);
    let start = match memchr::memchr(b'\n', &d[from..]) {
        Some(i) => from + i + 1,
        None => return &d[n..n],
    };
    if start >= off + len {
        return &d[start..start];
    }
    let end = match memchr::memchr(b'\n', &d[off + len - 1..]) {
        Some(i) => off + len - 1 + i,
        None => n,
    };
    let s = &d[start..end];
    madvise(s, 22 /* MADV_POPULATE_READ, Linux 5.14+; harmless elsewhere */);
    s
}

/// A whole small file, followed by PAD zero bytes.
pub struct Padded {
    buf: Vec<u8>,
    len: usize,
}

impl Padded {
    pub fn read(path: &std::path::Path) -> Option<Padded> {
        let mut buf = std::fs::read(path).ok()?;
        let len = buf.len();
        buf.extend_from_slice(&[0u8; PAD]);
        Some(Padded { buf, len })
    }
    /// Everything after the header line.
    pub fn body(&self) -> &[u8] {
        let d = &self.buf[..self.len];
        match memchr::memchr(b'\n', d) {
            Some(i) => &d[i + 1..],
            None => &d[d.len()..],
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// v1's straightforward splitter: the reference.
    fn ref_fields<const N: usize>(line: &[u8]) -> Option<Vec<Vec<u8>>> {
        let parts: Vec<&[u8]> = line.split(|&b| b == b',').collect();
        if parts.len() != N {
            return None;
        }
        let mut out = Vec::new();
        for f in parts {
            if f.first() == Some(&b'"') {
                if f.len() < 2 || f[f.len() - 1] != b'"' || f[1..f.len() - 1].contains(&b'"') {
                    return None;
                }
                out.push(f[1..f.len() - 1].to_vec());
            } else {
                if f.contains(&b'"') {
                    return None;
                }
                out.push(f.to_vec());
            }
        }
        Some(out)
    }

    fn check<const N: usize>(seed: u64) {
        let mut x = seed;
        let mut rnd = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        for _ in 0..3000 {
            // a chunk of random lines over a tiny alphabet rich in separators
            let mut text = Vec::new();
            let lines = 1 + rnd(40);
            for _ in 0..lines {
                let len = rnd(90);
                for _ in 0..len {
                    text.push(b"ab,,\"\"x\r 9"[rnd(10) as usize]);
                }
                if rnd(3) == 0 {
                    // a well-formed-ish line with some quoted fields
                    text.clear_tail_guard();
                }
                text.push(b'\n');
            }
            if rnd(2) == 0 {
                text.pop();
            }
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            let mut padded = text.clone();
            padded.extend_from_slice(&[b','; PAD]);
            for_each_record::<N>(&padded[..text.len()], |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            let want: Vec<Vec<Vec<u8>>> = text.split(|&b| b == b'\n').filter_map(|l| ref_fields::<N>(l)).collect();
            assert_eq!(got, want, "{:?}", String::from_utf8_lossy(&text));
        }
    }

    trait Guard {
        fn clear_tail_guard(&mut self);
    }
    impl Guard for Vec<u8> {
        fn clear_tail_guard(&mut self) {}
    }

    #[test]
    fn scanner_matches_reference() {
        for seed in 1..40 {
            check::<2>(seed * 7919);
            check::<4>(seed * 104729);
            check::<7>(seed * 1299709);
        }
    }

    #[test]
    fn scanner_matches_reference_structured() {
        let mut x = 42u64;
        let mut rnd = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        for _ in 0..20000 {
            let mut text = Vec::new();
            for _ in 0..1 + rnd(30) {
                let nf = 5 + rnd(4);
                for k in 0..nf {
                    if k > 0 {
                        text.push(b',');
                    }
                    let v: Vec<u8> = (0..rnd(12)).map(|_| b"abc0\r \""[rnd(7) as usize]).collect();
                    match rnd(5) {
                        0 => {
                            text.push(b'"');
                            text.extend_from_slice(&v);
                            text.push(b'"');
                        }
                        _ => text.extend_from_slice(&v),
                    }
                }
                text.push(b'\n');
            }
            let mut padded = text.clone();
            padded.extend_from_slice(&[b'"'; PAD]);
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            for_each_record::<7>(&padded[..text.len()], |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            let want: Vec<Vec<Vec<u8>>> = text.split(|&b| b == b'\n').filter_map(|l| ref_fields::<7>(l)).collect();
            assert_eq!(got, want, "{:?}", String::from_utf8_lossy(&text));
        }
    }
}
