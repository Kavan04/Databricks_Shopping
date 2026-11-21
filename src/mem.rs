//! Append-only memory: a lock-free bump allocator of 16 KiB blocks and
//! per-thread buckets built from those blocks. Nothing goes back to the OS
//! before exit, so there is no realloc copying and no mmap/munmap churn. A
//! worker that has consumed blocks may recycle them for its own output
//! (`Blocks::recycle`), which saves the kernel from zeroing fresh pages.

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering::{Acquire, Relaxed, Release}};
use std::sync::Mutex;

pub const BLK: usize = 16 << 10;
const SEG_BYTES: usize = 1 << 30;
const MAX_SEGS: usize = 96;

pub struct Arena {
    segs: Vec<AtomicPtr<u8>>,
    next: AtomicUsize,
    lock: Mutex<()>,
}

fn map_anon(n: usize) -> *mut u8 {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            n,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap failed");
        if std::env::var_os("FM_THP").is_some() {
            libc::madvise(p, n, libc::MADV_HUGEPAGE);
        }
        if std::env::var_os("FM_NOTHP").is_some() {
            libc::madvise(p, n, libc::MADV_NOHUGEPAGE);
        }
        p as *mut u8
    }
}

impl Arena {
    pub fn new() -> Self {
        Arena {
            segs: (0..MAX_SEGS).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect(),
            next: AtomicUsize::new(0),
            lock: Mutex::new(()),
        }
    }
    fn alloc(&self) -> *mut u8 {
        let per = SEG_BYTES / BLK;
        let i = self.next.fetch_add(1, Relaxed);
        let (sg, off) = (i / per, i % per);
        assert!(sg < MAX_SEGS, "arena exhausted");
        let mut p = self.segs[sg].load(Acquire);
        if p.is_null() {
            let _g = self.lock.lock().unwrap();
            p = self.segs[sg].load(Acquire);
            if p.is_null() {
                p = map_anon(SEG_BYTES);
                self.segs[sg].store(p, Release);
            }
        }
        unsafe { p.add(off * BLK) }
    }
}

/// A worker's source of blocks: recycled ones first, then fresh arena blocks.
pub struct Blocks<'a> {
    arena: &'a Arena,
    free: Vec<*mut u8>,
}

impl<'a> Blocks<'a> {
    pub fn new(arena: &'a Arena) -> Self {
        Blocks { arena, free: Vec::new() }
    }
    #[inline(always)]
    fn get(&mut self) -> *mut u8 {
        match self.free.pop() {
            Some(p) => p,
            None => self.arena.alloc(),
        }
    }
    /// Hand back blocks whose records have been consumed and will never be read again.
    pub fn recycle<T>(&mut self, segs: &[Seg<T>]) {
        for s in segs {
            self.free.push(s.p as *mut u8);
        }
    }
}

/// A finished run of records: exactly one arena block, possibly partly filled.
pub struct Seg<T> {
    p: *const T,
    n: usize,
}
unsafe impl<T> Send for Seg<T> {}
unsafe impl<T> Sync for Seg<T> {}
impl<T> Seg<T> {
    #[inline(always)]
    pub fn slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.p, self.n) }
    }
}

/// Append-only bucket made of blocks.
pub struct Bkt<T> {
    start: *mut T,
    cur: *mut T,
    end: *mut T,
    done: Vec<Seg<T>>,
}
unsafe impl<T> Send for Bkt<T> {}
impl<T: Copy> Bkt<T> {
    pub fn new() -> Self {
        let z = std::ptr::null_mut();
        Bkt { start: z, cur: z, end: z, done: Vec::new() }
    }
    #[inline(always)]
    pub fn push(&mut self, v: T, b: &mut Blocks) {
        if self.cur == self.end {
            self.refill(b);
        }
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
    }
    #[cold]
    #[inline(never)]
    fn refill(&mut self, b: &mut Blocks) {
        if !self.start.is_null() {
            self.done.push(Seg { p: self.start, n: unsafe { self.cur.offset_from(self.start) } as usize });
        }
        let p = b.get() as *mut T;
        self.start = p;
        self.cur = p;
        self.end = unsafe { p.add(BLK / std::mem::size_of::<T>()) };
    }
    /// Append one full 64-byte line of records (`R` of them) with a
    /// non-temporal store. The bucket must have received only whole lines so far.
    #[inline(always)]
    fn push_line<const R: usize>(&mut self, line: &Line<T, R>, b: &mut Blocks) {
        if self.cur == self.end {
            self.refill(b);
        }
        unsafe {
            store_line(self.cur as *mut u8, line as *const Line<T, R> as *const u8);
            self.cur = self.cur.add(R);
        }
    }
    pub fn finish(mut self) -> Vec<Seg<T>> {
        if !self.start.is_null() && self.cur != self.start {
            self.done.push(Seg { p: self.start, n: unsafe { self.cur.offset_from(self.start) } as usize });
        }
        self.done
    }
}

pub fn seg_len<T>(v: &[Seg<T>]) -> usize {
    v.iter().map(|s| s.n).sum()
}

/// One cache line of records.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct Line<T: Copy, const R: usize>([T; R]);

/// Copy 64 bytes to a 64-byte aligned destination, bypassing the cache.
#[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
#[inline(always)]
unsafe fn store_line(dst: *mut u8, src: *const u8) {
    use core::arch::x86_64::*;
    let a = _mm256_load_si256(src as *const __m256i);
    let b = _mm256_load_si256(src.add(32) as *const __m256i);
    _mm256_stream_si256(dst as *mut __m256i, a);
    _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
}
#[cfg(all(target_arch = "x86_64", not(target_feature = "avx")))]
#[inline(always)]
unsafe fn store_line(dst: *mut u8, src: *const u8) {
    use core::arch::x86_64::*;
    for i in 0..4 {
        _mm_stream_si128(dst.add(16 * i) as *mut __m128i, _mm_load_si128(src.add(16 * i) as *const __m128i));
    }
}
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn store_line(dst: *mut u8, src: *const u8) {
    use core::arch::aarch64::*;
    let a = vld1q_u8(src);
    let b = vld1q_u8(src.add(16));
    let c = vld1q_u8(src.add(32));
    let d = vld1q_u8(src.add(48));
    core::arch::asm!(
        "stnp {a:q}, {b:q}, [{p}]",
        "stnp {c:q}, {d:q}, [{p}, #32]",
        a = in(vreg) a, b = in(vreg) b, c = in(vreg) c, d = in(vreg) d, p = in(reg) dst,
        options(nostack, preserves_flags)
    );
}
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
unsafe fn store_line(dst: *mut u8, src: *const u8) {
    std::ptr::copy_nonoverlapping(src, dst, 64);
}

/// Make non-temporal stores visible before the data is handed to other threads.
#[inline(always)]
fn store_fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence();
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("dmb ishst", options(nostack, preserves_flags));
    }
}

/// A set of buckets fed through software write-combining: each bucket gathers
/// one cache line of records (R of them, R * size_of::<T>() == 64) in a small
/// hot buffer, and full lines go out with a non-temporal store. Scattering
/// into a thousand buckets otherwise misses the cache on almost every store.
pub struct WcBuckets<T: Copy, const R: usize> {
    lines: Vec<Line<T, R>>,
    fill: Vec<u8>,
    bkts: Vec<Bkt<T>>,
}

impl<T: Copy + Default, const R: usize> WcBuckets<T, R> {
    pub fn new(n: usize) -> Self {
        assert!(R * std::mem::size_of::<T>() == 64 && BLK % 64 == 0);
        WcBuckets { lines: vec![Line([T::default(); R]); n], fill: vec![0; n], bkts: (0..n).map(|_| Bkt::new()).collect() }
    }
    #[inline(always)]
    pub fn push(&mut self, b: usize, v: T, blocks: &mut Blocks) {
        unsafe {
            let f = *self.fill.get_unchecked(b) as usize;
            let line = self.lines.get_unchecked_mut(b);
            *line.0.get_unchecked_mut(f) = v;
            if f + 1 == R {
                self.bkts.get_unchecked_mut(b).push_line(line, blocks);
                *self.fill.get_unchecked_mut(b) = 0;
            } else {
                *self.fill.get_unchecked_mut(b) = (f + 1) as u8;
            }
        }
    }
    /// Flush the partial lines and return each bucket's segments.
    pub fn finish(mut self, blocks: &mut Blocks) -> Vec<Vec<Seg<T>>> {
        for b in 0..self.bkts.len() {
            for i in 0..self.fill[b] as usize {
                let v = self.lines[b].0[i];
                self.bkts[b].push(v, blocks);
            }
        }
        store_fence();
        self.bkts.into_iter().map(|b| b.finish()).collect()
    }
}
