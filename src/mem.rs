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
