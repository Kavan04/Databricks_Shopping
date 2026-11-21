//! Shopping Sessions pipeline. See rules.md for the exact spec.
//!
//! Layout of the run:
//!   0. mmap every input file, split into newline-aligned chunks
//!   1. links: parse, union-find over 64-bit string hashes, person key = hash(smallest id)
//!   2. events/clicks: parse + validate in parallel; events are scattered by
//!      event_id hash, clicks by person hash
//!   3. per event_id bucket: dedup (smallest ingest_ts), scatter survivors by person
//!   4. per person bucket: sort, sessionize, attribute, aggregate
//!   5. write parquet
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering::{Acquire, Relaxed, Release}};
use std::sync::Mutex;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use memmap2::Mmap;
use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use xxhash_rust::xxh3::xxh3_64;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const LATE: i64 = DAY;
const B1: usize = 512;
const B1_SHIFT: u32 = 64 - 9;
const B2: usize = 1024;
const B2_SHIFT: u32 = 64 - 10;
const CHUNK: usize = 8 << 20;
const NONE_CAT: u16 = u16::MAX;

// ---------------------------------------------------------------- hashing

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = self.0.rotate_left(8) ^ x as u64;
        }
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}
type IdMap<V> = HashMap<u64, V, BuildHasherDefault<IdHasher>>;

#[inline(always)]
fn h64(b: &[u8]) -> u64 {
    xxh3_64(b)
}

// ---------------------------------------------------------------- block arena

const BLK: usize = 16 << 10;
const SEG_BYTES: usize = 1 << 30;
const MAX_SEGS: usize = 96;

/// Lock-free bump allocator of fixed 16 KB blocks over lazily mapped 1 GB segments.
/// Nothing is ever freed, so there is no realloc copying and no mmap/munmap churn.
struct Arena {
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
        if std::env::var_os("USE_THP").is_some() {
            libc::madvise(p, n, libc::MADV_HUGEPAGE);
        }
        p as *mut u8
    }
}

impl Arena {
    fn new() -> Self {
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
        assert!(sg < MAX_SEGS);
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

/// A finished run of records inside the arena.
struct Seg<T> {
    p: *const T,
    n: usize,
}
unsafe impl<T> Send for Seg<T> {}
unsafe impl<T> Sync for Seg<T> {}
impl<T> Seg<T> {
    fn slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.p, self.n) }
    }
}

/// Append-only bucket made of arena blocks.
struct Bkt<T> {
    start: *mut T,
    cur: *mut T,
    end: *mut T,
    done: Vec<Seg<T>>,
}
unsafe impl<T> Send for Bkt<T> {}
impl<T: Copy> Bkt<T> {
    fn new() -> Self {
        let z = std::ptr::null_mut();
        Bkt { start: z, cur: z, end: z, done: Vec::new() }
    }
    #[inline(always)]
    fn push(&mut self, v: T, a: &Arena) {
        if self.cur == self.end {
            self.refill(a);
        }
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
    }
    #[cold]
    fn refill(&mut self, a: &Arena) {
        if !self.start.is_null() {
            self.done.push(Seg { p: self.start, n: unsafe { self.cur.offset_from(self.start) } as usize });
        }
        let p = a.alloc() as *mut T;
        self.start = p;
        self.cur = p;
        self.end = unsafe { p.add(BLK / std::mem::size_of::<T>()) };
    }
    fn finish(mut self) -> Vec<Seg<T>> {
        if !self.start.is_null() && self.cur != self.start {
            self.done.push(Seg { p: self.start, n: unsafe { self.cur.offset_from(self.start) } as usize });
        }
        self.done
    }
}

// ---------------------------------------------------------------- records

/// Event as parsed, before dedup. 40 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev1 {
    eid: u64,
    person: u64,
    /// ts_rel << 24 | price
    w2: u64,
    /// user string ptr << 16 | category rank
    w3: u64,
    /// (ingest_ms + 2^47) << 1 | ok
    w4: u64,
}

/// Surviving event. 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    person: u64,
    w2: u64,
    w3: u64,
}

/// Valid click. 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    person: u64,
    /// ts_rel << 24 | campaign index
    w1: u64,
    /// click_id string ptr
    ptr: u64,
}

#[derive(Clone, Copy)]
struct URow {
    ptr: u64,
    len: u32,
    day: i32,
    sessions: u32,
    events: u32,
    rev: u64,
    top: u16,
}

// ---------------------------------------------------------------- csv helpers

const LO: u64 = 0x0101_0101_0101_0101;
const HI7: u64 = 0x7F7F_7F7F_7F7F_7F7F;

/// 0x80 in every byte of `w` equal to `b`.
#[inline(always)]
fn eq_mask(w: u64, b: u8) -> u64 {
    let c = w ^ (LO * b as u64);
    !(((c & HI7) + HI7) | c | HI7)
}

/// Split a line into exactly N fields (commas always separate), unquoting each.
/// A quote is only legal as the first and last byte of a field.
#[inline(always)]
fn split_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let n = line.len();
    let p = line.as_ptr();
    let mut commas = [0usize; N];
    let mut nc = 0usize;
    let mut quotes = [0usize; 16];
    let mut nq = 0usize;
    let mut i = 0usize;
    while i < n {
        let (w, valid) = if i + 8 <= n {
            (unsafe { (p.add(i) as *const u64).read_unaligned() }, 8)
        } else {
            let mut t = [0u8; 8];
            t[..n - i].copy_from_slice(&line[i..]);
            (u64::from_le_bytes(t), n - i)
        };
        let mut mc = eq_mask(w, b',');
        let mut mq = eq_mask(w, b'"');
        if valid < 8 {
            let keep = (1u64 << (valid * 8)) - 1;
            mc &= keep;
            mq &= keep;
        }
        while mc != 0 {
            if nc >= N - 1 {
                return None;
            }
            commas[nc] = i + (mc.trailing_zeros() >> 3) as usize;
            nc += 1;
            mc &= mc - 1;
        }
        while mq != 0 {
            if nq >= 2 * N {
                return None;
            }
            quotes[nq] = i + (mq.trailing_zeros() >> 3) as usize;
            nq += 1;
            mq &= mq - 1;
        }
        i += 8;
    }
    if nc != N - 1 {
        return None;
    }
    let mut out: [&[u8]; N] = [&line[0..0]; N];
    let mut qi = 0usize;
    let mut s = 0usize;
    for k in 0..N {
        let e = if k < N - 1 { commas[k] } else { n };
        if s < e && line[s] == b'"' {
            if e - s < 2 || qi + 1 >= nq || quotes[qi] != s || quotes[qi + 1] != e - 1 {
                return None;
            }
            qi += 2;
            out[k] = &line[s + 1..e - 1];
        } else {
            if qi < nq && quotes[qi] < e {
                return None;
            }
            out[k] = &line[s..e];
        }
        s = e + 1;
    }
    if qi != nq {
        return None;
    }
    Some(out)
}

fn for_each_line<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let n = data.len();
    let mut pos = 0;
    while pos < n {
        let end = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => n,
        };
        f(&data[pos..end]);
        pos = end + 1;
    }
}

/// Body of a CSV file: everything after the first line.
fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &data[data.len()..],
    }
}

/// Length of a string that starts at `ptr` and ends at the next ',' or '"'.
fn delim_len(ptr: u64) -> u32 {
    let p = ptr as *const u8;
    let mut n = 0usize;
    unsafe {
        loop {
            let c = *p.add(n);
            if c == b',' || c == b'"' || c == b'\n' {
                break;
            }
            n += 1;
        }
    }
    n as u32
}

fn raw_slice(ptr: u64, len: u32) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
}

// ---------------------------------------------------------------- timestamps

#[inline(always)]
fn dig(b: u8) -> Option<u32> {
    let d = b.wrapping_sub(b'0');
    if d < 10 {
        Some(d as u32)
    } else {
        None
    }
}
#[inline(always)]
fn p2(a: u8, b: u8) -> Option<u32> {
    Some(dig(a)? * 10 + dig(b)?)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = ((m + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

/// Parse F1/F2/F3 into epoch ms. `None` is a malformed timestamp. A digits-only
/// value too long for the window comes back as i64::MAX (out of every window).
#[inline]
fn parse_ts(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if n == 0 {
        return None;
    }
    if f.iter().all(|b| b.is_ascii_digit()) {
        let mut i = 0;
        while i < n && f[i] == b'0' {
            i += 1;
        }
        let rest = &f[i..];
        if rest.len() > 18 {
            return Some(i64::MAX);
        }
        let mut v: i64 = 0;
        for &b in rest {
            v = v * 10 + (b - b'0') as i64;
        }
        return Some(v);
    }
    if n != 20 && n != 25 {
        return None;
    }
    let y = dig(f[0])? * 1000 + dig(f[1])? * 100 + dig(f[2])? * 10 + dig(f[3])?;
    if f[4] != b'-' || f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let mo = p2(f[5], f[6])?;
    let d = p2(f[8], f[9])?;
    let h = p2(f[11], f[12])?;
    let mi = p2(f[14], f[15])?;
    let s = p2(f[17], f[18])?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let off: i64 = if n == 20 {
        if f[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match f[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if f[22] != b':' {
            return None;
        }
        let oh = p2(f[20], f[21])?;
        let om = p2(f[23], f[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh as i64 * 3600 + om as i64 * 60)
    };
    let secs = days_from_civil(y as i64, mo, d) * 86400 + h as i64 * 3600 + mi as i64 * 60 + s as i64 - off;
    Some(secs * 1000)
}

/// Purchase price: ASCII digits, value 1..=10_000_000, leading zeros allowed.
#[inline]
fn parse_price(f: &[u8]) -> Option<u64> {
    if f.is_empty() || !f.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < f.len() && f[i] == b'0' {
        i += 1;
    }
    let rest = &f[i..];
    if rest.is_empty() || rest.len() > 8 {
        return None;
    }
    let mut v = 0u64;
    for &b in rest {
        v = v * 10 + (b - b'0') as u64;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------- context

struct Ctx {
    ws: i64,
    we: i64,
    /// ws - 7 days; every stored ts is relative to this
    base: i64,
    day0: i64,
    ndays: usize,
    /// non-minimal link ids -> person key (hash of the person's smallest id)
    links: IdMap<u64>,
    /// person key -> name, for every person that has at least one link
    lnames: IdMap<(u64, u32)>,
    camps: IdMap<u32>,
    prods: IdMap<u16>,
    unknown_cat: u16,
}

impl Ctx {
    #[inline(always)]
    fn person_of(&self, user: &[u8]) -> u64 {
        let h = h64(user);
        match self.links.get(&h) {
            Some(&k) => k,
            None => h,
        }
    }
}

struct Task {
    data: &'static [u8],
    kind: u8, // 0 events, 1 clicks, 2 links
}

struct S1 {
    ev: Vec<Vec<Seg<Ev1>>>,
    ck: Vec<Vec<Seg<Ck>>>,
}

#[derive(Clone, Copy)]
struct LinkRec {
    ha: u64,
    hb: u64,
    pa: u64,
    la: u32,
    pb: u64,
    lb: u32,
}

// ---------------------------------------------------------------- parsing

#[inline(always)]
fn parse_event(ctx: &Ctx, line: &[u8]) -> Option<Ev1> {
    let f = split_fields::<7>(line)?;
    if f[0].is_empty() {
        return None;
    }
    let eid = h64(f[0]);
    let ing = parse_ts(f[3]).unwrap_or(i64::MAX);
    let mut ok = !f[1].is_empty() && !f[5].is_empty();
    let mut ts_rel = 0u64;
    match parse_ts(f[2]) {
        Some(t) if t >= ctx.ws && t < ctx.we => {
            if ing.saturating_sub(t) > LATE {
                ok = false;
            }
            ts_rel = (t - ctx.base) as u64;
        }
        _ => ok = false,
    }
    let mut price = 0u64;
    let mut cat = 0u16;
    match f[4] {
        b"view" | b"cart" => {}
        b"purchase" => match parse_price(f[6]) {
            Some(p) => {
                price = p;
                cat = match ctx.prods.get(&h64(f[5])) {
                    Some(&c) => c,
                    None => ctx.unknown_cat,
                };
            }
            None => ok = false,
        },
        _ => ok = false,
    }
    let person = if ok { ctx.person_of(f[1]) } else { 0 };
    let ingc = ing.clamp(-(1 << 47), (1 << 47) - 1) + (1 << 47);
    Some(Ev1 {
        eid,
        person,
        w2: (ts_rel << 24) | price,
        w3: ((f[1].as_ptr() as u64) << 16) | cat as u64,
        w4: ((ingc as u64) << 1) | ok as u64,
    })
}

#[inline(always)]
fn parse_click(ctx: &Ctx, line: &[u8]) -> Option<Ck> {
    let f = split_fields::<4>(line)?;
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let t = parse_ts(f[3])?;
    if t < ctx.base || t >= ctx.we {
        return None;
    }
    let camp = *ctx.camps.get(&h64(f[2]))?;
    Some(Ck {
        person: ctx.person_of(f[1]),
        w1: (((t - ctx.base) as u64) << 24) | camp as u64,
        ptr: f[0].as_ptr() as u64,
    })
}

/// Ask the kernel to start reading a mapped range into the page cache.
fn prefetch(d: &[u8]) {
    madv(d, libc::MADV_WILLNEED);
}

fn madv(d: &[u8], advice: libc::c_int) {
    let a = d.as_ptr() as usize;
    let start = a & !4095;
    let len = (a + d.len() - start + 4095) & !4095;
    unsafe {
        libc::madvise(start as *mut libc::c_void, len, advice);
    }
}

fn stage1(ctx: &Ctx, arena: &Arena, tasks: &[Task], nt: usize) -> Vec<S1> {
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut ev: Vec<Bkt<Ev1>> = (0..B1).map(|_| Bkt::new()).collect();
                    let mut ck: Vec<Bkt<Ck>> = (0..B2).map(|_| Bkt::new()).collect();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= tasks.len() {
                            break;
                        }
                        if let Some(ahead) = tasks.get(i + 2 * nt) {
                            prefetch(ahead.data);
                        }
                        let t = &tasks[i];
                        if t.kind == 0 {
                            for_each_line(t.data, |l| {
                                if let Some(e) = parse_event(ctx, l) {
                                    ev[(e.eid >> B1_SHIFT) as usize].push(e, arena);
                                }
                            });
                        } else {
                            for_each_line(t.data, |l| {
                                if let Some(c) = parse_click(ctx, l) {
                                    ck[(c.person >> B2_SHIFT) as usize].push(c, arena);
                                }
                            });
                        }
                    }
                    S1 {
                        ev: ev.into_iter().map(|b| b.finish()).collect(),
                        ck: ck.into_iter().map(|b| b.finish()).collect(),
                    }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- stage 2: dedup

fn stage2(s1: &[S1], arena: &Arena, nt: usize) -> Vec<Vec<Vec<Seg<Ev>>>> {
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut out: Vec<Bkt<Ev>> = (0..B2).map(|_| Bkt::new()).collect();
                    let mut rows: Vec<Ev1> = Vec::new();
                    let mut table: Vec<u32> = Vec::new();
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= B1 {
                            break;
                        }
                        rows.clear();
                        for w in s1 {
                            for sg in &w.ev[b] {
                                rows.extend_from_slice(sg.slice());
                            }
                        }
                        let n = rows.len();
                        if n == 0 {
                            continue;
                        }
                        let cap = (2 * n).next_power_of_two().max(16);
                        table.clear();
                        table.resize(cap, 0);
                        let mask = cap - 1;
                        for i in 0..n {
                            let r = &rows[i];
                            let mut sl = (r.eid as usize) & mask;
                            loop {
                                let t = table[sl];
                                if t == 0 {
                                    table[sl] = i as u32 + 1;
                                    break;
                                }
                                let j = (t - 1) as usize;
                                if rows[j].eid == r.eid {
                                    if (r.w4 >> 1) < (rows[j].w4 >> 1) {
                                        table[sl] = i as u32 + 1;
                                    }
                                    break;
                                }
                                sl = (sl + 1) & mask;
                            }
                        }
                        for &t in table.iter() {
                            if t != 0 {
                                let r = &rows[(t - 1) as usize];
                                if r.w4 & 1 == 1 {
                                    out[(r.person >> B2_SHIFT) as usize].push(
                                        Ev { person: r.person, w2: r.w2, w3: r.w3 },
                                        arena,
                                    );
                                }
                            }
                        }
                    }
                    out.into_iter().map(|b| b.finish()).collect::<Vec<_>>()
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- stage 3: sessions

struct W3 {
    /// [(camp * ndays + day) * 4 + {sessions, purchasing, revenue, assisted}]
    cstat: Vec<u64>,
    urows: Vec<URow>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    present: Vec<u32>,
    catsum: Vec<u64>,
    touched: Vec<u16>,
}

impl W3 {
    #[inline(always)]
    fn add_click(&mut self, c: usize) {
        if self.cnt[c] == 0 {
            self.pos[c] = self.present.len() as u32;
            self.present.push(c as u32);
        }
        self.cnt[c] += 1;
    }
    #[inline(always)]
    fn remove_click(&mut self, c: usize) {
        self.cnt[c] -= 1;
        if self.cnt[c] == 0 {
            let p = self.pos[c] as usize;
            let last = *self.present.last().unwrap();
            self.present.swap_remove(p);
            if last as usize != c {
                self.pos[last as usize] = p as u32;
            }
        }
    }
}

#[inline(always)]
fn ck_ts(c: &Ck) -> i64 {
    (c.w1 >> 24) as i64
}
#[inline(always)]
fn ck_camp(c: &Ck) -> usize {
    (c.w1 & 0xFF_FFFF) as usize
}

fn click_id(c: &Ck) -> &'static [u8] {
    raw_slice(c.ptr, delim_len(c.ptr))
}

fn person_proc(w: &mut W3, ctx: &Ctx, evs: &[Ev], cks: &[Ck]) {
    let ncamp = ctx.camps.len();
    let ndays = ctx.ndays;
    let (nptr, nlen) = match ctx.lnames.get(&evs[0].person) {
        Some(&x) => x,
        None => {
            let p = evs[0].w3 >> 16;
            (p, delim_len(p))
        }
    };
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut d_s, mut d_e, mut d_r) = (0u32, 0u32, 0u64);
    let mut i = 0;
    while i < evs.len() {
        let start = (evs[i].w2 >> 24) as i64;
        let mut j = i;
        let mut prev = start;
        let mut rev = 0u64;
        let mut np = 0u32;
        while j < evs.len() {
            let t = (evs[j].w2 >> 24) as i64;
            if t - prev > GAP {
                break;
            }
            prev = t;
            let price = evs[j].w2 & 0xFF_FFFF;
            if price > 0 {
                rev += price;
                np += 1;
            }
            j += 1;
        }
        let day = (ctx.base + start).div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(w, nptr, nlen, cur_day, d_s, d_e, d_r);
            }
            cur_day = day;
            d_s = 0;
            d_e = 0;
            d_r = 0;
        }
        d_s += 1;
        d_e += (j - i) as u32;
        d_r += rev;
        if np > 0 {
            for e in &evs[i..j] {
                let price = e.w2 & 0xFF_FFFF;
                if price > 0 {
                    let cat = (e.w3 & 0xFFFF) as usize;
                    if w.catsum[cat] == 0 {
                        w.touched.push(cat as u16);
                    }
                    w.catsum[cat] += price;
                }
            }
        }

        // clicks
        while hi < cks.len() && ck_ts(&cks[hi]) <= start {
            w.add_click(ck_camp(&cks[hi]));
            hi += 1;
        }
        while lo < hi && ck_ts(&cks[lo]) < start - WEEK {
            w.remove_click(ck_camp(&cks[lo]));
            lo += 1;
        }
        let credited = if hi > lo {
            let mut best = hi - 1;
            let tb = ck_ts(&cks[best]);
            let mut k = best;
            while k > lo && ck_ts(&cks[k - 1]) == tb {
                k -= 1;
                if click_id(&cks[k]) < click_id(&cks[best]) {
                    best = k;
                }
            }
            ck_camp(&cks[best])
        } else {
            ncamp
        };
        let di = (day - ctx.day0) as usize;
        let base_i = (credited * ndays + di) * 4;
        w.cstat[base_i] += 1;
        if np > 0 {
            w.cstat[base_i + 1] += 1;
        }
        w.cstat[base_i + 2] += rev;
        for k in 0..w.present.len() {
            let c = w.present[k] as usize;
            w.cstat[(c * ndays + di) * 4 + 3] += 1;
        }
        i = j;
    }
    if cur_day != i64::MIN {
        flush_day(w, nptr, nlen, cur_day, d_s, d_e, d_r);
    }
    while lo < hi {
        w.remove_click(ck_camp(&cks[lo]));
        lo += 1;
    }
}

fn flush_day(w: &mut W3, nptr: u64, nlen: u32, day: i64, s: u32, e: u32, r: u64) {
    let mut top = NONE_CAT;
    let mut best = 0u64;
    for &c in w.touched.iter() {
        let v = w.catsum[c as usize];
        if v > best || (v == best && c < top) {
            best = v;
            top = c;
        }
    }
    for &c in w.touched.iter() {
        w.catsum[c as usize] = 0;
    }
    w.touched.clear();
    w.urows.push(URow { ptr: nptr, len: nlen, day: day as i32, sessions: s, events: e, rev: r, top });
}

fn stage3(ctx: &Ctx, s1: &[S1], s2: &[Vec<Vec<Seg<Ev>>>], nt: usize) -> Vec<W3> {
    let ncamp = ctx.camps.len();
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut w = W3 {
                        cstat: vec![0; (ncamp + 1) * ctx.ndays * 4],
                        urows: Vec::new(),
                        cnt: vec![0; ncamp + 1],
                        pos: vec![0; ncamp + 1],
                        present: Vec::new(),
                        catsum: vec![0; 65536],
                        touched: Vec::new(),
                    };
                    let mut ev: Vec<Ev> = Vec::new();
                    let mut ck: Vec<Ck> = Vec::new();
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= B2 {
                            break;
                        }
                        ev.clear();
                        ck.clear();
                        for o in s2 {
                            for sg in &o[b] {
                                ev.extend_from_slice(sg.slice());
                            }
                        }
                        for o in s1 {
                            for sg in &o.ck[b] {
                                ck.extend_from_slice(sg.slice());
                            }
                        }
                        if ev.is_empty() {
                            continue;
                        }
                        ev.sort_unstable_by_key(|e| (e.person, e.w2));
                        ck.sort_unstable_by_key(|c| (c.person, c.w1));
                        let mut ci = 0;
                        let mut i = 0;
                        while i < ev.len() {
                            let p = ev[i].person;
                            let mut j = i;
                            while j < ev.len() && ev[j].person == p {
                                j += 1;
                            }
                            while ci < ck.len() && ck[ci].person < p {
                                ci += 1;
                            }
                            let mut cj = ci;
                            while cj < ck.len() && ck[cj].person == p {
                                cj += 1;
                            }
                            person_proc(&mut w, ctx, &ev[i..j], &ck[ci..cj]);
                            ci = cj;
                            i = j;
                        }
                    }
                    w
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- parquet

fn props(dict_cols: &[&str], all: &[&str]) -> Arc<WriterProperties> {
    let mut b = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(false)
        .set_max_row_group_size(1 << 20);
    for c in all {
        b = b.set_column_encoding((*c).into(), Encoding::PLAIN);
    }
    for c in dict_cols {
        b = b.set_column_dictionary_enabled((*c).into(), true);
    }
    Arc::new(b.build())
}

fn write_user(path: &Path, rows: &[URow], cats: &[&'static [u8]]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily {
               REQUIRED BINARY user_id (STRING);
               REQUIRED INT32 day (DATE);
               REQUIRED INT64 sessions;
               REQUIRED INT64 events;
               REQUIRED INT64 revenue_cents;
               OPTIONAL BINARY top_category (STRING);
             }",
        )
        .unwrap(),
    );
    let p = props(&["top_category"], &["user_id", "day", "sessions", "events", "revenue_cents"]);
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, p).unwrap();
    for part in rows.chunks(1 << 20) {
        let mut rg = w.next_row_group().unwrap();
        // user_id
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<ByteArray> = part.iter().map(|r| ByteArray::from(raw_slice(r.ptr, r.len).to_vec())).collect();
            c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<i32> = part.iter().map(|r| r.day).collect();
            c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<i64> = part.iter().map(|r| r.sessions as i64).collect();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<i64> = part.iter().map(|r| r.events as i64).collect();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<i64> = part.iter().map(|r| r.rev as i64).collect();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let mut vals: Vec<ByteArray> = Vec::new();
            let mut defs: Vec<i16> = Vec::with_capacity(part.len());
            for r in part {
                if r.top == NONE_CAT {
                    defs.push(0);
                } else {
                    defs.push(1);
                    vals.push(ByteArray::from(cats[r.top as usize].to_vec()));
                }
            }
            c.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).unwrap();
            c.close().unwrap();
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

struct CRow {
    id: Vec<u8>,
    ch: Vec<u8>,
    day: i32,
    v: [u64; 4],
}

fn write_campaign(path: &Path, rows: &[CRow]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily {
               REQUIRED BINARY campaign_id (STRING);
               REQUIRED BINARY channel (STRING);
               REQUIRED INT32 day (DATE);
               REQUIRED INT64 sessions;
               REQUIRED INT64 purchasing_sessions;
               REQUIRED INT64 revenue_cents;
               REQUIRED INT64 assisted_sessions;
             }",
        )
        .unwrap(),
    );
    let p = props(&[], &["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"]);
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, p).unwrap();
    let mut rg = w.next_row_group().unwrap();
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.id.clone())).collect();
        c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.ch.clone())).collect();
        c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
        c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    for k in 0..4 {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<i64> = rows.iter().map(|r| r.v[k] as i64).collect();
        c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    rg.close().unwrap();
    w.close().unwrap();
}

// ---------------------------------------------------------------- main

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().map_or(false, |x| x == "csv"))
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<&'static [u8]> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f).ok()? };
    let m: &'static Mmap = Box::leak(Box::new(m));
    Some(&m[..])
}

fn push_chunks(tasks: &mut Vec<Task>, data: &'static [u8], kind: u8) {
    let b = body(data);
    let mut s = 0;
    while s < b.len() {
        let mut e = (s + CHUNK).min(b.len());
        if e < b.len() {
            e = match memchr::memchr(b'\n', &b[e - 1..]) {
                Some(i) => e - 1 + i + 1,
                None => b.len(),
            };
        }
        tasks.push(Task { data: &b[s..e], kind });
        s = e;
    }
}

fn json_str(txt: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = txt.find(&k).expect("manifest key") + k.len();
    let rest = &txt[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = rest[a..].find('"').unwrap() + a;
    rest[a..b].to_string()
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), what);
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .max(1);

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - WEEK;
    assert!(we - base < (1i64 << 39));
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // products
    let mut cat_set: BTreeSet<&'static [u8]> = BTreeSet::new();
    let mut prod_rows: Vec<(u64, &'static [u8])> = Vec::new();
    if let Some(d) = map_file(&data_dir.join("products.csv")) {
        for_each_line(body(d), |l| {
            if let Some(f) = split_fields::<2>(l) {
                prod_rows.push((h64(f[0]), f[1]));
                cat_set.insert(f[1]);
            }
        });
    }
    cat_set.insert(b"unknown");
    let cats: Vec<&'static [u8]> = cat_set.into_iter().collect();
    assert!(cats.len() < 60000);
    let unknown_cat = cats.iter().position(|c| *c == b"unknown").unwrap() as u16;
    let mut prods: IdMap<u16> = IdMap::default();
    for (h, c) in prod_rows {
        let r = cats.iter().position(|x| *x == c).unwrap() as u16;
        prods.insert(h, r);
    }

    // campaigns
    let mut camps: IdMap<u32> = IdMap::default();
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_ch: Vec<&'static [u8]> = Vec::new();
    if let Some(d) = map_file(&data_dir.join("campaigns.csv")) {
        for_each_line(body(d), |l| {
            if let Some(f) = split_fields::<2>(l) {
                if !f[0].is_empty() {
                    let h = h64(f[0]);
                    if !camps.contains_key(&h) {
                        camps.insert(h, camp_ids.len() as u32);
                        camp_ids.push(f[0]);
                        camp_ch.push(f[1]);
                    }
                }
            }
        });
    }
    assert!(camps.len() < (1 << 24) - 1);
    lap("small tables");

    // files
    let mut ev_tasks = Vec::new();
    let mut ck_tasks = Vec::new();
    let mut lk_tasks = Vec::new();
    for p in list_csv(&data_dir.join("events")) {
        if let Some(d) = map_file(&p) {
            push_chunks(&mut ev_tasks, d, 0);
        }
    }
    for p in list_csv(&data_dir.join("clicks")) {
        if let Some(d) = map_file(&p) {
            push_chunks(&mut ck_tasks, d, 1);
        }
    }
    for p in list_csv(&data_dir.join("links")) {
        if let Some(d) = map_file(&p) {
            push_chunks(&mut lk_tasks, d, 2);
        }
    }
    lap("mapped");

    // links
    let recs: Vec<LinkRec> = {
        let next = AtomicUsize::new(0);
        let lt = &lk_tasks;
        let parts: Vec<Vec<LinkRec>> = thread::scope(|s| {
            let hs: Vec<_> = (0..nt)
                .map(|_| {
                    s.spawn(|| {
                        let mut v = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Relaxed);
                            if i >= lt.len() {
                                break;
                            }
                            for_each_line(lt[i].data, |l| {
                                if let Some(f) = split_fields::<2>(l) {
                                    if !f[0].is_empty() && !f[1].is_empty() {
                                        v.push(LinkRec {
                                            ha: h64(f[0]),
                                            hb: h64(f[1]),
                                            pa: f[0].as_ptr() as u64,
                                            la: f[0].len() as u32,
                                            pb: f[1].as_ptr() as u64,
                                            lb: f[1].len() as u32,
                                        });
                                    }
                                }
                            });
                        }
                        v
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        parts.into_iter().flatten().collect()
    };
    lap("links parsed");
    let mut links: IdMap<u64> = IdMap::default();
    let mut lnames: IdMap<(u64, u32)> = IdMap::default();
    {
        let mut idx: IdMap<u32> = IdMap::default();
        idx.reserve(recs.len() * 2);
        let mut node_h: Vec<u64> = Vec::new();
        let mut node_s: Vec<(u64, u32)> = Vec::new();
        let mut parent: Vec<u32> = Vec::new();
        let mut get = |h: u64, p: u64, l: u32, parent: &mut Vec<u32>| -> u32 {
            *idx.entry(h).or_insert_with(|| {
                let i = node_h.len() as u32;
                node_h.push(h);
                node_s.push((p, l));
                parent.push(i);
                i
            })
        };
        for r in &recs {
            let a = get(r.ha, r.pa, r.la, &mut parent);
            let b = get(r.hb, r.pb, r.lb, &mut parent);
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
        let n = node_h.len();
        let mut best = vec![u32::MAX; n];
        for i in 0..n {
            let r = find(&mut parent, i as u32) as usize;
            let b = best[r];
            if b == u32::MAX || raw_slice(node_s[i].0, node_s[i].1) < raw_slice(node_s[b as usize].0, node_s[b as usize].1) {
                best[r] = i as u32;
            }
        }
        links.reserve(n);
        for i in 0..n {
            let r = find(&mut parent, i as u32) as usize;
            let b = best[r] as usize;
            let key = node_h[b];
            if key != node_h[i] {
                links.insert(node_h[i], key);
            }
            if i == b {
                lnames.insert(key, node_s[b]);
            }
        }
    }
    drop(recs);
    lap("links joined");

    let ctx = Ctx { ws, we, base, day0, ndays, links, lnames, camps, prods, unknown_cat };

    let mut tasks = ev_tasks;
    tasks.extend(ck_tasks);
    for t in tasks.iter().take(2 * nt) {
        prefetch(t.data);
    }
    let arena = Arena::new();
    let s1 = stage1(&ctx, &arena, &tasks, nt);
    lap("stage1 parse");
    let s2 = stage2(&s1, &arena, nt);
    lap("stage2 dedup");
    let w3 = stage3(&ctx, &s1, &s2, nt);
    lap("stage3 sessions");

    // campaign table
    let ncamp = ctx.camps.len();
    let mut cstat = vec![0u64; (ncamp + 1) * ndays * 4];
    for w in &w3 {
        for (a, b) in cstat.iter_mut().zip(w.cstat.iter()) {
            *a += *b;
        }
    }
    let mut crows: Vec<CRow> = Vec::new();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let i = (c * ndays + d) * 4;
            let v = [cstat[i], cstat[i + 1], cstat[i + 2], cstat[i + 3]];
            if v[0] > 0 || v[3] > 0 {
                let (id, ch) = if c == ncamp { (b"organic".to_vec(), b"organic".to_vec()) } else { (camp_ids[c].to_vec(), camp_ch[c].to_vec()) };
                crows.push(CRow { id, ch, day: (day0 + d as i64) as i32, v });
            }
        }
    }

    std::fs::create_dir_all(out_dir.join("user_daily")).unwrap();
    std::fs::create_dir_all(out_dir.join("campaign_daily")).unwrap();
    write_campaign(&out_dir.join("campaign_daily").join("part-0.parquet"), &crows);
    let total: usize = w3.iter().map(|w| w.urows.len()).sum();
    thread::scope(|s| {
        for (i, w) in w3.iter().enumerate() {
            if w.urows.is_empty() && !(i == 0 && total == 0) {
                continue;
            }
            let p = out_dir.join("user_daily").join(format!("part-{}.parquet", i));
            let cats = &cats;
            s.spawn(move || write_user(&p, &w.urows, cats));
        }
    });
    lap("written");
    eprintln!("user rows {} campaign rows {}", total, crows.len());
}
