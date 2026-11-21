//! Shopping Sessions pipeline (v3). See rules.md for the spec and
//! docs/ARCHITECTURE.md for the data flow.
//!
//!   A. links  : fully parallel parse, node index and lock-free union-find
//!   B. stage1 : events -> 32-byte records scattered by event_id
//!               clicks -> person resolved (prefetched), scattered by person
//!   C. stage2 : per event_id bucket: dedup in an open-addressing table, resolve
//!               survivors' person (prefetched), scatter 16-byte records by person
//!               into recycled blocks
//!   D. stage3 : per person bucket: radix-scatter into tiny groups, sort those,
//!               sessionize, credit + assists, aggregate
//!   E. output : parquet, one user_daily file per worker
mod keys;
mod links;
mod mem;
mod out;
mod scan;

use std::collections::{BTreeSet, HashMap};

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::thread;
use std::time::Instant;

use keys::*;
use links::LinkTable;
use mem::*;
use out::*;
use scan::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const LATE: i64 = DAY;
const CHUNK: usize = 4 << 20;
const PRICE_MASK: u64 = 0xFF_FFFF;
const CAT_MASK: u64 = 0x1FF;

// ---------------------------------------------------------------- records

/// Parsed event, before dedup. `w2` = ts_rel << 33 | price << 9 | category.
/// `ing` = (ingest_ms + 2^47) << 1 | ok. Invalid copies keep only eid + ing.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct E1 {
    eid: u64,
    user: u64,
    w2: u64,
    ing: u64,
}

/// Surviving event, keyed by its person.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    person: u64,
    w2: u64,
}

/// Valid click. `w1` = ts_rel << 24 | campaign index.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    person: u64,
    w1: u64,
    ckey: u64,
}

struct Ctx {
    ws: i64,
    we: i64,
    /// ws - 7 days; every stored ts is relative to this
    base: i64,
    day0: i64,
    ndays: usize,
    camps: U64Map,
    ncamp: usize,
    prods: U64Map,
    unknown_cat: u64,
    b1bits: u32,
    b2bits: u32,
}

/// Key for small exact-string tables (never 0).
#[inline(always)]
fn skey(s: &[u8]) -> u64 {
    h64(s) | 1
}

#[inline(always)]
fn bucket(k: u64, bits: u32) -> usize {
    (mix(k) >> (64 - bits)) as usize
}

struct S1 {
    ev: Vec<Vec<Seg<E1>>>,
    ck: Vec<Vec<Seg<Ck>>>,
    /// strings of keys that are hashes (rare), for names and click_id ties
    strs: Vec<(u64, Vec<u8>)>,
}

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");
const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

// ---------------------------------------------------------------- stage 1: parse

#[inline(always)]
fn parse_event(ctx: &Ctx, f: &[&[u8]; 7], dc: &mut DateCache, strs: &mut Vec<(u64, Vec<u8>)>) -> Option<E1> {
    if f[0].is_empty() {
        return None;
    }
    let eid = id_key_p(f[0]);
    let ing = ts_p(f[3], dc).unwrap_or(i64::MAX);
    let ingc = (ing.clamp(-(1 << 47), (1 << 47) - 1) + (1 << 47)) as u64;
    let bad = E1 { eid, user: 0, w2: 0, ing: ingc << 1 };
    if f[1].is_empty() || f[5].is_empty() {
        return Some(bad);
    }
    let ty = f[4];
    let purchase = match ty.len() {
        4 => {
            let x = u32::from_le_bytes([ty[0], ty[1], ty[2], ty[3]]);
            if x != VIEW && x != CART {
                return Some(bad);
            }
            false
        }
        8 => {
            if u64::from_le_bytes(ty.try_into().unwrap()) != PURCHASE {
                return Some(bad);
            }
            true
        }
        _ => return Some(bad),
    };
    let mut w2 = match ts_p(f[2], dc) {
        Some(t) if t >= ctx.ws && t < ctx.we && ing.saturating_sub(t) <= LATE => ((t - ctx.base) as u64) << 33,
        _ => return Some(bad),
    };
    if purchase {
        match parse_price(f[6]) {
            Some(p) => {
                let cat = ctx.prods.get(skey(f[5])).unwrap_or(ctx.unknown_cat);
                w2 |= (p << 9) | cat;
            }
            None => return Some(bad),
        }
    }
    let user = id_key_p(f[1]);
    if !is_packed(user) {
        strs.push((user, f[1].to_vec()));
    }
    Some(E1 { eid, user, w2, ing: (ingc << 1) | 1 })
}

/// A valid click with `person` still holding the user's key.
#[inline(always)]
fn parse_click(ctx: &Ctx, f: &[&[u8]; 4], dc: &mut DateCache, strs: &mut Vec<(u64, Vec<u8>)>) -> Option<Ck> {
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let t = ts_p(f[3], dc)?;
    if t < ctx.base || t >= ctx.we {
        return None;
    }
    let camp = ctx.camps.get(skey(f[2]))?;
    let ckey = id_key_p(f[0]);
    if !is_packed(ckey) {
        strs.push((ckey, f[0].to_vec()));
    }
    Some(Ck { person: id_key_p(f[1]), w1: (((t - ctx.base) as u64) << 24) | camp, ckey })
}

const AHEAD: usize = 16;

fn stage1(ctx: &Ctx, arena: &Arena, files: &[InFile], tasks: &[RTask], nt: usize, lt: &LinkTable) -> Vec<S1> {
    let next = AtomicUsize::new(0);
    let (nb1, nb2) = (1usize << ctx.b1bits, 1usize << ctx.b2bits);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut blocks = Blocks::new(arena);
                    let mut ev: Vec<Bkt<E1>> = (0..nb1).map(|_| Bkt::new()).collect();
                    let mut ck: Vec<Bkt<Ck>> = (0..nb2).map(|_| Bkt::new()).collect();
                    let mut strs = Vec::new();
                    let mut dc = DateCache::new();
                    let mut cks: Vec<Ck> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= tasks.len() {
                            break;
                        }
                        if let Some(a) = tasks.get(i + 2 * nt) {
                            prefetch_range(&files[a.file], a.off, a.len);
                        }
                        let t = &tasks[i];
                        let data = lines_in(&files[t.file], t.off, t.len);
                        if t.kind == 0 {
                            for_each_record::<7>(data, |f| {
                                if let Some(e) = parse_event(ctx, f, &mut dc, &mut strs) {
                                    ev[bucket(e.eid, ctx.b1bits)].push(e, &mut blocks);
                                }
                            });
                        } else {
                            cks.clear();
                            for_each_record::<4>(data, |f| {
                                if let Some(c) = parse_click(ctx, f, &mut dc, &mut strs) {
                                    cks.push(c);
                                }
                            });
                            for j in 0..cks.len() {
                                if j + AHEAD < cks.len() {
                                    lt.map.prefetch(cks[j + AHEAD].person);
                                }
                                let mut c = cks[j];
                                c.person = lt.resolve(c.person);
                                ck[bucket(c.person, ctx.b2bits)].push(c, &mut blocks);
                            }
                        }
                    }
                    S1 {
                        ev: ev.into_iter().map(|b| b.finish()).collect(),
                        ck: ck.into_iter().map(|b| b.finish()).collect(),
                        strs,
                    }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- stage 2: dedup

/// Returns, per worker, per person bucket, the segments it wrote.
fn stage2(ctx: &Ctx, s1: &[S1], lt: &LinkTable, arena: &Arena, nt: usize) -> Vec<Vec<Vec<Seg<Ev>>>> {
    let next = AtomicUsize::new(0);
    let (nb1, nb2) = (1usize << ctx.b1bits, 1usize << ctx.b2bits);
    let b1bits = ctx.b1bits;
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut blocks = Blocks::new(arena);
                    let mut out: Vec<Bkt<Ev>> = (0..nb2).map(|_| Bkt::new()).collect();
                    let mut table: Vec<E1> = Vec::new();
                    let mut surv: Vec<(u64, u64)> = Vec::new();
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= nb1 {
                            break;
                        }
                        let n: usize = s1.iter().map(|w| seg_len(&w.ev[b])).sum();
                        if n == 0 {
                            continue;
                        }
                        let cap = (n + n / 3 + 1).next_power_of_two().max(16);
                        let tbits = cap.trailing_zeros();
                        let mask = cap - 1;
                        table.clear();
                        table.resize(cap, E1::default());
                        for w in s1 {
                            for sg in &w.ev[b] {
                                for r in sg.slice() {
                                    let mut i = ((mix(r.eid) << b1bits) >> (64 - tbits)) as usize;
                                    loop {
                                        let t = unsafe { table.get_unchecked_mut(i) };
                                        if t.eid == 0 {
                                            *t = *r;
                                            break;
                                        }
                                        if t.eid == r.eid {
                                            if (r.ing >> 1) < (t.ing >> 1) {
                                                *t = *r;
                                            }
                                            break;
                                        }
                                        i = (i + 1) & mask;
                                    }
                                }
                            }
                        }
                        // this bucket's input blocks are dead now: reuse them for output
                        for w in s1 {
                            blocks.recycle(&w.ev[b]);
                        }
                        surv.clear();
                        for t in &table {
                            if t.ing & 1 == 1 {
                                surv.push((t.user, t.w2));
                            }
                        }
                        for j in 0..surv.len() {
                            if j + AHEAD < surv.len() {
                                lt.map.prefetch(surv[j + AHEAD].0);
                            }
                            let (u, w2) = surv[j];
                            let p = lt.resolve(u);
                            out[bucket(p, ctx.b2bits)].push(Ev { person: p, w2 }, &mut blocks);
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

type Names = HashMap<u64, Vec<u8>>;

fn name_of(k: u64, names: &Names, out: &mut Vec<u8>) {
    if is_packed(k) {
        decode_key(k, out);
    } else {
        out.extend_from_slice(names.get(&k).expect("name for hashed key"));
    }
}

/// click_id byte order
fn key_less(a: u64, b: u64, names: &Names) -> bool {
    if is_packed(a) && is_packed(b) {
        return a < b;
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    name_of(a, names, &mut x);
    name_of(b, names, &mut y);
    x < y
}

fn flush_day(w: &mut W3, person: u64, day: i64, s: u32, e: u32, r: u64) {
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
    w.urows.push(URow { person, rev: r, day: day as i32, sessions: s, events: e, top });
}

fn person_proc(w: &mut W3, ctx: &Ctx, names: &Names, evs: &[Ev], cks: &[Ck]) {
    let ncamp = ctx.ncamp;
    let ndays = ctx.ndays;
    let person = evs[0].person;
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut d_s, mut d_e, mut d_r) = (0u32, 0u32, 0u64);
    let mut i = 0;
    while i < evs.len() {
        let start = (evs[i].w2 >> 33) as i64;
        let mut j = i;
        let mut prev = start;
        let mut rev = 0u64;
        let mut np = 0u32;
        while j < evs.len() {
            let t = (evs[j].w2 >> 33) as i64;
            if t - prev > GAP {
                break;
            }
            prev = t;
            let price = (evs[j].w2 >> 9) & PRICE_MASK;
            rev += price;
            np += (price > 0) as u32;
            j += 1;
        }
        let day = (ctx.base + start).div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(w, person, cur_day, d_s, d_e, d_r);
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
                let price = (e.w2 >> 9) & PRICE_MASK;
                if price > 0 {
                    let cat = (e.w2 & CAT_MASK) as usize;
                    if w.catsum[cat] == 0 {
                        w.touched.push(cat as u16);
                    }
                    w.catsum[cat] += price;
                }
            }
        }

        // clicks in [start - 7d, start]
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
                if key_less(cks[k].ckey, cks[best].ckey, names) {
                    best = k;
                }
            }
            ck_camp(&cks[best])
        } else {
            ncamp
        };
        let di = (day - ctx.day0) as usize;
        let bi = (credited * ndays + di) * 4;
        w.cstat[bi] += 1;
        w.cstat[bi + 1] += (np > 0) as u64;
        w.cstat[bi + 2] += rev;
        for k in 0..w.present.len() {
            let c = w.present[k] as usize;
            w.cstat[(c * ndays + di) * 4 + 3] += 1;
        }
        i = j;
    }
    if cur_day != i64::MIN {
        flush_day(w, person, cur_day, d_s, d_e, d_r);
    }
    while lo < hi {
        w.remove_click(ck_camp(&cks[lo]));
        lo += 1;
    }
}

/// Sort a slice by key; tiny slices (the common case) use insertion sort.
#[inline(always)]
fn sort_small<T: Copy, K: Ord>(v: &mut [T], key: impl Fn(&T) -> K) {
    if v.len() <= 16 {
        for i in 1..v.len() {
            let x = v[i];
            let kx = key(&x);
            let mut j = i;
            while j > 0 && key(&v[j - 1]) > kx {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
    } else {
        v.sort_unstable_by_key(key);
    }
}

/// Counting-sort the records of `segs` into `dst` by `sub(record)`; `cnt` ends
/// up holding each group's start (nsub + 1 entries).
#[inline(always)]
fn scatter<T: Copy>(segs: &[&[Seg<T>]], n: usize, nsub: usize, sub: impl Fn(&T) -> usize, dst: &mut Vec<T>, cnt: &mut Vec<u32>, pos: &mut Vec<u32>) {
    cnt.clear();
    cnt.resize(nsub + 1, 0);
    for ss in segs {
        for sg in ss.iter() {
            for r in sg.slice() {
                cnt[sub(r) + 1] += 1;
            }
        }
    }
    for i in 0..nsub {
        cnt[i + 1] += cnt[i];
    }
    pos.clear();
    pos.extend_from_slice(&cnt[..nsub]);
    dst.clear();
    dst.reserve(n);
    let d = dst.as_mut_ptr();
    for ss in segs {
        for sg in ss.iter() {
            for r in sg.slice() {
                let j = sub(r);
                unsafe { d.add(pos[j] as usize).write(*r) };
                pos[j] += 1;
            }
        }
    }
    unsafe { dst.set_len(n) };
}

fn stage3(ctx: &Ctx, names: &Names, s1: &[S1], s2: &[Vec<Vec<Seg<Ev>>>], nt: usize, ncat: usize) -> Vec<W3> {
    let ncamp = ctx.ncamp;
    let nb2 = 1usize << ctx.b2bits;
    let b2bits = ctx.b2bits;
    // biggest buckets first, so a whale's bucket does not finish last
    let mut order: Vec<(usize, usize)> = (0..nb2).map(|b| (s2.iter().map(|o| seg_len(&o[b])).sum(), b)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
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
                        catsum: vec![0; ncat.max(1)],
                        touched: Vec::new(),
                    };
                    let mut ev: Vec<Ev> = Vec::new();
                    let mut ck: Vec<Ck> = Vec::new();
                    let (mut ecnt, mut ccnt, mut pos) = (Vec::new(), Vec::new(), Vec::new());
                    loop {
                        let k = next.fetch_add(1, Relaxed);
                        if k >= nb2 {
                            break;
                        }
                        let (ne, b) = order[k];
                        if ne == 0 {
                            continue;
                        }
                        // groups of ~4 events: same person -> same group
                        let sbits = log2_ceil(ne / 4).clamp(1, 20);
                        let nsub = 1usize << sbits;
                        let sh = 64 - sbits;
                        let sub = |p: u64| ((mix(p) << b2bits) >> sh) as usize;
                        let esegs: Vec<&[Seg<Ev>]> = s2.iter().map(|o| &o[b][..]).collect();
                        scatter(&esegs, ne, nsub, |e: &Ev| sub(e.person), &mut ev, &mut ecnt, &mut pos);
                        let csegs: Vec<&[Seg<Ck>]> = s1.iter().map(|o| &o.ck[b][..]).collect();
                        let nc: usize = csegs.iter().map(|x| seg_len(x)).sum();
                        scatter(&csegs, nc, nsub, |c: &Ck| sub(c.person), &mut ck, &mut ccnt, &mut pos);
                        for j in 0..nsub {
                            let (es, ee) = (ecnt[j] as usize, ecnt[j + 1] as usize);
                            if es == ee {
                                continue;
                            }
                            let evs = &mut ev[es..ee];
                            sort_small(evs, |e| (e.person, e.w2));
                            let cks = &mut ck[ccnt[j] as usize..ccnt[j + 1] as usize];
                            sort_small(cks, |c| (c.person, c.w1));
                            let mut ci = 0;
                            let mut i = 0;
                            while i < evs.len() {
                                let p = evs[i].person;
                                let mut e = i + 1;
                                while e < evs.len() && evs[e].person == p {
                                    e += 1;
                                }
                                while ci < cks.len() && cks[ci].person < p {
                                    ci += 1;
                                }
                                let mut ce = ci;
                                while ce < cks.len() && cks[ce].person == p {
                                    ce += 1;
                                }
                                person_proc(&mut w, ctx, names, &evs[i..e], &cks[ci..ce]);
                                ci = ce;
                                i = e;
                            }
                        }
                    }
                    w
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
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

fn json_str(txt: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = txt.find(&k).expect("manifest key") + k.len();
    let rest = &txt[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = rest[a..].find('"').unwrap() + a;
    rest[a..b].to_string()
}

fn log2_ceil(x: usize) -> u32 {
    x.max(1).next_power_of_two().trailing_zeros()
}

/// user + system CPU seconds used by the process so far
fn cpu_secs() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 * 1e-6;
    t(ru.ru_utime) + t(ru.ru_stime)
}

/// Open every CSV of `dir` and cut it into `chunk`-byte ranges of kind `kind`.
fn add_files(dir: &Path, kind: u8, chunk: usize, files: &mut Vec<InFile>, tasks: &mut Vec<RTask>) -> usize {
    let mut bytes = 0;
    for p in list_csv(dir) {
        let Some(f) = InFile::open(&p) else { continue };
        let size = f.size;
        bytes += size;
        let fi = files.len();
        files.push(f);
        let mut off = 0;
        while off < size {
            let len = chunk.min(size - off);
            tasks.push(RTask { file: fi, off, len, kind });
            off += len;
        }
    }
    bytes
}

fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("[{:7.3}s] {} cpu={:.3}", t0.elapsed().as_secs_f64(), what, cpu_secs());
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .max(1);
    let chunk: usize = std::env::var("FM_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(CHUNK).max(1);

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - WEEK;
    assert!(we - base < (1i64 << 31), "window too long for the record layout");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // products -> category rank (byte order, "unknown" included)
    let prod_file = Padded::read(&data_dir.join("products.csv"));
    let mut cat_set: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut prod_rows: Vec<(u64, Vec<u8>)> = Vec::new();
    if let Some(d) = &prod_file {
        for_each_record::<2>(d.body(), |f| {
            prod_rows.push((skey(f[0]), f[1].to_vec()));
            cat_set.insert(f[1].to_vec());
        });
    }
    cat_set.insert(b"unknown".to_vec());
    let cats: Vec<Vec<u8>> = cat_set.into_iter().collect();
    assert!(cats.len() <= CAT_MASK as usize, "too many categories for the record layout");
    let cat_rank: HashMap<&[u8], u64> = cats.iter().enumerate().map(|(i, c)| (&c[..], i as u64)).collect();
    let unknown_cat = cat_rank[&b"unknown"[..]];
    let mut prods = U64Map::with_capacity(prod_rows.len() + 16);
    for (k, c) in &prod_rows {
        prods.get_or_insert(*k, cat_rank[&c[..]]);
    }

    // campaigns
    let mut camps = U64Map::with_capacity(4096);
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_ch: Vec<Vec<u8>> = Vec::new();
    if let Some(d) = Padded::read(&data_dir.join("campaigns.csv")) {
        for_each_record::<2>(d.body(), |f| {
            if !f[0].is_empty() {
                let n = camp_ids.len() as u64;
                if camps.get_or_insert(skey(f[0]), n) == n {
                    camp_ids.push(f[0].to_vec());
                    camp_ch.push(f[1].to_vec());
                }
            }
        });
    }
    let ncamp = camp_ids.len();
    assert!(ncamp < (1 << 24) - 1);

    // inputs: byte ranges read with pread into per-worker buffers
    let mut files: Vec<InFile> = Vec::new();
    let mut tasks: Vec<RTask> = Vec::new();
    let mut lk_tasks: Vec<RTask> = Vec::new();
    let ev_bytes = add_files(&data_dir.join("events"), 0, chunk, &mut files, &mut tasks);
    add_files(&data_dir.join("clicks"), 1, chunk, &mut files, &mut tasks);
    add_files(&data_dir.join("links"), 2, chunk.min(1 << 20), &mut files, &mut lk_tasks);
    let est_rows = ev_bytes / 64 + 1;
    let b1bits = log2_ceil(est_rows / 32768).clamp(6, 11);
    let b2bits = log2_ceil(est_rows / 32768).clamp(6, 11);
    for t in tasks.iter().take(2 * nt) {
        prefetch_range(&files[t.file], t.off, t.len);
    }
    lap("opened");

    let lt = links::build(&files, &lk_tasks, nt);
    lap("links");

    let ctx = Ctx { ws, we, base, day0, ndays, camps, ncamp, prods, unknown_cat, b1bits, b2bits };
    let arena = Arena::new();
    let s1 = stage1(&ctx, &arena, &files, &tasks, nt, &lt);
    lap("stage1 parse");

    let s2 = stage2(&ctx, &s1, &lt, &arena, nt);
    lap("stage2 dedup");

    let mut names: Names = HashMap::new();
    for w in &s1 {
        for (k, s) in &w.strs {
            names.insert(*k, s.clone());
        }
    }
    for (k, s) in &lt.names {
        names.insert(*k, s.clone());
    }
    let w3 = stage3(&ctx, &names, &s1, &s2, nt, cats.len());
    lap("stage3 sessions");

    // campaign table
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
                let (id, ch) = if c == ncamp { (b"organic".to_vec(), b"organic".to_vec()) } else { (camp_ids[c].clone(), camp_ch[c].clone()) };
                crows.push(CRow { id, ch, day: (day0 + d as i64) as i32, v });
            }
        }
    }

    std::fs::create_dir_all(out_dir.join("user_daily")).unwrap();
    std::fs::create_dir_all(out_dir.join("campaign_daily")).unwrap();
    let total: usize = w3.iter().map(|w| w.urows.len()).sum();
    let name = |k: u64, buf: &mut Vec<u8>| name_of(k, &names, buf);
    let cat_refs: Vec<&[u8]> = cats.iter().map(|c| &c[..]).collect();
    thread::scope(|s| {
        s.spawn(|| write_campaign(&out_dir.join("campaign_daily").join("part-0.parquet"), &crows));
        for (i, w) in w3.iter().enumerate() {
            if w.urows.is_empty() && !(i == 0 && total == 0) {
                continue;
            }
            let p = out_dir.join("user_daily").join(format!("part-{}.parquet", i));
            let (cats, name) = (&cat_refs, &name);
            s.spawn(move || write_user(&p, &w.urows, cats, name));
        }
    });
    lap("written");
    eprintln!("user rows {} campaign rows {}", total, crows.len());
}
