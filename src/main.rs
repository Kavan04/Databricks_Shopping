//! Shopping Sessions pipeline (v2). See rules.md for the spec and
//! docs/ARCHITECTURE.md for the data flow.
//!
//!   A. links  : parallel parse -> union-find on its own thread (overlaps B)
//!   B. stage1 : events -> 32-byte records scattered by event_id
//!               clicks -> resolved to their person, scattered by person
//!   C. stage2 : per event_id bucket: dedup in an open-addressing table, resolve
//!               survivors' person (prefetched), scatter 16-byte records by person
//!   D. stage3 : per person bucket: sort, sessionize, credit + assists, aggregate
//!   E. output : parquet, one user_daily file per worker
mod keys;
mod mem;
mod out;
mod scan;

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;

use keys::*;
use mem::*;
use memmap2::Mmap;
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

struct Task {
    data: &'static [u8],
    kind: u8, // 0 events, 1 clicks, 2 links
}

struct S1 {
    ev: Vec<Vec<Seg<E1>>>,
    ck: Vec<Vec<Seg<Ck>>>,
    /// strings of keys that are hashes (rare), for names and click_id ties
    strs: Vec<(u64, &'static [u8])>,
}

// ---------------------------------------------------------------- links

struct LinkRec {
    ka: u64,
    kb: u64,
    sa: &'static [u8],
    sb: &'static [u8],
}

struct LinkTable {
    /// every linked id that is not its person's smallest -> person key
    map: U64Map,
    /// person key -> name, for people whose smallest id is a hashed key
    names: Vec<(u64, &'static [u8])>,
}

impl LinkTable {
    #[inline(always)]
    fn resolve(&self, k: u64) -> u64 {
        self.map.get(k).unwrap_or(k)
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

fn build_links(parts: Vec<Vec<LinkRec>>) -> LinkTable {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let mut idx = U64Map::with_capacity(total + 16);
    let mut node_k: Vec<u64> = Vec::with_capacity(total);
    let mut node_s: Vec<&'static [u8]> = Vec::with_capacity(total);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    for part in &parts {
        for r in part {
            let mut ids = [0u32; 2];
            for (slot, (k, s)) in [(r.ka, r.sa), (r.kb, r.sb)].into_iter().enumerate() {
                let n = node_k.len() as u64;
                let i = idx.get_or_insert(k, n);
                if i == n {
                    node_k.push(k);
                    node_s.push(s);
                    parent.push(n as u32);
                }
                ids[slot] = i as u32;
            }
            let ra = find(&mut parent, ids[0]);
            let rb = find(&mut parent, ids[1]);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let n = node_k.len();
    let less = |i: usize, j: usize| -> bool {
        if is_packed(node_k[i]) && is_packed(node_k[j]) {
            node_k[i] < node_k[j]
        } else {
            node_s[i] < node_s[j]
        }
    };
    let mut best = vec![u32::MAX; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if best[r] == u32::MAX || less(i, best[r] as usize) {
            best[r] = i as u32;
        }
    }
    let mut map = U64Map::with_capacity(n + 16);
    let mut names = Vec::new();
    for i in 0..n {
        let b = best[find(&mut parent, i as u32) as usize] as usize;
        let key = node_k[b];
        if key != node_k[i] {
            map.get_or_insert(node_k[i], key);
        } else if !is_packed(key) {
            names.push((key, node_s[i]));
        }
    }
    LinkTable { map, names }
}

// ---------------------------------------------------------------- stage 1: parse

#[inline(always)]
fn parse_event(ctx: &Ctx, f: &[&'static [u8]; 7], strs: &mut Vec<(u64, &'static [u8])>) -> Option<E1> {
    if f[0].is_empty() {
        return None;
    }
    let eid = id_key(f[0]);
    let ing = parse_ts(f[3]).unwrap_or(i64::MAX);
    let ingc = (ing.clamp(-(1 << 47), (1 << 47) - 1) + (1 << 47)) as u64;
    let bad = E1 { eid, user: 0, w2: 0, ing: ingc << 1 };
    if f[1].is_empty() || f[5].is_empty() {
        return Some(bad);
    }
    let mut w2 = match parse_ts(f[2]) {
        Some(t) if t >= ctx.ws && t < ctx.we && ing.saturating_sub(t) <= LATE => ((t - ctx.base) as u64) << 33,
        _ => return Some(bad),
    };
    match f[4] {
        b"view" | b"cart" => {}
        b"purchase" => match parse_price(f[6]) {
            Some(p) => {
                let cat = ctx.prods.get(skey(f[5])).unwrap_or(ctx.unknown_cat);
                w2 |= (p << 9) | cat;
            }
            None => return Some(bad),
        },
        _ => return Some(bad),
    }
    let user = id_key(f[1]);
    if !is_packed(user) {
        strs.push((user, f[1]));
    }
    Some(E1 { eid, user, w2, ing: (ingc << 1) | 1 })
}

#[inline(always)]
fn parse_click(ctx: &Ctx, lt: &LinkTable, f: &[&'static [u8]; 4], strs: &mut Vec<(u64, &'static [u8])>) -> Option<Ck> {
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let t = parse_ts(f[3])?;
    if t < ctx.base || t >= ctx.we {
        return None;
    }
    let camp = ctx.camps.get(skey(f[2]))?;
    let ckey = id_key(f[0]);
    if !is_packed(ckey) {
        strs.push((ckey, f[0]));
    }
    Some(Ck { person: lt.resolve(id_key(f[1])), w1: (((t - ctx.base) as u64) << 24) | camp, ckey })
}

fn madv_willneed(d: &[u8]) {
    if d.is_empty() {
        return;
    }
    let a = d.as_ptr() as usize;
    let start = a & !4095;
    let len = (a + d.len() - start + 4095) & !4095;
    unsafe {
        libc::madvise(start as *mut libc::c_void, len, libc::MADV_WILLNEED);
    }
}

fn stage1(ctx: &Ctx, arena: &Arena, tasks: &[Task], nt: usize, lt: &OnceLock<LinkTable>) -> Vec<S1> {
    let next = AtomicUsize::new(0);
    let (nb1, nb2) = (1usize << ctx.b1bits, 1usize << ctx.b2bits);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut ev: Vec<Bkt<E1>> = (0..nb1).map(|_| Bkt::new()).collect();
                    let mut ck: Vec<Bkt<Ck>> = (0..nb2).map(|_| Bkt::new()).collect();
                    let mut strs = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= tasks.len() {
                            break;
                        }
                        if let Some(ahead) = tasks.get(i + 2 * nt) {
                            madv_willneed(ahead.data);
                        }
                        let t = &tasks[i];
                        if t.kind == 0 {
                            for_each_record::<7>(t.data, |f| {
                                if let Some(e) = parse_event(ctx, f, &mut strs) {
                                    ev[bucket(e.eid, ctx.b1bits)].push(e, arena);
                                }
                            });
                        } else {
                            let lt = lt.wait();
                            for_each_record::<4>(t.data, |f| {
                                if let Some(c) = parse_click(ctx, lt, f, &mut strs) {
                                    ck[bucket(c.person, ctx.b2bits)].push(c, arena);
                                }
                            });
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
                        let cap = (n + n / 2 + 1).next_power_of_two().max(16);
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
                        surv.clear();
                        for t in &table {
                            if t.ing & 1 == 1 {
                                surv.push((t.user, t.w2));
                            }
                        }
                        const AHEAD: usize = 16;
                        for j in 0..surv.len() {
                            if j + AHEAD < surv.len() {
                                lt.map.prefetch(surv[j + AHEAD].0);
                            }
                            let (u, w2) = surv[j];
                            let p = lt.resolve(u);
                            out[bucket(p, ctx.b2bits)].push(Ev { person: p, w2 }, arena);
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

type Names = HashMap<u64, &'static [u8]>;

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

fn stage3(ctx: &Ctx, names: &Names, s1: &[S1], s2: &[Vec<Vec<Seg<Ev>>>], nt: usize, ncat: usize) -> Vec<W3> {
    let ncamp = ctx.ncamp;
    let nb2 = 1usize << ctx.b2bits;
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
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= nb2 {
                            break;
                        }
                        ev.clear();
                        ck.clear();
                        for o in s2 {
                            for sg in &o[b] {
                                ev.extend_from_slice(sg.slice());
                            }
                        }
                        if ev.is_empty() {
                            continue;
                        }
                        for o in s1 {
                            for sg in &o.ck[b] {
                                ck.extend_from_slice(sg.slice());
                            }
                        }
                        ev.sort_unstable_by_key(|e| (e.person, e.w2));
                        ck.sort_unstable_by_key(|c| (c.person, c.w1));
                        let mut ci = 0;
                        let mut i = 0;
                        while i < ev.len() {
                            let p = ev[i].person;
                            let mut j = i + 1;
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
                            person_proc(&mut w, ctx, names, &ev[i..j], &ck[ci..cj]);
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
    if f.metadata().ok()?.len() == 0 {
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

fn log2_ceil(x: usize) -> u32 {
    x.max(1).next_power_of_two().trailing_zeros()
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
    assert!(we - base < (1i64 << 31), "window too long for the record layout");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // products -> category rank (byte order, "unknown" included)
    let mut cat_set: BTreeSet<&'static [u8]> = BTreeSet::new();
    let mut prod_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    if let Some(d) = map_file(&data_dir.join("products.csv")) {
        for_each_record::<2>(body(d), |f| {
            prod_rows.push((f[0], f[1]));
            cat_set.insert(f[1]);
        });
    }
    cat_set.insert(b"unknown");
    let cats: Vec<&'static [u8]> = cat_set.into_iter().collect();
    assert!(cats.len() <= CAT_MASK as usize, "too many categories for the record layout");
    let cat_rank: HashMap<&[u8], u64> = cats.iter().enumerate().map(|(i, c)| (*c, i as u64)).collect();
    let unknown_cat = cat_rank[&b"unknown"[..]];
    let mut prods = U64Map::with_capacity(prod_rows.len() + 16);
    for (p, c) in &prod_rows {
        prods.get_or_insert(skey(p), cat_rank[c]);
    }

    // campaigns
    let mut camps = U64Map::with_capacity(4096);
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_ch: Vec<&'static [u8]> = Vec::new();
    if let Some(d) = map_file(&data_dir.join("campaigns.csv")) {
        for_each_record::<2>(body(d), |f| {
            if !f[0].is_empty() {
                let n = camp_ids.len() as u64;
                if camps.get_or_insert(skey(f[0]), n) == n {
                    camp_ids.push(f[0]);
                    camp_ch.push(f[1]);
                }
            }
        });
    }
    let ncamp = camp_ids.len();
    assert!(ncamp < (1 << 24) - 1);

    // inputs
    let mut tasks = Vec::new();
    let mut ck_tasks = Vec::new();
    let mut lk_tasks = Vec::new();
    let mut ev_bytes = 0usize;
    for p in list_csv(&data_dir.join("events")) {
        if let Some(d) = map_file(&p) {
            ev_bytes += d.len();
            push_chunks(&mut tasks, d, 0);
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
    tasks.extend(ck_tasks);
    let est_rows = ev_bytes / 64 + 1;
    let b1bits = log2_ceil(est_rows / 32768).clamp(6, 11);
    let b2bits = log2_ceil(est_rows / 24576).clamp(6, 11);
    lap("mapped");

    // links: parse in parallel
    let link_parts: Vec<Vec<LinkRec>> = {
        let next = AtomicUsize::new(0);
        let lt = &lk_tasks;
        thread::scope(|s| {
            let hs: Vec<_> = (0..nt)
                .map(|_| {
                    s.spawn(|| {
                        let mut v = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Relaxed);
                            if i >= lt.len() {
                                break;
                            }
                            for_each_record::<2>(lt[i].data, |f| {
                                if !f[0].is_empty() && !f[1].is_empty() {
                                    v.push(LinkRec { ka: id_key(f[0]), kb: id_key(f[1]), sa: f[0], sb: f[1] });
                                }
                            });
                        }
                        v
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        })
    };
    lap("links parsed");

    let ctx = Ctx { ws, we, base, day0, ndays, camps, ncamp, prods, unknown_cat, b1bits, b2bits };
    let arena = Arena::new();
    for t in tasks.iter().take(2 * nt) {
        madv_willneed(t.data);
    }

    // union-find runs on its own thread while stage 1 parses events
    let lt_cell: OnceLock<LinkTable> = OnceLock::new();
    let s1 = thread::scope(|s| {
        let cell = &lt_cell;
        s.spawn(move || {
            let lt = build_links(link_parts);
            eprintln!("[{:7.3}s] links joined ({} mapped ids)", t0.elapsed().as_secs_f64(), lt.map.len());
            let _ = cell.set(lt);
        });
        stage1(&ctx, &arena, &tasks, nt, cell)
    });
    let lt = lt_cell.into_inner().unwrap();
    lap("stage1 parse");

    let s2 = stage2(&ctx, &s1, &lt, &arena, nt);
    lap("stage2 dedup");

    let mut names: Names = HashMap::new();
    for w in &s1 {
        for &(k, s) in &w.strs {
            names.insert(k, s);
        }
    }
    for &(k, s) in &lt.names {
        names.insert(k, s);
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
                let (id, ch) = if c == ncamp { (b"organic".to_vec(), b"organic".to_vec()) } else { (camp_ids[c].to_vec(), camp_ch[c].to_vec()) };
                crows.push(CRow { id, ch, day: (day0 + d as i64) as i32, v });
            }
        }
    }

    std::fs::create_dir_all(out_dir.join("user_daily")).unwrap();
    std::fs::create_dir_all(out_dir.join("campaign_daily")).unwrap();
    let total: usize = w3.iter().map(|w| w.urows.len()).sum();
    let name = |k: u64, buf: &mut Vec<u8>| name_of(k, &names, buf);
    thread::scope(|s| {
        s.spawn(|| write_campaign(&out_dir.join("campaign_daily").join("part-0.parquet"), &crows));
        for (i, w) in w3.iter().enumerate() {
            if w.urows.is_empty() && !(i == 0 && total == 0) {
                continue;
            }
            let p = out_dir.join("user_daily").join(format!("part-{}.parquet", i));
            let (cats, name) = (&cats, &name);
            s.spawn(move || write_user(&p, &w.urows, cats, name));
        }
    });
    lap("written");
    eprintln!("user rows {} campaign rows {}", total, crows.len());
}
