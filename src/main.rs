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
mod diag;
mod keys;
mod links;
mod mem;
mod out;
mod scan;

use std::collections::HashMap;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::OnceLock;
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

/// Parsed event, before dedup (24 bytes).
/// `w` = ts_rel << 33 | ingest << 1 | ok, where ingest is in whole seconds,
/// offset so it fits 32 bits (`ingest_rel`); an invalid copy has ts_rel 0.
/// `user` carries PFLAG for a valid purchase: its price and category are in
/// a `Pc` record with the same event id (purchases are ~3% of events).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct E1 {
    eid: u64,
    user: u64,
    w: u64,
}

/// Price and category of a valid purchase: `pc` = price << 9 | category,
/// `ing` = its copy's ingest (as in `E1::w`), so dedup can pick the winner's.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Pc {
    eid: u64,
    pc: u64,
    ing: u64,
}

/// The ingest part of `E1::w`, the dedup order.
#[inline(always)]
fn e1_ingest(w: u64) -> u64 {
    (w >> 1) & 0xFFFF_FFFF
}

/// Surviving event, keyed by its person.
#[derive(Clone, Copy, Default)]
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
    /// base split into whole days and the remainder, for cheap day numbers
    base_day: i64,
    base_off: i64,
    /// base seconds - 2^31: the zero of 32-bit ingest seconds
    ing0: i64,
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
    pr: Vec<Vec<Seg<Pc>>>,
    ck: Vec<Vec<Seg<Ck>>>,
    /// strings of keys that are hashes (rare), for names and click_id ties
    strs: Vec<(u64, Vec<u8>)>,
}

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");
const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

// ---------------------------------------------------------------- stage 1: parse

/// One parsed event line: its E1 record and, for a valid purchase, its
/// price << 9 | category (0 otherwise).
#[inline(always)]
fn parse_event(ctx: &Ctx, f: &[&[u8]; 7], dc: &mut DateCache, strs: &mut Vec<(u64, Vec<u8>)>) -> Option<(E1, u64)> {
    if f[0].is_empty() {
        return None;
    }
    let eid = pkey_p(f[0], b'e');
    let ing_s = ingest_s(f[3], dc).unwrap_or(i64::MAX / 1000);
    let ing = ing_s * 1000;
    let ing_rel = (ing_s - ctx.ing0).clamp(0, 0xFFFF_FFFF) as u64;
    let bad = (E1 { eid, user: 0, w: ing_rel << 1 }, 0);
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
    let ts_rel = match ts_p(f[2], dc) {
        Some(t) if t >= ctx.ws && t < ctx.we && ing.saturating_sub(t) <= LATE => (t - ctx.base) as u64,
        _ => return Some(bad),
    };
    let mut pc = 0u64;
    if purchase {
        match parse_price(f[6]) {
            Some(p) => {
                let cat = ctx.prods.get(skey(f[5])).unwrap_or(ctx.unknown_cat);
                pc = (p << 9) | cat;
            }
            None => return Some(bad),
        }
    }
    let user = pkey_p(f[1], b'u');
    if !is_packed(user) {
        strs.push((user, f[1].to_vec()));
    }
    let flag = if purchase { PFLAG } else { 0 };
    Some((E1 { eid, user: user | flag, w: (ts_rel << 33) | (ing_rel << 1) | 1 }, pc))
}

/// Stage 1's per-worker state for event lines, as a scanner sink whose
/// per-line work is always inlined into the scanner loop.
struct EvSink<'c, 'b> {
    ctx: &'c Ctx,
    dc: DateCache,
    strs: Vec<(u64, Vec<u8>)>,
    ev: WcBuckets<E1, 8>,
    pr: WcBuckets<Pc, 8>,
    blocks: Blocks<'b>,
}

impl<'a, 'c, 'b> Sink<'a, 7> for EvSink<'c, 'b> {
    #[inline(always)]
    fn rec(&mut self, f: &[&'a [u8]; 7]) {
        if let Some((e, pc)) = parse_event(self.ctx, f, &mut self.dc, &mut self.strs) {
            let b = bucket(e.eid, self.ctx.b1bits);
            self.ev.push(b, e, &mut self.blocks);
            if pc != 0 {
                self.pr.push(b, Pc { eid: e.eid, pc, ing: e1_ingest(e.w) }, &mut self.blocks);
            }
        }
    }
}

/// Click lines of one range, parsed into `cks` (person lookups come after).
struct CkSink<'x, 'c, 'b> {
    es: &'x mut EvSink<'c, 'b>,
    cks: &'x mut Vec<Ck>,
}

impl<'a, 'x, 'c, 'b> Sink<'a, 4> for CkSink<'x, 'c, 'b> {
    #[inline(always)]
    fn rec(&mut self, f: &[&'a [u8]; 4]) {
        if let Some(c) = parse_click(self.es.ctx, f, &mut self.es.dc, &mut self.es.strs) {
            self.cks.push(c);
        }
    }
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
    let ckey = pkey_p(f[0], b'k');
    if !is_packed(ckey) {
        strs.push((ckey, f[0].to_vec()));
    }
    Some(Ck { person: pkey_p(f[1], b'u'), w1: (((t - ctx.base) as u64) << 24) | camp, ckey })
}

const AHEAD: usize = 16;
/// Lookahead of stage 2's person lookups.
static LAHEAD: AtomicUsize = AtomicUsize::new(32);
/// Event lines use the scanner's unconditional comma bookkeeping (FM_SCANBL).
static SCAN_BL: AtomicBool = AtomicBool::new(false);

fn stage1(ctx: &Ctx, arena: &Arena, files: &[InFile], tasks: &[RTask], nt: usize, lt_cell: &OnceLock<LinkTable>) -> Vec<S1> {
    let next = AtomicUsize::new(0);
    let keep_maps = std::env::var_os("FM_NORELEASE").is_some() || std::env::var_os("FM_NORELIN").is_some();
    let (nb1, nb2) = (1usize << ctx.b1bits, 1usize << ctx.b2bits);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut es = EvSink { ctx, dc: DateCache::new(), strs: Vec::new(), ev: WcBuckets::new(nb1), pr: WcBuckets::new(nb1), blocks: Blocks::new(arena) };
                    let mut ck: Vec<Bkt<Ck>> = (0..nb2).map(|_| Bkt::new()).collect();
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
                            if SCAN_BL.load(Relaxed) {
                                scan_records::<7, true, _>(data, &mut es);
                            } else {
                                scan_records::<7, false, _>(data, &mut es);
                            }
                        } else {
                            let lt = lt_cell.wait();
                            cks.clear();
                            scan_records::<4, false, _>(data, &mut CkSink { es: &mut es, cks: &mut cks });
                            for j in 0..cks.len() {
                                if j + 2 * AHEAD < cks.len() {
                                    lt.map.prefetch_filter(cks[j + 2 * AHEAD].person);
                                }
                                if j + AHEAD < cks.len() {
                                    lt.map.prefetch(cks[j + AHEAD].person);
                                }
                                let mut c = cks[j];
                                c.person = lt.resolve(c.person);
                                ck[bucket(c.person, ctx.b2bits)].push(c, &mut es.blocks);
                            }
                        }
                        if !keep_maps {
                            release_range(&files[t.file], t.off, t.len);
                        }
                    }
                    let EvSink { ev, pr, mut blocks, strs, .. } = es;
                    S1 {
                        ev: ev.finish(&mut blocks),
                        pr: pr.finish(&mut blocks),
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
/// Dedup tables take at most this many records: 16K slots of 24 bytes (384 KB)
/// stay in L2. Bigger buckets are first split by more hash bits into pieces
/// of about 2/3 of this (FM_SUBMAX).
static SUB_MAX: AtomicUsize = AtomicUsize::new(12 << 10);

/// log2 of the dedup table size for `m` records (load at most 3/4).
#[inline(always)]
fn tab_bits(m: usize) -> u32 {
    (m + m / 3 + 1).next_power_of_two().max(16).trailing_zeros()
}

/// Insert records into an open-addressing table keyed by event id, keeping the
/// copy with the smallest ingest. `shift` = hash bits already spent choosing
/// the bucket (and piece), so slots use the bits after them.
#[inline(always)]
fn dedup_into(table: &mut [E1], shift: u32, tbits: u32, recs: &[E1]) {
    let mask = table.len() - 1;
    for r in recs {
        let mut i = ((mix(r.eid) << shift) >> (64 - tbits)) as usize;
        loop {
            let t = unsafe { table.get_unchecked_mut(i) };
            if t.eid == 0 {
                *t = *r;
                break;
            }
            if t.eid == r.eid {
                if e1_ingest(r.w) < e1_ingest(t.w) {
                    *t = *r;
                }
                break;
            }
            i = (i + 1) & mask;
        }
    }
}

/// Purchases of one bucket by event id: for each, the copy with the smallest
/// ingest, which is the dedup winner's whenever the winner is a valid purchase
/// (copies with equal ingest are identical).
struct PcTable {
    t: Vec<Pc>,
    bits: u32,
    shift: u32,
}

impl PcTable {
    fn build(&mut self, s1: &[S1], b: usize) {
        let np: usize = s1.iter().map(|w| seg_len(&w.pr[b])).sum();
        let cap = (2 * np + 1).next_power_of_two().max(16);
        self.bits = cap.trailing_zeros();
        self.t.clear();
        self.t.resize(cap, Pc::default());
        let mask = cap - 1;
        for w in s1 {
            for sg in &w.pr[b] {
                for r in sg.slice() {
                    let mut i = self.slot(r.eid);
                    while self.t[i].eid != 0 && self.t[i].eid != r.eid {
                        i = (i + 1) & mask;
                    }
                    if self.t[i].eid == 0 || r.ing < self.t[i].ing {
                        self.t[i] = *r;
                    }
                }
            }
        }
    }
    #[inline(always)]
    fn slot(&self, eid: u64) -> usize {
        ((mix(eid) << self.shift) >> (64 - self.bits)) as usize
    }
    #[inline(always)]
    fn get(&self, eid: u64) -> u64 {
        let mask = self.t.len() - 1;
        let mut i = self.slot(eid);
        while self.t[i].eid != eid && self.t[i].eid != 0 {
            i = (i + 1) & mask;
        }
        self.t[i].pc
    }
}

/// The valid winners of a dedup table, as (user key, w2).
#[inline(always)]
fn survivors(table: &[E1], pt: &PcTable, surv: &mut Vec<(u64, u64)>) {
    for t in table {
        if t.w & 1 == 1 {
            let mut w2 = (t.w >> 33) << 33;
            if t.user & PFLAG != 0 {
                w2 |= pt.get(t.eid);
            }
            surv.push((t.user & !PFLAG, w2));
        }
    }
}

fn stage2(ctx: &Ctx, s1: &[S1], lt: &LinkTable, arena: &Arena, nt: usize) -> Vec<Vec<Vec<Seg<Ev>>>> {
    let next = AtomicUsize::new(0);
    let (nb1, nb2) = (1usize << ctx.b1bits, 1usize << ctx.b2bits);
    let b1bits = ctx.b1bits;
    let sub_max = SUB_MAX.load(Relaxed);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut blocks = Blocks::new(arena);
                    let mut out: WcBuckets<Ev, 4> = WcBuckets::new(nb2);
                    let mut table: Vec<E1> = Vec::new();
                    let mut scratch: Vec<E1> = Vec::new();
                    let mut fill: Vec<usize> = Vec::new();
                    let mut pt = PcTable { t: Vec::new(), bits: 4, shift: b1bits };
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
                        pt.build(s1, b);
                        surv.clear();
                        if n <= sub_max {
                            let tb = tab_bits(n);
                            table.clear();
                            table.resize(1 << tb, E1::default());
                            for w in s1 {
                                for sg in &w.ev[b] {
                                    dedup_into(&mut table, b1bits, tb, sg.slice());
                                }
                            }
                            // this bucket's input blocks are dead now: reuse them for output
                            for w in s1 {
                                blocks.recycle(&w.ev[b]);
                                blocks.recycle(&w.pr[b]);
                            }
                            survivors(&table, &pt, &mut surv);
                        } else {
                            // split into pieces whose tables fit L2: fixed-size regions,
                            // generous enough that a hash never overflows one in practice
                            let target = (sub_max * 2 / 3).max(1);
                            let sbits = n.div_ceil(target).next_power_of_two().trailing_zeros();
                            let nsub = 1usize << sbits;
                            let mut region = (n >> sbits) + (n >> (sbits + 2)) + 64;
                            loop {
                                if scratch.len() < region * nsub {
                                    scratch.resize(region * nsub, E1::default());
                                }
                                fill.clear();
                                fill.extend((0..nsub).map(|k| k * region));
                                let mut ok = true;
                                'scatter: for w in s1 {
                                    for sg in &w.ev[b] {
                                        for r in sg.slice() {
                                            let k = ((mix(r.eid) << b1bits) >> (64 - sbits)) as usize;
                                            let p = unsafe { *fill.get_unchecked(k) };
                                            if p == (k + 1) * region {
                                                ok = false;
                                                break 'scatter;
                                            }
                                            unsafe {
                                                *scratch.get_unchecked_mut(p) = *r;
                                                *fill.get_unchecked_mut(k) = p + 1;
                                            }
                                        }
                                    }
                                }
                                if ok {
                                    break;
                                }
                                region *= 2;
                            }
                            for w in s1 {
                                blocks.recycle(&w.ev[b]);
                                blocks.recycle(&w.pr[b]);
                            }
                            for k in 0..nsub {
                                let recs = &scratch[k * region..fill[k]];
                                let tb = tab_bits(recs.len());
                                table.clear();
                                table.resize(1 << tb, E1::default());
                                dedup_into(&mut table, b1bits + sbits, tb, recs);
                                survivors(&table, &pt, &mut surv);
                            }
                        }
                        let ahead = LAHEAD.load(Relaxed);
                        for j in 0..surv.len() {
                            if j + 2 * ahead < surv.len() {
                                lt.map.prefetch_filter(surv[j + 2 * ahead].0);
                            }
                            if j + ahead < surv.len() {
                                lt.map.prefetch(surv[j + ahead].0);
                            }
                            let (u, w2) = surv[j];
                            let p = lt.resolve(u);
                            out.push(bucket(p, ctx.b2bits), Ev { person: p, w2 }, &mut blocks);
                        }
                    }
                    let segs = out.finish(&mut blocks);
                    // event blocks not reused for output are dead: give them back now
                    blocks.release_free();
                    segs
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

/// Append the ID that key `k` stands for (`prefix` is the ID kind's letter).
fn name_of(k: u64, prefix: u8, names: &Names, out: &mut Vec<u8>) {
    if is_packed(k) {
        decode_pkey(k, prefix, out);
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
    name_of(a, b'k', names, &mut x);
    name_of(b, b'k', names, &mut y);
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
        let day = ctx.base_day + ((ctx.base_off + start) as u64 / DAY as u64) as i64;
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

/// Counting-sort the records of `srcs` (n in all) into `dst` by `sub(record)`;
/// `cnt` ends up holding each group's start (nsub + 1 entries).
#[inline(always)]
fn scatter<T: Copy>(srcs: &[&[T]], n: usize, nsub: usize, sub: impl Fn(&T) -> usize, dst: &mut Vec<T>, cnt: &mut Vec<u32>, pos: &mut Vec<u32>) {
    cnt.clear();
    cnt.resize(nsub + 1, 0);
    for s in srcs {
        for r in s.iter() {
            cnt[sub(r) + 1] += 1;
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
    for s in srcs {
        for r in s.iter() {
            let j = sub(r);
            unsafe { d.add(pos[j] as usize).write(*r) };
            pos[j] += 1;
        }
    }
    unsafe { dst.set_len(n) };
}

/// Per-worker buffers of the sessions stage.
#[derive(Default)]
struct S3Buf {
    ev: Vec<Ev>,
    ck: Vec<Ck>,
    ecnt: Vec<u32>,
    ccnt: Vec<u32>,
    pos: Vec<u32>,
}

/// Sessionize the events and clicks of `esrc`/`csrc` (whole persons): group
/// them by `shift`-shifted hash bits of the person into groups of ~4 events,
/// sort each group, and run every person through `person_proc`.
fn sessionize(w: &mut W3, ctx: &Ctx, names: &Names, esrc: &[&[Ev]], ne: usize, csrc: &[&[Ck]], nc: usize, shift: u32, bf: &mut S3Buf) {
    // groups of ~4 events: same person -> same group
    let sbits = log2_ceil(ne / 4).clamp(1, 20);
    let nsub = 1usize << sbits;
    let sh = 64 - sbits;
    let sub = |p: u64| ((mix(p) << shift) >> sh) as usize;
    scatter(esrc, ne, nsub, |e: &Ev| sub(e.person), &mut bf.ev, &mut bf.ecnt, &mut bf.pos);
    scatter(csrc, nc, nsub, |c: &Ck| sub(c.person), &mut bf.ck, &mut bf.ccnt, &mut bf.pos);
    let (ev, ck, ecnt, ccnt) = (&mut bf.ev, &mut bf.ck, &bf.ecnt, &bf.ccnt);
    for j in 0..nsub {
        let (es, ee) = (ecnt[j] as usize, ecnt[j + 1] as usize);
        if es == ee {
            continue;
        }
        let evs = &mut ev[es..ee];
        let p0 = evs[0].person;
        let cks = &mut ck[ccnt[j] as usize..ccnt[j + 1] as usize];
        if evs.iter().all(|e| e.person == p0) && cks.iter().all(|c| c.person == p0) {
            // one person (the usual case): order by time alone
            sort_small(evs, |e| e.w2);
            sort_small(cks, |c| c.w1);
        } else {
            sort_small(evs, |e| (e.person, e.w2));
            sort_small(cks, |c| (c.person, c.w1));
        }
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
            person_proc(w, ctx, names, &evs[i..e], &cks[ci..ce]);
            ci = ce;
            i = e;
        }
    }
}

/// Person buckets with more events than this are first split by more hash
/// bits into pieces of about half of it, so grouping runs in L2 (FM_S3MAX).
static S3_MAX: AtomicUsize = AtomicUsize::new(64 << 10);

/// Each worker writes its own user_daily file as soon as its buckets are done.
fn stage3(ctx: &Ctx, names: &Names, s1: &[S1], s2: &[Vec<Vec<Seg<Ev>>>], nt: usize, cats: &[&[u8]], out_dir: &Path) -> Vec<W3> {
    let ncat = cats.len();
    let ncamp = ctx.ncamp;
    let nb2 = 1usize << ctx.b2bits;
    let b2bits = ctx.b2bits;
    // biggest buckets first, so a whale's bucket does not finish last
    let mut order: Vec<(usize, usize)> = (0..nb2).map(|b| (s2.iter().map(|o| seg_len(&o[b])).sum(), b)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|wi| {
                let next = &next;
                let order = &order;
                s.spawn(move || {
                    let mut w = W3 {
                        cstat: vec![0; (ncamp + 1) * ctx.ndays * 4],
                        urows: Vec::new(),
                        cnt: vec![0; ncamp + 1],
                        pos: vec![0; ncamp + 1],
                        present: Vec::new(),
                        catsum: vec![0; ncat.max(1)],
                        touched: Vec::new(),
                    };
                    let mut bf = S3Buf::default();
                    let (mut pev, mut pck): (Vec<Ev>, Vec<Ck>) = (Vec::new(), Vec::new());
                    let (mut pecnt, mut pccnt, mut ppos): (Vec<u32>, Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new(), Vec::new());
                    let s3_max = S3_MAX.load(Relaxed);
                    loop {
                        let k = next.fetch_add(1, Relaxed);
                        if k >= nb2 {
                            break;
                        }
                        let (ne, b) = order[k];
                        if ne == 0 {
                            continue;
                        }
                        let esrc: Vec<&[Ev]> = s2.iter().flat_map(|o| o[b].iter().map(|sg| sg.slice())).collect();
                        let csrc: Vec<&[Ck]> = s1.iter().flat_map(|o| o.ck[b].iter().map(|sg| sg.slice())).collect();
                        let nc: usize = csrc.iter().map(|x| x.len()).sum();
                        if ne <= s3_max {
                            sessionize(&mut w, ctx, names, &esrc, ne, &csrc, nc, b2bits, &mut bf);
                            continue;
                        }
                        // big bucket: split into pieces of whole persons first
                        let target = (s3_max / 2).max(1);
                        let kbits = ne.div_ceil(target).next_power_of_two().trailing_zeros().max(1);
                        let np = 1usize << kbits;
                        let piece = |p: u64| ((mix(p) << b2bits) >> (64 - kbits)) as usize;
                        scatter(&esrc, ne, np, |e: &Ev| piece(e.person), &mut pev, &mut pecnt, &mut ppos);
                        scatter(&csrc, nc, np, |c: &Ck| piece(c.person), &mut pck, &mut pccnt, &mut ppos);
                        for q in 0..np {
                            let es = &pev[pecnt[q] as usize..pecnt[q + 1] as usize];
                            if es.is_empty() {
                                continue;
                            }
                            let cs = &pck[pccnt[q] as usize..pccnt[q + 1] as usize];
                            sessionize(&mut w, ctx, names, &[es], es.len(), &[cs], cs.len(), b2bits + kbits, &mut bf);
                        }
                    }
                    if !w.urows.is_empty() && std::env::var_os("FM_NOWRITE").is_none() {
                        let name = |k: u64, buf: &mut Vec<u8>| name_of(k, b'u', names, buf);
                        let tw = Instant::now();
                        write_user(&out_dir.join("user_daily").join(format!("part-{}.parquet", wi)), &w.urows, cats, &name);
                        if std::env::var_os("FM_TIMEW").is_some() {
                            eprintln!("write_user {} rows {:.4}s", w.urows.len(), tw.elapsed().as_secs_f64());
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

/// BENCH=1: single-thread throughput of each parsing layer on the largest
/// event file (warm, best of 5). A measuring tool, not part of a run.
fn bench(ctx: &Ctx, files: &[InFile], tasks: &[RTask]) {
    let fi = (0..files.len()).filter(|&i| tasks.iter().any(|t| t.file == i && t.kind == 0)).max_by_key(|&i| files[i].size).unwrap();
    let d = lines_in(&files[fi], 0, files[fi].size);
    let d = &d[..d.len().min(256 << 20)];
    let d = &d[..memchr::memrchr(b'\n', d).unwrap()];
    let lines = memchr::memchr_iter(b'\n', d).count() + 1;
    let arena = Arena::new();
    let mut warm = Blocks::new(&arena);
    let mut res = Vec::new();
    const NAMES: [&str; 10] = ["memchr-nl", "records", "records-bl", "+2 keys", "+2 ts", "parse_event", "parse_event-bl", "+push (fresh memory)", "+push (recycled memory)", "stage-1 sink (fresh memory)"];
    let mut ns = Vec::new();
    let reps = if diag::QUIET.load(Relaxed) { 3 } else { 5 };
    for mode in 0..NAMES.len() {
        let mut best = f64::MAX;
        for _ in 0..reps {
            let mut dc = DateCache::new();
            let mut strs = Vec::new();
            let mut blocks = Blocks::new(&arena);
            let bb: u32 = std::env::var("BENCH_B1").ok().and_then(|v| v.parse().ok()).unwrap_or(ctx.b1bits);
            let mut ev: WcBuckets<E1, 8> = WcBuckets::new(1usize << bb);
            let wb = if mode == 8 { &mut warm } else { &mut blocks };
            let t = Instant::now();
            let mut acc = 0u64;
            match mode {
                0 => acc = memchr::memchr_iter(b'\n', d).count() as u64,
                1 => for_each_record_v::<7, false>(d, |f| acc = acc.wrapping_add(f[2].len() as u64)),
                2 => for_each_record_v::<7, true>(d, |f| acc = acc.wrapping_add(f[2].len() as u64)),
                3 => for_each_record::<7>(d, |f| acc = acc.wrapping_add(pkey_p(f[0], b'e') ^ pkey_p(f[1], b'u'))),
                4 => for_each_record::<7>(d, |f| acc = acc.wrapping_add((ts_p(f[2], &mut dc).unwrap_or(0) ^ ts_p(f[3], &mut dc).unwrap_or(0)) as u64)),
                5 => for_each_record_v::<7, false>(d, |f| {
                    if let Some((e, pc)) = parse_event(ctx, f, &mut dc, &mut strs) {
                        acc = acc.wrapping_add(e.eid ^ e.w ^ e.user ^ pc);
                    }
                }),
                6 => for_each_record_v::<7, true>(d, |f| {
                    if let Some((e, pc)) = parse_event(ctx, f, &mut dc, &mut strs) {
                        acc = acc.wrapping_add(e.eid ^ e.w ^ e.user ^ pc);
                    }
                }),
                7 | 8 => for_each_record::<7>(d, |f| {
                    if let Some((e, _)) = parse_event(ctx, f, &mut dc, &mut strs) {
                        ev.push(bucket(e.eid, bb), e, wb);
                    }
                }),
                _ => {
                    let mut es = EvSink { ctx, dc: DateCache::new(), strs: Vec::new(), ev: WcBuckets::new(1usize << bb), pr: WcBuckets::new(1usize << bb), blocks: Blocks::new(&arena) };
                    scan_records::<7, false, _>(d, &mut es);
                    acc = es.strs.len() as u64;
                }
            }
            std::hint::black_box(acc);
            best = best.min(t.elapsed().as_secs_f64());
            if mode == 8 {
                for segs in ev.finish(&mut warm) {
                    warm.recycle(&segs);
                }
            }
        }
        ns.push(best * 1e9 / lines as f64);
        res.push(format!("{}: {:.0} MB/s {:.1} ns/line", NAMES[mode], d.len() as f64 / best / 1e6, best * 1e9 / lines as f64));
    }
    if diag::QUIET.load(Relaxed) {
        let v: Vec<String> = ns.iter().map(|x| format!("{:.1}", x)).collect();
        *diag::BENCH_OUT.lock().unwrap() = format!("nl {} rec {}/{} keys {} ts {} pe {}/{} push {}/{} sink {} ns/line ({} lines)", v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], lines);
        return;
    }
    eprintln!("bench on {} MB, {} lines:\n  {}", d.len() >> 20, lines, res.join("\n  "));
}

fn main() {
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .max(1);
    let chunk: usize = std::env::var("FM_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(CHUNK).max(1);
    if let Some(v) = std::env::var("FM_LAHEAD").ok().and_then(|v| v.parse().ok()) {
        LAHEAD.store(v, Relaxed);
    }
    if std::env::var_os("FM_DIAG").is_some() {
        // diagnostics for the scoring machine; never a real run
        diag::main(&data_dir, nt, &|d: &Path, o: &Path, n: usize| {
            let c = std::env::var("FM_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(CHUNK);
            run(d, o, n, c)
        });
        std::process::exit(1);
    }
    run(&data_dir, &out_dir, nt, chunk);
    // skip destructors: the kernel reclaims everything at exit anyway
    std::process::exit(0);
}

/// The whole pipeline: `data_dir` in, both tables written under `out_dir`.
fn run(data_dir: &Path, out_dir: &Path, nt: usize, chunk: usize) {
    let t0 = Instant::now();
    SUB_MAX.store(std::env::var("FM_SUBMAX").ok().and_then(|v| v.parse().ok()).unwrap_or(12 << 10), Relaxed);
    S3_MAX.store(std::env::var("FM_S3MAX").ok().and_then(|v| v.parse().ok()).unwrap_or(64 << 10), Relaxed);
    mem::PLAIN_STORES.store(std::env::var_os("FM_PLAINST").is_some(), Relaxed);
    SCAN_BL.store(std::env::var_os("FM_SCANBL").is_some(), Relaxed);
    let lap = |what: &str| {
        if diag::QUIET.load(Relaxed) {
            diag::LAPS.lock().unwrap().push((what.to_string(), t0.elapsed().as_secs_f64()));
            return;
        }
        let (u, s) = diag::rusage();
        eprintln!("[{:7.3}s] {} cpu={:.3} user={:.3} sys={:.3}", t0.elapsed().as_secs_f64(), what, u + s, u, s)
    };

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - WEEK;
    assert!(we - base < (1i64 << 31), "window too long for the record layout");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // products -> category rank (byte order, "unknown" included); slices into the file buffer
    let prod_file = Padded::read(&data_dir.join("products.csv"));
    let mut prod_rows: Vec<(u64, &[u8])> = Vec::new();
    let mut cat_seen: HashMap<&[u8], ()> = HashMap::new();
    if let Some(d) = &prod_file {
        for_each_record::<2>(d.body(), |f| {
            prod_rows.push((skey(f[0]), f[1]));
            cat_seen.insert(f[1], ());
        });
    }
    cat_seen.insert(b"unknown", ());
    let mut cat_list: Vec<&[u8]> = cat_seen.into_keys().collect();
    cat_list.sort_unstable();
    let cats: Vec<Vec<u8>> = cat_list.iter().map(|c| c.to_vec()).collect();
    assert!(cats.len() <= CAT_MASK as usize, "too many categories for the record layout");
    let cat_rank: HashMap<&[u8], u64> = cat_list.iter().enumerate().map(|(i, c)| (*c, i as u64)).collect();
    let unknown_cat = cat_rank[&b"unknown"[..]];
    let mut prods = U64Map::with_capacity(prod_rows.len() + 16);
    for (k, c) in &prod_rows {
        prods.get_or_insert(*k, cat_rank[c]);
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
    let env_bits = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u32>().ok());
    // 512 event and person buckets: on the scorer 512 beat 128, 256, 1024, 2048
    // and 4096 at 20M events, and 1024 and 2048 at 60M
    let b1bits = env_bits("FM_B1").unwrap_or(9);
    let b2bits = env_bits("FM_B2").unwrap_or(9);
    for t in tasks.iter().take(2 * nt) {
        prefetch_range(&files[t.file], t.off, t.len);
    }
    lap("opened");


    let (base_day, base_off) = (base.div_euclid(DAY), base.rem_euclid(DAY));
    let ing0 = base.div_euclid(1000) - (1i64 << 31);
    let ctx = Ctx { ws, we, base, day0, ndays, base_day, base_off, ing0, camps, ncamp, prods, unknown_cat, b1bits, b2bits };
    if std::env::var_os("BENCH").is_some() {
        bench(&ctx, &files, &tasks);
        return;
    }
    let arena = Arena::new();
    if let Ok(mode) = std::env::var("FM_POP") {
        // records: events 32 B each, clicks 24 B each, plus one partly filled block per bucket and worker
        let ck_bytes: usize = files.iter().zip(0..).filter(|(_, i)| tasks.iter().any(|t| t.file == *i && t.kind == 1)).map(|(f, _)| f.size).sum();
        let need = est_rows * 25 + ck_bytes / 45 * 24 + ((1usize << b1bits) + (1usize << b2bits)) * nt * BLK;
        arena.prepopulate(need + need / 16, nt, mode == "huge");
        lap("prepopulated");
    }
    // Link joining runs on a few threads of its own while stage 1 parses
    // events, which do not need it; clicks come last and wait for it.
    let lt_cell: OnceLock<LinkTable> = OnceLock::new();
    let s1 = thread::scope(|s| {
        let (cell, files, lk) = (&lt_cell, &files, &lk_tasks);
        let lnt = std::env::var("FM_LINK_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(3usize).clamp(1, nt.max(1));
        s.spawn(move || {
            let _ = cell.set(links::build(files, lk, lnt));
        });
        stage1(&ctx, &arena, &files, &tasks, nt, &lt_cell)
    });
    let lt = lt_cell.into_inner().unwrap();
    lap("stage1 parse + links");

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
    std::fs::create_dir_all(out_dir.join("user_daily")).unwrap();
    std::fs::create_dir_all(out_dir.join("campaign_daily")).unwrap();
    let cat_refs: Vec<&[u8]> = cats.iter().map(|c| &c[..]).collect();
    let w3 = stage3(&ctx, &names, &s1, &s2, nt, &cat_refs, &out_dir);
    lap("stage3 sessions + user_daily");
    if std::env::var_os("FM_NORELEASE").is_none() && std::env::var_os("FM_NORELAR").is_none() {
        arena.release(nt);
    }

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

    let total: usize = w3.iter().map(|w| w.urows.len()).sum();
    if total == 0 {
        let name = |k: u64, buf: &mut Vec<u8>| name_of(k, b'u', &names, buf);
        write_user(&out_dir.join("user_daily").join("part-0.parquet"), &[], &cat_refs, &name);
    }
    write_campaign(&out_dir.join("campaign_daily").join("part-0.parquet"), &crows);
    lap("written");
    if !diag::QUIET.load(Relaxed) {
        eprintln!("user rows {} campaign rows {}", total, crows.len());
    }
}
