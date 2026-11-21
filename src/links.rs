//! Linked IDs -> people, fully parallel.
//!
//!   1. parse link files; keep edges (key_a, key_b) and scatter both endpoints
//!      into NPART partitions by mix(key)
//!   2. per partition: dedup endpoints into a dense node index
//!   3. lock-free union-find (CAS on an AtomicU32 parent array; a root is only
//!      ever linked to a smaller index, so no cycles), edges processed by all workers
//!   4. per node: final root; per root: smallest key (atomic min; hashed keys,
//!      which are rare, are settled afterwards by comparing their bytes)
//!   5. per partition: map every id that is not its person's smallest -> person key

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::{AcqRel, Relaxed}};
use std::thread;

use crate::keys::*;
use crate::scan::*;

pub struct LinkTable {
    /// every linked id that is not its person's smallest -> person key
    pub map: PartMap,
    /// person key -> name, for people whose smallest id is a hashed key
    pub names: Vec<(u64, Vec<u8>)>,
}

impl LinkTable {
    #[inline(always)]
    pub fn resolve(&self, k: u64) -> u64 {
        self.map.get(k).unwrap_or(k)
    }
}

struct Parsed {
    edges: Vec<(u64, u64)>,
    ends: Vec<Vec<u64>>,
    strs: Vec<(u64, Vec<u8>)>,
}

#[inline(always)]
fn find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Relaxed);
        if px == x {
            return x;
        }
        let gx = p[px as usize].load(Relaxed);
        if gx != px {
            let _ = p[x as usize].compare_exchange_weak(px, gx, Relaxed, Relaxed);
        }
        x = gx;
    }
}

#[inline(always)]
fn union(p: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = find(p, a);
        b = find(p, b);
        if a == b {
            return;
        }
        let (hi, lo) = if a > b { (a, b) } else { (b, a) };
        if p[hi as usize].compare_exchange(hi, lo, AcqRel, Relaxed).is_ok() {
            return;
        }
    }
}

/// Run `f(i)` for i in 0..n on `nt` threads (dynamic), collecting results in index order.
pub fn par_map<R: Send>(n: usize, nt: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let mut out: Vec<(usize, R)> = thread::scope(|s| {
        let hs: Vec<_> = (0..nt.min(n.max(1)))
            .map(|_| {
                s.spawn(|| {
                    let mut v = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= n {
                            break;
                        }
                        v.push((i, f(i)));
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    out.sort_unstable_by_key(|x| x.0);
    out.into_iter().map(|x| x.1).collect()
}

pub fn build(files: &[InFile], tasks: &[RTask], nt: usize) -> LinkTable {
    // 1. parse
    let next = AtomicUsize::new(0);
    let parsed: Vec<Parsed> = thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut edges = Vec::new();
                    let mut ends: Vec<Vec<u64>> = (0..NPART).map(|_| Vec::new()).collect();
                    let mut strs = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= tasks.len() {
                            break;
                        }
                        let t = &tasks[i];
                        let data = lines_in(&files[t.file], t.off, t.len);
                        for_each_record::<2>(data, |f| {
                            if !f[0].is_empty() && !f[1].is_empty() {
                                let a = pkey_p(f[0], b'u');
                                let b = pkey_p(f[1], b'u');
                                if !is_packed(a) {
                                    strs.push((a, f[0].to_vec()));
                                }
                                if !is_packed(b) {
                                    strs.push((b, f[1].to_vec()));
                                }
                                edges.push((a, b));
                                ends[part_of(a)].push(a);
                                ends[part_of(b)].push(b);
                            }
                        });
                    }
                    Parsed { edges, ends, strs }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // 2. dense node index per partition
    let idx: Vec<(U64Map, Vec<u64>)> = par_map(NPART, nt, |p| {
        let total: usize = parsed.iter().map(|w| w.ends[p].len()).sum();
        let mut m = U64Map::with_capacity(total);
        let mut keys = Vec::new();
        for w in &parsed {
            for &k in &w.ends[p] {
                let n = keys.len() as u64;
                if m.get_or_insert(k, n) == n {
                    keys.push(k);
                }
            }
        }
        (m, keys)
    });
    let mut off = vec![0usize; NPART + 1];
    for p in 0..NPART {
        off[p + 1] = off[p] + idx[p].1.len();
    }
    let n = off[NPART];
    let keys: Vec<u64> = idx.iter().flat_map(|x| x.1.iter().copied()).collect();
    let gid = |k: u64| -> u32 {
        let p = part_of(k);
        (off[p] as u64 + idx[p].0.get(k).unwrap()) as u32
    };

    // 3. union-find, all workers
    let parent: Vec<AtomicU32> = (0..n as u32).map(AtomicU32::new).collect();
    par_map(parsed.len(), nt, |w| {
        for &(a, b) in &parsed[w].edges {
            union(&parent, gid(a), gid(b));
        }
    });

    // 4. roots and the smallest key per root
    const CH: usize = 1 << 16;
    let nch = n.div_ceil(CH);
    let best: Vec<AtomicU64> = (0..n).map(|_| AtomicU64::new(u64::MAX)).collect();
    let root_parts: Vec<(Vec<u32>, Vec<(u32, u32)>)> = par_map(nch, nt, |c| {
        let lo = c * CH;
        let hi = (lo + CH).min(n);
        let mut roots = Vec::with_capacity(hi - lo);
        let mut tagged = Vec::new();
        for i in lo..hi {
            let r = find(&parent, i as u32);
            roots.push(r);
            let k = keys[i];
            if is_packed(k) {
                best[r as usize].fetch_min(k, Relaxed);
            } else {
                tagged.push((r, i as u32));
            }
        }
        (roots, tagged)
    });
    let roots: Vec<u32> = root_parts.iter().flat_map(|x| x.0.iter().copied()).collect();

    // hashed keys: compare real bytes (rare)
    let strmap: HashMap<u64, Vec<u8>> = parsed.iter().flat_map(|w| w.strs.iter().cloned()).collect();
    let bytes_of = |k: u64| -> Vec<u8> {
        if is_packed(k) {
            let mut v = Vec::new();
            decode_pkey(k, b'u', &mut v);
            v
        } else {
            strmap[&k].clone()
        }
    };
    let mut over: HashMap<u32, u64> = HashMap::new();
    for (_, tagged) in &root_parts {
        for &(r, i) in tagged {
            let cand = keys[i as usize];
            let cur = over.get(&r).copied().unwrap_or(best[r as usize].load(Relaxed));
            if cur == u64::MAX || bytes_of(cand) < bytes_of(cur) {
                over.insert(r, cand);
            }
        }
    }
    let person_of = |r: u32| -> u64 { over.get(&r).copied().unwrap_or_else(|| best[r as usize].load(Relaxed)) };

    // 5. id -> person maps, one partition per task
    let parts: Vec<Part> = par_map(NPART, nt, |p| {
        let range = off[p]..off[p + 1];
        let cnt = range.clone().filter(|&i| person_of(roots[i]) != keys[i]).count();
        let mut part = Part::with_capacity(cnt);
        for i in range {
            let person = person_of(roots[i]);
            if person != keys[i] {
                part.insert(keys[i], person);
            }
        }
        part
    });
    let names = over.values().filter(|k| !is_packed(**k)).map(|&k| (k, bytes_of(k))).collect();
    LinkTable { map: PartMap { parts }, names }
}
