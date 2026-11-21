//! Value parsing: ID keys, timestamps, prices, and hash tables.
//!
//! The `*_p` fast paths read a few bytes past the end of their field. Callers
//! only pass fields from buffers that have scan::PAD readable bytes after them.
//! Each fast path has a plain reference version that the tests compare against.

use xxhash_rust::xxh3::xxh3_64;

/// Set on keys that are hashes of a string the exact packing cannot hold.
pub const TAG: u64 = 1 << 63;

#[allow(dead_code)]
const fn build_code() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut j = 0;
    while j < 26 {
        t[b'a' as usize + j] = 11 + j as u8;
        j += 1;
    }
    t
}
#[allow(dead_code)]
static CODE: [u8; 256] = build_code();
const DECODE: &[u8; 37] = b"\x000123456789abcdefghijklmnopqrstuvwxyz";

#[inline(always)]
pub fn h64(b: &[u8]) -> u64 {
    xxh3_64(b)
}

#[inline(always)]
fn tagged(f: &[u8]) -> u64 {
    TAG | (h64(f) >> 1)
}

/// Exact, order-preserving 60-bit key for IDs of 1..=10 chars in [0-9a-z]
/// (every ID the generator writes before dirt is applied). Anything else gets
/// TAG | hash; such strings are rare and kept in a side table when needed.
/// Reference version: safe on any slice.
#[allow(dead_code)]
#[inline(always)]
pub fn id_key(f: &[u8]) -> u64 {
    let n = f.len();
    if (1..=10).contains(&n) {
        let mut k = 0u64;
        let mut good = true;
        for &b in f {
            let c = CODE[b as usize];
            good &= c != 0;
            k = (k << 6) | c as u64;
        }
        if good {
            return k << (6 * (10 - n));
        }
    }
    tagged(f)
}

#[inline(always)]
unsafe fn ld8(p: *const u8) -> u64 {
    u64::from_le((p as *const u64).read_unaligned())
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

/// 0x80 in every byte of `x` that lies in [lo, hi]. Every byte of x must be < 0x80.
#[inline(always)]
fn in_range(x: u64, lo: u8, hi: u8) -> u64 {
    let ge = (x | HIGH).wrapping_sub(ONES * lo as u64);
    let le = (ONES * (0x80 + hi as u64)).wrapping_sub(x);
    ge & le & HIGH
}

/// `id_key` with SWAR: classify and convert 8 bytes at a time, then pack the
/// 6-bit codes with shifts. Needs 16 readable bytes from the field start.
#[inline(always)]
pub fn id_key_p(f: &[u8]) -> u64 {
    let n = f.len();
    if n == 0 || n > 10 {
        return tagged(f);
    }
    let p = f.as_ptr();
    let (w0, w1) = unsafe { (ld8(p), ld8(p.add(8)) & 0xFFFF) };
    let m0 = if n >= 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
    let m1 = if n <= 8 { 0 } else { (1u64 << (8 * (n - 8))) - 1 };
    let (x0, x1) = (w0 & m0, w1 & m1);
    if (x0 | x1) & HIGH != 0 {
        return tagged(f);
    }
    let (d0, l0) = (in_range(x0, b'0', b'9'), in_range(x0, b'a', b'z'));
    let (d1, l1) = (in_range(x1, b'0', b'9'), in_range(x1, b'a', b'z'));
    if ((d0 | l0) >> 7) != (m0 & ONES) || ((d1 | l1) >> 7) != (m1 & ONES) {
        return tagged(f);
    }
    let c0 = x0 - ((d0 >> 7) * 0x2F + (l0 >> 7) * 0x56);
    let c1 = x1 - ((d1 >> 7) * 0x2F + (l1 >> 7) * 0x56);
    let y = c0.swap_bytes();
    let y = ((y & 0x3F00_3F00_3F00_3F00) >> 2) | (y & 0x003F_003F_003F_003F);
    let y = ((y & 0x0FFF_0000_0FFF_0000) >> 4) | (y & 0x0000_0FFF_0000_0FFF);
    let y = ((y & 0x00FF_FFFF_0000_0000) >> 8) | (y & 0x0000_0000_00FF_FFFF);
    let z = ((c1 & 0x3F) << 6) | ((c1 >> 8) & 0x3F);
    (y << 12) | z
}

#[inline(always)]
pub fn is_packed(k: u64) -> bool {
    k & TAG == 0
}

pub fn decode_key(k: u64, out: &mut Vec<u8>) {
    for i in 0..10 {
        let c = (k >> (54 - 6 * i)) & 63;
        if c == 0 {
            break;
        }
        out.push(DECODE[c as usize]);
    }
}

/// Strong 64-bit finalizer for spreading keys over buckets and table slots.
#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let k = (k ^ (k >> 31)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let k = (k ^ (k >> 29)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    k ^ (k >> 32)
}

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
    }
}

// ---------------------------------------------------------------- hash tables

/// Open-addressing u64 -> u64 map; key 0 marks an empty slot (no real key is 0).
pub struct U64Map {
    slots: Vec<[u64; 2]>,
    shift: u32,
    mask: usize,
    len: usize,
}

impl U64Map {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![[0, 0]; cap], shift: 64 - cap.trailing_zeros(), mask: cap - 1, len: 0 }
    }
    /// Uses hash bits that partition and bucket choices never use, so a map
    /// filled with keys of one partition still spreads over all its slots.
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (mix(k).rotate_left(23) >> self.shift) as usize
    }
    /// Insert if absent; returns the stored value.
    pub fn get_or_insert(&mut self, k: u64, v: u64) -> u64 {
        let mut i = self.slot(k);
        loop {
            let s = self.slots[i];
            if s[0] == 0 {
                self.slots[i] = [k, v];
                self.len += 1;
                return v;
            }
            if s[0] == k {
                return s[1];
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            let s = unsafe { *self.slots.get_unchecked(i) };
            if s[0] == k {
                return Some(s[1]);
            }
            if s[0] == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Bits of `mix(key)` that pick a PartMap partition.
pub const PB: u32 = 6;
pub const NPART: usize = 1 << PB;

#[inline(always)]
pub fn part_of(k: u64) -> usize {
    (mix(k) >> (64 - PB)) as usize
}

/// One partition of a PartMap: open addressing on the bits of mix(key) below
/// the partition bits.
pub struct Part {
    slots: Vec<[u64; 2]>,
    bits: u32,
}

impl Part {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Part { slots: vec![[0, 0]; cap], bits: cap.trailing_zeros() }
    }
    #[inline(always)]
    fn slot(&self, h: u64) -> usize {
        ((h << PB) >> (64 - self.bits)) as usize
    }
    pub fn insert(&mut self, k: u64, v: u64) {
        let mask = self.slots.len() - 1;
        let mut i = self.slot(mix(k));
        loop {
            let s = &mut self.slots[i];
            if s[0] == 0 || s[0] == k {
                *s = [k, v];
                return;
            }
            i = (i + 1) & mask;
        }
    }
}

/// A u64 -> u64 map split into NPART independently built partitions, so it can
/// be built in parallel; lookups can be prefetched.
pub struct PartMap {
    pub parts: Vec<Part>,
}

impl PartMap {
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let h = mix(k);
        let part = unsafe { self.parts.get_unchecked((h >> (64 - PB)) as usize) };
        let mask = part.slots.len() - 1;
        let mut i = part.slot(h);
        loop {
            let s = unsafe { *part.slots.get_unchecked(i) };
            if s[0] == k {
                return Some(s[1]);
            }
            if s[0] == 0 {
                return None;
            }
            i = (i + 1) & mask;
        }
    }
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let h = mix(k);
        let part = unsafe { self.parts.get_unchecked((h >> (64 - PB)) as usize) };
        prefetch(unsafe { part.slots.as_ptr().add(part.slot(h)) });
    }
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

pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = ((m + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        2 => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Reference parser: F1/F2/F3 into epoch ms. `None` is a malformed timestamp. A
/// digits-only value too long for the window comes back as i64::MAX.
pub fn parse_ts(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if (n == 20 || n == 25) && f[4] == b'-' {
        return parse_iso(f);
    }
    if n == 0 {
        return None;
    }
    let mut i = 0;
    while i < n && f[i] == b'0' {
        i += 1;
    }
    let mut v: i64 = 0;
    for &b in &f[i..] {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add(d as i64);
    }
    if n - i > 18 {
        return Some(i64::MAX);
    }
    Some(v)
}

fn parse_iso(f: &[u8]) -> Option<i64> {
    let n = f.len();
    let days = date_days(&f[..10])?;
    if f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let h = p2(f[11], f[12])?;
    let mi = p2(f[14], f[15])?;
    let s = p2(f[17], f[18])?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let off = offset(f, n)?;
    Some((days * 86400 + h as i64 * 3600 + mi as i64 * 60 + s as i64 - off) * 1000)
}

/// Validate "YYYY-MM-DD" and return its day number.
fn date_days(f: &[u8]) -> Option<i64> {
    let y = dig(f[0])? * 1000 + dig(f[1])? * 100 + dig(f[2])? * 10 + dig(f[3])?;
    if f[4] != b'-' || f[7] != b'-' {
        return None;
    }
    let mo = p2(f[5], f[6])?;
    let d = p2(f[8], f[9])?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    Some(days_from_civil(y as i64, mo, d))
}

/// Zone suffix of an F1/F2 timestamp, in seconds to subtract.
#[inline(always)]
fn offset(f: &[u8], n: usize) -> Option<i64> {
    if n == 20 {
        return if f[19] == b'Z' { Some(0) } else { None };
    }
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
    Some(sign * (oh as i64 * 3600 + om as i64 * 60))
}

/// Small direct-mapped cache of "YYYY-MM-DD" -> day number (or invalid). A
/// dataset uses a few dozen distinct dates, so this almost always hits.
pub struct DateCache {
    k0: [u64; 64],
    k1: [u16; 64],
    days: [i64; 64],
}

impl DateCache {
    pub fn new() -> Self {
        DateCache { k0: [0; 64], k1: [0; 64], days: [0; 64] }
    }
    #[inline(always)]
    fn get(&mut self, w0: u64, w1: u16, f: &[u8]) -> Option<i64> {
        let i = ((w0 ^ ((w1 as u64) << 13)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
        if self.k0[i] != w0 || self.k1[i] != w1 {
            self.k0[i] = w0;
            self.k1[i] = w1;
            self.days[i] = date_days(&f[..10]).unwrap_or(i64::MIN);
        }
        let d = self.days[i];
        if d == i64::MIN {
            None
        } else {
            Some(d)
        }
    }
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// All 8 bytes are ASCII digits.
#[inline(always)]
fn eight_digits(w: u64) -> bool {
    ((w & 0xF0F0_F0F0_F0F0_F0F0) | (((w.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4)) == 0x3333_3333_3333_3333
}

/// Value of 8 ASCII digits, first digit in the lowest byte (Lemire).
#[inline(always)]
fn parse8(w: u64) -> u64 {
    let v = w.wrapping_sub(ZEROS);
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    ((v & 0x0000_00FF_0000_00FF)
        .wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32
}

/// `parse_ts` with a date cache and SWAR digit handling. Same results.
#[inline(always)]
pub fn ts_p(f: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = f.len();
    let p = f.as_ptr();
    if (n == 20 || n == 25) && f[4] == b'-' {
        let (w0, w1, t) = unsafe { (ld8(p), (ld8(p.add(8)) & 0xFFFF) as u16, ld8(p.add(11))) };
        let days = dc.get(w0, w1, f)?;
        if f[10] != b'T' {
            return None;
        }
        // "HH:MM:SS": colons at bytes 2 and 5, digits elsewhere
        const CM: u64 = 0x0000_FF00_00FF_0000;
        const CV: u64 = 0x0000_3A00_003A_0000;
        const DM: u64 = 0xF0F0_00F0_F000_F0F0;
        const DV: u64 = 0x3030_0030_3000_3030;
        const D6: u64 = 0x0606_0006_0600_0606;
        if t & CM != CV || t & DM != DV || t.wrapping_add(D6) & DM != DV {
            return None;
        }
        let d = t - DV;
        let b = |i: u32| ((d >> (8 * i)) & 0xFF) as i64;
        let (h, mi, s) = (b(0) * 10 + b(1), b(3) * 10 + b(4), b(6) * 10 + b(7));
        if h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let off = offset(f, n)?;
        return Some((days * 86400 + h * 3600 + mi * 60 + s - off) * 1000);
    }
    if n == 0 {
        return None;
    }
    let mut i = 0;
    while i < n && f[i] == b'0' {
        i += 1;
    }
    let m = n - i;
    if m > 18 {
        return if f[i..].iter().all(|b| b.is_ascii_digit()) { Some(i64::MAX) } else { None };
    }
    let q = unsafe { p.add(i) };
    let mut v = 0u64;
    let mut j = m & 7;
    if j != 0 {
        let w = (unsafe { ld8(q) } << (8 * (8 - j))) | (ZEROS >> (8 * j));
        if !eight_digits(w) {
            return None;
        }
        v = parse8(w);
    }
    while j < m {
        let w = unsafe { ld8(q.add(j)) };
        if !eight_digits(w) {
            return None;
        }
        v = v * 100_000_000 + parse8(w);
        j += 8;
    }
    Some(v as i64)
}

/// Purchase price: ASCII digits, value 1..=10_000_000, leading zeros allowed.
#[inline(always)]
pub fn parse_price(f: &[u8]) -> Option<u64> {
    if f.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < f.len() && f[i] == b'0' {
        i += 1;
    }
    let rest = &f[i..];
    if rest.len() > 8 {
        return None;
    }
    let mut v = 0u64;
    for &b in rest {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Put `s` in a buffer followed by junk, the way the scanner sees fields.
    fn padded(s: &[u8], junk: u8) -> Vec<u8> {
        let mut v = s.to_vec();
        v.extend(std::iter::repeat(junk).take(64));
        v
    }

    #[test]
    fn id_key_fast_matches_reference() {
        let alphabet: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABZ /:`{@\"\x7f\x80\xff-_";
        let mut r = Rng(0x1234_5678_9abc_def1);
        for _ in 0..2_000_000 {
            let n = r.below(14) as usize;
            let s: Vec<u8> = (0..n)
                .map(|_| if r.below(10) < 8 { b"0123456789abcdefghijklmnopqrstuvwxyz"[r.below(36) as usize] } else { alphabet[r.below(alphabet.len() as u64) as usize] })
                .collect();
            for junk in [b'a', b'0', 0u8, b'"', 0xffu8] {
                let buf = padded(&s, junk);
                assert_eq!(id_key_p(&buf[..n]), id_key(&s), "{:?}", String::from_utf8_lossy(&s));
            }
        }
    }

    #[test]
    fn packed_keys_keep_byte_order() {
        let mut r = Rng(99);
        for _ in 0..200_000 {
            let mk = |r: &mut Rng| -> Vec<u8> { (0..1 + r.below(10)).map(|_| b"0123456789abcdefghijklmnopqrstuvwxyz"[r.below(36) as usize]).collect() };
            let (a, b) = (mk(&mut r), mk(&mut r));
            assert_eq!(id_key(&a) < id_key(&b), a < b);
            let mut d = Vec::new();
            decode_key(id_key(&a), &mut d);
            assert_eq!(d, a);
        }
    }

    fn ts_cases(r: &mut Rng) -> Vec<u8> {
        let pick = r.below(12);
        let y = 1990 + r.below(60);
        let mo = r.below(14);
        let d = r.below(33);
        let h = r.below(26);
        let mi = r.below(62);
        let s = r.below(62);
        let mut t = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, mo, d, h, mi, s).into_bytes();
        match pick {
            0..=3 => t.push(b'Z'),
            4..=5 => {
                let sign = if r.below(2) == 0 { '+' } else { '-' };
                t.extend(format!("{}{:02}:{:02}", sign, r.below(26), r.below(62)).bytes());
            }
            6..=8 => {
                let z = "0".repeat(r.below(4) as usize);
                t = format!("{}{}", z, r.below(3_000_000_000_000)).into_bytes();
                if r.below(20) == 0 {
                    t = "9".repeat(17 + r.below(5) as usize).into_bytes();
                }
            }
            _ => {
                t.push(b'Z');
            }
        }
        // random corruption
        if r.below(6) == 0 && !t.is_empty() {
            let i = r.below(t.len() as u64) as usize;
            t[i] = b"Zz T:-+.0x \r\xd9"[r.below(13) as usize];
        }
        if r.below(30) == 0 {
            t.truncate(r.below(t.len() as u64 + 1) as usize);
        }
        t
    }

    #[test]
    fn ts_fast_matches_reference() {
        let mut r = Rng(7);
        let mut dc = DateCache::new();
        for _ in 0..3_000_000 {
            let t = ts_cases(&mut r);
            for junk in [b'7', b':', 0u8] {
                let buf = padded(&t, junk);
                assert_eq!(ts_p(&buf[..t.len()], &mut dc), parse_ts(&t), "{:?}", String::from_utf8_lossy(&t));
            }
        }
        for s in ["2028-02-29T00:00:00Z", "2027-02-29T00:00:00Z", "2026-10-03T19:58:10-04:00", "0001791071890000", "86400000", "1791071890000.0", "-5", "1.79e12", "2026-10-03T24:00:00Z", "2026-10-03T23:59:60Z", "2026-10-04T01:30:00+24:00", "2026-10-04T01:30:00+05:60", "2026-10-04T01:30:00-00:00"] {
            let buf = padded(s.as_bytes(), b'9');
            assert_eq!(ts_p(&buf[..s.len()], &mut dc), parse_ts(s.as_bytes()), "{}", s);
        }
    }
}
