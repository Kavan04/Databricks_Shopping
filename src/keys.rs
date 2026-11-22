//! Value parsing: ID keys, timestamps, prices, and hash tables.
//!
//! The `*_p` fast paths read a few bytes past the end of their field. Callers
//! only pass fields from buffers that have scan::PAD readable bytes after them.
//! Each fast path has a plain reference version that the tests compare against.

use xxhash_rust::xxh3::xxh3_64;

/// Set on keys that are hashes of a string the exact packing cannot hold.
pub const TAG: u64 = 1 << 63;

#[inline(always)]
pub fn h64(b: &[u8]) -> u64 {
    xxh3_64(b)
}

/// A bit that is 0 in every key: packed keys are ASCII bytes (high bit clear)
/// and hashed keys have it cleared. Stage 1 uses it to flag purchases.
pub const PFLAG: u64 = 1 << 55;

#[inline(always)]
fn tagged(f: &[u8]) -> u64 {
    TAG | ((h64(f) >> 1) & !PFLAG)
}

/// Key of an ID that should start with the letter `prefix` (generator IDs are
/// `e`, `u` or `k` followed by at most 8 base-36 characters). If the ID is the
/// prefix plus 1..=8 ASCII bytes, none of them NUL, the key is those bytes read
/// big-endian: exact, never 0, top bit clear, and integer order equals byte
/// order among IDs with the same prefix. Anything else gets TAG | hash; such IDs
/// are rare and their bytes are kept in a side table when needed.
/// Reference version: safe on any slice.
#[allow(dead_code)]
pub fn pkey(f: &[u8], prefix: u8) -> u64 {
    let n = f.len();
    if n < 2 || n > 9 || f[0] != prefix || f[1..].iter().any(|&b| b == 0 || b >= 0x80) {
        return tagged(f);
    }
    let mut w = [0u8; 8];
    w[..n - 1].copy_from_slice(&f[1..]);
    u64::from_be_bytes(w)
}

#[inline(always)]
unsafe fn ld8(p: *const u8) -> u64 {
    u64::from_le((p as *const u64).read_unaligned())
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

/// `pkey` with one unaligned load. Needs 9 readable bytes from the field start.
#[inline(always)]
pub fn pkey_p(f: &[u8], prefix: u8) -> u64 {
    let n = f.len();
    if n < 2 || n > 9 || f[0] != prefix {
        return tagged(f);
    }
    let m = u64::MAX >> (8 * (9 - n)); // the n-1 bytes after the prefix
    let w = unsafe { ld8(f.as_ptr().add(1)) } & m;
    // bytes past the ID become 0xFF so only real NULs count as zero bytes
    let z = w | !m;
    if (w & HIGH) != 0 || (z.wrapping_sub(ONES) & !z & HIGH) != 0 {
        return tagged(f);
    }
    w.swap_bytes()
}

/// Append the ID a packed key stands for.
pub fn decode_pkey(k: u64, prefix: u8, out: &mut Vec<u8>) {
    out.push(prefix);
    for b in k.to_be_bytes() {
        if b == 0 {
            break;
        }
        out.push(b);
    }
}

#[inline(always)]
pub fn is_packed(k: u64) -> bool {
    k & TAG == 0
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
/// the partition bits, fronted by a small blocked Bloom filter (2 bits in one
/// u64 word per key) that answers most absent keys without touching the table.
pub struct Part {
    slots: Vec<[u64; 2]>,
    bits: u32,
    bloom: Vec<u64>,
    wmask: usize,
}

#[inline(always)]
fn bloom_word(h: u64, wmask: usize) -> usize {
    ((h >> 12) as usize) & wmask
}
#[inline(always)]
fn bloom_bits(h: u64) -> u64 {
    (1u64 << (h & 63)) | (1u64 << ((h >> 6) & 63))
}

impl Part {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        let words = (n / 4).next_power_of_two().max(8);
        Part { slots: vec![[0, 0]; cap], bits: cap.trailing_zeros(), bloom: vec![0; words], wmask: words - 1 }
    }
    #[inline(always)]
    fn slot(&self, h: u64) -> usize {
        ((h << PB) >> (64 - self.bits)) as usize
    }
    pub fn insert(&mut self, k: u64, v: u64) {
        let h = mix(k);
        let w = bloom_word(h, self.wmask);
        self.bloom[w] |= bloom_bits(h);
        let mask = self.slots.len() - 1;
        let mut i = self.slot(h);
        loop {
            let s = &mut self.slots[i];
            if s[0] == 0 || s[0] == k {
                *s = [k, v];
                return;
            }
            i = (i + 1) & mask;
        }
    }
    #[inline(always)]
    fn maybe(&self, h: u64) -> bool {
        let b = bloom_bits(h);
        unsafe { *self.bloom.get_unchecked(bloom_word(h, self.wmask)) & b == b }
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
        if !part.maybe(h) {
            return None;
        }
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
    /// First half of a two-stage prefetch: bring in the filter word for `k`.
    /// (The filter is larger than the scorer's 1 MB L2, so checking it cold
    /// stalls; issue this well ahead of `prefetch`.)
    #[inline(always)]
    pub fn prefetch_filter(&self, k: u64) {
        let h = mix(k);
        let part = unsafe { self.parts.get_unchecked((h >> (64 - PB)) as usize) };
        prefetch(unsafe { part.bloom.as_ptr().add(bloom_word(h, part.wmask)) });
    }
    /// Prefetch the table slot of `k`, unless the filter already rules it out.
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let h = mix(k);
        let part = unsafe { self.parts.get_unchecked((h >> (64 - PB)) as usize) };
        if part.maybe(h) {
            prefetch(unsafe { part.slots.as_ptr().add(part.slot(h)) });
        }
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

/// An event's `ingest_ts` in whole seconds since the epoch. The rules make it
/// always valid F1 (`YYYY-MM-DDTHH:MM:SSZ`), so that shape skips the time
/// checks and the ms round trip; anything else takes the general parser.
#[inline(always)]
pub fn ingest_s(f: &[u8], dc: &mut DateCache) -> Option<i64> {
    let p = f.as_ptr();
    if f.len() == 20 && f[10] == b'T' && f[19] == b'Z' {
        let (w0, w1, t) = unsafe { (ld8(p), (ld8(p.add(8)) & 0xFFFF) as u16, ld8(p.add(11))) };
        let days = dc.get(w0, w1, f)?;
        let d = t.wrapping_sub(0x3030_0030_3000_3030);
        let b = |i: u32| ((d >> (8 * i)) & 0xFF) as i64;
        return Some(days * 86400 + (b(0) * 10 + b(1)) * 3600 + (b(3) * 10 + b(4)) * 60 + b(6) * 10 + b(7));
    }
    ts_p(f, dc).map(|ms| ms.div_euclid(1000))
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
    fn pkey_fast_matches_reference() {
        let alphabet: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzeuk EUK/:`{@\"\x00\x7f\x80\xff-_";
        let mut r = Rng(0x1234_5678_9abc_def1);
        for _ in 0..2_000_000 {
            let n = r.below(12) as usize;
            let mut s: Vec<u8> = (0..n)
                .map(|_| if r.below(10) < 8 { b"0123456789abcdefghijklmnopqrstuvwxyz"[r.below(36) as usize] } else { alphabet[r.below(alphabet.len() as u64) as usize] })
                .collect();
            if n > 0 && r.below(4) != 0 {
                s[0] = b"euk"[r.below(3) as usize];
            }
            for prefix in [b'e', b'u', b'k'] {
                for junk in [b'a', b'0', 0u8, b'"', 0xffu8] {
                    let buf = padded(&s, junk);
                    assert_eq!(pkey_p(&buf[..n], prefix), pkey(&s, prefix), "{:?}", String::from_utf8_lossy(&s));
                }
            }
        }
    }

    #[test]
    fn packed_keys_keep_byte_order_and_decode() {
        let mut r = Rng(99);
        let mk = |r: &mut Rng| -> Vec<u8> {
            let mut v = vec![b'u'];
            v.extend((0..1 + r.below(8)).map(|_| b"0123456789abcdefghijklmnopqrstuvwxyz"[r.below(36) as usize]));
            v
        };
        for _ in 0..500_000 {
            let (a, b) = (mk(&mut r), mk(&mut r));
            let (ka, kb) = (pkey(&a, b'u'), pkey(&b, b'u'));
            assert!(is_packed(ka) && ka != 0);
            assert_eq!(ka < kb, a < b);
            assert_eq!(ka == kb, a == b);
            let mut d = Vec::new();
            decode_pkey(ka, b'u', &mut d);
            assert_eq!(d, a);
        }
        // not packed: wrong prefix, too long, too short, NUL, non-ASCII
        for s in [&b"U1"[..], b"u123456789", b"u", b"x12", b"u1\x00", b"u\xc3\xa9", b""] {
            assert!(!is_packed(pkey(s, b'u')), "{:?}", s);
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

    /// ingest_s equals the reference in seconds on every valid F1 value (the
    /// only form ingest_ts takes), and matches the general parser elsewhere.
    #[test]
    fn ingest_matches_reference() {
        let mut r = Rng(11);
        let mut dc = DateCache::new();
        for _ in 0..1_000_000 {
            let (y, mo) = (1970 + r.below(100) as i64, 1 + r.below(12) as u32);
            let d = 1 + r.below(days_in_month(y as u32, mo) as u64) as u32;
            let s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, r.below(24), r.below(60), r.below(60));
            let buf = padded(s.as_bytes(), b'7');
            assert_eq!(ingest_s(&buf[..s.len()], &mut dc), parse_ts(s.as_bytes()).map(|ms| ms / 1000), "{}", s);
        }
        for _ in 0..1_000_000 {
            let t = ts_cases(&mut r);
            if t.len() == 20 && t[10] == b'T' && t[19] == b'Z' {
                continue; // the trusted shape: valid values are covered above
            }
            let buf = padded(&t, b':');
            assert_eq!(ingest_s(&buf[..t.len()], &mut dc), parse_ts(&t).map(|ms| ms.div_euclid(1000)), "{:?}", String::from_utf8_lossy(&t));
        }
    }
}
