//! Value parsing: ID keys, timestamps, prices, and small hash tables.

use xxhash_rust::xxh3::xxh3_64;

/// Set on keys that are hashes of a string the exact packing cannot hold.
pub const TAG: u64 = 1 << 63;

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
static CODE: [u8; 256] = build_code();
const DECODE: &[u8; 37] = b"\x000123456789abcdefghijklmnopqrstuvwxyz";

#[inline(always)]
pub fn h64(b: &[u8]) -> u64 {
    xxh3_64(b)
}

/// Exact, order-preserving 60-bit key for IDs of 1..=10 chars in [0-9a-z]
/// (every ID the generator writes before dirt is applied). Anything else gets
/// TAG | hash; such strings are rare and kept in a side table when needed.
#[inline(always)]
pub fn id_key(f: &[u8]) -> u64 {
    let n = f.len();
    if n >= 1 && n <= 10 {
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
    TAG | (h64(f) >> 1)
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
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (mix(k) >> self.shift) as usize
    }
    pub fn len(&self) -> usize {
        self.len
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
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        prefetch(unsafe { self.slots.as_ptr().add(self.slot(k)) });
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

/// Parse F1/F2/F3 into epoch ms. `None` is a malformed timestamp. A digits-only
/// value too long for the window comes back as i64::MAX (out of every window).
#[inline(always)]
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

#[inline(always)]
fn parse_iso(f: &[u8]) -> Option<i64> {
    let n = f.len();
    let y = dig(f[0])? * 1000 + dig(f[1])? * 100 + dig(f[2])? * 10 + dig(f[3])?;
    if f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
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
