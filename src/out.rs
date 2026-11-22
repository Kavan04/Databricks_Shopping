//! Parquet output, written by hand: one row group, plain-encoded uncompressed
//! data pages, no statistics, and a Thrift-compact footer. The parquet crate
//! spent about 180 ns per user_daily row on general-purpose machinery; these
//! two fixed tables need a fraction of that.
//!
//! Layout: "PAR1" | per column: (PageHeader, page bytes)* | FileMetaData | len | "PAR1"

use std::io::Write;
use std::path::Path;

pub const NONE_CAT: u16 = u16::MAX;

#[derive(Clone, Copy)]
pub struct URow {
    pub person: u64,
    pub rev: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub top: u16,
}

pub struct CRow {
    pub id: Vec<u8>,
    pub ch: Vec<u8>,
    pub day: i32,
    pub v: [u64; 4],
}

// ---------------------------------------------------------------- thrift compact protocol

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Thrift {
    b: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { b: Vec::new(), last: 0, stack: Vec::new() }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.varint((((id as i32) << 1) ^ ((id as i32) >> 31)) as u32 as u64);
        }
        self.last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.open();
    }
    /// Start a struct that is a list element (no field header).
    fn open(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last = self.stack.pop().unwrap_or(0);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn elem_i32(&mut self, v: i32) {
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn elem_bin(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
}

// ---------------------------------------------------------------- columns

const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_BYTE_ARRAY: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;
/// Rows per data page.
const PAGE_ROWS: usize = 1 << 17;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    OptStr,
    Date,
    I32,
    I64,
}

struct ColMeta {
    name: &'static str,
    kind: Kind,
    offset: u64,
    bytes: u64,
}

/// One output file being assembled in memory.
struct File {
    buf: Vec<u8>,
    cols: Vec<ColMeta>,
    rows: usize,
}

impl File {
    fn new(rows: usize, cap: usize) -> Self {
        let mut buf = Vec::with_capacity(cap);
        buf.extend_from_slice(b"PAR1");
        File { buf, cols: Vec::new(), rows }
    }

    /// Append one column chunk: for each page of rows, `page(start, end, body)`
    /// writes that page's encoded values (and definition levels) into `body`.
    fn column(&mut self, name: &'static str, kind: Kind, mut page: impl FnMut(usize, usize, &mut Vec<u8>)) {
        let offset = self.buf.len() as u64;
        let mut body = Vec::new();
        let mut s = 0;
        while s < self.rows {
            let e = (s + PAGE_ROWS).min(self.rows);
            body.clear();
            page(s, e, &mut body);
            let mut h = Thrift::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, body.len() as i32);
            h.i32(3, body.len() as i32);
            h.begin(5);
            h.i32(1, (e - s) as i32);
            h.i32(2, ENC_PLAIN);
            h.i32(3, ENC_RLE);
            h.i32(4, ENC_RLE);
            h.end();
            h.b.push(0);
            self.buf.extend_from_slice(&h.b);
            self.buf.extend_from_slice(&body);
            s = e;
        }
        self.cols.push(ColMeta { name, kind, offset, bytes: self.buf.len() as u64 - offset });
    }

    fn finish(mut self, path: &Path) {
        let mut t = Thrift::new();
        t.i32(1, 1); // version
        t.list(2, T_STRUCT, self.cols.len() + 1);
        t.open();
        t.bin(4, b"schema");
        t.i32(5, self.cols.len() as i32);
        t.end();
        for c in &self.cols {
            t.open();
            let (ty, rep) = match c.kind {
                Kind::Str => (PT_BYTE_ARRAY, 0),
                Kind::OptStr => (PT_BYTE_ARRAY, 1),
                Kind::Date | Kind::I32 => (PT_INT32, 0),
                Kind::I64 => (PT_INT64, 0),
            };
            t.i32(1, ty);
            t.i32(3, rep);
            t.bin(4, c.name.as_bytes());
            match c.kind {
                Kind::Str | Kind::OptStr => {
                    t.i32(6, CT_UTF8);
                    t.begin(10);
                    t.begin(1); // StringType
                    t.end();
                    t.end();
                }
                Kind::Date => {
                    t.i32(6, CT_DATE);
                    t.begin(10);
                    t.begin(6); // DateType
                    t.end();
                    t.end();
                }
                _ => {}
            }
            t.end();
        }
        t.i64(3, self.rows as i64);
        if self.rows == 0 {
            t.list(4, T_STRUCT, 0);
        } else {
            t.list(4, T_STRUCT, 1);
            t.open();
            t.list(1, T_STRUCT, self.cols.len());
            let mut total = 0u64;
            for c in &self.cols {
                total += c.bytes;
                t.open();
                t.i64(2, c.offset as i64);
                t.begin(3);
                t.i32(1, match c.kind {
                    Kind::Str | Kind::OptStr => PT_BYTE_ARRAY,
                    Kind::Date | Kind::I32 => PT_INT32,
                    Kind::I64 => PT_INT64,
                });
                t.list(2, T_I32, 2);
                t.elem_i32(ENC_PLAIN);
                t.elem_i32(ENC_RLE);
                t.list(3, T_BINARY, 1);
                t.elem_bin(c.name.as_bytes());
                t.i32(4, 0); // UNCOMPRESSED
                t.i64(5, self.rows as i64);
                t.i64(6, c.bytes as i64);
                t.i64(7, c.bytes as i64);
                t.i64(9, c.offset as i64);
                t.end();
                t.end();
            }
            t.i64(2, total as i64);
            t.i64(3, self.rows as i64);
            t.end();
        }
        t.bin(6, b"shopping-sessions");
        t.b.push(0);
        self.buf.extend_from_slice(&t.b);
        self.buf.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(b"PAR1");
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&self.buf).unwrap();
    }
}

/// PLAIN values of a required BYTE_ARRAY column.
fn plain_str<'a>(body: &mut Vec<u8>, vals: impl Iterator<Item = &'a [u8]>) {
    for v in vals {
        body.extend_from_slice(&(v.len() as u32).to_le_bytes());
        body.extend_from_slice(v);
    }
}

/// Definition levels (bit width 1) as one bit-packed RLE-hybrid run, with the
/// 4-byte length prefix of a v1 data page.
fn def_levels(body: &mut Vec<u8>, present: impl Iterator<Item = bool>, n: usize) {
    let groups = n.div_ceil(8);
    let mut run = Vec::with_capacity(groups + 10);
    let mut h = (groups << 1) | 1;
    while h >= 0x80 {
        run.push((h as u8) | 0x80);
        h >>= 7;
    }
    run.push(h as u8);
    let start = run.len();
    run.resize(start + groups, 0);
    for (i, p) in present.enumerate() {
        if p {
            run[start + i / 8] |= 1 << (i % 8);
        }
    }
    body.extend_from_slice(&(run.len() as u32).to_le_bytes());
    body.extend_from_slice(&run);
}

/// `name(person, buf)` appends the person's name to buf.
pub fn write_user(path: &Path, rows: &[URow], cats: &[&[u8]], name: &(dyn Fn(u64, &mut Vec<u8>) + Sync)) {
    let mut f = File::new(rows.len(), rows.len() * 48 + 4096);
    f.column("user_id", Kind::Str, |s, e, body| {
        let mut nb = Vec::with_capacity(16);
        for r in &rows[s..e] {
            nb.clear();
            name(r.person, &mut nb);
            body.extend_from_slice(&(nb.len() as u32).to_le_bytes());
            body.extend_from_slice(&nb);
        }
    });
    f.column("day", Kind::Date, |s, e, body| {
        for r in &rows[s..e] {
            body.extend_from_slice(&r.day.to_le_bytes());
        }
    });
    f.column("sessions", Kind::I32, |s, e, body| {
        for r in &rows[s..e] {
            body.extend_from_slice(&(r.sessions as i32).to_le_bytes());
        }
    });
    f.column("events", Kind::I32, |s, e, body| {
        for r in &rows[s..e] {
            body.extend_from_slice(&(r.events as i32).to_le_bytes());
        }
    });
    f.column("revenue_cents", Kind::I64, |s, e, body| {
        for r in &rows[s..e] {
            body.extend_from_slice(&(r.rev as i64).to_le_bytes());
        }
    });
    f.column("top_category", Kind::OptStr, |s, e, body| {
        let part = &rows[s..e];
        def_levels(body, part.iter().map(|r| r.top != NONE_CAT), part.len());
        plain_str(body, part.iter().filter(|r| r.top != NONE_CAT).map(|r| cats[r.top as usize]));
    });
    f.finish(path);
}

pub fn write_campaign(path: &Path, rows: &[CRow]) {
    let mut f = File::new(rows.len(), rows.len() * 64 + 4096);
    f.column("campaign_id", Kind::Str, |s, e, body| plain_str(body, rows[s..e].iter().map(|r| &r.id[..])));
    f.column("channel", Kind::Str, |s, e, body| plain_str(body, rows[s..e].iter().map(|r| &r.ch[..])));
    f.column("day", Kind::Date, |s, e, body| {
        for r in &rows[s..e] {
            body.extend_from_slice(&r.day.to_le_bytes());
        }
    });
    for (k, name) in ["sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"].iter().enumerate() {
        let kind = if k == 2 { Kind::I64 } else { Kind::I32 };
        f.column(name, kind, |s, e, body| {
            for r in &rows[s..e] {
                if kind == Kind::I64 {
                    body.extend_from_slice(&(r.v[k] as i64).to_le_bytes());
                } else {
                    body.extend_from_slice(&(r.v[k] as i32).to_le_bytes());
                }
            }
        });
    }
    f.finish(path);
}
