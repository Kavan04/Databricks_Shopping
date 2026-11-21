//! Parquet output: plain-encoded, uncompressed, no statistics (cheapest to write).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

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

fn col_i64<W: std::io::Write + Send>(rg: &mut parquet::file::writer::SerializedRowGroupWriter<'_, W>, v: &[i64]) {
    let mut c = rg.next_column().unwrap().unwrap();
    c.typed::<Int64Type>().write_batch(v, None, None).unwrap();
    c.close().unwrap();
}
fn col_i32<W: std::io::Write + Send>(rg: &mut parquet::file::writer::SerializedRowGroupWriter<'_, W>, v: &[i32]) {
    let mut c = rg.next_column().unwrap().unwrap();
    c.typed::<Int32Type>().write_batch(v, None, None).unwrap();
    c.close().unwrap();
}
fn col_str<W: std::io::Write + Send>(
    rg: &mut parquet::file::writer::SerializedRowGroupWriter<'_, W>,
    v: &[ByteArray],
    defs: Option<&[i16]>,
) {
    let mut c = rg.next_column().unwrap().unwrap();
    c.typed::<ByteArrayType>().write_batch(v, defs, None).unwrap();
    c.close().unwrap();
}

/// `name(person, buf)` appends the person's name to buf.
pub fn write_user(path: &Path, rows: &[URow], cats: &[&[u8]], name: &(dyn Fn(u64, &mut Vec<u8>) + Sync)) {
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
    let cat_bytes: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(c.to_vec())).collect();
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, p).unwrap();
    for part in rows.chunks(1 << 20) {
        let mut rg = w.next_row_group().unwrap();
        let mut buf = Vec::with_capacity(16);
        let ids: Vec<ByteArray> = part
            .iter()
            .map(|r| {
                buf.clear();
                name(r.person, &mut buf);
                ByteArray::from(buf.clone())
            })
            .collect();
        col_str(&mut rg, &ids, None);
        col_i32(&mut rg, &part.iter().map(|r| r.day).collect::<Vec<_>>());
        col_i64(&mut rg, &part.iter().map(|r| r.sessions as i64).collect::<Vec<_>>());
        col_i64(&mut rg, &part.iter().map(|r| r.events as i64).collect::<Vec<_>>());
        col_i64(&mut rg, &part.iter().map(|r| r.rev as i64).collect::<Vec<_>>());
        let mut vals = Vec::new();
        let mut defs = Vec::with_capacity(part.len());
        for r in part {
            if r.top == NONE_CAT {
                defs.push(0);
            } else {
                defs.push(1);
                vals.push(cat_bytes[r.top as usize].clone());
            }
        }
        col_str(&mut rg, &vals, Some(&defs));
        rg.close().unwrap();
    }
    w.close().unwrap();
}

pub fn write_campaign(path: &Path, rows: &[CRow]) {
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
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, p).unwrap();
    let mut rg = w.next_row_group().unwrap();
    col_str(&mut rg, &rows.iter().map(|r| ByteArray::from(r.id.clone())).collect::<Vec<_>>(), None);
    col_str(&mut rg, &rows.iter().map(|r| ByteArray::from(r.ch.clone())).collect::<Vec<_>>(), None);
    col_i32(&mut rg, &rows.iter().map(|r| r.day).collect::<Vec<_>>());
    for k in 0..4 {
        col_i64(&mut rg, &rows.iter().map(|r| r.v[k] as i64).collect::<Vec<_>>());
    }
    rg.close().unwrap();
    w.close().unwrap();
}
