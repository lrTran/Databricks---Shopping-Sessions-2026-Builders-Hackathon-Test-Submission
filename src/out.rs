//! Writes the two output tables with the parquet crate's low-level API
//! (no Arrow dependency). `day` is INT32 annotated DATE; strings are UTF8.

use bytes::Bytes;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub struct UserRow {
    pub name: &'static [u8],
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<&'static [u8]>,
}

pub struct CampaignRow {
    pub campaign: &'static [u8],
    pub channel: &'static [u8],
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

const USER_SCHEMA: &str = "message user_daily {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build(),
    )
}

#[inline]
fn ba(s: &'static [u8]) -> ByteArray {
    ByteArray::from(Bytes::from_static(s))
}

enum Col {
    Str(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write_file(path: &Path, schema: &str, cols: Vec<Col>) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(schema)?);
    let file = File::create(path).map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
    let mut w = SerializedFileWriter::new(file, schema, props())?;
    let mut rg = w.next_row_group()?;
    let mut it = cols.into_iter();
    while let Some(mut cw) = rg.next_column()? {
        match it.next().expect("column count") {
            Col::Str(v, def) => {
                cw.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None)?;
            }
            Col::I32(v) => {
                cw.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
            Col::I64(v) => {
                cw.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
        }
        cw.close()?;
    }
    rg.close()?;
    w.close()?;
    Ok(())
}

pub fn write_user(path: &Path, rows: &[UserRow]) -> parquet::errors::Result<()> {
    let mut top = Vec::new();
    let mut def = Vec::with_capacity(rows.len());
    for r in rows {
        match r.top {
            Some(t) => {
                top.push(ba(t));
                def.push(1i16);
            }
            None => def.push(0),
        }
    }
    let cols = vec![
        Col::Str(rows.iter().map(|r| ba(r.name)).collect(), None),
        Col::I32(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions).collect()),
        Col::I64(rows.iter().map(|r| r.events).collect()),
        Col::I64(rows.iter().map(|r| r.revenue).collect()),
        Col::Str(top, Some(def)),
    ];
    write_file(path, USER_SCHEMA, cols)
}

pub fn write_campaign(path: &Path, rows: &[CampaignRow]) -> parquet::errors::Result<()> {
    let cols = vec![
        Col::Str(rows.iter().map(|r| ba(r.campaign)).collect(), None),
        Col::Str(rows.iter().map(|r| ba(r.channel)).collect(), None),
        Col::I32(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions).collect()),
        Col::I64(rows.iter().map(|r| r.purchasing).collect()),
        Col::I64(rows.iter().map(|r| r.revenue).collect()),
        Col::I64(rows.iter().map(|r| r.assisted).collect()),
    ];
    write_file(path, CAMPAIGN_SCHEMA, cols)
}
