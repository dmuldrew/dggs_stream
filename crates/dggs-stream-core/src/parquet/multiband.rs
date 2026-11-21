//! Serialize wide scan batches directly, preserving their field order and nulls.
use std::path::Path;
use std::sync::Arc;

use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use super::{
    build_geoparquet_metadata, column_order::restore_type_defined_float_orders, ParquetExportConfig,
};
use crate::columnar::multiband::MultiBandBatchReader;
use crate::columnar::{ArrowDataType, ColumnArray};
use crate::encoding::extend_wkb_bounds;

/// Write one row group per bounded scan batch. Publish the file only after its
/// footer, geometry bounds and logical column orders have been finalized.
pub fn write_multiband_parquet(
    reader: &mut MultiBandBatchReader,
    path: &Path,
    geo: bool,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let schema = reader.schema();
    if geo && schema.index_of("wkb").is_none() {
        return Err("multi-band GeoParquet requires the wkb column".into());
    }
    let mut message = String::from("message cells {");
    for f in &schema.fields {
        let (physical, annotation) = match f.data_type {
            ArrowDataType::UInt64 => ("INT64", " (UINT_64)"),
            ArrowDataType::UInt8 => ("INT32", " (UINT_8)"),
            ArrowDataType::Float64 => ("DOUBLE", ""),
            ArrowDataType::Utf8 => ("BYTE_ARRAY", " (UTF8)"),
            ArrowDataType::Binary => ("BYTE_ARRAY", ""),
            _ => return Err("unexpected field in multi-band scan".into()),
        };
        let repetition = if f.nullable { "OPTIONAL" } else { "REQUIRED" };
        message.push_str(&format!(" {repetition} {physical} {}{annotation};", f.name));
    }
    message.push('}');
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temp = tempfile::NamedTempFile::new_in(parent)?;
    let props = WriterProperties::builder()
        .set_compression(ParquetExportConfig::default().compression)
        .build();
    let mut writer = SerializedFileWriter::new(
        temp.reopen()?,
        Arc::new(parse_message_type(&message)?),
        Arc::new(props),
    )?;
    let mut bounds = [
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    ];
    let mut rows = 0;
    for batch in reader {
        let batch = batch?;
        let mut group = writer.next_row_group()?;
        for (field, array) in schema.fields.iter().zip(batch.columns) {
            let mut col = group.next_column()?.ok_or("missing Parquet column")?;
            match array {
                ColumnArray::UInt64 { values, .. } => {
                    let values: Vec<_> = values.into_iter().map(|v| v as i64).collect();
                    col.typed::<Int64Type>().write_batch(&values, None, None)?;
                }
                ColumnArray::UInt8 { values, .. } => {
                    let values: Vec<_> = values.into_iter().map(i32::from).collect();
                    col.typed::<Int32Type>().write_batch(&values, None, None)?;
                }
                ColumnArray::Utf8 { values, .. } => {
                    let values: Vec<_> = values
                        .into_iter()
                        .map(|s| ByteArray::from(s.into_bytes()))
                        .collect();
                    col.typed::<ByteArrayType>()
                        .write_batch(&values, None, None)?;
                }
                ColumnArray::Binary { values, .. } => {
                    if geo {
                        for wkb in &values {
                            extend_wkb_bounds(wkb, &mut bounds);
                        }
                    }
                    let values: Vec<_> = values.into_iter().map(ByteArray::from).collect();
                    col.typed::<ByteArrayType>()
                        .write_batch(&values, None, None)?;
                }
                ColumnArray::Float64 {
                    mut values,
                    validity,
                } => {
                    let levels = if field.nullable {
                        Some(if let Some(valid) = validity {
                            let mut bits = valid.iter();
                            values.retain(|_| *bits.next().expect("one validity flag per row"));
                            valid.into_iter().map(i16::from).collect()
                        } else {
                            vec![1; values.len()]
                        })
                    } else {
                        None
                    };
                    col.typed::<DoubleType>()
                        .write_batch(&values, levels.as_deref(), None)?;
                }
                _ => return Err("unexpected column in multi-band scan".into()),
            }
            col.close()?;
        }
        group.close()?;
        rows += batch.num_rows;
    }
    if geo {
        if !bounds[0].is_finite() {
            bounds = [-180., -90., 180., 90.];
        }
        writer.append_key_value_metadata(KeyValue::new(
            "geo".into(),
            build_geoparquet_metadata("wkb", bounds),
        ));
    }
    let metadata = writer.close()?;
    restore_type_defined_float_orders(
        &mut temp.reopen()?,
        metadata.file_metadata().column_orders(),
    )?;
    temp.persist(path)?;
    Ok(rows)
}
