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

/// Approximate bytes of buffered column values allowed before a row group is
/// written, so wide band selections do not hold `row_group_size` rows of
/// hundreds of statistics columns in memory.
pub const MULTIBAND_ROW_GROUP_BUDGET_BYTES: usize = 64 << 20;

/// Rough in-memory size of one buffered value of `data_type`.
fn buffered_bytes(data_type: &ArrowDataType, nullable: bool) -> usize {
    match data_type {
        ArrowDataType::Float64 => 8 + 2 * usize::from(nullable),
        ArrowDataType::UInt64 => 8,
        ArrowDataType::UInt8 => 4,
        // `ByteArray` handle plus a hex string or a WKB polygon.
        ArrowDataType::Utf8 => 64,
        _ => 160,
    }
}

/// Rows per row group: `row_group_size`, lowered so buffered values stay within
/// [`MULTIBAND_ROW_GROUP_BUDGET_BYTES`].
pub(super) fn group_rows(schema: &crate::columnar::ColumnSchema, row_group_size: usize) -> usize {
    let row_bytes: usize = schema
        .fields
        .iter()
        .map(|f| buffered_bytes(&f.data_type, f.nullable))
        .sum();
    row_group_size
        .min(MULTIBAND_ROW_GROUP_BUDGET_BYTES / row_bytes.max(1))
        .max(1)
}

/// Values of one column buffered until the next row group is written.
enum ColumnBuffer {
    Int64(Vec<i64>),
    Int32(Vec<i32>),
    Bytes(Vec<ByteArray>),
    Double {
        values: Vec<f64>,
        levels: Option<Vec<i16>>,
    },
}

impl ColumnBuffer {
    fn new(data_type: &ArrowDataType, nullable: bool) -> Option<Self> {
        Some(match data_type {
            ArrowDataType::UInt64 => Self::Int64(Vec::new()),
            ArrowDataType::UInt8 => Self::Int32(Vec::new()),
            ArrowDataType::Utf8 | ArrowDataType::Binary => Self::Bytes(Vec::new()),
            ArrowDataType::Float64 => Self::Double {
                values: Vec::new(),
                levels: nullable.then(Vec::new),
            },
            _ => return None,
        })
    }

    fn append(
        &mut self,
        array: ColumnArray,
        geo: bool,
        bounds: &mut [f64; 4],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match (self, array) {
            (Self::Int64(out), ColumnArray::UInt64 { values, .. }) => {
                out.extend(values.into_iter().map(|v| v as i64));
            }
            (Self::Int32(out), ColumnArray::UInt8 { values, .. }) => {
                out.extend(values.into_iter().map(i32::from));
            }
            (Self::Bytes(out), ColumnArray::Utf8 { values, .. }) => {
                out.extend(values.into_iter().map(|s| ByteArray::from(s.into_bytes())));
            }
            (Self::Bytes(out), ColumnArray::Binary { values, .. }) => {
                if geo {
                    for wkb in &values {
                        extend_wkb_bounds(wkb, bounds);
                    }
                }
                out.extend(values.into_iter().map(ByteArray::from));
            }
            (
                Self::Double {
                    values: out,
                    levels,
                },
                ColumnArray::Float64 { values, validity },
            ) => match (levels, validity) {
                (Some(levels), Some(valid)) => {
                    out.extend(
                        values
                            .into_iter()
                            .zip(&valid)
                            .filter(|(_, v)| **v)
                            .map(|(x, _)| x),
                    );
                    levels.extend(valid.into_iter().map(i16::from));
                }
                (Some(levels), None) => {
                    levels.extend(std::iter::repeat_n(1, values.len()));
                    out.extend(values);
                }
                (None, _) => out.extend(values),
            },
            _ => return Err("unexpected column in multi-band scan".into()),
        }
        Ok(())
    }

    fn write(
        &mut self,
        col: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    ) -> parquet::errors::Result<()> {
        match self {
            Self::Int64(values) => {
                col.typed::<Int64Type>().write_batch(values, None, None)?;
                values.clear();
            }
            Self::Int32(values) => {
                col.typed::<Int32Type>().write_batch(values, None, None)?;
                values.clear();
            }
            Self::Bytes(values) => {
                col.typed::<ByteArrayType>()
                    .write_batch(values, None, None)?;
                values.clear();
            }
            Self::Double { values, levels } => {
                col.typed::<DoubleType>()
                    .write_batch(values, levels.as_deref(), None)?;
                values.clear();
                if let Some(levels) = levels {
                    levels.clear();
                }
            }
        }
        Ok(())
    }
}

/// Write the scan to `path`, coalescing bounded scan batches into row groups of
/// at least `row_group_size` rows (the last may be smaller); pass
/// [`ParquetExportConfig::row_group_size`] for the default. Wide schemas flush
/// earlier, once buffered values reach [`MULTIBAND_ROW_GROUP_BUDGET_BYTES`]. Publish the file only
/// after its footer, geometry bounds and logical column orders have been finalized.
pub fn write_multiband_parquet(
    reader: &mut MultiBandBatchReader,
    path: &Path,
    geo: bool,
    row_group_size: usize,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    write_multiband_parquet_with_config(
        reader,
        path,
        &ParquetExportConfig {
            geoparquet: geo,
            row_group_size,
            ..Default::default()
        },
    )
}

/// Write a projected wide scan with the requested codec, row-group size and GeoParquet metadata.
pub fn write_multiband_parquet_with_config(
    reader: &mut MultiBandBatchReader,
    path: &Path,
    config: &ParquetExportConfig,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    write_batches(reader.schema(), reader, path, config)
}

pub(super) fn write_batches(
    schema: Arc<crate::columnar::ColumnSchema>,
    batches: impl IntoIterator<Item = crate::error::Result<crate::columnar::RecordBatch>>,
    path: &Path,
    config: &ParquetExportConfig,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let geo = config.geoparquet;
    let row_group_size = config.row_group_size;
    if geo && schema.index_of("wkb").is_none() {
        return Err("multi-band GeoParquet requires the wkb column".into());
    }
    for (i, f) in schema.fields.iter().enumerate() {
        if schema.fields[..i]
            .iter()
            .any(|g| g.name.eq_ignore_ascii_case(&f.name))
        {
            return Err(format!("duplicate Parquet column '{}'", f.name).into());
        }
    }
    let mut message = String::from("message cells {");
    let mut buffers = Vec::with_capacity(schema.fields.len());
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
        buffers.push(ColumnBuffer::new(&f.data_type, f.nullable).ok_or("unexpected field")?);
    }
    message.push('}');
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temp = tempfile::NamedTempFile::new_in(parent)?;
    let props = WriterProperties::builder()
        .set_compression(config.compression)
        .build();
    let mut writer = SerializedFileWriter::new(
        temp.reopen()?,
        Arc::new(parse_message_type(&message)?),
        Arc::new(props),
    )?;
    let flush = |writer: &mut SerializedFileWriter<std::fs::File>,
                 buffers: &mut [ColumnBuffer]|
     -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut group = writer.next_row_group()?;
        for buffer in buffers {
            let mut col = group.next_column()?.ok_or("missing Parquet column")?;
            buffer.write(&mut col)?;
            col.close()?;
        }
        group.close()?;
        Ok(())
    };
    let mut bounds = [
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    ];
    let group_rows = group_rows(&schema, row_group_size);
    let (mut rows, mut pending) = (0, 0);
    for batch in batches {
        let batch = batch?;
        for (buffer, array) in buffers.iter_mut().zip(batch.columns) {
            buffer.append(array, geo, &mut bounds)?;
        }
        rows += batch.num_rows;
        pending += batch.num_rows;
        if pending >= group_rows {
            flush(&mut writer, &mut buffers)?;
            pending = 0;
        }
    }
    if pending > 0 {
        flush(&mut writer, &mut buffers)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::multiband::wide_schema;
    use crate::GridKind;

    #[test]
    fn wide_selections_shrink_row_groups_to_the_memory_budget() {
        let few = wide_schema(GridKind::H3, &[3, 1], &[]);
        assert_eq!(group_rows(&few, 131_072), 131_072);
        assert_eq!(group_rows(&few, 8), 8);

        let bands: Vec<usize> = (1..=224).collect();
        let many = wide_schema(GridKind::H3, &bands, &[]);
        let rows = group_rows(&many, 131_072);
        assert!(rows < 131_072 / 8, "{rows}");
        let row_bytes: usize = many
            .fields
            .iter()
            .map(|f| buffered_bytes(&f.data_type, f.nullable))
            .sum();
        assert!(rows * row_bytes <= MULTIBAND_ROW_GROUP_BUDGET_BYTES);
    }
}
