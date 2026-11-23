//! Stream the core's area-weighted records through the CLI's IPC and JSONL writers.
use super::*;
use dggs_stream_core::aggregator::multi_horizon::area_continuous::scan_ground_area;
use dggs_stream_core::columnar::{ArrowDataType, ColumnField};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;

pub(super) fn stream<W: Write>(
    opts: &CliStreamOptions,
    paths: &[PathBuf],
    config: &MultiResolutionConfig,
    writer: &mut W,
) -> Result<usize> {
    let [path] = paths else {
        return Err(DggsStreamError::InvalidParameter(
            "--ground-area requires one raster".into(),
        ));
    };
    if opts.bands.is_some()
        || opts.categorical
        || opts.histogram.is_some()
        || opts.partition.is_some()
    {
        return Err(DggsStreamError::InvalidParameter(
            "--ground-area requires an unpartitioned single-band continuous scan".into(),
        ));
    }
    let schema = ColumnSchema::new(vec![
        ColumnField::new(opts.grid.index_column_name(), ArrowDataType::UInt64, false),
        ColumnField::new(opts.grid.hex_column_name(), ArrowDataType::Utf8, false),
        ColumnField::new("resolution", ArrowDataType::UInt8, false),
        ColumnField::new("pixel_count", ArrowDataType::Float64, false),
        ColumnField::new("retained_area_m2", ArrowDataType::Float64, false),
        ColumnField::new("area_weighted_sum", ArrowDataType::Float64, false),
        ColumnField::new("area_weighted_mean", ArrowDataType::Float64, false),
    ]);
    let projection = resolve_projection_indices(&schema, &opts.columns)?
        .unwrap_or_else(|| (0..schema.fields.len()).collect());
    let schema = Arc::new(schema.project(&projection)?);
    let mut scan = scan_ground_area(GeoTiffStreamReader::open(path)?, config)?;
    let mut remaining = opts.limit.unwrap_or(usize::MAX);
    let batches = std::iter::from_fn(|| {
        if remaining == 0 {
            return None;
        }
        let rows = match scan.fetch_next_batch(opts.batch_size.max(1).min(remaining)) {
            Ok(rows) if rows.is_empty() => return None,
            Ok(rows) => rows,
            Err(e) => return Some(Err(e)),
        };
        remaining -= rows.len();
        if let Some(profile) = &config.profile {
            use dggs_stream_core::run_profile::Counter;
            profile.count(Counter::Cells, rows.len() as u64);
            let samples: f64 = rows
                .iter()
                .filter(|row| Some(&row.resolution) == config.resolutions.first())
                .map(|row| row.accumulator.pixel_count())
                .sum();
            profile.count(Counter::SamplesIndexed, samples.round() as u64);
        }
        let build = || {
            let columns = projection
                .iter()
                .map(|&col| match col {
                    0 => ColumnArray::UInt64 {
                        values: rows.iter().map(|r| r.cell_id).collect(),
                        validity: None,
                    },
                    1 => ColumnArray::Utf8 {
                        values: rows.iter().map(|r| format!("{:x}", r.cell_id)).collect(),
                        validity: None,
                    },
                    2 => ColumnArray::UInt8 {
                        values: rows.iter().map(|r| r.resolution).collect(),
                        validity: None,
                    },
                    _ => ColumnArray::Float64 {
                        values: rows
                            .iter()
                            .map(|r| {
                                let a = &r.accumulator;
                                opts.output.round(match col {
                                    3 => a.pixel_count(),
                                    4 => a.retained_area_m2(),
                                    5 => a.area_weighted_sum(),
                                    6 => a.area_stats().mean(),
                                    _ => unreachable!("indices belong to the ground-area schema"),
                                })
                            })
                            .collect(),
                        validity: None,
                    },
                })
                .collect();
            RecordBatch::try_new(schema.clone(), columns)
        };
        Some(match &config.profile {
            Some(profile) => {
                profile.count(
                    dggs_stream_core::run_profile::Counter::RowsSerialized,
                    rows.len() as u64,
                );
                profile.time(dggs_stream_core::run_profile::Phase::Serialize, build)
            }
            None => build(),
        })
    });
    let count = match opts.output_format {
        StreamOutputFormat::Ipc => write_ipc_stream(writer, &schema, batches),
        StreamOutputFormat::Jsonl => write_jsonl_stream(writer, batches),
    };
    scan.publish_profile();
    warn_transform_failures(scan.transform_failures());
    count
}
