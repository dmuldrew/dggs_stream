//! Projected ground-area batches shared by SQL and CLI streams.
use super::{ArrowDataType, ColumnArray, ColumnField, ColumnSchema, RecordBatch};
use crate::aggregator::multi_horizon::area_continuous::{
    scan_ground_area, GroundAreaKernel, GroundAreaRecord,
};
use crate::aggregator::multi_horizon::{MultiHorizonStreamer, MultiResolutionConfig};
use crate::aggregator::output_policy::OutputPolicy;
use crate::encoding::{cell_index_to_wkb, WKB_BUF_LEN};
use crate::error::{DggsStreamError, Result};
use crate::grid::GridKind;
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::mosaic::MosaicReader;
use crate::run_profile::{Counter, Phase, RunProfile};
use std::sync::Arc;

/// Cell identifiers and four physical-area statistics, optionally followed by WKB.
pub fn ground_area_schema(grid: GridKind, with_wkb: bool) -> ColumnSchema {
    let mut fields = vec![
        ColumnField::new(grid.index_column_name(), ArrowDataType::UInt64, false),
        ColumnField::new(grid.hex_column_name(), ArrowDataType::Utf8, false),
        ColumnField::new("resolution", ArrowDataType::UInt8, false),
        ColumnField::new("pixel_count", ArrowDataType::Float64, false),
        ColumnField::new("retained_area_m2", ArrowDataType::Float64, false),
        ColumnField::new("area_weighted_sum", ArrowDataType::Float64, false),
        ColumnField::new("area_weighted_mean", ArrowDataType::Float64, false),
    ];
    if with_wkb {
        fields.push(ColumnField::new("wkb", ArrowDataType::Binary, true));
    }
    ColumnSchema::new(fields)
}

enum GroundAreaScan {
    Center(Box<MultiHorizonStreamer<GroundAreaKernel>>),
    Overlay(Box<dyn Iterator<Item = Result<GroundAreaRecord>> + Send>),
}

impl GroundAreaScan {
    fn fetch_next_batch(&mut self, limit: usize) -> Result<Vec<GroundAreaRecord>> {
        match self {
            Self::Center(scan) => scan.fetch_next_batch(limit),
            Self::Overlay(rows) => rows.take(limit).collect(),
        }
    }
}

/// Bounded batches of completed, area-weighted cells in projection order.
pub struct GroundAreaBatchReader {
    scan: GroundAreaScan,
    grid: GridKind,
    projection: Vec<usize>,
    schema: Arc<ColumnSchema>,
    policy: OutputPolicy,
    profile: Option<Arc<RunProfile>>,
    first_resolution: Option<u8>,
    remaining: usize,
    batch_size: usize,
}
impl GroundAreaBatchReader {
    /// Start a ground-area scan of `reader` (see [`scan_ground_area`] for the
    /// supported rasters and options).
    ///
    /// `projection` lists indices into [`ground_area_schema`]`(config.grid, true)`
    /// in output order; index 7 is WKB. An empty projection yields batches with no
    /// columns that still report `num_rows`, for row counts. Each batch holds at
    /// most `batch_size` rows, and at most `limit` rows are returned in total.
    pub fn open(
        reader: GeoTiffStreamReader,
        config: &MultiResolutionConfig,
        projection: Vec<usize>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        Self::open_with(
            || {
                Ok(GroundAreaScan::Center(Box::new(scan_ground_area(
                    reader, config,
                )?)))
            },
            config,
            projection,
            batch_size,
            limit,
        )
    }

    /// Use a single-raster mosaic prepared with this scan's bbox and source CRS.
    /// Supports center assignment and weighted ground-area overlay.
    pub fn open_mosaic(
        mosaic: Arc<MosaicReader>,
        config: &MultiResolutionConfig,
        projection: Vec<usize>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        Self::open_with(
            || {
                let [tile] = mosaic.tiles.as_slice() else {
                    return Err(DggsStreamError::InvalidParameter(
                        "ground_area requires one raster".into(),
                    ));
                };
                if matches!(
                    config.transfer,
                    crate::transfer::TransferMode::Overlay { .. }
                ) {
                    let rows = crate::transfer::ground_area_overlay_tile(tile.clone(), config)?
                        .map(|row| {
                            row.map(|(resolution, cell_id, accumulator)| GroundAreaRecord {
                                resolution,
                                cell_id,
                                accumulator,
                            })
                        });
                    return Ok(GroundAreaScan::Overlay(Box::new(rows)));
                }
                let kernel = GroundAreaKernel::for_reader(&tile.reader, config)?;
                Ok(GroundAreaScan::Center(Box::new(
                    MultiHorizonStreamer::new_mosaic(mosaic, config, kernel)?,
                )))
            },
            config,
            projection,
            batch_size,
            limit,
        )
    }

    fn open_with(
        open_scan: impl FnOnce() -> Result<GroundAreaScan>,
        config: &MultiResolutionConfig,
        projection: Vec<usize>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let schema = Arc::new(ground_area_schema(config.grid, true).project(&projection)?);
        Ok(Self {
            scan: open_scan()?,
            grid: config.grid,
            schema,
            projection,
            policy: config.output_policy(),
            profile: config.profile.clone(),
            first_resolution: config.resolutions.first().copied(),
            remaining: limit.unwrap_or(usize::MAX),
            batch_size: batch_size.max(1),
        })
    }
    /// The projected schema of every batch, in projection order.
    pub fn schema(&self) -> Arc<ColumnSchema> {
        self.schema.clone()
    }
    /// Valid sample points dropped so far because their coordinate transform
    /// failed or left the valid latitude/longitude domain.
    pub fn transform_failures(&self) -> u64 {
        match &self.scan {
            GroundAreaScan::Center(scan) => scan.transform_failures(),
            GroundAreaScan::Overlay(_) => 0,
        }
    }
}
impl Iterator for GroundAreaBatchReader {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let rows = match self
            .scan
            .fetch_next_batch(self.remaining.min(self.batch_size))
        {
            Ok(rows) if rows.is_empty() => {
                self.remaining = 0;
                return None;
            }
            Err(e) => {
                self.remaining = 0;
                return Some(Err(e));
            }
            Ok(rows) => rows,
        };
        self.remaining -= rows.len();
        let build = || {
            let columns = self
                .projection
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
                    7 => {
                        let mut buf = [0; WKB_BUF_LEN];
                        let values: Vec<_> = rows
                            .iter()
                            .map(|r| {
                                cell_index_to_wkb(self.grid, r.cell_id, &mut buf)
                                    .map(|len| buf[..len].to_vec())
                                    .unwrap_or_default()
                            })
                            .collect();
                        let validity = values
                            .iter()
                            .any(Vec::is_empty)
                            .then(|| values.iter().map(|v| !v.is_empty()).collect());
                        ColumnArray::Binary { values, validity }
                    }
                    _ => ColumnArray::Float64 {
                        values: rows
                            .iter()
                            .map(|r| {
                                let a = &r.accumulator;
                                self.policy.round(match col {
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
            // An empty projection (SQL COUNT(*)) still carries its row count.
            Ok(RecordBatch {
                schema: self.schema.clone(),
                columns,
                num_rows: rows.len(),
            })
        };
        Some(match &self.profile {
            Some(profile) => {
                let samples: f64 = rows
                    .iter()
                    .filter(|r| Some(r.resolution) == self.first_resolution)
                    .map(|r| r.accumulator.pixel_count())
                    .sum();
                profile.count(Counter::Cells, rows.len() as u64);
                profile.count(Counter::SamplesIndexed, samples.round() as u64);
                profile.count(Counter::RowsSerialized, rows.len() as u64);
                profile.time(Phase::Serialize, build)
            }
            None => build(),
        })
    }
}
impl Drop for GroundAreaBatchReader {
    fn drop(&mut self) {
        if let GroundAreaScan::Center(scan) = &mut self.scan {
            scan.publish_profile();
        }
        if let Some(p) = &self.profile {
            p.stream_finished();
        }
    }
}
