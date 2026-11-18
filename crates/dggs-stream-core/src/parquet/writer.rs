//! Native Parquet Streaming Writer for H3 Raster Hexification
//!
//! Streams aggregated H3 records directly from `MultiScanHorizonStreamer` or
//! `MultiCategoricalHorizonStreamer` into Snappy- or ZSTD-compressed Parquet row groups
//! without crossing DuckDB SQL/C-FFI boundaries.
//!
//! Employs lock-free double-buffered channel streaming via [`super::pipeline`].

use std::borrow::Cow;
use std::fs::File;
use std::path::Path;

use parquet::basic::Compression;
use std::sync::OnceLock;

use parquet::data_type::ByteArray;
use parquet::file::writer::SerializedFileWriter;

use crate::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiCategoricalRecord, MultiContinuousRecord,
    MultiResolutionConfig, MultiScanHorizonStreamer,
};
use crate::aggregator::output_policy::OutputPolicy;
use crate::encoding::{cell_index_to_wkb, extend_wkb_bounds, fast_hex_u64, WkbBuf, WKB_BUF_LEN};
use crate::grid::GridKind;

/// The statistics columns of a continuous export, after the cell columns.
const CONTINUOUS_VALUE_COLUMNS: &[&str] = &[
    "DOUBLE min_value",
    "DOUBLE max_value",
    "DOUBLE sum_value",
    "DOUBLE avg_value",
    "DOUBLE pixel_count",
];

/// The statistics columns of a categorical export, after the cell columns.
const CATEGORICAL_VALUE_COLUMNS: &[&str] = &[
    "INT64 majority",
    "DOUBLE majority_fraction",
    "DOUBLE pixel_count",
    "INT64 distinct_classes",
    "DOUBLE entropy",
];

/// The Parquet message type of a cell export on `grid`: the cell index (annotated
/// `UINT_64` where the grid's spec says so), then, unless `compact`, the
/// hexadecimal id; the WKB `geometry` if `geoparquet`; `values`; and, unless `compact`,
/// the centre `lat` / `lng`.
fn cell_schema_message(grid: GridKind, values: &[&str], compact: bool, geoparquet: bool) -> String {
    let spec = grid.spec();
    let unsigned = if spec.parquet_index_unsigned {
        " (UINT_64)"
    } else {
        ""
    };
    let mut schema = format!(
        "message schema {{\n    REQUIRED INT64 {}{unsigned};\n",
        spec.index_column
    );
    let mut column = |definition: &str| {
        schema.push_str("    REQUIRED ");
        schema.push_str(definition);
        schema.push_str(";\n");
    };
    if !compact {
        column(&format!("BYTE_ARRAY {} (UTF8)", spec.hex_column));
    }
    if geoparquet {
        column("BYTE_ARRAY geometry");
    }
    for value in values {
        column(value);
    }
    if !compact {
        column("DOUBLE lat");
        column("DOUBLE lng");
    }
    schema.push_str("}\n");
    schema
}

/// The H3 schema for `(compact, geoparquet)`, built once per buffer type so the
/// `&'static str` form of `ParquetBuffer::schema_message` keeps working.
fn h3_schema(
    cache: &'static OnceLock<[String; 4]>,
    values: &[&str],
    compact: bool,
    geoparquet: bool,
) -> &'static str {
    let schemas = cache.get_or_init(|| {
        [(false, false), (false, true), (true, false), (true, true)].map(|(compact, geoparquet)| {
            cell_schema_message(GridKind::H3, values, compact, geoparquet)
        })
    });
    &schemas[usize::from(compact) * 2 + usize::from(geoparquet)]
}

/// Bounds that any point extends (`min > max` until the first point).
const EMPTY_BOUNDS: [f64; 4] = [
    f64::INFINITY,
    f64::INFINITY,
    f64::NEG_INFINITY,
    f64::NEG_INFINITY,
];
use crate::raster::geotiff::GeoTiffStreamReader;

use super::pipeline::{
    compute_sort_permutation, reorder_by_perm, reorder_by_perm_take, write_byte_array_column,
    write_f64_column, write_i64_column,
};

pub use super::geoparquet_metadata::build_geoparquet_metadata;
pub use super::pipeline::{
    run_parquet_streaming_pipeline, run_parquet_streaming_pipeline_with_progress,
    ParquetRowGroupBuffer, ParquetStreamer, RecordStreamer,
};

#[derive(Debug, Clone)]
pub struct ParquetExportConfig {
    /// Whether to omit redundant columns (`h3_hex`, `lat`, `lng`).
    pub omit_redundant_columns: bool,
    /// Legacy external adapter parameter: whether to omit redundant columns (`h3_hex`, `lat`, `lng`).
    pub compact: bool,
    pub compression: Compression,
    pub row_group_size: usize,
    pub is_categorical: bool,
    pub geoparquet: bool,
}

impl ParquetExportConfig {
    /// Create a new ParquetExportConfig with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Internal adapter: whether redundant columns (`lat`, `lng`, `h3_hex`) should be omitted.
    #[inline(always)]
    pub fn should_omit_redundant_columns(&self) -> bool {
        self.omit_redundant_columns && self.compact
    }
}

impl Default for ParquetExportConfig {
    fn default() -> Self {
        Self {
            omit_redundant_columns: true,
            compact: true,
            compression: Compression::SNAPPY,
            row_group_size: 131_072,
            is_categorical: false,
            geoparquet: false,
        }
    }
}

/// Columnar buffer for continuous raster aggregation row groups
pub struct ContinuousRowGroupBuffer {
    grid: GridKind,
    policy: OutputPolicy,
    compact: bool,
    geoparquet: bool,
    h3_indices: Vec<i64>,
    geometries: Vec<ByteArray>,
    /// `[min_x, min_y, max_x, max_y]` of the geometries pushed since `clear`.
    geometry_bounds: [f64; 4],
    min_values: Vec<f64>,
    max_values: Vec<f64>,
    sum_values: Vec<f64>,
    avg_values: Vec<f64>,
    pixel_counts: Vec<f64>,
    h3_hexes: Vec<ByteArray>,
    lats: Vec<f64>,
    lngs: Vec<f64>,
}

impl ParquetRowGroupBuffer for ContinuousRowGroupBuffer {
    type Record = MultiContinuousRecord;

    fn with_capacity_and_options(capacity: usize, compact: bool, geoparquet: bool) -> Self {
        Self::with_capacity_and_grid_options(capacity, compact, geoparquet, GridKind::H3)
    }

    fn with_capacity_and_grid_options(
        capacity: usize,
        compact: bool,
        geoparquet: bool,
        grid: GridKind,
    ) -> Self {
        Self {
            grid,
            policy: OutputPolicy::default(),
            compact,
            geoparquet,
            h3_indices: Vec::with_capacity(capacity),
            geometries: if geoparquet {
                Vec::with_capacity(capacity)
            } else {
                Vec::new()
            },
            geometry_bounds: EMPTY_BOUNDS,
            min_values: Vec::with_capacity(capacity),
            max_values: Vec::with_capacity(capacity),
            sum_values: Vec::with_capacity(capacity),
            avg_values: Vec::with_capacity(capacity),
            pixel_counts: Vec::with_capacity(capacity),
            h3_hexes: if compact {
                Vec::new()
            } else {
                Vec::with_capacity(capacity)
            },
            lats: if compact {
                Vec::new()
            } else {
                Vec::with_capacity(capacity)
            },
            lngs: if compact {
                Vec::new()
            } else {
                Vec::with_capacity(capacity)
            },
        }
    }

    fn set_output_policy(&mut self, policy: OutputPolicy) {
        self.policy = policy;
    }

    #[inline(always)]
    fn push_record(&mut self, record: Self::Record) {
        let cell_u64 = record.h3_index;
        let acc = record.accumulator;
        self.h3_indices.push(cell_u64 as i64);

        if self.geoparquet {
            let mut wkb_buf: WkbBuf = [0u8; WKB_BUF_LEN];
            if let Some(len) = cell_index_to_wkb(self.grid, cell_u64, &mut wkb_buf) {
                extend_wkb_bounds(&wkb_buf[..len], &mut self.geometry_bounds);
                self.geometries.push(ByteArray::from(&wkb_buf[..len]));
            } else {
                self.geometries.push(ByteArray::from(Vec::new()));
            }
        }

        // The columns are REQUIRED: a statistic without a value is NaN.
        let stats = self.policy.continuous(&acc);
        self.min_values.push(stats.min.unwrap_or(f64::NAN));
        self.max_values.push(stats.max.unwrap_or(f64::NAN));
        self.sum_values.push(stats.sum);
        self.avg_values.push(stats.mean.unwrap_or(f64::NAN));
        self.pixel_counts.push(stats.count);

        if !self.compact {
            let mut hex_buf = [0u8; 16];
            let hex_slice = fast_hex_u64(cell_u64, &mut hex_buf);
            self.h3_hexes.push(ByteArray::from(hex_slice));

            if let Some((lat, lng)) = self.grid.center_lat_lng(cell_u64) {
                self.lats.push(lat);
                self.lngs.push(lng);
            } else {
                self.lats.push(0.0);
                self.lngs.push(0.0);
            }
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.h3_indices.len()
    }

    fn geometry_bounds(&self) -> Option<[f64; 4]> {
        (self.geometry_bounds[0] <= self.geometry_bounds[2]).then_some(self.geometry_bounds)
    }

    fn clear(&mut self) {
        self.h3_indices.clear();
        if self.geoparquet {
            self.geometries.clear();
            self.geometry_bounds = EMPTY_BOUNDS;
        }
        self.min_values.clear();
        self.max_values.clear();
        self.sum_values.clear();
        self.avg_values.clear();
        self.pixel_counts.clear();
        if !self.compact {
            self.h3_hexes.clear();
            self.lats.clear();
            self.lngs.clear();
        }
    }

    fn sort_by_h3_index(&mut self) {
        let perm = match compute_sort_permutation(&self.h3_indices) {
            Some(p) => p,
            None => return,
        };

        reorder_by_perm(&mut self.h3_indices, &perm);
        if self.geoparquet {
            reorder_by_perm_take(&mut self.geometries, &perm);
        }
        if !self.compact {
            reorder_by_perm_take(&mut self.h3_hexes, &perm);
        }
        reorder_by_perm(&mut self.min_values, &perm);
        reorder_by_perm(&mut self.max_values, &perm);
        reorder_by_perm(&mut self.sum_values, &perm);
        reorder_by_perm(&mut self.avg_values, &perm);
        reorder_by_perm(&mut self.pixel_counts, &perm);
        if !self.compact {
            reorder_by_perm(&mut self.lats, &perm);
            reorder_by_perm(&mut self.lngs, &perm);
        }
    }

    fn flush_to_row_group(
        &self,
        writer: &mut SerializedFileWriter<File>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut row_group_writer = writer.next_row_group()?;

        write_i64_column(&mut row_group_writer, &self.h3_indices)?;
        if !self.compact {
            write_byte_array_column(&mut row_group_writer, &self.h3_hexes)?;
        }
        if self.geoparquet {
            write_byte_array_column(&mut row_group_writer, &self.geometries)?;
        }
        write_f64_column(&mut row_group_writer, &self.min_values)?;
        write_f64_column(&mut row_group_writer, &self.max_values)?;
        write_f64_column(&mut row_group_writer, &self.sum_values)?;
        write_f64_column(&mut row_group_writer, &self.avg_values)?;
        write_f64_column(&mut row_group_writer, &self.pixel_counts)?;
        if !self.compact {
            write_f64_column(&mut row_group_writer, &self.lats)?;
            write_f64_column(&mut row_group_writer, &self.lngs)?;
        }

        row_group_writer.close()?;
        Ok(())
    }

    fn schema_message(compact: bool, geoparquet: bool) -> &'static str {
        static H3: OnceLock<[String; 4]> = OnceLock::new();
        h3_schema(&H3, CONTINUOUS_VALUE_COLUMNS, compact, geoparquet)
    }

    fn schema_message_for(grid: GridKind, compact: bool, geoparquet: bool) -> Cow<'static, str> {
        if grid == GridKind::default() {
            return Cow::Borrowed(Self::schema_message(compact, geoparquet));
        }
        Cow::Owned(cell_schema_message(
            grid,
            CONTINUOUS_VALUE_COLUMNS,
            compact,
            geoparquet,
        ))
    }
}

/// Columnar buffer for categorical raster aggregation row groups
pub struct CategoricalRowGroupBuffer {
    grid: GridKind,
    policy: OutputPolicy,
    compact: bool,
    geoparquet: bool,
    h3_indices: Vec<i64>,
    geometries: Vec<ByteArray>,
    /// `[min_x, min_y, max_x, max_y]` of the geometries pushed since `clear`.
    geometry_bounds: [f64; 4],
    majorities: Vec<i64>,
    fractions: Vec<f64>,
    pixel_counts: Vec<f64>,
    distinct_classes: Vec<i64>,
    entropies: Vec<f64>,
    h3_hexes: Vec<ByteArray>,
    lats: Vec<f64>,
    lngs: Vec<f64>,
}

impl ParquetRowGroupBuffer for CategoricalRowGroupBuffer {
    type Record = MultiCategoricalRecord;

    fn with_capacity_and_options(capacity: usize, compact: bool, geoparquet: bool) -> Self {
        Self::with_capacity_and_grid_options(capacity, compact, geoparquet, GridKind::H3)
    }

    fn with_capacity_and_grid_options(
        capacity: usize,
        compact: bool,
        geoparquet: bool,
        grid: GridKind,
    ) -> Self {
        Self {
            grid,
            policy: OutputPolicy::default(),
            compact,
            geoparquet,
            h3_indices: Vec::with_capacity(capacity),
            geometries: if geoparquet {
                Vec::with_capacity(capacity)
            } else {
                Vec::new()
            },
            geometry_bounds: EMPTY_BOUNDS,
            majorities: Vec::with_capacity(capacity),
            fractions: Vec::with_capacity(capacity),
            pixel_counts: Vec::with_capacity(capacity),
            distinct_classes: Vec::with_capacity(capacity),
            entropies: Vec::with_capacity(capacity),
            h3_hexes: if compact {
                Vec::new()
            } else {
                Vec::with_capacity(capacity)
            },
            lats: if compact {
                Vec::new()
            } else {
                Vec::with_capacity(capacity)
            },
            lngs: if compact {
                Vec::new()
            } else {
                Vec::with_capacity(capacity)
            },
        }
    }

    fn set_output_policy(&mut self, policy: OutputPolicy) {
        self.policy = policy;
    }

    fn check_output_policy(policy: &OutputPolicy) -> Result<(), String> {
        if policy.emits_nodata() && policy.nodata_class().is_none() {
            return Err(
                "a categorical Parquet export with nodata_policy 'emit' needs an integer \
                 emit_nodata_value: its majority column cannot hold NULL"
                    .into(),
            );
        }
        Ok(())
    }

    #[inline(always)]
    fn push_record(&mut self, record: Self::Record) {
        let cell_u64 = record.h3_index;
        let acc = record.accumulator;
        let stats = self.policy.categorical(&acc);
        self.h3_indices.push(cell_u64 as i64);

        if self.geoparquet {
            let mut wkb_buf: WkbBuf = [0u8; WKB_BUF_LEN];
            if let Some(len) = cell_index_to_wkb(self.grid, cell_u64, &mut wkb_buf) {
                extend_wkb_bounds(&wkb_buf[..len], &mut self.geometry_bounds);
                self.geometries.push(ByteArray::from(&wkb_buf[..len]));
            } else {
                self.geometries.push(ByteArray::from(Vec::new()));
            }
        }

        // `check_output_policy` guarantees a class for cells without samples.
        self.majorities.push(stats.majority_class.unwrap_or(0));
        self.fractions.push(stats.majority_fraction);
        self.pixel_counts.push(stats.total_count);
        self.distinct_classes.push(stats.unique_classes);
        self.entropies.push(stats.shannon_entropy);

        if !self.compact {
            let mut hex_buf = [0u8; 16];
            let hex_slice = fast_hex_u64(cell_u64, &mut hex_buf);
            self.h3_hexes.push(ByteArray::from(hex_slice));

            if let Some((lat, lng)) = self.grid.center_lat_lng(cell_u64) {
                self.lats.push(lat);
                self.lngs.push(lng);
            } else {
                self.lats.push(0.0);
                self.lngs.push(0.0);
            }
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.h3_indices.len()
    }

    fn geometry_bounds(&self) -> Option<[f64; 4]> {
        (self.geometry_bounds[0] <= self.geometry_bounds[2]).then_some(self.geometry_bounds)
    }

    fn clear(&mut self) {
        self.h3_indices.clear();
        if self.geoparquet {
            self.geometries.clear();
            self.geometry_bounds = EMPTY_BOUNDS;
        }
        self.majorities.clear();
        self.fractions.clear();
        self.pixel_counts.clear();
        self.distinct_classes.clear();
        self.entropies.clear();
        if !self.compact {
            self.h3_hexes.clear();
            self.lats.clear();
            self.lngs.clear();
        }
    }

    fn sort_by_h3_index(&mut self) {
        let perm = match compute_sort_permutation(&self.h3_indices) {
            Some(p) => p,
            None => return,
        };

        reorder_by_perm(&mut self.h3_indices, &perm);
        if self.geoparquet {
            reorder_by_perm_take(&mut self.geometries, &perm);
        }
        if !self.compact {
            reorder_by_perm_take(&mut self.h3_hexes, &perm);
        }
        reorder_by_perm(&mut self.majorities, &perm);
        reorder_by_perm(&mut self.fractions, &perm);
        reorder_by_perm(&mut self.pixel_counts, &perm);
        reorder_by_perm(&mut self.distinct_classes, &perm);
        reorder_by_perm(&mut self.entropies, &perm);
        if !self.compact {
            reorder_by_perm(&mut self.lats, &perm);
            reorder_by_perm(&mut self.lngs, &perm);
        }
    }

    fn flush_to_row_group(
        &self,
        writer: &mut SerializedFileWriter<File>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut row_group_writer = writer.next_row_group()?;

        write_i64_column(&mut row_group_writer, &self.h3_indices)?;
        if !self.compact {
            write_byte_array_column(&mut row_group_writer, &self.h3_hexes)?;
        }
        if self.geoparquet {
            write_byte_array_column(&mut row_group_writer, &self.geometries)?;
        }
        write_i64_column(&mut row_group_writer, &self.majorities)?;
        write_f64_column(&mut row_group_writer, &self.fractions)?;
        write_f64_column(&mut row_group_writer, &self.pixel_counts)?;
        write_i64_column(&mut row_group_writer, &self.distinct_classes)?;
        write_f64_column(&mut row_group_writer, &self.entropies)?;
        if !self.compact {
            write_f64_column(&mut row_group_writer, &self.lats)?;
            write_f64_column(&mut row_group_writer, &self.lngs)?;
        }

        row_group_writer.close()?;
        Ok(())
    }

    fn schema_message(compact: bool, geoparquet: bool) -> &'static str {
        static H3: OnceLock<[String; 4]> = OnceLock::new();
        h3_schema(&H3, CATEGORICAL_VALUE_COLUMNS, compact, geoparquet)
    }

    fn schema_message_for(grid: GridKind, compact: bool, geoparquet: bool) -> Cow<'static, str> {
        if grid == GridKind::default() {
            return Cow::Borrowed(Self::schema_message(compact, geoparquet));
        }
        Cow::Owned(cell_schema_message(
            grid,
            CATEGORICAL_VALUE_COLUMNS,
            compact,
            geoparquet,
        ))
    }
}

/// Writes raster aggregations to Parquet or GeoParquet using the grid selected by the scan.
pub struct ParquetWriter;

impl ParquetWriter {
    /// Convenience helper to run continuous or categorical GeoTIFF/Mosaic-to-Parquet pipeline
    pub fn process_raster_source_to_parquet<P1: AsRef<Path>, P2: AsRef<Path>>(
        source: P1,
        parquet_path: P2,
        config: MultiResolutionConfig,
        parquet_config: ParquetExportConfig,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let source_str = source.as_ref().to_string_lossy();
        let resolved_paths = crate::raster::mosaic::resolve_raster_sources(&source_str)?;

        if parquet_config.is_categorical {
            if resolved_paths.len() == 1
                && !crate::raster::http_range::is_remote_url(
                    resolved_paths[0].to_str().unwrap_or(""),
                )
            {
                let reader = GeoTiffStreamReader::open(&resolved_paths[0])?;
                let streamer = MultiCategoricalHorizonStreamer::new(reader, &config)?;
                Self::write_categorical_streamer_to_parquet(streamer, parquet_path, parquet_config)
            } else {
                let mosaic = std::sync::Arc::new(
                    crate::raster::mosaic::MosaicReader::open_with_remote_source_factory(
                        &resolved_paths,
                        config.bbox,
                        config.custom_crs.as_deref(),
                        config.overlap_rule,
                        config.remote_source_factory.as_ref(),
                    )?,
                );
                let streamer = MultiCategoricalHorizonStreamer::new_mosaic(mosaic, &config)?;
                Self::write_categorical_streamer_to_parquet(streamer, parquet_path, parquet_config)
            }
        } else {
            if resolved_paths.len() == 1
                && !crate::raster::http_range::is_remote_url(
                    resolved_paths[0].to_str().unwrap_or(""),
                )
            {
                let reader = GeoTiffStreamReader::open(&resolved_paths[0])?;
                let streamer = MultiScanHorizonStreamer::new(reader, &config)?;
                Self::write_continuous_streamer_to_parquet(streamer, parquet_path, parquet_config)
            } else {
                let mosaic = std::sync::Arc::new(
                    crate::raster::mosaic::MosaicReader::open_with_remote_source_factory(
                        &resolved_paths,
                        config.bbox,
                        config.custom_crs.as_deref(),
                        config.overlap_rule,
                        config.remote_source_factory.as_ref(),
                    )?,
                );
                let streamer = MultiScanHorizonStreamer::new_mosaic(mosaic, &config)?;
                Self::write_continuous_streamer_to_parquet(streamer, parquet_path, parquet_config)
            }
        }
    }

    /// Stream continuous raster aggregation directly into Parquet with progress reporting
    pub fn write_continuous_streamer_to_parquet_with_progress<P: AsRef<Path>, F: FnMut(usize)>(
        streamer: MultiScanHorizonStreamer,
        parquet_path: P,
        parquet_config: ParquetExportConfig,
        progress: F,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        run_parquet_streaming_pipeline_with_progress::<_, ContinuousRowGroupBuffer, _, F>(
            streamer,
            parquet_path,
            parquet_config,
            progress,
        )
    }

    /// Stream continuous raster aggregation directly into Parquet
    pub fn write_continuous_streamer_to_parquet<P: AsRef<Path>>(
        streamer: MultiScanHorizonStreamer,
        parquet_path: P,
        parquet_config: ParquetExportConfig,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        run_parquet_streaming_pipeline::<_, ContinuousRowGroupBuffer, _>(
            streamer,
            parquet_path,
            parquet_config,
        )
    }

    /// Stream categorical raster aggregation directly into Parquet
    pub fn write_categorical_streamer_to_parquet<P: AsRef<Path>>(
        streamer: MultiCategoricalHorizonStreamer,
        parquet_path: P,
        parquet_config: ParquetExportConfig,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        run_parquet_streaming_pipeline::<_, CategoricalRowGroupBuffer, _>(
            streamer,
            parquet_path,
            parquet_config,
        )
    }
}
