//! [`PyRasterScanner`]: schema resolution, batch iteration and Arrow / JSON export.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use dggs_stream_core::aggregator::multi_horizon::MultiResolutionConfig;
use dggs_stream_core::aggregator::{NodataPolicy, SamplingPattern};
use dggs_stream_core::columnar::ground_area::{ground_area_schema, GroundAreaBatchReader};
use dggs_stream_core::columnar::multiband::{wide_schema, MultiBandBatchReader};
use dggs_stream_core::columnar::{
    partitioned_output_schema, ArrowArrayStream, ArrowSchema, CategoricalBatchReader,
    CategoricalFormat, ColumnArray, ColumnSchema, ContinuousBatchReader, PartitionedBatchReader,
    RecordBatch, PARTIAL_STATE_COLUMN,
};
use dggs_stream_core::error::{DggsStreamError, Result};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::raster::mosaic::{resolve_raster_sources, MosaicReader};
use dggs_stream_core::transfer::TransferMode;

use crate::config::PyScanConfig;

/// Boxed streaming iterator of [`RecordBatch`]es.
pub type BatchIter = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

/// Pull-based batch reader handed to Python, which wraps each batch as its own
/// single-batch Arrow stream so Polars can consume results incrementally.
pub struct PyBatchReader {
    pub(crate) schema: Arc<ColumnSchema>,
    pub(crate) iter: BatchIter,
    pub(crate) transform_failures: Arc<AtomicU64>,
}

/// Scanner backing the Python `RasterH3Scanner` object (`__arrow_c_schema__` and `__arrow_c_stream__`).
#[derive(Debug, Clone)]
pub struct PyRasterScanner {
    pub config: PyScanConfig,
}

impl PyRasterScanner {
    pub fn new(config: PyScanConfig) -> Self {
        Self { config }
    }

    fn base_schema(&self) -> ColumnSchema {
        let grid = self.config.grid;
        if self.config.ground_area {
            return ground_area_schema(grid, true);
        }
        if let Some(bands) = &self.config.bands {
            return wide_schema(grid, bands, &self.config.quantiles);
        }
        let schema = if !self.config.categorical {
            ColumnSchema::continuous_for(grid, &self.config.quantiles)
        } else {
            match self.config.format {
                CategoricalFormat::Wide => ColumnSchema::categorical_wide_for(grid),
                CategoricalFormat::Long => ColumnSchema::categorical_long_for(grid),
            }
        };
        schema
            .with_output_policy(&self.config.output)
            .with_histogram(self.config.histogram.as_ref())
    }

    pub(crate) fn resolve_projection(&self) -> Result<Option<Vec<usize>>> {
        let cfg = &self.config;
        if cfg.ground_area
            && (cfg.categorical
                || cfg.bands.is_some()
                || cfg.band != 1
                || !cfg.quantiles.is_empty()
                || cfg.spectral_formula.is_some()
                || cfg.transfer != TransferMode::AssignCenters
                || cfg.sampling != SamplingPattern::center()
                || cfg.output.nodata != NodataPolicy::Omit
                || cfg.valid_coverage_threshold.is_some())
        {
            return Err(DggsStreamError::InvalidParameter(
            "ground_area requires a continuous single-band center scan with omitted NoData and no bands, formulas, quantiles or coverage threshold".into(),
        ));
        }
        if self.config.ground_area && self.config.partition.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "ground_area cannot be combined with partitions".into(),
            ));
        }
        if self.config.bands.is_some() && self.config.partition.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "bands cannot be combined with partitions".into(),
            ));
        }
        let Some(cols) = &self.config.columns else {
            return Ok(None);
        };
        if cols.is_empty() {
            return Ok(None);
        }
        let base = self.base_schema();
        let mut indices = Vec::with_capacity(cols.len());
        // A partitioned scan appends `partial_state` itself, so the caller may list the
        // output columns as they appear.
        let partitioned = self.config.partition.is_some();
        for name in cols
            .iter()
            .filter(|c| !(partitioned && *c == PARTIAL_STATE_COLUMN))
        {
            let idx = base.index_of(name).ok_or_else(|| {
                DggsStreamError::InvalidParameter(format!(
                    "Column '{}' not found in dggs_stream schema (grid '{}')",
                    name, self.config.grid
                ))
            })?;
            indices.push(idx);
        }
        Ok(Some(indices))
    }

    /// Return the output [`ColumnSchema`] (respecting `columns` projection).
    pub fn schema(&self) -> Result<Arc<ColumnSchema>> {
        let base = self.base_schema();
        if self.config.partition.is_some() {
            self.check_partitionable()?;
            let projection = self.resolve_projection()?;
            return Ok(Arc::new(partitioned_output_schema(
                base,
                projection.as_deref(),
            )?));
        }
        match self.resolve_projection()? {
            Some(indices) => Ok(Arc::new(base.project(&indices)?)),
            None => Ok(Arc::new(base)),
        }
    }

    /// Options a partitioned scan cannot honour.
    fn check_partitionable(&self) -> Result<()> {
        if self.config.categorical && self.config.format == CategoricalFormat::Long {
            return Err(DggsStreamError::InvalidParameter(
                "partitioned scans support the wide categorical format only; \
                 reshape the merged result to long format afterwards"
                    .into(),
            ));
        }
        if self.config.limit.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "limit cannot be combined with a partition: a per-partition row limit has no \
                 meaning for the merged result"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Export an [`ArrowSchema`] struct for `__arrow_c_schema__`.
    pub fn to_arrow_c_schema(&self) -> Result<ArrowSchema> {
        let schema = self.schema()?;
        Ok(ArrowSchema::from_column_schema(&schema))
    }

    /// Open the streaming [`RecordBatch`] iterator for this scan.
    pub fn batch_iter(&self) -> Result<(Arc<ColumnSchema>, BatchIter)> {
        let paths = resolve_raster_sources(&self.config.path)?;
        let res = if self.config.resolutions.is_empty() {
            vec![8]
        } else {
            self.config.resolutions.clone()
        };
        let mut mr_cfg = MultiResolutionConfig::new(res);
        mr_cfg.custom_crs = self.config.crs.clone();
        mr_cfg.custom_nodata = self.config.nodata;
        mr_cfg.bbox = self.config.bbox;
        mr_cfg.overlap_rule = self.config.overlap_rule;
        mr_cfg.band = self.config.band;
        mr_cfg.sampling = self.config.sampling.clone();
        mr_cfg.spectral_formula = self.config.spectral_formula;
        mr_cfg.remapper = self.config.remapper.clone();
        mr_cfg.transfer = self.config.transfer;
        mr_cfg.valid_coverage_threshold = self.config.valid_coverage_threshold;
        if let Some(spec) = &self.config.histogram {
            mr_cfg.set_histogram(spec.clone())?;
        }
        if (self.config.aggregation_budget_bytes.is_some()
            || self.config.decode_budget_bytes.is_some())
            && mr_cfg.transfer != TransferMode::AssignCenters
        {
            return Err(DggsStreamError::InvalidParameter(
                "aggregation_budget_mb and decode_budget_mb require transfer=assign_centers".into(),
            ));
        }
        if let Some(bytes) = self.config.aggregation_budget_bytes {
            mr_cfg.aggregation_budget_bytes = bytes;
        }
        if let Some(bytes) = self.config.decode_budget_bytes {
            mr_cfg.decode_budget_bytes = bytes;
        }
        mr_cfg.profile = self.config.profile.clone();
        self.config.apply_cell_options(&mut mr_cfg);
        mr_cfg.strict_transforms = self.config.strict_transforms;
        mr_cfg.transform_failure_counter = self.config.transform_failure_counter.clone();

        let projection = self.resolve_projection()?;
        if self.config.ground_area {
            let [path] = paths.as_slice() else {
                return Err(DggsStreamError::InvalidParameter(
                    "ground_area requires one input raster".into(),
                ));
            };
            // Open the mosaic as the ordinary readers do: a bbox that misses the raster
            // is then an error, where a single-reader scan would quietly be empty.
            let mosaic = Arc::new(MosaicReader::open(
                std::slice::from_ref(path),
                mr_cfg.bbox,
                mr_cfg.custom_crs.as_deref(),
                mr_cfg.overlap_rule,
            )?);
            let reader = GroundAreaBatchReader::open_mosaic(
                mosaic,
                &mr_cfg,
                projection.unwrap_or_else(|| (0..self.base_schema().fields.len()).collect()),
                self.config.batch_size,
                self.config.limit,
            )?;
            return Ok((reader.schema(), Box::new(reader)));
        }
        if let Some(bands) = &self.config.bands {
            let [path] = paths.as_slice() else {
                return Err(DggsStreamError::InvalidParameter(
                    "bands requires one input raster".into(),
                ));
            };
            let reader = MultiBandBatchReader::open(
                GeoTiffStreamReader::open(path)?,
                &mr_cfg,
                bands,
                projection.as_deref(),
                self.config.batch_size,
                self.config.limit,
            )?;
            return Ok((reader.schema(), Box::new(reader)));
        }
        if let Some(partition) = &self.config.partition {
            self.check_partitionable()?;
            mr_cfg.partition = Some(partition.clone());
            let reader = if self.config.categorical {
                PartitionedBatchReader::open_categorical(
                    &paths,
                    &mr_cfg,
                    projection,
                    self.config.batch_size,
                )?
            } else {
                PartitionedBatchReader::open_continuous(
                    &paths,
                    &mr_cfg,
                    projection,
                    self.config.batch_size,
                )?
            };
            return Ok((reader.schema(), Box::new(reader)));
        }
        if !self.config.categorical {
            let reader = ContinuousBatchReader::open(
                &paths,
                &mr_cfg,
                projection,
                self.config.batch_size,
                self.config.limit,
            )?;
            Ok((reader.schema(), Box::new(reader)))
        } else {
            let reader = CategoricalBatchReader::open(
                &paths,
                &mr_cfg,
                self.config.format,
                projection,
                self.config.batch_size,
                self.config.limit,
            )?;
            Ok((reader.schema(), Box::new(reader)))
        }
    }

    /// Export an [`ArrowArrayStream`] struct for `__arrow_c_stream__`.
    pub fn to_arrow_c_stream(&self) -> Result<ArrowArrayStream> {
        let (schema, iter) = self.batch_iter()?;
        Ok(ArrowArrayStream::from_iterator(schema, iter))
    }

    /// Collect all batches into a columnar JSON object `{"col_name": [v1, v2, ...], ...}`.
    pub fn to_columnar_json(&self) -> Result<String> {
        let mut stream = self.to_arrow_c_stream()?;
        // SAFETY: `stream` was just built by `ArrowArrayStream::from_iterator`, so it is a
        // valid, unreleased stream owned by this frame.
        let (schema, batches) = unsafe { stream.collect_batches()? };
        batches_to_columnar_json(&schema, &batches)
    }
}

fn batches_to_columnar_json(schema: &ColumnSchema, batches: &[RecordBatch]) -> Result<String> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows).sum();
    let mut map = serde_json::Map::with_capacity(schema.fields.len());

    for (col_idx, field) in schema.fields.iter().enumerate() {
        let mut arr = Vec::with_capacity(total_rows);
        for batch in batches {
            let col = &batch.columns[col_idx];
            let validity = col.validity();
            for row in 0..batch.num_rows {
                if let Some(v) = validity {
                    if !v[row] {
                        arr.push(serde_json::Value::Null);
                        continue;
                    }
                }
                let val = match col {
                    ColumnArray::UInt64 { values, .. } => serde_json::json!(values[row]),
                    ColumnArray::Int64 { values, .. } => serde_json::json!(values[row]),
                    ColumnArray::UInt8 { values, .. } => serde_json::json!(values[row]),
                    ColumnArray::Float64 { values, .. } => {
                        let f = values[row];
                        if f.is_finite() {
                            serde_json::json!(f)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    ColumnArray::Boolean { values, .. } => serde_json::json!(values[row]),
                    ColumnArray::Utf8 { values, .. } => serde_json::json!(values[row]),
                    ColumnArray::Binary { values, .. } => {
                        let mut hex = String::with_capacity(values[row].len() * 2);
                        for &b in &values[row] {
                            use std::fmt::Write as _;
                            let _ = write!(&mut hex, "{:02x}", b);
                        }
                        serde_json::Value::String(hex)
                    }
                    ColumnArray::Histogram(column) => serde_json::from_str(&column.row_json(row))
                        .unwrap_or(serde_json::Value::Null),
                };
                arr.push(val);
            }
        }
        map.insert(field.name.clone(), serde_json::Value::Array(arr));
    }

    serde_json::to_string(&serde_json::Value::Object(map))
        .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))
}
