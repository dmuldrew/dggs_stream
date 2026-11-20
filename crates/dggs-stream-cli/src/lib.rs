//! # `dggs-stream-cli`
//!
//! Command-line pipeline utilities for [`dggs_stream_core`], supporting:
//! - Binary Columnar IPC (`RH3IPC01`) streaming to `stdout` or file
//! - Newline-delimited JSON (`JSONEachRow` / NDJSON) streaming to `stdout` for ClickHouse
//!   executable table functions and shell pipelines
//! - Native Parquet / GeoParquet and PMTiles v3 exports
//! - Transcoding any H3-indexed Parquet file to PMTiles v3 ([`transcode_parquet_to_pmtiles`])
//! - Partitioned scans for shell fan-outs: [`plan_raster_partitions`], a partition per
//!   [`CliStreamOptions::partition`] scan, and [`merge_partition_files`]

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use dggs_stream_core::aggregator::multi_horizon::{
    plan_partitions, ChunkBlock, MultiCategoricalHorizonStreamer, MultiResolutionConfig,
    MultiScanHorizonStreamer, ScanPartition,
};
use dggs_stream_core::aggregator::output_policy::{NodataPolicy, OutputPolicy, MAX_DECIMALS};
use dggs_stream_core::columnar::ipc::{write_ipc_stream, write_jsonl_stream, IpcStreamReader};
use dggs_stream_core::columnar::partitioned::{
    merge_partial_batches, PartitionedBatchReader, PARTIAL_STATE_COLUMN,
};
use dggs_stream_core::columnar::{
    CategoricalBatchReader, CategoricalFormat, ColumnArray, ColumnSchema, ContinuousBatchReader,
    RecordBatch,
};
use dggs_stream_core::error::{DggsStreamError, Result};
use dggs_stream_core::grid::GridKind;
use dggs_stream_core::histogram::HistogramSpec;
use dggs_stream_core::parquet::{ParquetExportConfig, ParquetWriter};
use dggs_stream_core::pmtiles::{PmtilesExportSummary, PmtilesTiler};
use dggs_stream_core::raster::mosaic::{resolve_raster_sources, MosaicReader, OverlapRule};
use dggs_stream_core::resolution::ResolutionSpec;
use dggs_stream_core::run_profile::RunProfile;
use dggs_stream_core::scalar_ops::{parse_bbox_str, try_parse_resolutions_str_for};
use dggs_stream_core::transfer::TransferMode;

/// Output wire format for streaming CLI queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOutputFormat {
    /// Binary columnar IPC stream (`RH3IPC01`).
    Ipc,
    /// Newline-delimited JSON (`JSONEachRow`), compatible with ClickHouse executable table functions.
    Jsonl,
}

/// Configuration for streaming a raster aggregation to a writer (`stdout` or file).
#[derive(Debug, Clone)]
pub struct CliStreamOptions {
    pub input_path: String,
    pub bands: Option<String>,
    pub grid: GridKind,
    pub resolutions: Vec<u8>,
    pub categorical: bool,
    pub categorical_format: CategoricalFormat,
    pub source_crs: Option<String>,
    pub nodata: Option<f64>,
    /// Calibrate stored values with GDAL band scale/offset (`--raw-values` disables).
    pub apply_scale_offset: bool,
    pub bbox: Option<[f64; 4]>,
    pub columns: Option<Vec<String>>,
    pub batch_size: usize,
    pub limit: Option<usize>,
    pub output_format: StreamOutputFormat,
    /// Scan only this partition (from [`plan_raster_partitions`]). The output gains a
    /// nullable `partial_state` column; combine the partitions with [`merge_partition_files`].
    pub partition: Option<ScanPartition>,
    /// `--nodata-policy`, `--emit-nodata-value` and `--decimals`: whether all-NoData
    /// cells are output and with what value, and the decimal rounding of statistics.
    pub output: OutputPolicy,
    /// `--transfer`, `--interpolation`, `--overlay-weighted` and `--mass-preserve`: how
    /// pixels map onto cells (default: each pixel into the cell holding its centre).
    pub transfer: TransferMode,
    /// Minimum valid fraction of selected raster overlap.
    pub valid_coverage_threshold: Option<f64>,
    /// `--histogram`, `--histogram-weight` and `--histogram-normalize`: a structured
    /// per-cell histogram (see [`dggs_stream_core::histogram`]). Implies `--categorical`.
    pub histogram: Option<HistogramSpec>,
    /// `--profile`: collect runtime telemetry of the scan (see
    /// [`dggs_stream_core::run_profile`]); the binary prints the report on stderr.
    pub profile: Option<Arc<RunProfile>>,
}

impl Default for CliStreamOptions {
    fn default() -> Self {
        Self {
            input_path: String::new(),
            bands: None,
            grid: GridKind::H3,
            resolutions: vec![8],
            categorical: false,
            categorical_format: CategoricalFormat::Wide,
            source_crs: None,
            nodata: None,
            apply_scale_offset: true,
            bbox: None,
            columns: None,
            batch_size: 2048,
            limit: None,
            output_format: StreamOutputFormat::Ipc,
            partition: None,
            output: OutputPolicy::default(),
            transfer: TransferMode::AssignCenters,
            valid_coverage_threshold: None,
            histogram: None,
            profile: None,
        }
    }
}

fn resolve_projection_indices(
    schema: &ColumnSchema,
    columns: &Option<Vec<String>>,
) -> Result<Option<Vec<usize>>> {
    let Some(cols) = columns else {
        return Ok(None);
    };
    if cols.is_empty() {
        return Ok(None);
    }
    let mut indices = Vec::with_capacity(cols.len());
    for name in cols {
        let idx = schema.index_of(name).ok_or_else(|| {
            DggsStreamError::InvalidParameter(format!(
                "Unknown column name in projection: '{}'",
                name
            ))
        })?;
        indices.push(idx);
    }
    Ok(Some(indices))
}

/// Copy the NoData policy, NoData value and decimals onto a scan configuration.
fn apply_output_policy(config: &mut MultiResolutionConfig, policy: &OutputPolicy) {
    config.nodata_policy = policy.nodata;
    config.emit_nodata_value = policy.nodata_value;
    config.decimals = policy.decimals;
}

/// Print the runtime profile of a finished scan on stderr (`--profile`).
fn print_profile(profile: &Option<Arc<RunProfile>>) {
    if let Some(profile) = profile {
        eprintln!("{}", profile.report());
    }
}

/// Report valid samples the coordinate transform could not place, on stderr
/// so streamed output stays clean.
fn warn_transform_failures(failures: u64) {
    if failures > 0 {
        eprintln!(
            "warning: {failures} valid sample(s) were dropped because their coordinate \
             transform failed or fell outside the valid latitude/longitude range; check the source CRS"
        );
    }
}

impl CliStreamOptions {
    /// Shared scan settings for streamed and file outputs.
    pub fn scan_config(&self) -> MultiResolutionConfig {
        let mut config = MultiResolutionConfig::new(self.resolutions.clone()).with_grid(self.grid);
        config.custom_crs = self.source_crs.clone();
        config.custom_nodata = self.nodata;
        config.apply_scale_offset = self.apply_scale_offset;
        config.bbox = self.bbox;
        config.overlap_rule = OverlapRule::Cutline;
        apply_output_policy(&mut config, &self.output);
        config.transfer = self.transfer;
        config.profile = self.profile.clone();
        config.valid_coverage_threshold = self.valid_coverage_threshold;
        config
    }
}

/// Stream a GeoTIFF/COG aggregation directly to `writer` in binary IPC or JSONL format.
/// Returns the total number of rows emitted.
pub fn stream_raster_to_writer<W: Write>(opts: &CliStreamOptions, writer: &mut W) -> Result<usize> {
    let rows = stream_raster(opts, writer);
    // The scan's stream has been dropped by now, so its totals are in the profile.
    print_profile(&opts.profile);
    rows
}

fn stream_raster<W: Write>(opts: &CliStreamOptions, writer: &mut W) -> Result<usize> {
    let paths = resolve_raster_sources(&opts.input_path)?;
    let mut config = opts.scan_config();
    if let Some(spec) = &opts.histogram {
        if opts.partition.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "--histogram cannot be combined with --partition".into(),
            ));
        }
        config.set_histogram(spec.clone())?;
    }

    if let Some(selector) = &opts.bands {
        use dggs_stream_core::columnar::multiband::{
            parse_bands, wide_schema, MultiBandBatchReader,
        };
        if opts.categorical || opts.partition.is_some() || opts.histogram.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "--bands requires an unpartitioned continuous scan".into(),
            ));
        }
        let [path] = paths.as_slice() else {
            return Err(DggsStreamError::InvalidParameter(
                "--bands requires one raster".into(),
            ));
        };
        let raster = dggs_stream_core::raster::geotiff::GeoTiffStreamReader::open(path)?;
        let bands = parse_bands(selector, raster.metadata.samples_per_pixel)?;
        let projection = resolve_projection_indices(
            &wide_schema(opts.grid, &bands, &config.quantiles),
            &opts.columns,
        )?;
        let mut reader = MultiBandBatchReader::open(
            raster,
            &config,
            &bands,
            projection.as_deref(),
            opts.batch_size,
            opts.limit,
        )?;
        let schema = reader.schema();
        let rows = match opts.output_format {
            StreamOutputFormat::Ipc => write_ipc_stream(writer, &schema, &mut reader),
            StreamOutputFormat::Jsonl => write_jsonl_stream(writer, &mut reader),
        }?;
        warn_transform_failures(reader.transform_failures());
        return Ok(rows);
    }
    if let Some(partition) = &opts.partition {
        config.partition = Some(partition.clone());
        return stream_partition(opts, &paths, &config, writer);
    }

    if !opts.categorical {
        let full_schema = ColumnSchema::continuous_for(opts.grid, &config.quantiles);
        let projection = resolve_projection_indices(&full_schema, &opts.columns)?;
        let mut reader =
            ContinuousBatchReader::open(&paths, &config, projection, opts.batch_size, opts.limit)?;
        let schema = reader.schema();
        let rows = match opts.output_format {
            StreamOutputFormat::Ipc => write_ipc_stream(writer, &schema, &mut reader),
            StreamOutputFormat::Jsonl => write_jsonl_stream(writer, &mut reader),
        }?;
        warn_transform_failures(reader.transform_failures());
        Ok(rows)
    } else {
        let full_schema = match opts.categorical_format {
            CategoricalFormat::Wide => ColumnSchema::categorical_wide_for(opts.grid),
            CategoricalFormat::Long => ColumnSchema::categorical_long_for(opts.grid),
        };
        let projection = resolve_projection_indices(&full_schema, &opts.columns)?;
        let mut reader = CategoricalBatchReader::open(
            &paths,
            &config,
            opts.categorical_format,
            projection,
            opts.batch_size,
            opts.limit,
        )?;
        let schema = reader.schema();
        let rows = match opts.output_format {
            StreamOutputFormat::Ipc => write_ipc_stream(writer, &schema, &mut reader),
            StreamOutputFormat::Jsonl => write_jsonl_stream(writer, &mut reader),
        }?;
        warn_transform_failures(reader.transform_failures());
        Ok(rows)
    }
}

/// Scan one partition (`config.partition`) to `writer`: the projected columns plus
/// `partial_state`. Only IPC output is mergeable, so other formats are refused.
fn stream_partition<W: Write>(
    opts: &CliStreamOptions,
    paths: &[PathBuf],
    config: &MultiResolutionConfig,
    writer: &mut W,
) -> Result<usize> {
    let refuse = |why: &str| {
        Err(DggsStreamError::InvalidParameter(format!(
            "--partition {why}"
        )))
    };
    if opts.output_format != StreamOutputFormat::Ipc {
        return refuse("needs `ipc` output: `merge` reads the partitions' IPC files");
    }
    if opts.limit.is_some() {
        return refuse("cannot be combined with --limit (a partition holds parts of cells)");
    }
    if opts.categorical && opts.categorical_format == CategoricalFormat::Long {
        return refuse("needs the wide categorical format");
    }
    // `partial_state` may be listed in --columns; it is always appended.
    let columns = opts.columns.as_ref().map(|cols| {
        cols.iter()
            .filter(|c| !c.eq_ignore_ascii_case(PARTIAL_STATE_COLUMN))
            .cloned()
            .collect::<Vec<_>>()
    });
    let mut reader = if opts.categorical {
        let projection =
            resolve_projection_indices(&ColumnSchema::categorical_wide_for(opts.grid), &columns)?;
        PartitionedBatchReader::open_categorical(paths, config, projection, opts.batch_size)?
    } else {
        let full = ColumnSchema::continuous_for(opts.grid, &config.quantiles);
        let projection = resolve_projection_indices(&full, &columns)?;
        PartitionedBatchReader::open_continuous(paths, config, projection, opts.batch_size)?
    };
    let header = PartitionHeader {
        partition: config
            .partition
            .clone()
            .expect("a partitioned scan has a partition"),
        scan: scan_identity(opts),
    };
    write_partition_header(writer, &header)?;
    let schema = reader.schema();
    let rows = write_ipc_stream(writer, &schema, &mut reader)?;
    warn_transform_failures(reader.transform_failures());
    Ok(rows)
}

/// Magic prefix of a partition file: the header below, then an `RH3IPC01` stream.
const PARTITION_MAGIC: &[u8; 8] = b"RH3PART1";

/// What a partition file was scanned from, so `merge` can check that its inputs are the
/// complete set of partitions of one scan.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct PartitionHeader {
    partition: ScanPartition,
    /// The scan's input and the options that change its values (columns are checked
    /// through the stream schema).
    scan: serde_json::Value,
}

fn scan_identity(opts: &CliStreamOptions) -> serde_json::Value {
    serde_json::json!({
        "input": opts.input_path,
        "grid": opts.grid.as_str(),
        "resolutions": opts.resolutions,
        "categorical": opts.categorical,
        "crs": opts.source_crs,
        "nodata": opts.nodata,
        "apply_scale_offset": opts.apply_scale_offset,
        "bbox": opts.bbox,
        "nodata_policy": opts.output.nodata.as_str(),
        "emit_nodata_value": opts.output.nodata_value,
        "decimals": opts.output.decimals,
    })
}

/// The output policy a partition file's header records (the default for files written
/// before it was recorded).
fn header_output_policy(scan: &serde_json::Value) -> OutputPolicy {
    OutputPolicy {
        nodata: scan
            .get("nodata_policy")
            .and_then(|v| v.as_str())
            .and_then(NodataPolicy::parse)
            .unwrap_or_default(),
        nodata_value: scan.get("emit_nodata_value").and_then(|v| v.as_f64()),
        decimals: scan
            .get("decimals")
            .and_then(|v| v.as_u64())
            .and_then(|d| u8::try_from(d).ok()),
    }
}

fn write_partition_header<W: Write>(writer: &mut W, header: &PartitionHeader) -> Result<()> {
    let json =
        serde_json::to_vec(header).map_err(|e| DggsStreamError::StreamFailed(e.to_string()))?;
    writer.write_all(PARTITION_MAGIC)?;
    writer.write_all(&(json.len() as u32).to_le_bytes())?;
    writer.write_all(&json)?;
    Ok(())
}

/// Largest header accepted, guarding against reading a huge length from a corrupt file.
const MAX_PARTITION_HEADER: usize = 64 << 20;

fn read_partition_header<R: Read>(reader: &mut R) -> Result<PartitionHeader> {
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != PARTITION_MAGIC {
        return Err(DggsStreamError::InvalidParameter(
            "not a partition file (missing RH3PART1 header)".into(),
        ));
    }
    let mut len = [0u8; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_PARTITION_HEADER {
        return Err(DggsStreamError::InvalidParameter(format!(
            "partition header of {len} bytes is too large"
        )));
    }
    let mut json = vec![0u8; len];
    reader.read_exact(&mut json)?;
    serde_json::from_slice(&json)
        .map_err(|e| DggsStreamError::InvalidParameter(format!("invalid partition header: {e}")))
}

/// Split a raster or mosaic scan into partitions of about `target_chunks` TIFF tiles or
/// strips each, reading only metadata. `source_crs` and `bbox` must be the ones the
/// partitions will be scanned with (a scan with another layout refuses the partition).
pub fn plan_raster_partitions(
    input_path: &str,
    target_chunks: usize,
    source_crs: Option<&str>,
    bbox: Option<[f64; 4]>,
) -> Result<Vec<ScanPartition>> {
    if target_chunks == 0 {
        return Err(DggsStreamError::InvalidParameter(
            "--target-chunks must be at least 1".into(),
        ));
    }
    let paths = resolve_raster_sources(input_path)?;
    // Open the mosaic exactly as the scans do, so the fingerprints match.
    let mosaic = MosaicReader::open(&paths, bbox, source_crs, OverlapRule::Cutline)?;
    plan_partitions(&mosaic, target_chunks)
}

/// Write a plan as JSON Lines, one partition spec per line (line `k` is partition `k`).
pub fn write_plan<W: Write>(plan: &[ScanPartition], writer: &mut W) -> Result<()> {
    for partition in plan {
        writeln!(writer, "{}", partition.to_json())?;
    }
    writer.flush()?;
    Ok(())
}

/// Read a partition spec: a JSON object, or `@FILE` holding one spec or a plan written by
/// [`write_plan`], from which `index` picks a line (required when it has several).
pub fn read_partition_spec(spec: &str, index: Option<usize>) -> Result<ScanPartition> {
    let Some(path) = spec.strip_prefix('@') else {
        if index.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "--partition-index needs --partition @PLAN_FILE".into(),
            ));
        }
        return ScanPartition::from_json(spec);
    };
    let text = std::fs::read_to_string(path).map_err(|e| {
        DggsStreamError::InvalidParameter(format!("cannot read partition file '{path}': {e}"))
    })?;
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let line = match (index, lines.as_slice()) {
        (None, [only]) => *only,
        (None, _) => {
            return Err(DggsStreamError::InvalidParameter(format!(
                "'{path}' holds {} partition specs: pick one with --partition-index",
                lines.len()
            )))
        }
        (Some(i), _) => *lines.get(i).ok_or_else(|| {
            DggsStreamError::InvalidParameter(format!(
                "--partition-index {i} is out of range: '{path}' holds {} partition specs",
                lines.len()
            ))
        })?,
    };
    ScanPartition::from_json(line)
}

/// Options for [`merge_partition_files`].
#[derive(Debug, Clone)]
pub struct CliMergeOptions {
    /// Partition files written by `ipc --partition`: every partition of one plan, once,
    /// scanned from the same input with the same options and columns.
    pub inputs: Vec<PathBuf>,
    /// Categorical partitions (also inferred from categorical-only columns).
    pub categorical: bool,
    pub output_format: StreamOutputFormat,
    pub batch_size: usize,
}

/// Combine the partition files of a scan into its final rows: every file's final rows are
/// passed through (without `partial_state`), then the partial rows of all files are merged
/// per cell. The result equals an unpartitioned scan's rows, in another order.
///
/// Before writing anything, the files' headers are checked to be every partition of one
/// plan exactly once, scanned from the same input with the same options. Files are then
/// streamed one batch at a time; only partial rows are held in memory. Returns the number
/// of rows written.
pub fn merge_partition_files<W: Write>(opts: &CliMergeOptions, writer: &mut W) -> Result<usize> {
    if opts.inputs.is_empty() {
        return Err(DggsStreamError::InvalidParameter(
            "merge needs at least one partition file".into(),
        ));
    }
    let mut headers = Vec::with_capacity(opts.inputs.len());
    let mut schema: Option<Arc<ColumnSchema>> = None;
    for path in &opts.inputs {
        let (header, stream) = open_partition_file(path)?;
        match &schema {
            None => schema = Some(stream.schema()),
            Some(first) if **first != *stream.schema() => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "'{}' has other columns than '{}'; merge the partitions of one scan",
                    path.display(),
                    opts.inputs[0].display()
                )))
            }
            Some(_) => {}
        }
        headers.push(header);
    }
    check_partition_set(&opts.inputs, &headers)?;

    let mut files: VecDeque<PathBuf> = opts.inputs.iter().cloned().collect();
    let first_path = files.pop_front().expect("inputs are not empty");
    let (_, first) = open_partition_file(&first_path)?;
    let input_schema = first.schema();
    let state_idx = input_schema.index_of(PARTIAL_STATE_COLUMN).ok_or_else(|| {
        DggsStreamError::InvalidParameter(format!(
            "'{}' has no {PARTIAL_STATE_COLUMN} column",
            first_path.display()
        ))
    })?;
    let keep: Vec<usize> = (0..input_schema.fields.len())
        .filter(|&i| i != state_idx)
        .collect();
    let names: Vec<&str> = keep
        .iter()
        .map(|&i| input_schema.fields[i].name.as_str())
        .collect();

    // The partitions' own header names the grid; older files only show it in the
    // index column's name.
    let grid = headers[0]
        .scan
        .get("grid")
        .and_then(|v| v.as_str())
        .and_then(GridKind::parse)
        .or_else(|| {
            GridKind::ALL.into_iter().find(|grid| {
                input_schema.index_of(grid.index_column_name()).is_some()
                    || input_schema.index_of(grid.hex_column_name()).is_some()
            })
        })
        .unwrap_or(GridKind::H3);
    let continuous = ColumnSchema::continuous_for(grid, &[]);
    let wide = ColumnSchema::categorical_wide_for(grid);
    let categorical_only = names.iter().any(|n| continuous.index_of(n).is_none());
    let categorical = opts.categorical || categorical_only;
    let base = if categorical { &wide } else { &continuous };
    let projection = names
        .iter()
        .map(|n| {
            base.index_of(n).ok_or_else(|| {
                DggsStreamError::InvalidParameter(format!(
                    "column '{n}' is not a {} scan column",
                    if categorical {
                        "categorical"
                    } else {
                        "continuous"
                    }
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let output_schema = Arc::new(input_schema.project(&keep)?);

    let mut merger = PartitionMerge {
        files,
        input_schema: Arc::clone(&input_schema),
        state_idx,
        keep,
        current: Some(first),
        partials: Vec::new(),
        merged: VecDeque::new(),
        merge: Some(MergeSpec {
            grid,
            categorical,
            projection,
            batch_size: opts.batch_size,
            output: header_output_policy(&headers[0].scan),
        }),
    };
    let rows = match opts.output_format {
        StreamOutputFormat::Ipc => write_ipc_stream(writer, &output_schema, &mut merger),
        StreamOutputFormat::Jsonl => write_jsonl_stream(writer, &mut merger),
    }?;
    Ok(rows)
}

type PartitionStream = IpcStreamReader<BufReader<File>>;

/// Open a partition file: its header, and a reader positioned at its first batch.
fn open_partition_file(path: &Path) -> Result<(PartitionHeader, PartitionStream)> {
    let context = |e: DggsStreamError| {
        DggsStreamError::InvalidParameter(format!(
            "'{}' is not a partition file written by `ipc --partition`: {e}",
            path.display()
        ))
    };
    let file = File::open(path).map_err(|e| {
        DggsStreamError::InvalidParameter(format!("cannot open '{}': {e}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    let header = read_partition_header(&mut reader).map_err(context)?;
    let stream = IpcStreamReader::new(reader).map_err(context)?;
    Ok((header, stream))
}

/// Check that `headers` are every partition of one plan exactly once, from one scan.
fn check_partition_set(paths: &[PathBuf], headers: &[PartitionHeader]) -> Result<()> {
    let invalid = |msg: String| Err(DggsStreamError::InvalidParameter(msg));
    let first = &headers[0];
    let name = |i: usize| paths[i].display().to_string();
    for (i, h) in headers.iter().enumerate().skip(1) {
        if h.scan != first.scan {
            return invalid(format!(
                "'{}' was scanned with other options than '{}' ({} vs {}); merge the partitions of one scan",
                name(i),
                name(0),
                h.scan,
                first.scan
            ));
        }
        if h.partition.count != first.partition.count
            || h.partition.mosaic_fingerprint != first.partition.mosaic_fingerprint
        {
            return invalid(format!(
                "'{}' belongs to another plan than '{}'",
                name(i),
                name(0)
            ));
        }
    }
    let count = first.partition.count as usize;
    let mut seen: Vec<Option<usize>> = vec![None; count];
    for (i, h) in headers.iter().enumerate() {
        let index = h.partition.index as usize;
        match seen.get_mut(index) {
            None => {
                return invalid(format!(
                    "'{}' has partition index {index} of {count}",
                    name(i)
                ))
            }
            Some(Some(j)) => {
                return invalid(format!(
                    "partition {index} is given twice ('{}' and '{}')",
                    name(*j),
                    name(i)
                ))
            }
            Some(slot) => *slot = Some(i),
        }
    }
    let missing: Vec<String> = seen
        .iter()
        .enumerate()
        .filter(|(_, s)| s.is_none())
        .map(|(k, _)| k.to_string())
        .collect();
    if !missing.is_empty() {
        return invalid(format!(
            "missing partition(s) {} of {count}: merge needs every partition of the plan",
            missing.join(", ")
        ));
    }
    // Plans with the same partition count for one layout could still differ; their
    // partitions would then overlap.
    let blocks: Vec<(usize, &ChunkBlock)> = headers
        .iter()
        .enumerate()
        .flat_map(|(i, h)| h.partition.blocks.iter().map(move |b| (i, b)))
        .collect();
    for (a, (i, x)) in blocks.iter().enumerate() {
        for (j, y) in &blocks[a + 1..] {
            let overlap = x.tile_idx == y.tile_idx
                && x.col_start < y.col_end
                && y.col_start < x.col_end
                && x.row_start < y.row_end
                && y.row_start < x.row_end;
            if overlap {
                return invalid(format!(
                    "'{}' and '{}' cover the same chunks: they come from different plans",
                    name(*i),
                    name(*j)
                ));
            }
        }
    }
    Ok(())
}

struct MergeSpec {
    grid: GridKind,
    categorical: bool,
    projection: Vec<usize>,
    batch_size: usize,
    /// NoData policy, NoData value and decimals of the scan, so merged boundary cells
    /// are written as the interior ones were.
    output: OutputPolicy,
}

/// Yields every file's final rows, streaming the files one batch at a time and keeping
/// their partial rows, then the merged partial rows.
struct PartitionMerge {
    files: VecDeque<PathBuf>,
    input_schema: Arc<ColumnSchema>,
    state_idx: usize,
    keep: Vec<usize>,
    /// The file being read.
    current: Option<PartitionStream>,
    partials: Vec<RecordBatch>,
    /// Merged output batches.
    merged: VecDeque<RecordBatch>,
    /// Taken when the merged rows are queued.
    merge: Option<MergeSpec>,
}

impl PartitionMerge {
    /// The next output batch; `Ok(None)` when done.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.merged.pop_front() {
                return Ok(Some(batch));
            }
            if let Some(stream) = &mut self.current {
                match stream.next().transpose()? {
                    Some(batch) => {
                        if let Some(finals) = self.split(batch)? {
                            return Ok(Some(finals));
                        }
                    }
                    None => self.current = None,
                }
                continue;
            }
            if let Some(path) = self.files.pop_front() {
                let (_, stream) = open_partition_file(&path)?;
                if *stream.schema() != *self.input_schema {
                    return Err(DggsStreamError::InvalidParameter(format!(
                        "'{}' changed while merging",
                        path.display()
                    )));
                }
                self.current = Some(stream);
                continue;
            }
            let Some(spec) = self.merge.take() else {
                return Ok(None);
            };
            // The CLI's scans have no value filters or quantiles, so the default merge
            // settings match them apart from the output policy; the resolutions come from
            // the partial rows.
            let mut config = MultiResolutionConfig::new(vec![8]).with_grid(spec.grid);
            apply_output_policy(&mut config, &spec.output);
            let (_, merged) = merge_partial_batches(
                &std::mem::take(&mut self.partials),
                spec.categorical,
                &config,
                Some(&spec.projection),
                spec.batch_size,
            )?;
            self.merged = merged.into();
        }
    }

    /// Keep the partial rows of an input batch; return its final rows without
    /// `partial_state` (`None` when it has none). Rows are split into contiguous runs.
    fn split(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let is_partial: Vec<bool> = match &batch.columns[self.state_idx] {
            ColumnArray::Binary { validity, .. } => match validity {
                Some(v) => v.clone(),
                None => vec![true; batch.num_rows],
            },
            _ => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "{PARTIAL_STATE_COLUMN} must be a Binary column"
                )))
            }
        };
        let mut finals: Vec<RecordBatch> = Vec::new();
        let mut start = 0;
        while start < batch.num_rows {
            let partial = is_partial[start];
            let len = is_partial[start..]
                .iter()
                .take_while(|&&p| p == partial)
                .count();
            let run = batch.slice(start, len);
            if partial {
                self.partials.push(run);
            } else {
                finals.push(run.project(&self.keep)?);
            }
            start += len;
        }
        Ok(match finals.len() {
            0 => None,
            1 => finals.pop(),
            _ => Some(concat_batches(finals)?),
        })
    }
}

impl Iterator for PartitionMerge {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

/// Concatenate batches of one schema.
fn concat_batches(batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    let schema = Arc::clone(&batches[0].schema);
    let columns = (0..schema.fields.len())
        .map(|c| {
            let mut parts = batches.iter().map(|b| b.columns[c].clone());
            let mut acc = parts.next().expect("at least one batch");
            for part in parts {
                append_column(&mut acc, part);
            }
            acc
        })
        .collect();
    RecordBatch::try_new(schema, columns)
}

fn append_column(acc: &mut ColumnArray, part: ColumnArray) {
    let (acc_len, part_len) = (acc.len(), part.len());
    fn join(a: &mut Option<Vec<bool>>, b: Option<Vec<bool>>, a_len: usize, b_len: usize) {
        if a.is_none() && b.is_none() {
            return;
        }
        let mut v = a.take().unwrap_or_else(|| vec![true; a_len]);
        v.extend(b.unwrap_or_else(|| vec![true; b_len]));
        *a = Some(v);
    }
    macro_rules! extend {
        ($($variant:ident),*) => {
            match (acc, part) {
                $((
                    ColumnArray::$variant { values, validity },
                    ColumnArray::$variant { values: more, validity: more_validity },
                ) => {
                    values.extend(more);
                    join(validity, more_validity, acc_len, part_len);
                })*
                _ => unreachable!("batches of one schema have equal column types"),
            }
        };
    }
    extend!(UInt64, Int64, UInt8, Float64, Boolean, Utf8, Binary);
}

/// Export a raster directly to a Parquet / GeoParquet file.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_parquet(
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    geoparquet: bool,
    apply_scale_offset: bool,
) -> Result<usize> {
    export_raster_to_parquet_with_grid(
        GridKind::H3,
        input_path,
        output_path,
        resolutions,
        categorical,
        source_crs,
        nodata,
        bbox,
        geoparquet,
        apply_scale_offset,
    )
}

/// Export a raster directly to a Parquet / GeoParquet file using `grid`.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_parquet_with_grid(
    grid: GridKind,
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    geoparquet: bool,
    apply_scale_offset: bool,
) -> Result<usize> {
    export_raster_to_parquet_with_policy(
        grid,
        input_path,
        output_path,
        resolutions,
        categorical,
        source_crs,
        nodata,
        bbox,
        geoparquet,
        apply_scale_offset,
        &OutputPolicy::default(),
    )
}

/// [`export_raster_to_parquet_with_grid`] under an [`OutputPolicy`] (NoData policy and
/// value, decimal rounding). Parquet statistics columns are `REQUIRED`, so a statistic
/// without a value is written as NaN.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_parquet_with_policy(
    grid: GridKind,
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    geoparquet: bool,
    apply_scale_offset: bool,
    policy: &OutputPolicy,
) -> Result<usize> {
    export_raster_to_parquet_with_transfer(
        grid,
        input_path,
        output_path,
        resolutions,
        categorical,
        source_crs,
        nodata,
        bbox,
        geoparquet,
        apply_scale_offset,
        policy,
        TransferMode::AssignCenters,
        None,
    )
}

/// [`export_raster_to_parquet_with_policy`] with a [`TransferMode`] (`interpolate` or
/// `overlay` instead of pixel-centre assignment) and an optional runtime profile.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_parquet_with_transfer(
    grid: GridKind,
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    geoparquet: bool,
    apply_scale_offset: bool,
    policy: &OutputPolicy,
    transfer: TransferMode,
    profile: Option<Arc<RunProfile>>,
) -> Result<usize> {
    let mut config = MultiResolutionConfig::new(resolutions).with_grid(grid);
    config.custom_crs = source_crs.clone();
    config.custom_nodata = nodata;
    config.apply_scale_offset = apply_scale_offset;
    config.bbox = bbox;
    config.transfer = transfer;
    config.profile = profile;
    apply_output_policy(&mut config, policy);
    export_raster_to_parquet_with_config(input_path, output_path, categorical, geoparquet, config)
}

/// Export a raster using the supplied scan configuration.
pub fn export_raster_to_parquet_with_config(
    input_path: &str,
    output_path: &Path,
    categorical: bool,
    geoparquet: bool,
    mut config: MultiResolutionConfig,
) -> Result<usize> {
    let report_profile = config.profile.clone();
    let paths = resolve_raster_sources(input_path)?;
    let failures = Arc::clone(
        config
            .transform_failure_counter
            .get_or_insert_with(Default::default),
    );
    let before = failures.load(Ordering::Relaxed);

    let mosaic = Arc::new(MosaicReader::open_with_remote_source_factory(
        &paths,
        config.bbox,
        config.custom_crs.as_deref(),
        config.overlap_rule,
        config.remote_source_factory.as_ref(),
    )?);

    let pq_cfg = ParquetExportConfig {
        geoparquet,
        is_categorical: categorical,
        ..Default::default()
    };

    let rows = if !categorical {
        let streamer = MultiScanHorizonStreamer::new_mosaic(mosaic, &config)?;
        ParquetWriter::write_continuous_streamer_to_parquet(streamer, output_path, pq_cfg)
    } else {
        let streamer = MultiCategoricalHorizonStreamer::new_mosaic(mosaic, &config)?;
        ParquetWriter::write_categorical_streamer_to_parquet(streamer, output_path, pq_cfg)
    }
    .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))?;
    warn_transform_failures(failures.load(Ordering::Relaxed).saturating_sub(before));
    print_profile(&report_profile);
    Ok(rows)
}

/// Export a raster directly to a PMTiles v3 archive.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_pmtiles(
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    apply_scale_offset: bool,
) -> Result<usize> {
    export_raster_to_pmtiles_with_grid(
        GridKind::H3,
        input_path,
        output_path,
        resolutions,
        categorical,
        source_crs,
        nodata,
        bbox,
        apply_scale_offset,
    )
}

/// Export a raster directly to a PMTiles v3 archive using `grid`.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_pmtiles_with_grid(
    grid: GridKind,
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    apply_scale_offset: bool,
) -> Result<usize> {
    export_raster_to_pmtiles_with_policy(
        grid,
        input_path,
        output_path,
        resolutions,
        categorical,
        source_crs,
        nodata,
        bbox,
        apply_scale_offset,
        &OutputPolicy::default(),
    )
}

/// [`export_raster_to_pmtiles_with_grid`] under an [`OutputPolicy`]. Vector tile
/// properties have no NULL, so a statistic without a value is written as NaN.
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_pmtiles_with_policy(
    grid: GridKind,
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    apply_scale_offset: bool,
    policy: &OutputPolicy,
) -> Result<usize> {
    export_raster_to_pmtiles_with_transfer(
        grid,
        input_path,
        output_path,
        resolutions,
        categorical,
        source_crs,
        nodata,
        bbox,
        apply_scale_offset,
        policy,
        TransferMode::AssignCenters,
        None,
    )
}

/// [`export_raster_to_pmtiles_with_policy`] with a [`TransferMode`].
#[allow(clippy::too_many_arguments)]
pub fn export_raster_to_pmtiles_with_transfer(
    grid: GridKind,
    input_path: &str,
    output_path: &Path,
    resolutions: Vec<u8>,
    categorical: bool,
    source_crs: Option<String>,
    nodata: Option<f64>,
    bbox: Option<[f64; 4]>,
    apply_scale_offset: bool,
    policy: &OutputPolicy,
    transfer: TransferMode,
    profile: Option<Arc<RunProfile>>,
) -> Result<usize> {
    let mut config = MultiResolutionConfig::new(resolutions).with_grid(grid);
    config.transfer = transfer;
    config.profile = profile;
    config.custom_crs = source_crs;
    config.custom_nodata = nodata;
    config.apply_scale_offset = apply_scale_offset;
    config.bbox = bbox;
    apply_output_policy(&mut config, policy);
    export_raster_to_pmtiles_with_config(input_path, output_path, categorical, config)
}

/// Export a raster using the supplied scan configuration.
pub fn export_raster_to_pmtiles_with_config(
    input_path: &str,
    output_path: &Path,
    categorical: bool,
    mut config: MultiResolutionConfig,
) -> Result<usize> {
    let report_profile = config.profile.clone();
    let failures = Arc::clone(
        config
            .transform_failure_counter
            .get_or_insert_with(Default::default),
    );
    let before = failures.load(Ordering::Relaxed);

    let tiles = if !categorical {
        PmtilesTiler::process_raster_source_to_pmtiles(input_path, output_path, config)
    } else {
        PmtilesTiler::process_categorical_source_to_pmtiles(input_path, output_path, config)
    }
    .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))?;
    warn_transform_failures(failures.load(Ordering::Relaxed).saturating_sub(before));
    print_profile(&report_profile);
    Ok(tiles)
}

/// Transcode an H3-indexed Parquet file (from `dggs-stream-cli parquet`, DuckDB, Spark, Polars,
/// ...) into a PMTiles v3 archive: one hexagon feature per row, the other columns as
/// properties. `h3_column` names the H3 column (integer or hex string, matched
/// case-insensitively); `None` picks the first of `h3_index`, `h3_hex`, `h3`, `cell`, `hex`.
/// Rows whose H3 value is null or not a valid cell are skipped and counted in
/// [`PmtilesExportSummary::invalid_features_dropped`].
pub fn transcode_parquet_to_pmtiles(
    input_path: &Path,
    output_path: &Path,
    h3_column: Option<&str>,
) -> Result<PmtilesExportSummary> {
    if !input_path.is_file() {
        return Err(DggsStreamError::InvalidParameter(format!(
            "Input Parquet file does not exist: {}",
            input_path.display()
        )));
    }
    PmtilesTiler::process_parquet_to_pmtiles(input_path, output_path, h3_column)
        .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))
}

/// Arguments of the `parquet-to-pmtiles` subcommand.
#[derive(Debug, Clone, PartialEq)]
pub struct CliTranscodeArgs {
    pub input_path: String,
    pub output_path: String,
    pub h3_column: Option<String>,
}

/// Parse `parquet-to-pmtiles <input.parquet> -o <out.pmtiles> [--h3-column NAME]`. The
/// archive needs a seekable file, so `-o` is required and cannot be stdout.
pub fn parse_parquet_to_pmtiles_args(args: &[String]) -> Result<CliTranscodeArgs> {
    let mut input_path: Option<String> = None;
    let mut output_path: Option<String> = None;
    let mut h3_column: Option<String> = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let flag = arg.as_str();
        let mut value = || {
            iter.next().cloned().ok_or_else(|| {
                DggsStreamError::InvalidParameter(format!("Missing value for {flag}"))
            })
        };
        match flag {
            "-i" | "--input" => input_path = Some(value()?),
            "-o" | "--output" => {
                let v = value()?;
                if v == "-" {
                    return Err(DggsStreamError::InvalidParameter(
                        "parquet-to-pmtiles cannot write to stdout: pass a file with -o".into(),
                    ));
                }
                output_path = Some(v);
            }
            "--h3-column" | "--h3-col" => h3_column = Some(value()?),
            _ if flag.starts_with('-') && flag != "-" => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unknown option '{flag}' for parquet-to-pmtiles"
                )));
            }
            _ if input_path.is_none() => input_path = Some(flag.to_string()),
            _ => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unexpected argument '{flag}' (input path already set to '{}')",
                    input_path.unwrap_or_default()
                )));
            }
        }
    }
    let input_path = input_path.ok_or_else(|| {
        DggsStreamError::InvalidParameter("Missing required input Parquet path".into())
    })?;
    let output_path = output_path.ok_or_else(|| {
        DggsStreamError::InvalidParameter(
            "Missing -o / --output path for parquet-to-pmtiles".into(),
        )
    })?;
    Ok(CliTranscodeArgs {
        input_path,
        output_path,
        h3_column,
    })
}

/// Parse CLI arguments into [`CliStreamOptions`] and an optional output file path (`None` = stdout).
///
/// Unknown flags, missing flag values, malformed numbers/lists and extra positional
/// arguments are rejected rather than silently falling back to defaults.
pub fn parse_stream_args(
    args: &[String],
    default_format: StreamOutputFormat,
) -> Result<(CliStreamOptions, Option<String>)> {
    let mut opts = CliStreamOptions {
        output_format: default_format,
        ..Default::default()
    };
    let mut output_file: Option<String> = None;
    let mut partition_spec: Option<String> = None;
    let mut partition_index: Option<usize> = None;
    let mut raw_resolutions: Option<(String, String)> = None;
    let (mut transfer, mut interpolation) = (None::<String>, None::<String>);
    let (mut overlay_weighted, mut mass_preserve) = (None::<bool>, None::<bool>);
    let mut histogram = [None::<String>, None, None];

    let invalid = |flag: &str, value: &str, expected: &str| {
        DggsStreamError::InvalidParameter(format!(
            "Invalid value '{value}' for {flag}: expected {expected}"
        ))
    };

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let flag = arg.as_str();
        let mut value = || {
            iter.next().map(String::as_str).ok_or_else(|| {
                DggsStreamError::InvalidParameter(format!("Missing value for {flag}"))
            })
        };
        match flag {
            "-i" | "--input" => opts.input_path = value()?.to_string(),
            "-o" | "--output" => {
                let v = value()?;
                output_file = (v != "-").then(|| v.to_string());
            }
            "--grid" => {
                let v = value()?;
                opts.grid =
                    GridKind::parse(v).ok_or_else(|| invalid(flag, v, GridKind::EXPECTED))?;
            }
            "-r" | "--resolution" | "--resolutions" => {
                let v = value()?;
                raw_resolutions = Some((flag.to_string(), v.to_string()));
            }
            "-c" | "--categorical" => opts.categorical = true,
            "--format" => {
                let v = value()?;
                opts.categorical_format = if v.eq_ignore_ascii_case("long") {
                    CategoricalFormat::Long
                } else if v.eq_ignore_ascii_case("wide") {
                    CategoricalFormat::Wide
                } else {
                    return Err(invalid(flag, v, "'wide' or 'long'"));
                };
            }
            "--crs" | "--source-crs" => opts.source_crs = Some(value()?.to_string()),
            "--nodata" => {
                let v = value()?;
                opts.nodata = Some(v.parse().map_err(|_| invalid(flag, v, "a number"))?);
            }
            "--raw-values" => opts.apply_scale_offset = false,
            "--bbox" => {
                let v = value()?;
                opts.bbox = Some(
                    parse_bbox_str(v)
                        .ok_or_else(|| invalid(flag, v, "min_lon,min_lat,max_lon,max_lat"))?,
                );
            }
            "--columns" => {
                let v = value()?;
                let cols: Vec<String> = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if cols.is_empty() {
                    return Err(invalid(flag, v, "a comma-separated column list"));
                }
                opts.columns = Some(cols);
            }
            "--transfer" => transfer = Some(value()?.to_string()),
            "--interpolation" => interpolation = Some(value()?.to_string()),
            "--overlay-weighted" => {
                let v = value()?;
                overlay_weighted = Some(match v.to_ascii_lowercase().as_str() {
                    "true" | "1" | "yes" => true,
                    "false" | "0" | "no" => false,
                    _ => return Err(invalid(flag, v, "true or false")),
                });
            }
            "--valid-coverage-threshold" => {
                let v = value()?;
                opts.valid_coverage_threshold = Some(
                    v.parse()
                        .map_err(|_| invalid(flag, v, "a fraction in [0, 1]"))?,
                );
            }
            "--mass-preserve" => mass_preserve = Some(true),
            "--histogram" => histogram[0] = Some(value()?.to_string()),
            "--histogram-weight" => histogram[1] = Some(value()?.to_string()),
            "--histogram-normalize" => histogram[2] = Some(value()?.to_string()),
            "--bands" => opts.bands = Some(value()?.to_string()),
            "--profile" => opts.profile = Some(RunProfile::new()),
            "--nodata-policy" => {
                let v = value()?;
                opts.output.nodata = NodataPolicy::parse(v)
                    .ok_or_else(|| invalid(flag, v, NodataPolicy::EXPECTED))?;
            }
            "--emit-nodata-value" => {
                let v = value()?;
                opts.output.nodata_value = Some(
                    v.parse::<f64>()
                        .ok()
                        .filter(|x| x.is_finite())
                        .ok_or_else(|| invalid(flag, v, "a finite number"))?,
                );
            }
            "--decimals" => {
                let v = value()?;
                opts.output.decimals = Some(
                    v.parse::<u8>()
                        .ok()
                        .filter(|d| *d <= MAX_DECIMALS)
                        .ok_or_else(|| invalid(flag, v, "an integer between 0 and 15"))?,
                );
            }
            "--limit" => {
                let v = value()?;
                opts.limit = Some(
                    v.parse()
                        .map_err(|_| invalid(flag, v, "a non-negative integer"))?,
                );
            }
            "--partition" => partition_spec = Some(value()?.to_string()),
            "--partition-index" => {
                let v = value()?;
                partition_index = Some(
                    v.parse()
                        .map_err(|_| invalid(flag, v, "a non-negative integer"))?,
                );
            }
            _ if flag.starts_with('-') && flag != "-" => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unknown option '{flag}'"
                )));
            }
            _ if opts.input_path.is_empty() => opts.input_path = flag.to_string(),
            _ => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unexpected argument '{flag}' (input path already set to '{}')",
                    opts.input_path
                )));
            }
        }
    }

    let mut auto = None;
    if let Some((flag, v)) = raw_resolutions {
        let expected = format!(
            "'auto', 'auto+N', 'auto-N' or {} resolutions between 0 and {}, e.g. 7,8",
            opts.grid.label(),
            opts.grid.max_resolution()
        );
        match ResolutionSpec::parse_auto(&v) {
            Some(spec) => auto = Some(spec.map_err(|_| invalid(&flag, &v, &expected))?),
            None => {
                opts.resolutions = try_parse_resolutions_str_for(opts.grid, &v)
                    .ok_or_else(|| invalid(&flag, &v, &expected))?
            }
        }
    }

    opts.transfer = TransferMode::from_options(
        transfer.as_deref(),
        interpolation.as_deref(),
        overlay_weighted,
        mass_preserve,
    )?;
    opts.histogram = HistogramSpec::from_options(
        histogram[0].as_deref(),
        histogram[1].as_deref(),
        histogram[2].as_deref(),
    )?;
    if let Some(spec) = &opts.histogram {
        // A histogram is a categorical aggregation; area weights run `overlay`.
        opts.categorical = true;
        opts.transfer = spec.transfer_mode(opts.transfer)?;
    }
    if opts.output.nodata_value.is_some() && !opts.output.emits_nodata() {
        return Err(DggsStreamError::InvalidParameter(
            "--emit-nodata-value needs --nodata-policy emit".into(),
        ));
    }
    if opts.input_path.is_empty() {
        return Err(DggsStreamError::InvalidParameter(
            "Missing required input raster path".into(),
        ));
    }
    // `auto` is measured on the raster, so it is resolved once the path and CRS are known.
    if let Some(spec) = auto {
        let paths = resolve_raster_sources(&opts.input_path)?;
        opts.resolutions = spec.resolve_paths(opts.grid, &paths, opts.source_crs.as_deref())?;
        eprintln!(
            "dggs-stream: -r {spec} selected {} resolution {}",
            opts.grid.label(),
            opts.resolutions[0]
        );
    }
    match (partition_spec, partition_index) {
        (Some(spec), index) => opts.partition = Some(read_partition_spec(&spec, index)?),
        (None, Some(_)) => {
            return Err(DggsStreamError::InvalidParameter(
                "--partition-index needs --partition @PLAN_FILE".into(),
            ))
        }
        (None, None) => {}
    }

    Ok((opts, output_file))
}

/// Arguments of the `plan` subcommand.
#[derive(Debug, Clone, PartialEq)]
pub struct CliPlanArgs {
    pub input_path: String,
    pub target_chunks: usize,
    pub source_crs: Option<String>,
    pub bbox: Option<[f64; 4]>,
    /// `None` = stdout.
    pub output: Option<String>,
}

/// Parse `plan <input> [--target-chunks N] [--crs CRS] [--bbox ...] [-o FILE]`.
pub fn parse_plan_args(args: &[String]) -> Result<CliPlanArgs> {
    let mut out = CliPlanArgs {
        input_path: String::new(),
        target_chunks: 256,
        source_crs: None,
        bbox: None,
        output: None,
    };
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let flag = arg.as_str();
        let mut value = || {
            iter.next().map(String::as_str).ok_or_else(|| {
                DggsStreamError::InvalidParameter(format!("Missing value for {flag}"))
            })
        };
        let invalid = |v: &str, expected: &str| {
            DggsStreamError::InvalidParameter(format!(
                "Invalid value '{v}' for {flag}: expected {expected}"
            ))
        };
        match flag {
            "-i" | "--input" => out.input_path = value()?.to_string(),
            "-o" | "--output" => {
                let v = value()?;
                out.output = (v != "-").then(|| v.to_string());
            }
            "--target-chunks" => {
                let v = value()?;
                out.target_chunks = v
                    .parse()
                    .ok()
                    .filter(|&n: &usize| n > 0)
                    .ok_or_else(|| invalid(v, "a positive integer"))?;
            }
            "--crs" | "--source-crs" => out.source_crs = Some(value()?.to_string()),
            "--bbox" => {
                let v = value()?;
                out.bbox = Some(
                    parse_bbox_str(v)
                        .ok_or_else(|| invalid(v, "min_lon,min_lat,max_lon,max_lat"))?,
                );
            }
            _ if flag.starts_with('-') && flag != "-" => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unknown option '{flag}' for plan"
                )));
            }
            _ if out.input_path.is_empty() => out.input_path = flag.to_string(),
            _ => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unexpected argument '{flag}' (input path already set to '{}')",
                    out.input_path
                )));
            }
        }
    }
    if out.input_path.is_empty() {
        return Err(DggsStreamError::InvalidParameter(
            "Missing required input raster path".into(),
        ));
    }
    Ok(out)
}

/// Parse `merge <part>... [-c] [--jsonl] [-o FILE]` into options and an output path
/// (`None` = stdout).
pub fn parse_merge_args(args: &[String]) -> Result<(CliMergeOptions, Option<String>)> {
    let mut opts = CliMergeOptions {
        inputs: Vec::new(),
        categorical: false,
        output_format: StreamOutputFormat::Ipc,
        batch_size: 2048,
    };
    let mut output = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-o" | "--output" => {
                let v = iter.next().ok_or_else(|| {
                    DggsStreamError::InvalidParameter(format!("Missing value for {arg}"))
                })?;
                output = (v != "-").then(|| v.clone());
            }
            "-c" | "--categorical" => opts.categorical = true,
            "--jsonl" => opts.output_format = StreamOutputFormat::Jsonl,
            flag if flag.starts_with('-') => {
                return Err(DggsStreamError::InvalidParameter(format!(
                    "Unknown option '{flag}' for merge"
                )));
            }
            path => opts.inputs.push(PathBuf::from(path)),
        }
    }
    if opts.inputs.is_empty() {
        return Err(DggsStreamError::InvalidParameter(
            "merge needs at least one partition file".into(),
        ));
    }
    Ok((opts, output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dggs_stream_core::columnar::ipc::read_ipc_stream;
    use std::fs::File;
    use tempfile::NamedTempFile;
    use tiff::encoder::colortype::Gray32Float;
    use tiff::encoder::TiffEncoder;

    fn write_test_tif(width: u32, height: u32, val: f32) -> NamedTempFile {
        write_test_tif_at(width, height, val, -122.45, 37.80, 0.001)
    }

    fn write_test_tif_at(
        width: u32,
        height: u32,
        val: f32,
        west: f64,
        north: f64,
        pixel: f64,
    ) -> NamedTempFile {
        let tmp = NamedTempFile::new().unwrap();
        let file = File::create(tmp.path()).unwrap();
        let mut encoder = TiffEncoder::new(file).unwrap();
        let mut img = encoder.new_image::<Gray32Float>(width, height).unwrap();
        img.encoder()
            .write_tag(tiff::tags::Tag::Unknown(33550), &[pixel, pixel, 0.0][..])
            .unwrap();
        img.encoder()
            .write_tag(
                tiff::tags::Tag::Unknown(33922),
                &[0.0f64, 0.0f64, 0.0f64, west, north, 0.0f64][..],
            )
            .unwrap();
        let data = vec![val; (width * height) as usize];
        img.write_data(&data).unwrap();
        tmp
    }

    #[test]
    fn parquet_export_honors_remote_factory_and_overlap_rule() {
        use dggs_stream_core::raster::http_range::{
            ProbeResult, RemoteHttpSource, RemoteSourceFactory, RemoteTransport,
        };
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::record::RowAccessor;

        struct Bytes(Vec<u8>);
        impl RemoteTransport for Bytes {
            fn probe(&self, _: usize) -> Result<ProbeResult> {
                Ok(ProbeResult::Complete {
                    data: self.0.clone(),
                    total_size: self.0.len() as u64,
                })
            }
            fn fetch_range(&self, start: u64, end: u64, _: Option<u64>) -> Result<Vec<u8>> {
                Ok(self.0[start as usize..=end as usize].to_vec())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let first = write_test_tif(16, 16, 10.0);
        let second = write_test_tif(16, 16, 20.0);
        let first_bytes = std::fs::read(first.path()).unwrap();
        let second_bytes = std::fs::read(second.path()).unwrap();
        let first_url = "https://fixture.invalid/first.tif";
        let second_url = "https://fixture.invalid/second.tif";
        let factory = RemoteSourceFactory::new(move |path| {
            let bytes = match path {
                p if p == first_url => &first_bytes,
                p if p == second_url => &second_bytes,
                other => panic!("unexpected fixture URL {other}"),
            };
            Ok(Arc::new(RemoteHttpSource::from_transport(
                path.to_owned(),
                Box::new(Bytes(bytes.clone())),
            )?))
        });
        let sources = format!("{first_url},{second_url}");
        let out = dir.path().join("mosaic.parquet");
        for (rule, expected_sum) in [(OverlapRule::First, 2560.0), (OverlapRule::Average, 7680.0)] {
            let mut cfg = MultiResolutionConfig::single(7);
            cfg.custom_crs = Some("EPSG:4326".into());
            cfg.remote_source_factory = Some(factory.clone());
            cfg.overlap_rule = rule;
            export_raster_to_parquet_with_config(&sources, &out, false, false, cfg).unwrap();
            let reader = SerializedFileReader::new(File::open(&out).unwrap()).unwrap();
            let schema = reader.metadata().file_metadata().schema_descr();
            let sum_col = schema
                .columns()
                .iter()
                .position(|c| c.name() == "sum_value")
                .unwrap();
            let sum: f64 = reader
                .get_row_iter(None)
                .unwrap()
                .map(|row| row.unwrap().get_double(sum_col).unwrap())
                .sum();
            assert_eq!(sum, expected_sum, "{rule:?}");
        }
    }

    #[test]
    fn configured_exports_increment_the_callers_transform_failure_counter() {
        let tif = write_test_tif_at(10, 20, 3.0, 0.0, 95.0, 1.0);
        let input = tif.path().to_str().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let failures = Arc::new(std::sync::atomic::AtomicU64::new(7));
        let mut cfg = MultiResolutionConfig::single(3);
        cfg.custom_crs = Some("EPSG:4326".into());
        cfg.transform_failure_counter = Some(Arc::clone(&failures));
        export_raster_to_parquet_with_config(
            input,
            &dir.path().join("out.parquet"),
            false,
            false,
            cfg.clone(),
        )
        .unwrap();
        assert_eq!(failures.load(Ordering::Relaxed), 57);
        export_raster_to_pmtiles_with_config(input, &dir.path().join("out.pmtiles"), false, cfg)
            .unwrap();
        assert_eq!(failures.load(Ordering::Relaxed), 107);
    }

    #[test]
    fn wide_bands_preserve_projection_order_and_independent_nodata() {
        let input = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../dggs-stream-core/tests/fixtures/multiband-rgb.tif"
        );
        let args = [
            input,
            "-r",
            "12",
            "--bands",
            "3,1",
            "--columns",
            "band_1_mean,band_3_mean,band_1_count",
        ]
        .map(str::to_owned);
        let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Ipc).unwrap();
        let mut bytes = Vec::new();
        assert_eq!(stream_raster_to_writer(&opts, &mut bytes).unwrap(), 2);
        let (schema, _) = read_ipc_stream(&mut bytes.as_slice()).unwrap();
        assert_eq!(schema.fields[0].name, "band_1_mean");
        assert!(schema.fields[0].nullable);
        let mut json = Vec::new();
        let mut opts = opts;
        opts.output_format = StreamOutputFormat::Jsonl;
        stream_raster_to_writer(&opts, &mut json).unwrap();
        let mut rows: Vec<serde_json::Value> = std::str::from_utf8(&json)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        rows.sort_by_key(|v| v["band_3_mean"].as_f64().unwrap() as u8);
        assert_eq!(rows[0]["band_1_mean"], 2.0);
        assert_eq!(rows[1]["band_3_mean"], 12.0);
        assert!(rows[1]["band_1_mean"].is_null());
        assert_eq!(rows[1]["band_1_count"], 0.0);
    }

    #[test]
    fn test_cli_ipc_and_jsonl_streaming_with_projection() {
        let tif = write_test_tif(16, 16, 42.0);
        let args = vec![
            tif.path().to_string_lossy().to_string(),
            "--resolution".to_string(),
            "8".to_string(),
            "--crs".to_string(),
            "EPSG:4326".to_string(),
            "--columns".to_string(),
            "h3_index,h3_hex,mean,count".to_string(),
        ];
        let (opts, out_file) = parse_stream_args(&args, StreamOutputFormat::Ipc).unwrap();
        assert!(out_file.is_none());

        let mut ipc_buf = Vec::new();
        let rows = stream_raster_to_writer(&opts, &mut ipc_buf).unwrap();
        assert!(rows > 0);

        let mut cursor = std::io::Cursor::new(ipc_buf);
        let (schema, batches) = read_ipc_stream(&mut cursor).unwrap();
        assert_eq!(schema.fields.len(), 4);
        assert_eq!(schema.fields[0].name, "h3_index");
        assert_eq!(schema.fields[1].name, "h3_hex");
        assert_eq!(schema.fields[2].name, "mean");
        assert_eq!(schema.fields[3].name, "count");
        let total_decoded: usize = batches.iter().map(|b| b.num_rows).sum();
        assert_eq!(total_decoded, rows);

        // Also test JSONL mode (for ClickHouse executable table functions)
        let mut jsonl_opts = opts;
        jsonl_opts.output_format = StreamOutputFormat::Jsonl;
        jsonl_opts.limit = Some(2);
        let mut jsonl_buf = Vec::new();
        let jrows = stream_raster_to_writer(&jsonl_opts, &mut jsonl_buf).unwrap();
        assert!(jrows <= 2 && jrows > 0);
        let text = String::from_utf8(jsonl_buf).unwrap();
        assert_eq!(text.lines().count(), jrows);
        assert!(text.contains("\"mean\":42.0"));
    }

    #[test]
    fn test_cli_rejects_invalid_arguments() {
        let parse = |extra: &[&str]| {
            let mut args = vec!["in.tif".to_string()];
            args.extend(extra.iter().map(|s| s.to_string()));
            parse_stream_args(&args, StreamOutputFormat::Ipc)
        };

        let (opts, out) =
            parse(&["-r", "7,8", "--bbox", "1,2,3,4", "--limit", "5", "-o", "x"]).unwrap();
        assert_eq!(opts.resolutions, vec![7, 8]);
        assert_eq!(opts.bbox, Some([1.0, 2.0, 3.0, 4.0]));
        assert_eq!(opts.limit, Some(5));
        assert_eq!(out.as_deref(), Some("x"));
        assert!(opts.apply_scale_offset);
        assert!(!parse(&["--raw-values"]).unwrap().0.apply_scale_offset);

        for bad in [
            &["--bbox", "10,20,30"][..],
            &["-r", "16"],
            &["--grid", "a5", "-r", "31"],
            &["--grid", "healpix", "-r", "30"],
            &["--grid", "s9"],
            &["-r", "8,abc"],
            &["--nodata", "abc"],
            &["--limit", "-1"],
            &["--format", "tall"],
            &["--limt", "10"],
            &["--bbox"],
            &["second.tif"],
        ] {
            assert!(parse(bad).is_err(), "{bad:?} should be rejected");
        }
        assert_eq!(
            parse(&["-r", "20", "--grid", "a5"]).unwrap().0.resolutions,
            vec![20]
        );
        assert!(parse_stream_args(&[], StreamOutputFormat::Ipc).is_err());
    }

    #[test]
    fn test_cli_a5_ipc_and_jsonl_streaming() {
        let tif = write_test_tif(16, 16, 42.0);
        let args = vec![
            tif.path().to_string_lossy().to_string(),
            "--grid".to_string(),
            "a5".to_string(),
            "--resolution".to_string(),
            "10".to_string(),
            "--crs".to_string(),
            "EPSG:4326".to_string(),
            "--columns".to_string(),
            "a5_index,a5_hex,mean,count".to_string(),
        ];
        let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Ipc).unwrap();
        assert_eq!(opts.grid, GridKind::A5);

        let mut ipc_buf = Vec::new();
        let rows = stream_raster_to_writer(&opts, &mut ipc_buf).unwrap();
        assert!(rows > 0);

        let mut cursor = std::io::Cursor::new(ipc_buf);
        let (schema, batches) = read_ipc_stream(&mut cursor).unwrap();
        assert_eq!(schema.fields[0].name, "a5_index");
        assert_eq!(schema.fields[1].name, "a5_hex");
        assert_eq!(schema.fields[2].name, "mean");
        assert_eq!(schema.fields[3].name, "count");
        let total_decoded: usize = batches.iter().map(|b| b.num_rows).sum();
        assert_eq!(total_decoded, rows);
        let (json_opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl).unwrap();
        let mut json = Vec::new();
        assert_eq!(
            stream_raster_to_writer(&json_opts, &mut json).unwrap(),
            rows
        );
        let decoded: Vec<serde_json::Value> = String::from_utf8(json)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(decoded.len(), rows);
        assert!(decoded.iter().all(|row| row["mean"] == 42.0
            && row["a5_index"].is_u64()
            && row["a5_hex"].is_string()));
    }

    #[test]
    fn test_cli_s2_and_healpix_streaming_and_partition_merge() {
        let tif = write_test_tif(16, 16, 42.0);
        for grid in [GridKind::S2, GridKind::Healpix] {
            let name = grid.as_str();
            let args = |extra: &[&str]| -> Vec<String> {
                [
                    tif.path().to_str().unwrap(),
                    "--grid",
                    name,
                    "--resolution",
                    "10",
                    "--crs",
                    "EPSG:4326",
                ]
                .iter()
                .chain(extra)
                .map(|s| s.to_string())
                .collect()
            };
            let columns = format!("{name}_index,{name}_hex,resolution,mean,count");
            let (opts, _) =
                parse_stream_args(&args(&["--columns", &columns]), StreamOutputFormat::Jsonl)
                    .unwrap();
            assert_eq!(opts.grid, grid);
            let mut json = Vec::new();
            let rows = stream_raster_to_writer(&opts, &mut json).unwrap();
            assert!(rows > 0);
            let whole: Vec<serde_json::Value> = String::from_utf8(json)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            for row in &whole {
                let cell = row[format!("{name}_index")].as_u64().unwrap();
                assert_eq!(grid.resolution(cell), Some(10));
                assert_eq!(row["mean"], 42.0);
                assert!(row[format!("{name}_hex")].is_string());
            }

            // The same scan split in two partitions and merged gives the same cells.
            let plan =
                plan_raster_partitions(tif.path().to_str().unwrap(), 1, Some("EPSG:4326"), None)
                    .unwrap();
            let dir = tempfile::tempdir().unwrap();
            let mut parts = Vec::new();
            for (i, partition) in plan.iter().enumerate() {
                let spec = partition.to_json();
                let (opts, _) = parse_stream_args(
                    &args(&["--columns", &columns, "--partition", &spec]),
                    StreamOutputFormat::Ipc,
                )
                .unwrap();
                let path = dir.path().join(format!("part{i}.rh3"));
                let mut file = std::fs::File::create(&path).unwrap();
                stream_raster_to_writer(&opts, &mut file).unwrap();
                parts.push(path.to_string_lossy().to_string());
            }
            let (merge, _) = parse_merge_args(&parts).unwrap();
            let mut merged = Vec::new();
            merge_partition_files(&merge, &mut merged).unwrap();
            let (schema, batches) = read_ipc_stream(&mut std::io::Cursor::new(merged)).unwrap();
            assert_eq!(schema.fields[0].name, format!("{name}_index"));
            let total: usize = batches.iter().map(|b| b.num_rows).sum();
            assert_eq!(total, whole.len(), "{grid}");
        }
    }

    #[test]
    fn test_cli_partition_merge_keeps_the_output_policy() {
        // Strips of 4 rows, so the plan has several partitions.
        let tif = NamedTempFile::new().unwrap();
        {
            let mut encoder = TiffEncoder::new(File::create(tif.path()).unwrap()).unwrap();
            let mut img = encoder.new_image::<Gray32Float>(32, 32).unwrap();
            img.rows_per_strip(4).unwrap();
            img.encoder()
                .write_tag(tiff::tags::Tag::Unknown(33550), &[0.001f64, 0.001, 0.0][..])
                .unwrap();
            img.encoder()
                .write_tag(
                    tiff::tags::Tag::Unknown(33922),
                    &[0.0f64, 0.0, 0.0, -122.45, 37.80, 0.0][..],
                )
                .unwrap();
            img.write_data(&[1.23456f32; 32 * 32]).unwrap();
        }
        let input = tif.path().to_str().unwrap();
        let plan = plan_raster_partitions(input, 1, Some("EPSG:4326"), None).unwrap();
        assert!(plan.len() > 1);
        let dir = tempfile::tempdir().unwrap();
        let write_parts = |extra: &[&str], tag: &str| -> Vec<String> {
            let mut parts = Vec::new();
            for (i, partition) in plan.iter().enumerate() {
                let spec = partition.to_json();
                let mut args: Vec<String> = [
                    input,
                    "--crs",
                    "EPSG:4326",
                    "-r",
                    "9",
                    "--columns",
                    "h3_index,resolution,mean",
                    "--partition",
                    &spec,
                ]
                .iter()
                .chain(extra)
                .map(|s| s.to_string())
                .collect();
                args.dedup();
                let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Ipc).unwrap();
                let path = dir.path().join(format!("{tag}{i}.rh3"));
                let mut file = std::fs::File::create(&path).unwrap();
                stream_raster_to_writer(&opts, &mut file).unwrap();
                parts.push(path.to_string_lossy().to_string());
            }
            parts
        };
        let rounded = write_parts(&["--decimals", "2"], "rounded");
        let (merge, _) = parse_merge_args(&rounded).unwrap();
        let mut out = Vec::new();
        merge_partition_files(&merge, &mut out).unwrap();
        let (_, batches) = read_ipc_stream(&mut std::io::Cursor::new(out)).unwrap();
        let mut means = Vec::new();
        for batch in &batches {
            let ColumnArray::Float64 { values, .. } = &batch.columns[2] else {
                panic!("mean is Float64");
            };
            means.extend_from_slice(values);
        }
        assert!(means.len() > 1);
        assert!(means.iter().all(|&m| m == 1.23), "{means:?}");

        // Partitions of scans with other output settings do not merge together.
        let plain = write_parts(&[], "plain");
        let mixed = vec![rounded[0].clone(), plain[1].clone()];
        let (merge, _) = parse_merge_args(&mixed).unwrap();
        assert!(merge_partition_files(&merge, &mut Vec::new()).is_err());
    }

    #[test]
    fn test_cli_nodata_policy_and_decimals() {
        let tif = write_test_tif(24, 24, 1.23456);
        let input = tif.path().to_str().unwrap();
        let run = |extra: &[&str]| -> Result<Vec<serde_json::Value>> {
            // Fine enough for several cells on every grid; a later `-r` wins.
            let mut args: Vec<String> = [input, "--crs", "EPSG:4326"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            args.extend(extra.iter().map(|s| s.to_string()));
            let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl)?;
            let mut out = Vec::new();
            stream_raster_to_writer(&opts, &mut out)?;
            Ok(String::from_utf8(out)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect())
        };
        for grid in GridKind::ALL {
            let res = if grid == GridKind::H3 { "9" } else { "14" };
            let g = ["--grid", grid.as_str(), "-r", res];
            let plain = run(&g).unwrap();
            assert!(plain.len() > 1);
            assert!(plain[0]["mean"].as_f64().unwrap() != 1.23);

            // Rounded statistics; counts untouched.
            let rounded = run(&[g[0], g[1], g[2], g[3], "--decimals", "2"]).unwrap();
            assert_eq!(rounded.len(), plain.len());
            for row in &rounded {
                assert_eq!(row["mean"], 1.23, "{grid}");
                assert_eq!(row["min"], 1.23);
                assert!(row["count"].as_f64().unwrap() >= 1.0);
            }

            // Every pixel is NoData: `omit` has no rows, `emit` has them all, empty.
            let nodata = ["--grid", grid.as_str(), "-r", res, "--nodata", "1.23456"];
            assert!(run(&nodata).unwrap().is_empty(), "{grid}");
            let mut emit = nodata.to_vec();
            emit.extend(["--nodata-policy", "emit"]);
            let rows = run(&emit).unwrap();
            assert_eq!(rows.len(), plain.len(), "{grid}");
            for row in &rows {
                assert_eq!(row["count"], 0.0);
                assert!(row["mean"].is_null() && row["min"].is_null() && row["max"].is_null());
            }
            emit.extend(["--emit-nodata-value", "-9999"]);
            for row in run(&emit).unwrap() {
                assert_eq!(
                    (&row["mean"], &row["max"]),
                    (&serde_json::json!(-9999.0), &serde_json::json!(-9999.0))
                );
            }
        }
        for bad in [
            &["--nodata-policy", "drop"][..],
            &["--decimals", "16"],
            &["--decimals", "-1"],
            &["--emit-nodata-value", "nan"],
            &["--emit-nodata-value", "0"],
        ] {
            assert!(run(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_cli_histograms() {
        // 24 x 24 pixels, all 2.5.
        let tif = write_test_tif(24, 24, 2.5);
        let input = tif.path().to_str().unwrap();
        let run = |extra: &[&str]| -> Result<Vec<serde_json::Value>> {
            let mut args: Vec<String> = [input, "--crs", "EPSG:4326"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            args.extend(extra.iter().map(|s| s.to_string()));
            let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl)?;
            assert!(opts.categorical);
            let mut out = Vec::new();
            stream_raster_to_writer(&opts, &mut out)?;
            Ok(String::from_utf8(out)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect())
        };
        // The weight of bin `bin` over all cells, and that of the other bins.
        let totals = |rows: &[serde_json::Value], bin: usize| -> (f64, f64) {
            let mut totals = (0.0, 0.0);
            for row in rows {
                let bins = row["histogram"].as_array().unwrap();
                assert_eq!(bins.len(), 4);
                for (i, entry) in bins.iter().enumerate() {
                    assert_eq!(entry[0].as_f64().unwrap(), i as f64);
                    assert_eq!(entry[1].as_f64().unwrap(), i as f64 + 1.0);
                    let weight = entry[2].as_f64().unwrap();
                    if i == bin {
                        totals.0 += weight;
                    } else {
                        totals.1 += weight;
                    }
                }
            }
            totals
        };
        for grid in GridKind::ALL {
            let base = [
                "--grid",
                grid.as_str(),
                "-r",
                "auto-1",
                "--histogram",
                "4:0:4",
            ];
            let with = |extra: &[&str]| run(&[&base[..], extra].concat()).unwrap();
            // 2.5 is in bin 2 of [0, 1, 2, 3, 4].
            let counts = with(&[]);
            assert_eq!(totals(&counts, 2), (576.0, 0.0), "{grid}");
            let area = with(&["--histogram-weight", "area"]);
            assert!(area.len() >= counts.len(), "{grid}");
            let (in_bin, outside) = totals(&area, 2);
            assert!(
                (in_bin / 576.0 - 1.0).abs() < 1e-6 && outside == 0.0,
                "{grid}"
            );
            let fractions = with(&["--histogram-normalize", "fraction"]);
            assert_eq!(
                totals(&fractions, 2),
                (fractions.len() as f64, 0.0),
                "{grid}"
            );

            // By value: 2.5 rounds to class 3 (ties away from zero).
            let values = run(&[
                "--grid",
                grid.as_str(),
                "-r",
                "auto-1",
                "--histogram",
                "values",
            ])
            .unwrap();
            let total: f64 = values
                .iter()
                .map(|row| {
                    let object = row["histogram"].as_object().unwrap();
                    assert_eq!(object.len(), 1);
                    object["3"].as_f64().unwrap()
                })
                .sum();
            assert_eq!(total, 576.0, "{grid}");
        }
        for bad in [
            &["--histogram", "0:0:1"][..],
            &["--histogram", "values", "--histogram-normalize", "density"],
            &["--histogram-weight", "mass"],
            &["--histogram-weight", "area", "--transfer", "interpolate"],
        ] {
            assert!(run(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_cli_transfer_modes() {
        let tif = write_test_tif(24, 24, 2.5);
        let input = tif.path().to_str().unwrap();
        let run = |extra: &[&str]| -> Result<Vec<serde_json::Value>> {
            let mut args: Vec<String> = [input, "--crs", "EPSG:4326"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            args.extend(extra.iter().map(|s| s.to_string()));
            let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl)?;
            let mut out = Vec::new();
            stream_raster_to_writer(&opts, &mut out)?;
            Ok(String::from_utf8(out)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect())
        };
        let sum = |rows: &[serde_json::Value], column: &str| -> f64 {
            rows.iter().map(|r| r[column].as_f64().unwrap()).sum()
        };
        for grid in GridKind::ALL {
            let g = grid.as_str();
            let centres = run(&["--grid", g, "-r", "auto"]).unwrap();
            assert_eq!(sum(&centres, "count"), 576.0);

            let overlay = run(&["--grid", g, "-r", "auto-1", "--transfer", "overlay"]).unwrap();
            assert!(
                (sum(&overlay, "count") / 576.0 - 1.0).abs() < 1e-6,
                "{grid}"
            );
            assert!((sum(&overlay, "sum") / 1440.0 - 1.0).abs() < 1e-6, "{grid}");
            let preserved = run(&[
                "--grid",
                g,
                "-r",
                "auto-1",
                "--transfer",
                "overlay",
                "--mass-preserve",
            ])
            .unwrap();
            assert!((sum(&preserved, "sum") - 1440.0).abs() < 1e-6, "{grid}");
            let touched = run(&[
                "--grid",
                g,
                "-r",
                "auto-1",
                "--transfer",
                "overlay",
                "--overlay-weighted",
                "false",
            ])
            .unwrap();
            assert!(sum(&touched, "count") > 576.0, "{grid}");

            // Sampling finer than the pixels returns one value per cell on the raster.
            let fine = run(&["--grid", g, "-r", "auto+2"]).unwrap();
            for method in ["nearest", "bilinear", "bicubic", "lanczos"] {
                let sampled = run(&[
                    "--grid",
                    g,
                    "-r",
                    "auto+2",
                    "--transfer",
                    "interpolate",
                    "--interpolation",
                    method,
                ])
                .unwrap();
                assert!(sampled.len() > 3 * fine.len(), "{grid} {method}");
                for row in &sampled {
                    assert_eq!(row["count"], 1.0);
                    assert!(
                        (row["mean"].as_f64().unwrap() - 2.5).abs() < 1e-9,
                        "{grid} {method}"
                    );
                }
            }
        }
        for bad in [
            &["--transfer", "smear"][..],
            &["--interpolation", "bilinear"],
            &["--mass-preserve"],
            &["--transfer", "overlay", "--overlay-weighted", "maybe"],
            &["--transfer", "interpolate", "--interpolation", "cubic9"],
        ] {
            assert!(run(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_cli_auto_resolution_on_every_grid() {
        use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule};
        use dggs_stream_core::resolution::{auto_resolution, offset_resolution};
        let tif = write_test_tif(32, 32, 5.0);
        let input = tif.path().to_str().unwrap();
        let mosaic = MosaicReader::open(
            &[tif.path().to_path_buf()],
            None,
            Some("EPSG:4326"),
            OverlapRule::default(),
        )
        .unwrap();
        for grid in GridKind::ALL {
            let matched = auto_resolution(grid, &mosaic, 0).unwrap();
            for (arg, offset) in [("auto", 0), ("AUTO+1", 1), ("auto-2", -2), ("auto+64", 64)] {
                let args: Vec<String> = [
                    input,
                    "--crs",
                    "EPSG:4326",
                    "--grid",
                    grid.as_str(),
                    "-r",
                    arg,
                ]
                .iter()
                .map(|s| s.to_string())
                .collect();
                let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl).unwrap();
                assert_eq!(
                    opts.resolutions,
                    vec![offset_resolution(grid, matched, offset)],
                    "{grid} {arg}"
                );
            }
            // The scan runs at the selected resolution.
            let args: Vec<String> = [
                input,
                "--crs",
                "EPSG:4326",
                "--grid",
                grid.as_str(),
                "-r",
                "auto",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect();
            let (opts, _) = parse_stream_args(&args, StreamOutputFormat::Jsonl).unwrap();
            let mut out = Vec::new();
            let rows = stream_raster_to_writer(&opts, &mut out).unwrap();
            assert!(rows > 0);
            let first: serde_json::Value =
                serde_json::from_str(String::from_utf8(out).unwrap().lines().next().unwrap())
                    .unwrap();
            assert_eq!(first["resolution"], u64::from(matched), "{grid}");
        }
        for bad in ["auto+", "auto1", "auto+1.5", "automatic"] {
            let args: Vec<String> = [input, "-r", bad].iter().map(|s| s.to_string()).collect();
            let err = parse_stream_args(&args, StreamOutputFormat::Jsonl).unwrap_err();
            assert!(err.to_string().contains("auto"), "{bad}: {err}");
        }
        // `auto` needs a raster to measure.
        let args: Vec<String> = ["/nonexistent.tif", "-r", "auto"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(parse_stream_args(&args, StreamOutputFormat::Jsonl).is_err());
    }

    #[test]
    fn test_cli_s2_and_healpix_parquet_and_pmtiles_exports() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::record::RowAccessor;
        let tif = write_test_tif(16, 16, 42.0);
        let input = tif.path().to_str().unwrap();
        let dir = tempfile::tempdir().unwrap();
        for grid in [GridKind::S2, GridKind::Healpix] {
            for categorical in [false, true] {
                for geoparquet in [false, true] {
                    let parquet = dir
                        .path()
                        .join(format!("{grid}-{categorical}-{geoparquet}.parquet"));
                    let rows = export_raster_to_parquet_with_grid(
                        grid,
                        input,
                        &parquet,
                        vec![10],
                        categorical,
                        Some("EPSG:4326".into()),
                        None,
                        None,
                        geoparquet,
                        false,
                    )
                    .unwrap();
                    assert!(rows > 0);
                    let reader =
                        SerializedFileReader::new(std::fs::File::open(&parquet).unwrap()).unwrap();
                    let schema = reader.metadata().file_metadata().schema_descr();
                    assert_eq!(schema.column(0).name(), grid.index_column_name());
                    let cells: Vec<u64> = reader
                        .get_row_iter(None)
                        .unwrap()
                        .map(|row| row.unwrap().get_ulong(0).unwrap())
                        .collect();
                    assert_eq!(cells.len(), rows);
                    assert!(cells.iter().all(|&c| grid.resolution(c) == Some(10)));

                    // The Parquet file transcodes to tiles on the grid its column names.
                    let tiles = dir.path().join("from-parquet.pmtiles");
                    let summary = transcode_parquet_to_pmtiles(&parquet, &tiles, None).unwrap();
                    assert_eq!(summary.valid_features, rows, "{grid}");
                    assert_eq!(summary.invalid_features_dropped, 0);
                    assert_eq!(summary.max_zoom, 10);
                }
                let tiles = dir.path().join(format!("{grid}-{categorical}.pmtiles"));
                let written = export_raster_to_pmtiles_with_grid(
                    grid,
                    input,
                    &tiles,
                    vec![10],
                    categorical,
                    Some("EPSG:4326".into()),
                    None,
                    None,
                    false,
                )
                .unwrap();
                assert!(written > 0, "{grid}");
                assert!(std::fs::metadata(&tiles).unwrap().len() > 127);
            }
        }
    }
}
