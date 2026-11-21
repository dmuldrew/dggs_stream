//! Wide continuous batches with shared cell columns and band-prefixed statistics.
use std::path::PathBuf;
use std::sync::Arc;

use super::{ColumnSchema, RecordBatch};
use crate::aggregator::multi_horizon::multiband::{Bands, MultiBandStreamer};
use crate::aggregator::multi_horizon::{
    MultiContinuousRecord, MultiResolutionConfig, QuantileTarget,
};
use crate::aggregator::output_policy::OutputPolicy;
use crate::error::{DggsStreamError, Result};
use crate::grid::GridKind;
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::http_range::RemoteSourceFactory;

/// Resolve a SQL/CLI band selector against the input whose header defines the schema.
pub fn resolve_bands(
    paths: &[PathBuf],
    selector: &str,
    factory: Option<&RemoteSourceFactory>,
) -> Result<Vec<usize>> {
    open_band_selection(paths, selector, factory).map(|(_, bands)| bands)
}

/// Open a single raster and resolve a selector against the same reader used for scanning.
pub fn open_band_selection(
    paths: &[PathBuf],
    selector: &str,
    factory: Option<&RemoteSourceFactory>,
) -> Result<(GeoTiffStreamReader, Vec<usize>)> {
    let [path] = paths else {
        return Err(DggsStreamError::InvalidParameter(
            "bands requires one input raster".into(),
        ));
    };
    let reader = GeoTiffStreamReader::open_with_remote_source_factory(path, factory)?;
    let bands = parse_bands(selector, reader.metadata.samples_per_pixel)?;
    Ok((reader, bands))
}

/// Resolve an ordered selector when the input is already open.
pub fn parse_bands(selector: &str, samples_per_pixel: u16) -> Result<Vec<usize>> {
    let count = usize::from(samples_per_pixel);
    if selector.trim().eq_ignore_ascii_case("all") {
        return Ok((1..=count).collect());
    }
    let text = selector.trim();
    // DuckDB also converts SQL list literals to their bracketed text representation.
    let text = text
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(text);
    let mut seen = std::collections::HashSet::new();
    text.split(',')
        .map(|s| {
            s.trim()
                .parse::<usize>()
                .ok()
                .filter(|&b| (1..=count).contains(&b) && seen.insert(b))
                .ok_or_else(|| {
                    DggsStreamError::InvalidParameter(format!(
                        "expected all or distinct band numbers in 1..={count}, got {selector}"
                    ))
                })
        })
        .collect()
}

// Map each wide column to a selected band slot and its standard continuous column.
// Shared grid columns are serialized once, alongside the first band's statistics.
fn layout(
    grid: GridKind,
    bands: &[usize],
    quantiles: &[QuantileTarget],
) -> (ColumnSchema, Vec<(usize, usize)>) {
    let base = ColumnSchema::continuous_for(grid, quantiles);
    let mut fields = Vec::new();
    let mut sources = Vec::new();
    for col in [0, 1, 8, 9] {
        fields.push(base.fields[col].clone());
        sources.push((0, col));
    }
    for (slot, band) in bands.iter().enumerate() {
        for col in (2..8).chain(10..base.fields.len()) {
            let mut f = base.fields[col].clone();
            f.name = format!("band_{band}_{}", f.name);
            // Another band can keep the cell even when this band has no valid samples.
            f.nullable |= matches!(col, 2 | 5 | 6);
            fields.push(f);
            sources.push((slot, col));
        }
    }
    (ColumnSchema::new(fields), sources)
}

/// Shared cell columns followed by continuous statistics in the requested band order.
pub fn wide_schema(grid: GridKind, bands: &[usize], quantiles: &[QuantileTarget]) -> ColumnSchema {
    layout(grid, bands, quantiles).0
}

/// Projected, bounded batches from a single multi-band raster scan.
pub struct MultiBandBatchReader {
    scan: MultiBandStreamer,
    grid: GridKind,
    policy: OutputPolicy,
    quantiles: Vec<QuantileTarget>,
    // (output slot, standard continuous column) per band.
    groups: Vec<Vec<(usize, usize)>>,
    schema: Arc<ColumnSchema>,
    batch_size: usize,
    remaining: usize,
    profile: Option<Arc<crate::run_profile::RunProfile>>,
}

impl MultiBandBatchReader {
    pub fn open(
        reader: GeoTiffStreamReader,
        config: &MultiResolutionConfig,
        bands: &[usize],
        projection: Option<&[usize]>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        if bands.is_empty() {
            return Err(DggsStreamError::InvalidParameter(
                "at least one band is required".into(),
            ));
        }
        let (full, sources) = layout(config.grid, bands, &config.quantiles);
        let indices = projection.map_or_else(|| (0..sources.len()).collect(), |p| p.to_vec());
        let schema = Arc::new(full.project(&indices)?);
        let mut groups = vec![Vec::new(); bands.len()];
        let mut sketches = false;
        for (output, idx) in indices.into_iter().enumerate() {
            let (band, col) = sources[idx];
            groups[band].push((output, col));
            sketches |= col >= 10;
        }
        let mut scan_config = config.clone();
        if !sketches {
            scan_config.quantiles.clear();
        }
        let scan = MultiBandStreamer::new(reader, &scan_config, Bands::Selected(bands))?;
        Ok(Self {
            scan,
            grid: config.grid,
            policy: config.output_policy(),
            quantiles: config.quantiles.clone(),
            groups,
            schema,
            batch_size: batch_size.max(1),
            remaining: limit.unwrap_or(usize::MAX),
            profile: config.profile.clone(),
        })
    }
    pub fn schema(&self) -> Arc<ColumnSchema> {
        self.schema.clone()
    }
    pub fn transform_failures(&self) -> u64 {
        self.scan.transform_failures()
    }
    fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows = self
            .scan
            .fetch_next_batch(self.batch_size.min(self.remaining))?;
        let n = rows.len();
        if n == 0 {
            return Ok(None);
        }
        self.remaining -= n;
        let mut build = || {
            let mut columns = vec![None; self.schema.fields.len()];
            for (band, group) in self
                .groups
                .iter()
                .enumerate()
                .filter(|(_, g)| !g.is_empty())
            {
                let records: Vec<_> = rows
                    .iter_mut()
                    .map(|r| MultiContinuousRecord {
                        resolution: r.resolution,
                        cell_id: r.cell_id,
                        accumulator: std::mem::take(&mut r.statistics[band]),
                    })
                    .collect();
                let indices: Vec<_> = group.iter().map(|&(_, col)| col).collect();
                let batch = RecordBatch::from_continuous_records_with(
                    self.grid,
                    &self.policy,
                    &records,
                    &self.quantiles,
                    Some(&indices),
                )?;
                for (&(output, _), col) in group.iter().zip(batch.columns) {
                    columns[output] = Some(col);
                }
            }
            Ok(Some(RecordBatch {
                schema: self.schema(),
                columns: columns
                    .into_iter()
                    .map(|c| c.expect("each projected column belongs to one band"))
                    .collect(),
                num_rows: n,
            }))
        };
        if let Some(profile) = &self.profile {
            profile.count(crate::run_profile::Counter::RowsSerialized, n as u64);
            profile.time(crate::run_profile::Phase::Serialize, build)
        } else {
            build()
        }
    }
}

impl Iterator for MultiBandBatchReader {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        match self.read_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            result => {
                self.remaining = 0;
                result.transpose()
            }
        }
    }
}
