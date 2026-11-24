//! The streaming driver of the `interpolate` and `overlay` transfer modes.

mod ground_area;
pub(crate) use ground_area::ground_area_overlay_tile;
pub use ground_area::scan_ground_area_overlay;

use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::categorical::{CategoricalAccumulator, CategoricalUniformity};
use crate::aggregator::multi_horizon::borrowed::BorrowedSamples;
use crate::aggregator::multi_horizon::coordinates::is_point_in_bbox;
use crate::aggregator::multi_horizon::MultiResolutionConfig;
use crate::aggregator::output_policy::NodataPolicy;
use crate::aggregator::remap::CategoryRemapper;
use crate::aggregator::simd::SimdSpanAccumulate;
use crate::crs::transformer::CrsTransformer;
use crate::error::{DggsStreamError, Result};
use crate::grid::{wrap_lon_180, GridKind, PolygonFill};
use crate::raster::metadata::BandScaling;
use crate::raster::mosaic::{MosaicReader, TileDescriptor};
use crate::raster::prefetch::PrefetchedChunkReader;
use crate::scalar_ops::{
    grid_polygon_to_cells_wkb_into, ContainmentMode, PolygonToCellsError,
    H3_POLYGON_TO_CELLS_MAX_CELLS,
};

use super::clip::{merge_overlapping_runs, visit_overlap_spans_into, ClipScratch, PixelRun};
use super::interp::{interpolate, Interpolation};
use super::window::{chunk_is_absent, BandSelection, ChunkCache, RowWindow};
use super::{TransferMode, COARSE_CELL_DIAMETER_KM};

/// Values of one window: enough for wide rasters without holding much more than a
/// chunk band.
const MAX_WINDOW_VALUES: usize = 4 << 20;
/// Cap expanded native chunk-row windows so very tall strips do not widen blocks indefinitely.
pub(super) const MAX_NATIVE_CHUNK_ROW_WINDOW: u32 = 1024;
/// Cells listed per block, so the cell list stays small next to the window.
const TARGET_BLOCK_CELLS: f64 = 150_000.0;
// Limit the speculative overlap lists to 128 × 1024 `OverlapRun`s (16 bytes each).
// Leave room for irregular shapes around the estimated mean cell area.
const OVERLAP_BATCH_CELLS: usize = 128;
const MAX_PREPARED_OVERLAPS: usize = 1024;
const MAX_MEAN_PIXELS_PER_CELL: f64 = 512.0;
/// Batch size and threshold for end-to-end parallel clip + accumulate workers.
pub(super) const DIRECT_BATCH_CELLS: usize = 4096;
pub(super) const MIN_PARALLEL_DIRECT_CELLS: usize = 16;
const MIN_PARALLEL_INTERPOLATE_CELLS: usize = 64;
/// Shortest unit-weight run routed through the vectorized slice accumulator;
/// shorter runs are cheaper pixel by pixel.
const MIN_SIMD_RUN: usize = 4;

/// Widest rectangle handed to a polygon fill, in degrees of longitude: fills read a
/// polygon's edges as the short way round, so wide boxes are cut into slices.
const MAX_FILL_LON_SPAN: f64 = 60.0;

/// An accumulator the transfer modes can fill: the continuous statistics or the class
/// histogram.
pub trait TransferAccumulator: Send + 'static {
    /// A fresh accumulator; `track_quantiles` asks for a quantile sketch where supported.
    fn empty(track_quantiles: bool) -> Self;
    /// Add `value` with `weight`.
    fn add(&mut self, value: f64, weight: f64);
    /// Accumulate a contiguous slice of unit-weight pixel values (`weight == 1.0`).
    /// Returns the number of finite (valid) observations in `slice`.
    ///
    /// Precondition: `slice` holds raw band samples that need no value mapping, so
    /// callers must only use this when there are no histogram bins and, for
    /// categorical accumulators, no category remapper (NaN marks NoData).
    ///
    /// Continuous implementations combine the slice with a two-pass reduction and a
    /// parallel merge rather than per-sample Welford updates, so the variance can
    /// differ from per-pixel accumulation in the last bits.
    fn add_band_unit_slice(&mut self, slot: usize, slice: &[f64]) -> usize {
        let mut valid = 0;
        for &value in slice {
            if value.is_finite() {
                self.add_band(slot, value, 1.0);
                valid += 1;
            }
        }
        valid
    }
    /// Accumulate a contiguous native-typed slice of unit-weight pixel values (`weight == 1.0`).
    /// Returns the number of valid observations accumulated.
    fn add_band_native_unit_slice(
        &mut self,
        slot: usize,
        samples: BorrowedSamples<'_>,
        nodata: Option<f64>,
        scaling: BandScaling,
    ) -> usize {
        // Route through `add_band_unit_slice` so accumulators overriding it behave
        // the same on native and dense windows.
        let values: Vec<f64> = crate::dispatch_samples!(samples, nodata, |slice, nd| {
            slice
                .iter()
                .filter(|&&raw| SimdSpanAccumulate::is_valid(raw, nd))
                .map(|&raw| scaling.apply(SimdSpanAccumulate::to_f64_val(raw)))
                .filter(|v| v.is_finite())
                .collect()
        });
        self.add_band_unit_slice(slot, &values)
    }
    /// Record geometric overlap, including missing pixels, for partition merging.
    fn cover(&mut self, _area: f64) {}
    /// Initialize the selected bands of an overlay cell.
    fn empty_bands(track_quantiles: bool, bands: usize) -> Self
    where
        Self: Sized,
    {
        debug_assert_eq!(bands, 1);
        Self::empty(track_quantiles)
    }
    /// Add a contribution to one selected band.
    fn add_band(&mut self, slot: usize, value: f64, weight: f64) {
        debug_assert_eq!(slot, 0);
        self.add(value, weight);
    }
    /// Discard bands below the required area. Keep the cell if any band qualifies.
    fn retain_covered(&mut self, valid: &[f64], required: f64) -> bool {
        valid[0] >= required
    }
    /// Whether anything was added.
    fn has_samples(&self) -> bool;
    /// Whether the cell passes the configured output filters.
    fn passes(&self, filters: &Filters) -> bool;
    /// Whether values are class codes (only whole pixels' values make sense).
    const CATEGORICAL: bool;
    /// Whether [`Self::merge_from`] losslessly combines two accumulators for the same cell.
    const SUPPORTS_MERGE: bool = false;
    /// Whether geometric overlap over NoData pixels must still be recorded (e.g. partial coverage).
    const NEEDS_COVER: bool = false;
    /// Combine another accumulator built for the same cell into `self`.
    fn merge_from(&mut self, _other: &Self) {
        debug_assert!(Self::SUPPORTS_MERGE);
    }
}

/// The cell filters of a scan (`min_count`, `min_mean`, `max_mean`,
/// `min_majority_fraction`).
#[derive(Debug, Clone, Copy, Default)]
pub struct Filters {
    pub min_count: Option<f64>,
    pub min_mean: Option<f64>,
    pub max_mean: Option<f64>,
    pub min_majority_fraction: Option<f64>,
}

impl TransferAccumulator for ContinuousAccumulator {
    const CATEGORICAL: bool = false;
    const SUPPORTS_MERGE: bool = true;
    fn merge_from(&mut self, other: &Self) {
        self.merge(other);
    }

    fn empty(track_quantiles: bool) -> Self {
        if track_quantiles {
            Self::with_quantiles()
        } else {
            Self::default()
        }
    }

    fn add(&mut self, value: f64, weight: f64) {
        if weight == 1.0 {
            self.update(value);
        } else {
            self.update_weighted(value, weight);
        }
    }

    fn add_band_unit_slice(&mut self, slot: usize, slice: &[f64]) -> usize {
        debug_assert_eq!(slot, 0);
        // `merge` never touches `self.quantiles` for a sketch-less span, so the
        // sketch is fed here and the moments come from one vectorized reduction.
        if let Some(sketch) = self.quantiles.as_mut() {
            for &value in slice {
                if value.is_finite() {
                    sketch.update(value, 1.0);
                }
            }
        }
        let span = <f64 as SimdSpanAccumulate>::accumulate_span(slice, None);
        let valid = span.count as usize;
        if valid > 0 {
            self.merge(&span);
        }
        valid
    }

    fn add_band_native_unit_slice(
        &mut self,
        slot: usize,
        samples: BorrowedSamples<'_>,
        nodata: Option<f64>,
        scaling: BandScaling,
    ) -> usize {
        debug_assert_eq!(slot, 0);
        crate::dispatch_samples!(samples, nodata, |slice, nd| {
            if self.quantiles.is_some() {
                let mut valid = 0;
                for &raw in slice {
                    if SimdSpanAccumulate::is_valid(raw, nd) {
                        let val = scaling.apply(SimdSpanAccumulate::to_f64_val(raw));
                        if val.is_finite() {
                            self.update(val);
                            valid += 1;
                        }
                    }
                }
                return valid;
            }
            let mut span = <_ as SimdSpanAccumulate>::accumulate_span(slice, nd);
            if span.count == 0.0 {
                return 0;
            }
            if scaling.is_identity() {
                self.merge(&span);
                return span.count as usize;
            }
            // Calibrating the reduced span equals calibrating each sample unless the
            // offset cancels most of the raw magnitude: the raw sums then cannot hold
            // the small differences that calibrating first would keep. Losing at most
            // three bits of the mean is accepted; anything more takes the per-sample path.
            let raw_mean = scaling.scale * (span.sum / span.count);
            let cancels = (raw_mean + scaling.offset).abs() * 8.0 < raw_mean.abs();
            if !cancels
                && scaling.scale.is_finite()
                && scaling.offset.is_finite()
                && (scaling.scale * scaling.scale).is_finite()
                && scaling.apply(span.min).is_finite()
                && scaling.apply(span.max).is_finite()
            {
                span.apply_affine(scaling.scale, scaling.offset);
                if span.sum.is_finite() && span.m2.is_finite() {
                    self.merge(&span);
                    return span.count as usize;
                }
            }
            let mut valid = 0;
            for &raw in slice {
                if SimdSpanAccumulate::is_valid(raw, nd) {
                    let val = scaling.apply(SimdSpanAccumulate::to_f64_val(raw));
                    if val.is_finite() {
                        self.update(val);
                        valid += 1;
                    }
                }
            }
            valid
        })
    }

    fn has_samples(&self) -> bool {
        self.count > 0.0
    }

    fn passes(&self, filters: &Filters) -> bool {
        if self.count <= 0.0 {
            return true;
        }
        let mean = self.mean();
        filters.min_count.is_none_or(|c| self.count >= c)
            && filters.min_mean.is_none_or(|m| mean >= m)
            && filters.max_mean.is_none_or(|m| mean <= m)
    }
}

impl TransferAccumulator for Vec<ContinuousAccumulator> {
    const CATEGORICAL: bool = false;
    const SUPPORTS_MERGE: bool = true;
    fn merge_from(&mut self, other: &Self) {
        for (a, b) in self.iter_mut().zip(other) {
            a.merge(b);
        }
    }
    fn empty(track_quantiles: bool) -> Self {
        Self::empty_bands(track_quantiles, 1)
    }
    fn empty_bands(track_quantiles: bool, bands: usize) -> Self {
        (0..bands)
            .map(|_| ContinuousAccumulator::empty(track_quantiles))
            .collect()
    }
    fn add(&mut self, value: f64, weight: f64) {
        self[0].add(value, weight);
    }
    fn add_band(&mut self, slot: usize, value: f64, weight: f64) {
        self[slot].add(value, weight);
    }
    fn add_band_unit_slice(&mut self, slot: usize, slice: &[f64]) -> usize {
        self[slot].add_band_unit_slice(0, slice)
    }
    fn add_band_native_unit_slice(
        &mut self,
        slot: usize,
        samples: BorrowedSamples<'_>,
        nodata: Option<f64>,
        scaling: BandScaling,
    ) -> usize {
        self[slot].add_band_native_unit_slice(0, samples, nodata, scaling)
    }
    fn has_samples(&self) -> bool {
        self.iter().any(TransferAccumulator::has_samples)
    }
    fn passes(&self, _: &Filters) -> bool {
        true
    } // Single-band row filters are rejected at bind time.
    fn retain_covered(&mut self, valid: &[f64], required: f64) -> bool {
        let mut keep = false;
        for (stats, &area) in self.iter_mut().zip(valid) {
            if area >= required {
                keep = true;
            } else {
                stats.clear();
            }
        }
        keep
    }
}

impl TransferAccumulator for CategoricalAccumulator {
    const CATEGORICAL: bool = true;
    const SUPPORTS_MERGE: bool = true;
    fn merge_from(&mut self, other: &Self) {
        self.merge(other);
    }

    fn empty(_track_quantiles: bool) -> Self {
        Self::default()
    }

    fn add(&mut self, value: f64, weight: f64) {
        self.update_weighted(value.round() as i64, weight);
    }

    fn add_band_unit_slice(&mut self, slot: usize, slice: &[f64]) -> usize {
        debug_assert_eq!(slot, 0);
        let mut valid = 0;
        for &value in slice {
            if value.is_finite() {
                self.update(value.round() as i64);
                valid += 1;
            }
        }
        valid
    }

    fn add_band_native_unit_slice(
        &mut self,
        slot: usize,
        samples: BorrowedSamples<'_>,
        nodata: Option<f64>,
        scaling: BandScaling,
    ) -> usize {
        debug_assert_eq!(slot, 0);
        crate::dispatch_samples!(samples, nodata, |slice, nd| {
            if scaling.is_identity() {
                accumulate_categorical_native(self, slice, nd)
            } else {
                let mut valid = 0;
                for &raw in slice {
                    if SimdSpanAccumulate::is_valid(raw, nd) {
                        let val = scaling.apply(SimdSpanAccumulate::to_f64_val(raw));
                        if val.is_finite() {
                            self.update(val.round() as i64);
                            valid += 1;
                        }
                    }
                }
                valid
            }
        })
    }

    fn has_samples(&self) -> bool {
        self.total_count > 0.0
    }

    fn passes(&self, filters: &Filters) -> bool {
        if self.total_count <= 0.0 {
            return true;
        }
        filters.min_count.is_none_or(|c| self.total_count >= c)
            && filters
                .min_majority_fraction
                .is_none_or(|f| self.majority().2 >= f)
    }
}

fn accumulate_categorical_native<T: SimdSpanAccumulate + CategoricalUniformity>(
    acc: &mut CategoricalAccumulator,
    slice: &[T],
    nd: Option<T>,
) -> usize {
    if slice.is_empty() {
        return 0;
    }
    if T::is_uniform(slice) {
        let first = slice[0];
        if first.is_valid(nd) {
            if let Some(cat) = first.to_category() {
                acc.update_weighted(cat, slice.len() as f64);
                return slice.len();
            }
        }
        return 0;
    }
    let mut valid = 0usize;
    let mut curr_cat: Option<i64> = None;
    let mut curr_count = 0usize;
    for &raw in slice {
        if !raw.is_valid(nd) {
            continue;
        }
        let Some(cat) = raw.to_category() else {
            continue;
        };
        valid += 1;
        if Some(cat) == curr_cat {
            curr_count += 1;
        } else {
            if let Some(prev) = curr_cat {
                acc.update_weighted(prev, curr_count as f64);
            }
            curr_cat = Some(cat);
            curr_count = 1;
        }
    }
    if let Some(last) = curr_cat {
        acc.update_weighted(last, curr_count as f64);
    }
    valid
}

/// A rectangle of pixels of one tile at one resolution, processed as a unit.
#[derive(Debug, Clone)]
struct Block {
    resolution: u8,
    tile: usize,
    rows: Range<u32>,
    cols: Range<u32>,
    /// `[west, south, east, north]` of the block in WGS84, padded; `west > east` when it
    /// crosses the antimeridian. `None` if no corner of it could be located.
    bbox: Option<[f64; 4]>,
    /// The last block of its tile and resolution.
    last_of_tile: bool,
}

/// A cell an overlay has started and not finished.
struct Pending<A> {
    acc: A,
    /// Whether any pixel (valid or not) overlapped it.
    touched: bool,
    /// Exclusive end row in `last_tile`, once the cell has been located on that tile.
    end_row: Option<i64>,
    /// The last tile whose footprint the cell may overlap.
    last_tile: usize,
}

#[derive(Default)]
struct BandCoverage {
    valid_area: Vec<f64>,
    covered_area: f64,
}

struct CoverageFilter {
    minimum: f64,
    cells: HashMap<u64, BandCoverage>,
}

pub(super) enum OverlapParallelism {
    Serial,
    Ambient,
    Dedicated(Arc<rayon::ThreadPool>),
}

impl OverlapParallelism {
    pub(super) fn from_workers(workers: Option<usize>) -> Result<Self> {
        Ok(match workers {
            Some(1) => Self::Serial,
            Some(n) => Self::Dedicated(
                crate::aggregator::multi_horizon::controller::shared_aggregation_pool(n)?,
            ),
            None => Self::Ambient,
        })
    }

    #[inline]
    pub(super) fn should_run_parallel(&self, len: usize, min_len: usize) -> bool {
        len >= min_len && rayon::current_thread_index().is_none() && !matches!(self, Self::Serial)
    }

    #[inline]
    pub(super) fn install(&self, f: impl FnOnce() + Send) {
        match self {
            Self::Dedicated(pool) => pool.install(f),
            Self::Ambient => f(),
            Self::Serial => unreachable!(),
        }
    }
}

/// Streams `(resolution, cell, accumulator)` for the `interpolate` and `overlay` modes.
pub struct TransferStream<A: TransferAccumulator> {
    mosaic: Arc<MosaicReader>,
    grid: GridKind,
    mode: TransferMode,
    overlap_parallelism: OverlapParallelism,
    selection: BandSelection,
    bands: Vec<usize>,
    emit_nodata: bool,
    bbox: Option<[f64; 4]>,
    track_quantiles: bool,
    filters: Filters,
    coverage_filter: Option<CoverageFilter>,
    remapper: Option<Arc<CategoryRemapper>>,
    /// Bins of a binned histogram scan: a value's class is the index of its bin.
    histogram_bins: Option<Arc<crate::histogram::HistogramBins>>,
    blocks: Vec<Block>,
    /// `horizon[i]`: the northernmost latitude any cell of blocks `i..` can reach.
    horizon: Vec<f64>,
    next_block: usize,
    decode_workers: usize,
    fetch_workers: usize,
    prefetch_chunks: usize,
    decode_budget_bytes: usize,
    prefetcher: Option<PrefetchedChunkReader>,
    prefetch_run_end: usize,
    cache: ChunkCache,
    cache_tile: usize,
    pending: HashMap<u64, Pending<A>>,
    output: VecDeque<(u8, u64, A)>,
    transform_failures: u64,
    /// Set when a block fails, so later reads cannot mistake the failed scan for EOF.
    failure: Option<String>,
    failure_counter: Option<Arc<AtomicU64>>,
    strict_transforms: bool,
    /// Runtime telemetry, updated once per block.
    profile: Option<Arc<crate::run_profile::RunProfile>>,
    /// Worker batches run, and cells those workers handed back to the serial
    /// reducer, so tests can tell the parallel path from the serial one.
    #[cfg(test)]
    overlap_batches: usize,
    #[cfg(test)]
    overlap_retries: usize,
}

impl<A: TransferAccumulator> TransferStream<A> {
    /// A stream over `mosaic` for the non-default `config.transfer`.
    pub fn new(mosaic: Arc<MosaicReader>, config: &MultiResolutionConfig) -> Result<Self> {
        Self::new_bands(mosaic, config, vec![config.band])
    }

    pub(crate) fn new_bands(
        mosaic: Arc<MosaicReader>,
        config: &MultiResolutionConfig,
        bands: Vec<usize>,
    ) -> Result<Self> {
        Self::build(mosaic, config, bands, false)
    }

    fn build(
        mosaic: Arc<MosaicReader>,
        config: &MultiResolutionConfig,
        bands: Vec<usize>,
        partial_overlay: bool,
    ) -> Result<Self> {
        config.validate_coverage_threshold()?;
        let mode = config.transfer;
        let histogram_bins = config
            .histogram
            .as_ref()
            .filter(|spec| A::CATEGORICAL && spec.is_binned())
            .map(|spec| Arc::new(spec.bins.clone()));
        let invalid = |message: &str| {
            Err(DggsStreamError::InvalidParameter(format!(
                "transfer '{}' {message}",
                mode.as_str()
            )))
        };
        match mode {
            TransferMode::AssignCenters => {
                return invalid("is the default scan, not a transfer stream")
            }
            TransferMode::Auto => {
                return invalid("must be resolved before opening a transfer stream")
            }
            TransferMode::Interpolate(method)
                if A::CATEGORICAL && method != Interpolation::Nearest =>
            {
                return invalid(
                    "of class codes needs interpolation 'nearest': classes cannot be \
                     interpolated",
                )
            }
            _ => {}
        }
        if config.partition.is_some() && !partial_overlay {
            return invalid("cannot be combined with a partitioned scan");
        }
        if config.should_compact_h3_children() {
            return invalid("cannot be combined with compaction");
        }
        if config.spectral_formula.is_some() {
            return invalid("cannot be combined with a spectral formula");
        }
        if !config.sampling.is_single_point() {
            return invalid("replaces sub-pixel sampling: leave sampling at 'center'");
        }
        let allow_chunk_row_window = matches!(
            mode,
            TransferMode::Overlay {
                mass_preserve: false,
                ..
            }
        );
        let grid = config.grid;
        let mut blocks = Vec::new();
        for &resolution in &config.resolutions {
            for (tile_idx, tile) in mosaic.tiles.iter().enumerate() {
                plan_tile(
                    grid,
                    mode,
                    resolution,
                    tile_idx,
                    tile,
                    bands.len(),
                    allow_chunk_row_window,
                    &mut blocks,
                );
            }
        }
        if partial_overlay {
            let partition = config.partition.as_ref().expect("partitioned overlay");
            partition.validate_for(&mosaic)?;
            blocks = partition_blocks(&mosaic, partition, blocks);
        }
        let overlay = matches!(mode, TransferMode::Overlay { .. });
        for block in &mut blocks {
            block.bbox = block_bbox(
                grid,
                block.resolution,
                &mosaic.tiles[block.tile],
                &block.rows,
                &block.cols,
                overlay,
            );
        }
        // Everything a later block can still emit lies south of this latitude.
        let mut horizon = vec![f64::NEG_INFINITY; blocks.len() + 1];
        for i in (0..blocks.len()).rev() {
            let reach = match blocks[i].bbox {
                Some(bbox) => bbox[3] + grid.spec().max_cell_radius_deg(blocks[i].resolution),
                None => f64::NEG_INFINITY,
            };
            horizon[i] = horizon[i + 1].max(reach);
        }
        Ok(Self {
            mosaic,
            grid,
            mode,
            bands,
            overlap_parallelism: OverlapParallelism::from_workers(config.aggregation_workers)?,
            selection: BandSelection {
                band: config.band,
                nodata: config.custom_nodata,
                // Classes are stored values; bins are in calibrated units.
                apply_scale_offset: config.apply_scale_offset
                    && (!A::CATEGORICAL || histogram_bins.is_some()),
            },
            emit_nodata: config.nodata_policy == NodataPolicy::Emit,
            bbox: config.bbox,
            track_quantiles: config.track_quantiles(),
            coverage_filter: match mode {
                TransferMode::Overlay {
                    mass_preserve: false,
                    ..
                } => config
                    .valid_coverage_threshold
                    .filter(|_| !partial_overlay)
                    .filter(|&t| t > 0.0)
                    .map(|minimum| CoverageFilter {
                        minimum,
                        cells: HashMap::new(),
                    }),
                _ => None,
            },
            filters: Filters {
                min_count: config.min_count,
                min_mean: config.min_mean,
                max_mean: config.max_mean,
                min_majority_fraction: config.min_majority_fraction,
            },
            remapper: config.remapper.clone(),
            histogram_bins,
            blocks,
            horizon,
            next_block: 0,
            decode_workers: config.decode_workers,
            fetch_workers: config.fetch_workers,
            prefetch_chunks: config.prefetch_chunks,
            decode_budget_bytes: config.decode_budget_bytes,
            prefetcher: None,
            prefetch_run_end: 0,
            cache: ChunkCache::default(),
            cache_tile: usize::MAX,
            pending: HashMap::new(),
            output: VecDeque::new(),
            transform_failures: 0,
            failure: None,
            failure_counter: config.transform_failure_counter.clone(),
            strict_transforms: config.strict_transforms,
            profile: config.profile.clone(),
            #[cfg(test)]
            overlap_batches: 0,
            #[cfg(test)]
            overlap_retries: 0,
        })
    }

    /// Whether every cell has been drained.
    pub fn is_finished(&self) -> bool {
        self.next_block >= self.blocks.len() && self.output.is_empty()
    }

    /// Every cell not yet drained lies south of this latitude (`-inf` once finished).
    pub fn current_lat_horizon(&self) -> f64 {
        if !self.output.is_empty() || !self.pending.is_empty() {
            // Cells already built may lie north of the remaining blocks.
            return f64::INFINITY;
        }
        self.horizon[self.next_block.min(self.blocks.len())]
    }

    /// Cells whose position could not be taken through the raster's CRS.
    pub fn transform_failures(&self) -> u64 {
        self.transform_failures
    }

    /// Process blocks until at least `min_rows` finished cells wait to be drained, or
    /// every block is processed.
    pub fn advance_until_completed(&mut self, min_rows: usize) -> Result<()> {
        self.check_failed()?;
        while self.output.len() < min_rows && self.next_block < self.blocks.len() {
            self.process_next()?;
        }
        Ok(())
    }

    /// Hand up to `max_rows` finished cells to `consumer`; `0` means the end.
    pub fn drain_into(
        &mut self,
        max_rows: usize,
        mut consumer: impl FnMut(usize, u8, u64, A),
    ) -> Result<usize> {
        self.check_failed()?;
        let mut drained = 0;
        while drained < max_rows {
            if let Some((resolution, cell, acc)) = self.output.pop_front() {
                consumer(drained, resolution, cell, acc);
                drained += 1;
                continue;
            }
            if self.next_block >= self.blocks.len() {
                break;
            }
            self.process_next()?;
        }
        Ok(drained)
    }

    fn check_failed(&self) -> Result<()> {
        match &self.failure {
            Some(reason) => Err(DggsStreamError::StreamFailed(reason.clone())),
            None => Ok(()),
        }
    }

    /// Process the next block. A failure is latched: the block is already passed, so
    /// resuming would silently drop its contribution.
    fn process_next(&mut self) -> Result<()> {
        let block_idx = self.next_block;
        let block = self.blocks[block_idx].clone();
        self.next_block += 1;
        let result = self.process(block_idx, &block);
        if let Err(e) = &result {
            self.failure = Some(e.to_string());
        }
        result
    }

    fn record_failures(&mut self, failures: u64) -> Result<()> {
        if failures == 0 {
            return Ok(());
        }
        self.transform_failures += failures;
        if let Some(profile) = &self.profile {
            profile.count(crate::run_profile::Counter::TransformFailures, failures);
        }
        if let Some(counter) = &self.failure_counter {
            counter.fetch_add(failures, Ordering::Relaxed);
        }
        if self.strict_transforms {
            return Err(DggsStreamError::TransformFailures(self.transform_failures));
        }
        Ok(())
    }

    /// Read a window of `tile`, timed as decoding when no background prefetcher is running.
    fn load_window(
        &mut self,
        tile: &TileDescriptor,
        rows: Range<u32>,
        cols: Range<u32>,
    ) -> Result<RowWindow> {
        use crate::run_profile::{Phase, PhaseTimer};
        let timer = self
            .prefetcher
            .is_none()
            .then(|| self.profile.as_ref().map(|_| PhaseTimer::start()))
            .flatten();
        let window = RowWindow::load_bands(
            tile,
            &self.bands,
            self.selection,
            rows,
            cols,
            &mut self.cache,
            self.prefetcher.as_ref(),
            self.profile.as_deref(),
        )?;
        if let (Some(profile), Some(timer)) = (&self.profile, timer) {
            timer.stop(profile, Phase::Decode);
        }
        Ok(window)
    }

    fn process(&mut self, block_idx: usize, block: &Block) -> Result<()> {
        use crate::run_profile::{Counter, Phase, PhaseTimer};
        let timer = self.profile.as_ref().map(|_| PhaseTimer::start());
        let outcome = self.process_block(block_idx, block);
        if let (Some(profile), Some(timer)) = (&self.profile, timer) {
            // The block's time, window reading included, is the transfer kernel's.
            timer.stop(profile, Phase::Kernel);
            profile.count(Counter::Windows, 1);
            profile.count(
                Counter::PixelsRead,
                block.rows.len() as u64 * block.cols.len() as u64,
            );
        }
        outcome
    }

    fn process_block(&mut self, block_idx: usize, block: &Block) -> Result<()> {
        if block_idx >= self.prefetch_run_end || self.cache_tile != block.tile {
            self.cache.clear_and_recycle(self.prefetcher.as_ref());
            self.prefetcher = None;
            self.cache_tile = block.tile;
            let run_end = block_idx
                + self.blocks[block_idx..]
                    .iter()
                    .take_while(|b| b.tile == block.tile && b.resolution == block.resolution)
                    .count();
            let tile = &self.mosaic.tiles[block.tile];
            let (misses, blocks_covered) = plan_run_chunk_misses(
                tile,
                &self.blocks[block_idx..run_end],
                &self.bands,
                self.mode,
            );
            self.prefetch_run_end = block_idx + blocks_covered;
            if !misses.is_empty()
                && (tile.reader.is_remote() || (self.decode_workers > 1 && misses.len() > 2))
            {
                let chunk_bytes = tile.reader.decoded_chunk_bytes().max(1);
                let budget_chunks = (self.decode_budget_bytes / chunk_bytes).max(16);
                let target_lookahead = (tile.reader.chunk_layout.chunks_across as usize)
                    .max(self.prefetch_chunks)
                    .max(self.decode_workers * 4)
                    .min(512);
                let queue_cap = target_lookahead
                    .min(budget_chunks)
                    .min(misses.len())
                    .max(16);
                self.prefetcher = Some(PrefetchedChunkReader::spawn_with_profile(
                    tile.reader.clone(),
                    misses,
                    queue_cap,
                    self.decode_workers,
                    self.fetch_workers,
                    self.profile.clone(),
                ));
            }
        }
        match block.bbox {
            None => self.record_failures(1)?,
            Some(bbox) => match self.mode {
                TransferMode::Interpolate(method) => self.interpolate_block(block, bbox, method)?,
                TransferMode::Overlay {
                    weighted,
                    mass_preserve,
                } => self.overlay_block(block, bbox, weighted, mass_preserve)?,
                TransferMode::AssignCenters | TransferMode::Auto => {}
            },
        }
        self.finish_cells(block);
        Ok(())
    }

    /// The class a value stands for after remapping, or the value itself.
    fn mapped(
        bins: Option<&crate::histogram::HistogramBins>,
        remapper: Option<&CategoryRemapper>,
        value: f64,
    ) -> Option<f64> {
        if let Some(bins) = bins {
            return bins.bin_of(value).map(|bin| bin as f64);
        }
        match remapper {
            Some(remapper) if A::CATEGORICAL => {
                remapper.remap(value.round() as i64).map(|c| c as f64)
            }
            _ => Some(value),
        }
    }

    fn interpolate_block(
        &mut self,
        block: &Block,
        bbox: [f64; 4],
        method: Interpolation,
    ) -> Result<()> {
        let mosaic = Arc::clone(&self.mosaic);
        let tile = &mosaic.tiles[block.tile];
        let margin = method.margin();
        let rows = block.rows.start.saturating_sub(margin)..block.rows.end.saturating_add(margin);
        let window = self.load_window(tile, rows, 0..tile.reader.metadata.width)?;
        let cells = cells_in_bbox(
            self.grid,
            bbox,
            block.resolution,
            ContainmentMode::ContainsCentroid,
        )?;
        let histogram_bins_arc = self.histogram_bins.clone();
        let remapper_arc = self.remapper.clone();
        let ctx = InterpBlockCtx {
            grid: self.grid,
            mosaic: &mosaic,
            tile,
            tile_idx: block.tile,
            window: &window,
            method,
            rows: block.rows.clone(),
            cols: block.cols.clone(),
            track_quantiles: self.track_quantiles,
            emit_nodata: self.emit_nodata,
            bbox: self.bbox,
            filters: self.filters,
            histogram_bins: histogram_bins_arc.as_deref(),
            remapper: remapper_arc.as_deref(),
        };
        let mut failures = 0;
        let output = &mut self.output;
        let mut apply_outcome = |cell: u64, outcome: InterpOutcome<A>| match outcome {
            InterpOutcome::Skip => {}
            InterpOutcome::Failed => failures += 1,
            InterpOutcome::Emit(acc) => output.push_back((block.resolution, cell, acc)),
        };

        if self
            .overlap_parallelism
            .should_run_parallel(cells.len(), MIN_PARALLEL_INTERPOLATE_CELLS)
        {
            let mut outcomes: Vec<InterpOutcome<A>> = Vec::new();
            for batch in cells.chunks(DIRECT_BATCH_CELLS) {
                #[cfg(test)]
                {
                    self.overlap_batches += 1;
                }
                outcomes.resize_with(batch.len(), InterpOutcome::default);
                self.overlap_parallelism.install(|| {
                    outcomes.par_iter_mut().zip(batch).for_each(|(out, &cell)| {
                        *out = interpolate_cell(cell, &ctx);
                    });
                });
                for (&cell, outcome) in batch.iter().zip(outcomes.drain(..)) {
                    apply_outcome(cell, outcome);
                }
            }
        } else {
            for &cell in &cells {
                apply_outcome(cell, interpolate_cell(cell, &ctx));
            }
        }
        self.record_failures(failures)
    }

    fn overlay_block(
        &mut self,
        block: &Block,
        bbox: [f64; 4],
        weighted: bool,
        mass_preserve: bool,
    ) -> Result<()> {
        let mosaic = Arc::clone(&self.mosaic);
        let tile = &mosaic.tiles[block.tile];
        let window = self.load_window(tile, block.rows.clone(), block.cols.clone())?;
        let width = i64::from(window.width());
        let rows = (i64::from(block.rows.start), i64::from(block.rows.end));
        let band_width = window.width() as usize;
        let cells = cells_in_bbox(self.grid, bbox, block.resolution, ContainmentMode::Covers)?;
        let several_tiles = mosaic.tiles.len() > 1;
        let gt = &tile.reader.metadata.geotransform;

        // Ordinary overlay reuses this buffer per cell. Conservation needs a second pass.
        let mut overlaps: Vec<OverlapRun> = Vec::new();
        let mut clip_scratch = ClipScratch::default();
        let mut totals = if mass_preserve {
            vec![0.0f64; band_width * block.rows.len()]
        } else {
            Vec::new()
        };
        let mut cell_ends = Vec::new();
        let longitude_bounds = periodic_longitude_bounds(tile);
        let mut failures = 0;
        // Cells this large are assigned whole pixels by centre (see below).
        let coarse =
            self.grid.spec().max_cell_diameter_km(block.resolution) > COARSE_CELL_DIAMETER_KM;
        // The cell holding each pixel centre of the block, built when first needed.
        let mut pixel_cells: Option<Vec<Option<u64>>> = None;
        let owners_of_block = || {
            let mut owners = Vec::with_capacity(band_width * block.rows.len());
            for r in rows.0..rows.1 {
                for c in 0..width {
                    let (x, y) = gt.pixel_to_coord(c as f64 + 0.5, r as f64 + 0.5);
                    owners.push(tile.crs_transformer.transform_point(x, y).ok().and_then(
                        |(lon, lat)| self.grid.cell_at(lat, wrap_lon_180(lon), block.resolution),
                    ));
                }
            }
            owners
        };

        let band_count = self.bands.len();
        let track_quantiles = self.track_quantiles;
        let grid = self.grid;
        let bbox_filter = self.bbox;
        let histogram_bins_arc = self.histogram_bins.clone();
        let remapper_arc = self.remapper.clone();
        let histogram_bins = histogram_bins_arc.as_deref();
        let remapper = remapper_arc.as_deref();

        let mut selected = (!(!mass_preserve && !coarse && A::SUPPORTS_MERGE)
            && (bbox_filter.is_some() || several_tiles))
            .then(|| vec![None; band_width * block.rows.len()]);
        // `min_mean` and `max_mean` compare the rounded mean with an inclusive bound.
        // Slice accumulation sums in a different order than per-pixel updates, so a
        // mean that is exactly on the bound could land one ulp outside it. Keep the
        // per-pixel order whenever such a filter is set.
        let mean_filtered = self.filters.min_mean.is_some() || self.filters.max_mean.is_some();
        let mut contribute_run = |pending: &mut Pending<A>,
                                  mut coverage: Option<&mut BandCoverage>,
                                  run: OverlapRun,
                                  weight: f64| {
            let start_pixel = run.pixel as usize;
            let row_off = start_pixel / band_width;
            let col0 = start_pixel % band_width;
            let col1 = col0 + run.len as usize;
            let clipped_c0 = col0.max(block.cols.start as usize);
            let clipped_c1 = col1.min(block.cols.end as usize);
            if clipped_c0 >= clipped_c1 {
                return;
            }
            let clipped_len = clipped_c1 - clipped_c0;
            let start = row_off * band_width + clipped_c0;
            let row = rows.0 + row_off as i64;
            // Whole, unit-weight pixels whose samples need no mapping can be
            // accumulated as contiguous slices.
            let unit = run.area == 1.0
                && weight == 1.0
                && !mean_filtered
                && histogram_bins.is_none()
                && (!A::CATEGORICAL || remapper.is_none());
            // Whether a pixel centre lies in the bbox and, in a mosaic, this tile.
            let pixel_selected = |offset: usize| {
                let col = (clipped_c0 + offset) as f64;
                let (x, y) = gt.pixel_to_coord(col + 0.5, row as f64 + 0.5);
                tile.crs_transformer
                    .transform_point(x, y)
                    .is_ok_and(|(lon, lat)| {
                        is_point_in_bbox(wrap_lon_180(lon), lat, bbox_filter)
                            && (!several_tiles || mosaic.is_point_owned_by(block.tile, lon, lat))
                    })
            };
            // The direct parallel path keeps no cache (its polygon spans select pixels
            // themselves), but this centre-assignment fallback must still filter.
            let filtered = bbox_filter.is_some() || several_tiles;
            let mut is_selected = |offset: usize| match selected.as_mut() {
                Some(cache) => *cache[start + offset].get_or_insert_with(|| pixel_selected(offset)),
                None => !filtered || pixel_selected(offset),
            };
            let mut offset = 0;
            while offset < clipped_len {
                if !is_selected(offset) {
                    offset += 1;
                    continue;
                }
                // Extend over the following selected pixels when they can be
                // accumulated together.
                let mut end = offset + 1;
                if unit {
                    while end < clipped_len && is_selected(end) {
                        end += 1;
                    }
                }
                let len = end - offset;
                let index = start + offset;
                pending.touched = true;
                if len >= MIN_SIMD_RUN {
                    pending.acc.cover(len as f64);
                    if let Some(c) = coverage.as_mut() {
                        c.covered_area += len as f64;
                    }
                    for slot in 0..band_count {
                        let valid = window.accumulate_unit_span(
                            slot,
                            row_off,
                            clipped_c0 + offset,
                            clipped_c0 + offset + len,
                            &mut pending.acc,
                        );
                        if let Some(c) = coverage.as_mut() {
                            c.valid_area[slot] += valid as f64;
                        }
                    }
                } else {
                    for index in index..index + len {
                        let col = index - row_off * band_width;
                        pending.acc.cover(run.area);
                        if let Some(c) = coverage.as_mut() {
                            c.covered_area += run.area;
                        }
                        for slot in 0..band_count {
                            if let Some(sample) = window.band_value_at(slot, row_off, col) {
                                if let Some(c) = coverage.as_mut() {
                                    c.valid_area[slot] += run.area;
                                }
                                if let Some(value) = Self::mapped(histogram_bins, remapper, sample)
                                {
                                    pending.acc.add_band(slot, value, weight);
                                }
                            }
                        }
                    }
                }
                offset = end;
            }
        };

        if !mass_preserve && !coarse && A::SUPPORTS_MERGE {
            let ctx = OverlayBlockCtx {
                grid,
                mosaic: &mosaic,
                tile,
                tile_idx: block.tile,
                several_tiles,
                window: &window,
                longitude_bounds,
                rows,
                cols: block.cols.clone(),
                width,
                band_count,
                weighted,
                track_quantiles,
                need_coverage: self.coverage_filter.is_some(),
                emit_nodata: self.emit_nodata,
                bbox: bbox_filter,
                histogram_bins,
                remapper,
                mean_filtered,
            };
            let use_parallel = cells.len() >= MIN_PARALLEL_DIRECT_CELLS
                && rayon::current_thread_index().is_none()
                && !matches!(self.overlap_parallelism, OverlapParallelism::Serial);
            let mut apply_outcome = |cell: u64, outcome: CellOutcome<A>| match outcome {
                CellOutcome::NoDataOnly { end_row } => {
                    if let Some(pending) = self.pending.get_mut(&cell) {
                        if block.tile == pending.last_tile {
                            pending.end_row = Some(end_row);
                        }
                    }
                }
                CellOutcome::Failed => failures += 1,
                CellOutcome::Pole => {
                    let before = overlaps.len();
                    let end_row = i64::from(tile.reader.metadata.height);
                    let owners = pixel_cells.get_or_insert_with(&owners_of_block);
                    for (pixel, owner) in owners.iter().enumerate() {
                        if *owner == Some(cell) {
                            overlaps.push(OverlapRun {
                                pixel: pixel as u32,
                                len: 1,
                                area: 1.0,
                            });
                        }
                    }
                    if overlaps.len() > before || self.pending.contains_key(&cell) {
                        let pending = self.pending.entry(cell).or_insert_with(|| Pending {
                            acc: A::empty_bands(track_quantiles, band_count),
                            touched: false,
                            end_row: None,
                            last_tile: if several_tiles {
                                last_overlapping_tile(&mosaic, grid, cell, block.tile)
                            } else {
                                block.tile
                            },
                        });
                        if block.tile == pending.last_tile {
                            pending.end_row = Some(end_row);
                        }
                        let mut coverage = self.coverage_filter.as_mut().map(|filter| {
                            filter.cells.entry(cell).or_insert_with(|| BandCoverage {
                                valid_area: vec![0.0; band_count],
                                covered_area: 0.0,
                            })
                        });
                        for run in overlaps.drain(..) {
                            let weight = if weighted { run.area } else { 1.0 };
                            contribute_run(pending, coverage.as_deref_mut(), run, weight);
                        }
                    }
                }
                CellOutcome::Accumulated {
                    end_row,
                    acc,
                    touched,
                    coverage: cell_cov,
                } => {
                    let last_tile = if several_tiles {
                        last_overlapping_tile(&mosaic, grid, cell, block.tile)
                    } else {
                        block.tile
                    };
                    match self.pending.entry(cell) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(Pending {
                                acc,
                                touched,
                                end_row: (block.tile == last_tile).then_some(end_row),
                                last_tile,
                            });
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            let pending = slot.get_mut();
                            pending.acc.merge_from(&acc);
                            pending.touched |= touched;
                            if block.tile == pending.last_tile {
                                pending.end_row = Some(end_row);
                            }
                        }
                    }
                    if let (Some(filter), Some(cov)) = (self.coverage_filter.as_mut(), cell_cov) {
                        let entry = filter.cells.entry(cell).or_insert_with(|| BandCoverage {
                            valid_area: vec![0.0; band_count],
                            covered_area: 0.0,
                        });
                        entry.covered_area += cov.covered_area;
                        for (dst, src) in entry.valid_area.iter_mut().zip(cov.valid_area) {
                            *dst += src;
                        }
                    }
                }
            };

            if use_parallel {
                let mut outcomes: Vec<CellOutcome<A>> = Vec::new();
                for batch in cells.chunks(DIRECT_BATCH_CELLS) {
                    #[cfg(test)]
                    {
                        self.overlap_batches += 1;
                    }
                    outcomes.resize_with(batch.len(), CellOutcome::default);
                    let mut run_batch = || {
                        outcomes.par_iter_mut().zip(batch).for_each_init(
                            CellWorkerScratch::default,
                            |scratch, (out, &cell)| {
                                *out = clip_and_accumulate_cell(cell, &ctx, scratch);
                            },
                        );
                    };
                    match &self.overlap_parallelism {
                        OverlapParallelism::Dedicated(pool) => pool.install(run_batch),
                        OverlapParallelism::Ambient => run_batch(),
                        OverlapParallelism::Serial => unreachable!(),
                    }
                    for (&cell, outcome) in batch.iter().zip(outcomes.drain(..)) {
                        apply_outcome(cell, outcome);
                    }
                }
            } else {
                let mut direct_scratch = CellWorkerScratch::default();
                for &cell in &cells {
                    let outcome = clip_and_accumulate_cell(cell, &ctx, &mut direct_scratch);
                    apply_outcome(cell, outcome);
                }
            }
            return self.record_failures(failures);
        }

        // Coarse cells are exactly those holding a pixel centre.
        let cells = if coarse {
            let owners = owners_of_block();
            let mut cells: Vec<u64> = owners.iter().flatten().copied().collect();
            cells.sort_unstable();
            cells.dedup();
            pixel_cells = Some(owners);
            cells
        } else {
            cells
        };
        let geometry = OverlayGeometry {
            grid,
            tile,
            longitude_bounds,
            rows,
            width,
        };
        // Installing work while the caller is a Rayon worker could steal an
        // unrelated query that waits for the caller's engine lock.
        let parallel_prep = !coarse
            && cells.len() >= OVERLAP_BATCH_CELLS / 2
            && rayon::current_thread_index().is_none()
            && !matches!(self.overlap_parallelism, OverlapParallelism::Serial)
            && crate::resolution::pixel_ground_area_m2(tile).is_ok_and(|area| {
                self.grid.area_m2(block.resolution) <= MAX_MEAN_PIXELS_PER_CELL * area
            });
        let mut prepared = Vec::<(Option<i64>, Vec<OverlapRun>)>::new();
        for (i, &cell) in cells.iter().enumerate() {
            // Keep the serial reducer in original cell and pixel order.
            let slot = i % OVERLAP_BATCH_CELLS;
            if parallel_prep && slot == 0 {
                #[cfg(test)]
                {
                    self.overlap_batches += 1;
                }
                let batch = &cells[i..cells.len().min(i + OVERLAP_BATCH_CELLS)];
                prepared.resize_with(batch.len(), Default::default);
                let mut run_prep = || {
                    prepared.par_iter_mut().zip(batch).for_each_init(
                        ClipScratch::default,
                        |scratch, ((end, pixels), &id)| {
                            pixels.clear();
                            *end = geometry.clip(id, pixels, MAX_PREPARED_OVERLAPS, scratch);
                        },
                    );
                };
                match &self.overlap_parallelism {
                    OverlapParallelism::Dedicated(pool) => pool.install(run_prep),
                    OverlapParallelism::Ambient => run_prep(),
                    OverlapParallelism::Serial => unreachable!(),
                }
            }
            let before = overlaps.len();
            let clipped_end = match prepared.get_mut(slot) {
                Some((Some(end), pixels)) => {
                    overlaps.append(pixels);
                    Some(*end)
                }
                _ if !coarse => {
                    #[cfg(test)]
                    if parallel_prep {
                        self.overlap_retries += 1;
                    }
                    geometry.clip(cell, &mut overlaps, usize::MAX, &mut clip_scratch)
                }
                _ => None,
            };
            let end_row;
            if let Some(end) = clipped_end {
                end_row = end;
            } else if coarse || self.grid.interior_pole(cell).is_some() {
                // A cell holding a pole has no polygon in a lon/lat raster, and the edges
                // of a very large cell are far from straight in pixel space: such a cell
                // takes the pixels whose centres it holds.
                end_row = i64::from(tile.reader.metadata.height);
                let owners = pixel_cells.get_or_insert_with(&owners_of_block);
                for (pixel, owner) in owners.iter().enumerate() {
                    if *owner == Some(cell) {
                        // Extend the previous run along its row so whole-pixel
                        // stretches reach the slice accumulator.
                        match overlaps[before..].last_mut() {
                            Some(last)
                                if last.pixel as usize + last.len as usize == pixel
                                    && pixel % band_width != 0 =>
                            {
                                last.len += 1;
                            }
                            _ => overlaps.push(OverlapRun {
                                pixel: pixel as u32,
                                len: 1,
                                area: 1.0,
                            }),
                        }
                    }
                }
            } else {
                // A later longitude copy can fail after the first contributed.
                overlaps.truncate(before);
                failures += 1;
                continue;
            }
            if overlaps.len() == before && !self.pending.contains_key(&cell) {
                continue;
            }
            let pending = self.pending.entry(cell).or_insert_with(|| Pending {
                acc: A::empty_bands(track_quantiles, band_count),
                touched: false,
                end_row: None,
                last_tile: if several_tiles {
                    last_overlapping_tile(&mosaic, grid, cell, block.tile)
                } else {
                    block.tile
                },
            });
            if block.tile == pending.last_tile {
                pending.end_row = Some(end_row);
            }
            if mass_preserve {
                for &run in &overlaps[before..] {
                    let start = run.pixel as usize;
                    for total in &mut totals[start..start + run.len as usize] {
                        *total += run.area;
                    }
                }
                cell_ends.push((cell, overlaps.len()));
            } else {
                let mut coverage = self.coverage_filter.as_mut().map(|filter| {
                    filter.cells.entry(cell).or_insert_with(|| BandCoverage {
                        valid_area: vec![0.0; band_count],
                        covered_area: 0.0,
                    })
                });
                for run in overlaps.drain(..) {
                    let weight = if weighted { run.area } else { 1.0 };
                    contribute_run(pending, coverage.as_deref_mut(), run, weight);
                }
            }
        }
        let mut start = 0;
        for (cell, end) in cell_ends {
            let pending = self
                .pending
                .get_mut(&cell)
                .expect("cell remains pending until block completion");
            for &run in &overlaps[start..end] {
                let run_start = run.pixel as usize;
                let run_len = run.len as usize;
                if run_len >= MIN_SIMD_RUN
                    && !mean_filtered
                    && run.area == 1.0
                    && totals[run_start..run_start + run_len]
                        .iter()
                        .all(|&t| t == 1.0)
                {
                    contribute_run(pending, None, run, 1.0);
                } else {
                    for offset in 0..run_len {
                        let px = run_start + offset;
                        contribute_run(
                            pending,
                            None,
                            OverlapRun {
                                pixel: px as u32,
                                len: 1,
                                area: run.area,
                            },
                            run.area / totals[px],
                        );
                    }
                }
            }
            start = end;
        }
        self.record_failures(failures)
    }

    /// Move the cells no later pixel can reach from `pending` to the output.
    fn finish_cells(&mut self, block: &Block) {
        if self.pending.is_empty() {
            return;
        }
        let end = i64::from(
            self.blocks
                .get(self.next_block)
                .filter(|b| b.tile == block.tile && b.resolution == block.resolution)
                .map_or(block.rows.end, |b| b.rows.start),
        );
        let last_block_of_resolution = self
            .blocks
            .get(self.next_block)
            .is_none_or(|next| next.resolution != block.resolution);
        let mut done: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                last_block_of_resolution
                    || (p.last_tile == block.tile
                        && (p.end_row.is_some_and(|row| row <= end) || block.last_of_tile))
                    || p.last_tile < block.tile
            })
            .map(|(cell, _)| *cell)
            .collect();
        // Deterministic output order within a band.
        done.sort_unstable();
        for cell in done {
            if let Some(mut pending) = self.pending.remove(&cell) {
                let keep = pending.acc.has_samples() || (self.emit_nodata && pending.touched);
                let valid = self.coverage_filter.as_mut().is_none_or(|filter| {
                    filter.cells.remove(&cell).is_some_and(|coverage| {
                        pending.acc.retain_covered(
                            &coverage.valid_area,
                            filter.minimum * coverage.covered_area,
                        )
                    })
                });
                if keep && valid && pending.acc.passes(&self.filters) {
                    self.output.push_back((block.resolution, cell, pending.acc));
                }
            }
        }
    }
}

/// Intersect serial windows with this worker's owned chunks. Group adjacent columns
/// so each row band is decoded once for the common strip and rectangular partitions.
fn partition_blocks(
    mosaic: &MosaicReader,
    partition: &crate::aggregator::multi_horizon::ScanPartition,
    blocks: Vec<Block>,
) -> Vec<Block> {
    use std::collections::BTreeMap;
    let mut owned = vec![BTreeMap::<u32, Vec<u32>>::new(); mosaic.tiles.len()];
    for chunk in &mosaic.chunk_refs {
        if partition.contains_chunk(mosaic, chunk) {
            let across = mosaic.tiles[chunk.tile_idx]
                .reader
                .chunk_layout
                .chunks_across;
            owned[chunk.tile_idx]
                .entry(chunk.chunk_idx / across)
                .or_default()
                .push(chunk.chunk_idx % across);
        }
    }
    for tile in &mut owned {
        for cols in tile.values_mut() {
            cols.sort_unstable();
        }
    }
    let mut out = Vec::new();
    for block in blocks {
        let tile = &mosaic.tiles[block.tile];
        let layout = tile.reader.chunk_layout;
        for (&row, cols) in owned[block.tile].range(
            block.rows.start / layout.chunk_height..block.rows.end.div_ceil(layout.chunk_height),
        ) {
            let rows = block.rows.start.max(row * layout.chunk_height)
                ..block
                    .rows
                    .end
                    .min((row + 1).saturating_mul(layout.chunk_height));
            for run in cols.chunk_by(|a, b| *b == *a + 1) {
                let cols = run[0] * layout.chunk_width
                    ..tile
                        .reader
                        .metadata
                        .width
                        .min((run[run.len() - 1] + 1).saturating_mul(layout.chunk_width));
                out.push(Block {
                    bbox: None,
                    rows: rows.clone(),
                    cols,
                    last_of_tile: false,
                    ..block
                });
            }
        }
    }
    out
}

/// Cut one tile into blocks for `mode` at `resolution`.
#[allow(clippy::too_many_arguments)]
fn plan_tile(
    grid: GridKind,
    mode: TransferMode,
    resolution: u8,
    tile_idx: usize,
    tile: &TileDescriptor,
    band_count: usize,
    allow_chunk_row_window: bool,
    blocks: &mut Vec<Block>,
) {
    let (width, height) = (tile.reader.metadata.width, tile.reader.metadata.height);
    if width == 0 || height == 0 {
        return;
    }
    // Cells per pixel, to size blocks by the cells they will list.
    let cells_per_pixel = crate::resolution::pixel_ground_area_m2(tile)
        .map(|area| area / grid.area_m2(resolution))
        .unwrap_or(1.0)
        .max(1e-9);
    let block_pixels = (TARGET_BLOCK_CELLS / cells_per_pixel).max(1.0);
    let margin = match mode {
        TransferMode::Interpolate(method) => 2 * method.margin(),
        _ => 0,
    };
    let coarse = grid.spec().max_cell_diameter_km(resolution) > COARSE_CELL_DIAMETER_KM;
    let base_window_rows = ((MAX_WINDOW_VALUES / band_count / width as usize) as u32)
        .saturating_sub(margin)
        .max(1);
    // Whole chunk rows where the budgets allow, so chunks are not decoded twice.
    let chunk_height = tile.reader.chunk_layout.chunk_height.max(1);
    let window_rows = if allow_chunk_row_window
        && !coarse
        && super::window::supports_native_tiled_window(tile, band_count)
    {
        base_window_rows.max(chunk_height.min(MAX_NATIVE_CHUNK_ROW_WINDOW))
    } else {
        base_window_rows
    };
    let mut rows = ((block_pixels / f64::from(width)).floor() as u32).clamp(1, window_rows);
    if rows > chunk_height {
        rows -= rows % chunk_height;
    }
    let overlay = matches!(mode, TransferMode::Overlay { .. });
    // An interpolation block may also be narrower than the raster; an overlay band is not, as
    // every pixel's weights are totalled within its band.
    let cols = if overlay || rows > 1 {
        width
    } else {
        (block_pixels.floor() as u32).clamp(1, width)
    };
    let first = blocks.len();
    let mut row = 0;
    while row < height {
        let row_end = if overlay && rows < chunk_height {
            let next_chunk_row = ((row / chunk_height) + 1).saturating_mul(chunk_height);
            (row + rows).min(next_chunk_row).min(height)
        } else {
            (row + rows).min(height)
        };
        let mut col = 0;
        while col < width {
            let col_end = (col + cols).min(width);
            let block_rows = row..row_end;
            let block_cols = col..col_end;
            blocks.push(Block {
                resolution,
                tile: tile_idx,
                rows: block_rows,
                cols: block_cols,
                bbox: None,
                last_of_tile: false,
            });
            col = col_end;
        }
        row = row_end;
    }
    if let Some(last) = blocks[first..].last_mut() {
        last.last_of_tile = true;
    }
}

/// Simulate chunk cache retention across `run_blocks` and return `(misses, blocks_covered)`
/// in exact `RowWindow::load_bands` iteration order, stopping early if a later block would
/// re-miss an already scheduled chunk.
fn plan_run_chunk_misses(
    tile: &TileDescriptor,
    run_blocks: &[Block],
    bands: &[usize],
    mode: TransferMode,
) -> (Vec<u32>, usize) {
    let meta = &tile.reader.metadata;
    let (width, height) = (meta.width, meta.height);
    let layout = tile.reader.chunk_layout;
    let spp = usize::from(meta.samples_per_pixel.max(1));
    let alpha_index = meta.alpha_sample.map(usize::from).filter(|&a| a < spp);
    let band_indices: Vec<_> = bands
        .iter()
        .map(|&band| band.saturating_sub(1).min(spp - 1))
        .collect();
    let margin = match mode {
        TransferMode::Interpolate(method) => method.margin(),
        _ => 0,
    };

    let mut misses = Vec::new();
    let mut cached: FxHashSet<u32> = FxHashSet::default();
    let mut seen_misses: FxHashSet<u32> = FxHashSet::default();
    let mut blocks_covered = 0;

    for block in run_blocks {
        if block.bbox.is_none() || matches!(mode, TransferMode::AssignCenters) {
            blocks_covered += 1;
            continue;
        }
        let (raw_rows, raw_cols) = match mode {
            TransferMode::Interpolate(_) => (
                block.rows.start.saturating_sub(margin)..block.rows.end.saturating_add(margin),
                0..width,
            ),
            TransferMode::Overlay { .. } => (block.rows.clone(), block.cols.clone()),
            TransferMode::AssignCenters | TransferMode::Auto => unreachable!(),
        };
        let rows = raw_rows.start.min(height)..raw_rows.end.min(height);
        let cols = raw_cols.start.min(width)..raw_cols.end.min(width);
        if rows.is_empty() || cols.is_empty() || width == 0 {
            blocks_covered += 1;
            continue;
        }

        let chunk_rows = rows.start / layout.chunk_height..=(rows.end - 1) / layout.chunk_height;
        let chunk_cols = cols.start / layout.chunk_width..cols.end.div_ceil(layout.chunk_width);

        let mut block_misses = Vec::new();
        let mut collision = false;
        for chunk_row in chunk_rows.clone() {
            for chunk_col in chunk_cols.clone() {
                let index = chunk_row * layout.chunks_across + chunk_col;
                let bounds = layout.get_chunk_bounds(index, width, height);
                if bounds.width == 0 || bounds.height == 0 {
                    continue;
                }
                if chunk_is_absent(&tile.reader, index, &band_indices, alpha_index) {
                    continue;
                }
                if cached.contains(&index) {
                    continue;
                }
                if seen_misses.contains(&index) {
                    collision = true;
                    break;
                }
                block_misses.push(index);
            }
            if collision {
                break;
            }
        }
        if collision && blocks_covered > 0 {
            break;
        }
        cached.retain(|&index| {
            let chunk_row = index / layout.chunks_across;
            let chunk_col = index % layout.chunks_across;
            chunk_rows.contains(&chunk_row) && chunk_cols.contains(&chunk_col)
        });
        for &index in &block_misses {
            cached.insert(index);
            seen_misses.insert(index);
        }
        misses.extend_from_slice(&block_misses);
        blocks_covered += 1;
    }

    (misses, blocks_covered.max(1))
}

/// The WGS84 box `[west, south, east, north]` of a block of pixels, padded so that it
/// holds the centre of every cell whose centre is on the block (and, for an overlay,
/// every cell that touches the block).
fn block_bbox(
    grid: GridKind,
    resolution: u8,
    tile: &TileDescriptor,
    rows: &Range<u32>,
    cols: &Range<u32>,
    overlay: bool,
) -> Option<[f64; 4]> {
    const STEPS: u32 = 8;
    let gt = &tile.reader.metadata.geotransform;
    let (c0, c1) = (f64::from(cols.start), f64::from(cols.end));
    let (r0, r1) = (f64::from(rows.start), f64::from(rows.end));
    let mut points = Vec::with_capacity(4 * STEPS as usize + 1);
    let mut locate = |col: f64, row: f64| {
        let (x, y) = gt.pixel_to_coord(col, row);
        if let Ok((lon, lat)) = tile.crs_transformer.transform_point(x, y) {
            if lon.is_finite() && (-90.0..=90.0).contains(&lat) {
                points.push((lon, lat));
            }
        }
    };
    locate((c0 + c1) / 2.0, (r0 + r1) / 2.0);
    for step in 0..STEPS {
        let t = f64::from(step) / f64::from(STEPS);
        locate(c0 + t * (c1 - c0), r0);
        locate(c1, r0 + t * (r1 - r0));
        locate(c1 - t * (c1 - c0), r1);
        locate(c0, r1 - t * (r1 - r0));
    }
    let &(centre_lon, _) = points.first()?;
    let (mut south, mut north) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut west, mut east) = (0.0f64, 0.0f64);
    // These transforms return continuous, unwrapped longitudes. Taking the
    // short way around would alias both edges of a full turn to the same meridian.
    let affine_longitude = matches!(
        tile.crs_transformer,
        CrsTransformer::Wgs84Identity | CrsTransformer::WebMercatorFast
    );
    for &(lon, lat) in &points {
        south = south.min(lat);
        north = north.max(lat);
        let offset = lon - centre_lon;
        let offset = if affine_longitude {
            offset
        } else {
            (offset + 540.0).rem_euclid(360.0) - 180.0
        };
        west = west.min(offset);
        east = east.max(offset);
    }
    // A pole inside the block: every longitude, up to the pole.
    let mut full_lon = east - west >= 359.0;
    for pole in [90.0, -90.0] {
        let inside = to_pixel_raw(tile, 0.0, pole).is_some_and(|(col, row)| {
            col >= c0 - 0.5 && col <= c1 + 0.5 && row >= r0 - 0.5 && row <= r1 + 0.5
        });
        if inside {
            full_lon = true;
            if pole > 0.0 {
                north = 90.0;
            } else {
                south = -90.0;
            }
        }
    }
    // Edges curve between the sampled points, and an overlay needs the cells that only
    // touch the block.
    let reach = if overlay {
        grid.spec().max_cell_radius_deg(resolution)
    } else {
        0.0
    };
    let pad_lat = 0.05 * (north - south) + 1e-7 + reach;
    let south = (south - pad_lat).max(-90.0);
    let north = (north + pad_lat).min(90.0);
    let cos = south.abs().max(north.abs()).min(89.9).to_radians().cos();
    let pad_lon = 0.05 * (east - west) + 1e-7 + 2.0 * reach / cos;
    if full_lon || (east - west) + 2.0 * pad_lon >= 360.0 || north >= 90.0 || south <= -90.0 {
        return Some([-180.0, south, 180.0, north]);
    }
    Some([
        wrap_lon_180(centre_lon + west - pad_lon),
        south,
        wrap_lon_180(centre_lon + east + pad_lon),
        north,
    ])
}

/// Widest slice handed to A5's spherical great-circle polygon fill, in degrees of
/// longitude: shorter slices limit inward chord bowing along parallels.
const MAX_A5_FILL_LON_SPAN: f64 = 10.0;
/// Conservative upper bound (degrees / deg²) on the latitude sagitta of a great-circle
/// chord spanning `d_deg` degrees of longitude (`pi / 1440`, ~2x the small-angle
/// maximum `pi / 2880`).
const A5_CHORD_SAGITTA_COEFF: f64 = std::f64::consts::PI / 1440.0;

/// Maximum longitude slice width (degrees) for `PolygonFill::A5Native`, kept short
/// enough that both chord bowing and `check_a5_fill_budget`'s circular spherical cap
/// stay bounded on wide, thin raster strips.
fn a5_fill_lon_span(grid: GridKind, south: f64, north: f64, resolution: u8) -> f64 {
    let max_cap_diam_m = a5_max_cap_diameter_m(grid, resolution);
    let min_abs_lat = if south <= 0.0 && north >= 0.0 {
        0.0
    } else {
        south.abs().min(north.abs())
    };
    let cos_lat = min_abs_lat.min(89.9).to_radians().cos().max(0.05);
    let cap_lon_span = (max_cap_diam_m / (EARTH_RADIUS_M * cos_lat)).to_degrees();
    // No floor: a floor wider than the budget lets the fill guard reject strips
    // that would fit once sliced finely enough (metre-scale pixels at fine resolutions).
    cap_lon_span.clamp(MIN_A5_FILL_SPAN, MAX_A5_FILL_LON_SPAN)
}

/// Diameter (metres) of the largest spherical cap whose cell estimate stays inside the
/// A5 fill budget, with a safety factor.
fn a5_max_cap_diameter_m(grid: GridKind, resolution: u8) -> f64 {
    let budget_cells = (H3_POLYGON_TO_CELLS_MAX_CELLS / 4) as f64;
    let max_cap_area_m2 = budget_cells * grid.area_m2(resolution);
    2.0 * (max_cap_area_m2 / std::f64::consts::PI).sqrt() * 0.7
}

/// Tallest latitude slice (degrees) that keeps the enclosing cap inside the fill budget.
fn a5_fill_lat_span(grid: GridKind, resolution: u8) -> f64 {
    const METRES_PER_DEG_LAT: f64 = 110_000.0;
    (a5_max_cap_diameter_m(grid, resolution) / METRES_PER_DEG_LAT).clamp(MIN_A5_FILL_SPAN, 180.0)
}

/// Narrowest slice (degrees) the A5 fill planner will cut.
const MIN_A5_FILL_SPAN: f64 = 1e-7;
/// Most slices one box may be cut into before asking for a coarser resolution.
const MAX_A5_FILL_SLICES: u64 = 200_000;

const EARTH_RADIUS_M: f64 = 6_371_007.2;

/// The cells of `grid` at `resolution` selected by `mode` against the lon/lat box
/// `[west, south, east, north]` (`west > east` across the antimeridian), sorted and
/// without repeats.
fn cells_in_bbox(
    grid: GridKind,
    bbox: [f64; 4],
    resolution: u8,
    mode: ContainmentMode,
) -> Result<Vec<u64>> {
    let [west, south, east, north] = bbox;
    let spans: Vec<(f64, f64)> = if west <= east {
        vec![(west, east)]
    } else {
        vec![(west, 180.0), (-180.0, east)]
    };
    let a5_native = grid.spec().polygon_fill == PolygonFill::A5Native;
    let max_lon_span = if a5_native {
        a5_fill_lon_span(grid, south, north, resolution)
    } else {
        MAX_FILL_LON_SPAN
    };
    let max_lat_span = if a5_native {
        a5_fill_lat_span(grid, resolution)
    } else {
        f64::INFINITY
    };
    let lat_slices = ((north - south) / max_lat_span).ceil().max(1.0) as u64;
    if a5_native {
        let lon_total: f64 = spans.iter().map(|(w, e)| e - w).sum();
        let total = ((lon_total / max_lon_span).ceil().max(1.0) as u64).saturating_mul(lat_slices);
        if total > MAX_A5_FILL_SLICES {
            return Err(DggsStreamError::InvalidParameter(format!(
                "transfer: a block of the raster needs {total} A5 fill slices at \
                 resolution {resolution} (limit {MAX_A5_FILL_SLICES}); use a coarser resolution"
            )));
        }
    }
    let max_seg_deg = if a5_native {
        let tol = (0.05 * (north - south))
            .min(0.05 * grid.spec().max_cell_radius_deg(resolution))
            .max(1e-5);
        (tol / A5_CHORD_SAGITTA_COEFF).sqrt()
    } else {
        0.0
    };
    let mut cells = Vec::new();
    let mut part = Vec::new();
    for (span_west, span_east) in spans {
        let slices = ((span_east - span_west) / max_lon_span).ceil().max(1.0) as u32;
        for (slice, lat_slice) in (0..slices).flat_map(|i| (0..lat_slices).map(move |j| (i, j))) {
            let w = span_west + (span_east - span_west) * f64::from(slice) / f64::from(slices);
            let e = span_west + (span_east - span_west) * f64::from(slice + 1) / f64::from(slices);
            let (south, north) = (
                south + (north - south) * lat_slice as f64 / lat_slices as f64,
                south + (north - south) * (lat_slice + 1) as f64 / lat_slices as f64,
            );
            if e - w < 1e-12 || north - south < 1e-12 {
                continue;
            }
            // A cell that holds the whole rectangle has neither its centre nor an edge
            // crossing in it, so the fill can miss it; the rectangle's corners are in it.
            if mode == ContainmentMode::Covers {
                for (lon, lat) in [(w, south), (e, south), (e, north), (w, north)] {
                    cells.extend(grid.cell_at(lat, lon, resolution));
                }
            }
            // A5 connects polygon vertices along great-circle arcs rather than parallels.
            // Densify and slightly pad north/south by the residual chord sagitta so wide,
            // thin strips do not bow inside the target parallel band.
            let (wkb_south, wkb_north, lon_segments) = if a5_native {
                let lon_segments = ((e - w) / max_seg_deg).ceil().clamp(1.0, 256.0) as usize;
                let actual_seg_deg = (e - w) / (lon_segments as f64);
                let sagitta = A5_CHORD_SAGITTA_COEFF * actual_seg_deg * actual_seg_deg;
                (
                    (south - sagitta).max(-90.0),
                    (north + sagitta).min(90.0),
                    lon_segments,
                )
            } else {
                (south, north, 1)
            };
            let wkb = densified_rectangle_wkb(w, wkb_south, e, wkb_north, lon_segments);
            match grid_polygon_to_cells_wkb_into(grid, &wkb, i64::from(resolution), mode, &mut part)
            {
                Ok(()) => cells.extend_from_slice(&part),
                Err(PolygonToCellsError::TooManyCells { estimate, limit }) => {
                    return Err(DggsStreamError::InvalidParameter(format!(
                        "transfer: a block of the raster holds about {estimate} cells at \
                         resolution {resolution} (limit {limit}); use a coarser resolution"
                    )))
                }
                Err(e) => {
                    return Err(DggsStreamError::StreamFailed(format!(
                        "transfer: listing cells of [{w}, {south}, {e}, {north}] failed: {e}"
                    )))
                }
            }
        }
    }
    cells.sort_unstable();
    cells.dedup();
    Ok(cells)
}

/// WKB of the lon/lat rectangle `[west, south, east, north]`, counter-clockwise, with
/// `lon_segments` equal steps along the south and north edges.
fn densified_rectangle_wkb(
    west: f64,
    south: f64,
    east: f64,
    north: f64,
    lon_segments: usize,
) -> Vec<u8> {
    let segs = lon_segments.max(1);
    let point_count = 2 * segs + 3;
    let mut wkb = Vec::with_capacity(13 + 16 * point_count);
    wkb.push(1u8);
    wkb.extend(3u32.to_le_bytes());
    wkb.extend(1u32.to_le_bytes());
    wkb.extend((point_count as u32).to_le_bytes());
    let span = east - west;
    let inv_segs = 1.0 / (segs as f64);
    for i in 0..=segs {
        let lon = if i == segs {
            east
        } else {
            west + span * (i as f64) * inv_segs
        };
        wkb.extend(lon.to_le_bytes());
        wkb.extend(south.to_le_bytes());
    }
    for i in 0..=segs {
        let lon = if i == segs {
            west
        } else {
            east - span * (i as f64) * inv_segs
        };
        wkb.extend(lon.to_le_bytes());
        wkb.extend(north.to_le_bytes());
    }
    wkb.extend(west.to_le_bytes());
    wkb.extend(south.to_le_bytes());
    wkb
}

/// Pixel position `(col, row)` of a WGS84 point in `tile`, wherever it falls.
fn to_pixel_raw(tile: &TileDescriptor, lon: f64, lat: f64) -> Option<(f64, f64)> {
    let (x, y) = tile.crs_transformer.wgs84_to_source(lon, lat).ok()?;
    let (col, row) = tile.reader.metadata.geotransform.coord_to_pixel(x, y)?;
    (col.is_finite() && row.is_finite()).then_some((col, row))
}

/// Pixel position of a WGS84 point in `tile`. A lon/lat raster may store longitudes a
/// whole turn away (0 to 360), so those are tried when the point misses the columns.
fn to_pixel(tile: &TileDescriptor, lon: f64, lat: f64) -> Option<(f64, f64)> {
    let width = f64::from(tile.reader.metadata.width);
    let first = to_pixel_raw(tile, lon, lat)?;
    if (0.0..width).contains(&first.0)
        || !matches!(tile.crs_transformer, CrsTransformer::Wgs84Identity)
    {
        return Some(first);
    }
    for turn in [360.0, -360.0] {
        if let Some(shifted) = to_pixel_raw(tile, lon + turn, lat) {
            if (0.0..width).contains(&shifted.0) {
                return Some(shifted);
            }
        }
    }
    Some(first)
}

/// A contiguous horizontal run of pixels within one row sharing the same overlap area.
#[derive(Clone, Copy)]
struct OverlapRun {
    pixel: u32,
    len: u32,
    area: f64,
}

impl PixelRun for OverlapRun {
    fn start(&self) -> u64 {
        u64::from(self.pixel)
    }
    fn end(&self) -> u64 {
        u64::from(self.pixel) + u64::from(self.len)
    }
    fn with_range(self, start: u64, end: u64) -> Self {
        Self {
            pixel: start as u32,
            len: (end - start) as u32,
            area: self.area,
        }
    }
    fn add_payload(self, other: Self) -> Self {
        Self {
            area: self.area + other.area,
            ..self
        }
    }
}

struct CellCoverage {
    valid_area: Vec<f64>,
    covered_area: f64,
}

#[derive(Default)]
enum CellOutcome<A> {
    NoDataOnly {
        end_row: i64,
    },
    #[default]
    Failed,
    Pole,
    Accumulated {
        end_row: i64,
        acc: A,
        touched: bool,
        coverage: Option<CellCoverage>,
    },
}

#[derive(Default)]
struct CellWorkerScratch {
    clip: ClipScratch,
    seam_runs: Vec<OverlapRun>,
}

#[derive(Default)]
enum InterpOutcome<A> {
    #[default]
    Skip,
    Failed,
    Emit(A),
}

struct InterpBlockCtx<'a> {
    grid: GridKind,
    mosaic: &'a MosaicReader,
    tile: &'a TileDescriptor,
    tile_idx: usize,
    window: &'a RowWindow,
    method: Interpolation,
    rows: Range<u32>,
    cols: Range<u32>,
    track_quantiles: bool,
    emit_nodata: bool,
    bbox: Option<[f64; 4]>,
    filters: Filters,
    histogram_bins: Option<&'a crate::histogram::HistogramBins>,
    remapper: Option<&'a CategoryRemapper>,
}

fn interpolate_cell<A: TransferAccumulator>(
    cell: u64,
    ctx: &InterpBlockCtx<'_>,
) -> InterpOutcome<A> {
    let Some((lat, lng)) = ctx.grid.center_lat_lng(cell) else {
        return InterpOutcome::Skip;
    };
    if !is_point_in_bbox(lng, lat, ctx.bbox) {
        return InterpOutcome::Skip;
    }
    let Some((col, row)) = to_pixel(ctx.tile, lng, lat) else {
        return InterpOutcome::Failed;
    };
    // A cell belongs to the block holding its centre, so it is emitted once.
    let in_block = col >= f64::from(ctx.cols.start)
        && col < f64::from(ctx.cols.end)
        && row >= f64::from(ctx.rows.start)
        && row < f64::from(ctx.rows.end);
    if !in_block || !ctx.mosaic.is_point_owned_by(ctx.tile_idx, lng, lat) {
        return InterpOutcome::Skip;
    }
    let mut acc = A::empty(ctx.track_quantiles);
    if let Some(value) = interpolate(ctx.window, ctx.method, col, row)
        .and_then(|v| TransferStream::<A>::mapped(ctx.histogram_bins, ctx.remapper, v))
    {
        acc.add(value, 1.0);
    }
    if (acc.has_samples() || ctx.emit_nodata) && acc.passes(&ctx.filters) {
        InterpOutcome::Emit(acc)
    } else {
        InterpOutcome::Skip
    }
}

struct DirectState<A> {
    acc: A,
    touched: bool,
    coverage: Option<CellCoverage>,
}

struct OverlayBlockCtx<'a> {
    grid: GridKind,
    mosaic: &'a MosaicReader,
    tile: &'a TileDescriptor,
    tile_idx: usize,
    several_tiles: bool,
    window: &'a RowWindow,
    longitude_bounds: Option<(f64, f64)>,
    rows: (i64, i64),
    cols: Range<u32>,
    width: i64,
    band_count: usize,
    weighted: bool,
    track_quantiles: bool,
    need_coverage: bool,
    emit_nodata: bool,
    bbox: Option<[f64; 4]>,
    histogram_bins: Option<&'a crate::histogram::HistogramBins>,
    remapper: Option<&'a CategoryRemapper>,
    mean_filtered: bool,
}

impl OverlayBlockCtx<'_> {
    #[inline]
    fn is_pixel_selected(&self, col: i64, row: i64) -> bool {
        if self.bbox.is_none() && !self.several_tiles {
            return true;
        }
        let gt = &self.tile.reader.metadata.geotransform;
        let (x, y) = gt.pixel_to_coord(col as f64 + 0.5, row as f64 + 0.5);
        self.tile
            .crs_transformer
            .transform_point(x, y)
            .is_ok_and(|(lon, lat)| {
                is_point_in_bbox(wrap_lon_180(lon), lat, self.bbox)
                    && (!self.several_tiles
                        || self.mosaic.is_point_owned_by(self.tile_idx, lon, lat))
            })
    }
}

fn accumulate_span_direct<A: TransferAccumulator>(
    ctx: &OverlayBlockCtx<'_>,
    row: i64,
    c0: i64,
    c1: i64,
    area: f64,
    weight: f64,
    state: &mut DirectState<A>,
) {
    let clipped_c0 = (c0.max(0) as usize).max(ctx.cols.start as usize);
    let clipped_c1 = (c1.max(0) as usize).min(ctx.cols.end as usize);
    if clipped_c0 >= clipped_c1 {
        return;
    }
    let row_off = (row - ctx.rows.0) as usize;
    let unit = area == 1.0
        && weight == 1.0
        && !ctx.mean_filtered
        && ctx.histogram_bins.is_none()
        && (!A::CATEGORICAL || ctx.remapper.is_none());
    let filter_pixels = ctx.bbox.is_some() || ctx.several_tiles;
    let mut col = clipped_c0;
    while col < clipped_c1 {
        if filter_pixels && !ctx.is_pixel_selected(col as i64, row) {
            col += 1;
            continue;
        }
        // Extend over following selected pixels when they can be summed as a slice.
        let mut end = col + 1;
        if unit {
            while end < clipped_c1 && (!filter_pixels || ctx.is_pixel_selected(end as i64, row)) {
                end += 1;
            }
        }
        state.touched = true;
        if end - col >= MIN_SIMD_RUN {
            let len = (end - col) as f64;
            state.acc.cover(len);
            if let Some(cov) = state.coverage.as_mut() {
                cov.covered_area += len;
            }
            for slot in 0..ctx.band_count {
                let valid =
                    ctx.window
                        .accumulate_unit_span(slot, row_off, col, end, &mut state.acc);
                if let Some(cov) = state.coverage.as_mut() {
                    cov.valid_area[slot] += valid as f64;
                }
            }
        } else {
            for col in col..end {
                state.acc.cover(area);
                if let Some(cov) = state.coverage.as_mut() {
                    cov.covered_area += area;
                }
                for slot in 0..ctx.band_count {
                    if let Some(sample) = ctx.window.band_value_at(slot, row_off, col) {
                        if let Some(cov) = state.coverage.as_mut() {
                            cov.valid_area[slot] += area;
                        }
                        if let Some(value) =
                            TransferStream::<A>::mapped(ctx.histogram_bins, ctx.remapper, sample)
                        {
                            state.acc.add_band(slot, value, weight);
                        }
                    }
                }
            }
        }
        col = end;
    }
}

fn clip_and_accumulate_cell<A: TransferAccumulator>(
    cell: u64,
    ctx: &OverlayBlockCtx<'_>,
    scratch: &mut CellWorkerScratch,
) -> CellOutcome<A> {
    let mut rings = [[(0.0, 0.0); 10]; 3];
    let (copies, len) =
        match project_cell_polygons(ctx.grid, cell, ctx.tile, ctx.longitude_bounds, &mut rings) {
            ProjectedCell::Polygons { count, len } => (count, len),
            ProjectedCell::Pole => return CellOutcome::Pole,
            ProjectedCell::Failed => return CellOutcome::Failed,
        };
    if copies == 0 {
        return CellOutcome::NoDataOnly { end_row: i64::MIN };
    }
    let mut last_row = f64::NEG_INFINITY;
    let mut any_data = false;
    let check_data = !ctx.emit_nodata && !ctx.need_coverage && !A::NEEDS_COVER;
    for poly in &rings[..copies] {
        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        for &(x, y) in &poly[..len] {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
            last_row = last_row.max(y);
        }
        if !any_data {
            let bx0 = min_x.max(f64::from(ctx.cols.start));
            let bx1 = max_x.min(f64::from(ctx.cols.end));
            let by0 = min_y.max(ctx.rows.0 as f64);
            let by1 = max_y.min(ctx.rows.1 as f64);
            if bx0 < bx1
                && by0 < by1
                && (!check_data || ctx.window.bbox_has_any_data(bx0, bx1, by0, by1))
            {
                any_data = true;
            }
        }
    }
    let end_row = last_row.ceil() as i64;
    if !any_data {
        return CellOutcome::NoDataOnly { end_row };
    }

    let mut state = DirectState {
        acc: A::empty_bands(ctx.track_quantiles, ctx.band_count),
        touched: false,
        coverage: ctx.need_coverage.then(|| CellCoverage {
            valid_area: vec![0.0; ctx.band_count],
            covered_area: 0.0,
        }),
    };

    if copies == 1 {
        let completed = visit_overlap_spans_into(
            &rings[0][..len],
            ctx.rows,
            ctx.width,
            &mut scratch.clip,
            |r, c0, c1, area| {
                if area > 1e-12 {
                    let weight = if ctx.weighted { area } else { 1.0 };
                    accumulate_span_direct(ctx, r, c0, c1, area, weight, &mut state);
                }
                true
            },
        );
        if !completed {
            return CellOutcome::Failed;
        }
    } else {
        scratch.seam_runs.clear();
        for poly in &rings[..copies] {
            let completed = visit_overlap_spans_into(
                &poly[..len],
                ctx.rows,
                ctx.width,
                &mut scratch.clip,
                |r, c0, c1, area| {
                    if area > 1e-12 {
                        let pixel = (r - ctx.rows.0) as usize * ctx.width as usize + c0 as usize;
                        scratch.seam_runs.push(OverlapRun {
                            pixel: pixel as u32,
                            len: (c1 - c0) as u32,
                            area,
                        });
                    }
                    true
                },
            );
            if !completed {
                return CellOutcome::Failed;
            }
        }
        if !scratch.seam_runs.is_empty() {
            merge_overlapping_runs(&mut scratch.seam_runs, 0);
            for &run in &scratch.seam_runs {
                let pixel = run.pixel as usize;
                let row = ctx.rows.0 + (pixel as i64 / ctx.width);
                let c0 = pixel as i64 % ctx.width;
                let c1 = c0 + i64::from(run.len);
                let weight = if ctx.weighted { run.area } else { 1.0 };
                accumulate_span_direct(ctx, row, c0, c1, run.area, weight, &mut state);
            }
        }
    }

    if !state.touched || (check_data && !state.acc.has_samples()) {
        return CellOutcome::NoDataOnly { end_row };
    }
    CellOutcome::Accumulated {
        end_row,
        acc: state.acc,
        touched: state.touched,
        coverage: state.coverage,
    }
}

/// Geometry shared by workers processing different cells in one row window.
struct OverlayGeometry<'a> {
    grid: GridKind,
    tile: &'a TileDescriptor,
    longitude_bounds: Option<(f64, f64)>,
    rows: (i64, i64),
    width: i64,
}

impl OverlayGeometry<'_> {
    /// A worker retains at most `limit` overlaps. Larger cells are processed
    /// by the serial reducer with its reusable buffer.
    fn clip(
        &self,
        cell: u64,
        overlaps: &mut Vec<OverlapRun>,
        limit: usize,
        scratch: &mut ClipScratch,
    ) -> Option<i64> {
        let start = overlaps.len();
        // A bounded visit also bounds the clipper's edge events, which are buffered
        // before any run is emitted. A cell with over twice `limit` events nearly
        // always yields more than `limit` runs (each boundary pixel adds about one
        // event and two runs). The rare exception costs only a serial retry.
        scratch.set_event_limit((limit != usize::MAX).then(|| limit.saturating_mul(2)));
        let mut pieces = 0;
        let mut last_row = f64::NEG_INFINITY;
        let completed = visit_cell_polygons(
            self.grid,
            cell,
            self.tile,
            self.longitude_bounds,
            |polygon| {
                pieces += 1;
                for &(_, row) in polygon {
                    last_row = last_row.max(row);
                }
                visit_overlap_spans_into(
                    polygon,
                    self.rows,
                    self.width,
                    scratch,
                    |r, c0, c1, area| {
                        if area > 1e-12 {
                            if overlaps.len() == limit {
                                return false;
                            }
                            let pixel =
                                (r - self.rows.0) as usize * self.width as usize + c0 as usize;
                            overlaps.push(OverlapRun {
                                pixel: pixel as u32,
                                len: (c1 - c0) as u32,
                                area,
                            });
                        }
                        true
                    },
                )
            },
        );
        if !completed {
            return None;
        }
        if pieces > 1 {
            // Two seam pieces can touch the same source pixel (e.g. a one-column
            // global raster). Combine its weight before counting it or updating
            // variance. Merging splits runs only where they overlap.
            merge_overlapping_runs(overlaps, start);
            if overlaps.len() > limit {
                return None;
            }
        }
        Some(last_row.ceil() as i64)
    }
}

/// The continuous longitude extent of a tile whose longitude is affine in pixel space.
fn periodic_longitude_bounds(tile: &TileDescriptor) -> Option<(f64, f64)> {
    let meta = &tile.reader.metadata;
    let gt = &meta.geotransform;
    let dx = gt.a * f64::from(meta.width);
    let dy = gt.b * f64::from(meta.height);
    let west = gt.c0 + dx.min(0.0) + dy.min(0.0);
    let east = gt.c0 + dx.max(0.0) + dy.max(0.0);
    match tile.crs_transformer {
        CrsTransformer::Wgs84Identity => Some((west, east)),
        CrsTransformer::WebMercatorFast => {
            use crate::crs::transformer::web_mercator_lon;
            Some((web_mercator_lon(west), web_mercator_lon(east)))
        }
        _ => None,
    }
}

pub(super) enum ProjectedCell {
    Polygons { count: usize, len: usize },
    Pole,
    Failed,
}

pub(super) fn project_cell_polygons(
    grid: GridKind,
    cell: u64,
    tile: &TileDescriptor,
    longitude_bounds: Option<(f64, f64)>,
    rings: &mut [[(f64, f64); 10]; 3],
) -> ProjectedCell {
    let mut ring = [(0.0, 0.0); 10];
    let n = grid.boundary_lng_lat(cell, &mut ring);
    if n < 3 || (longitude_bounds.is_some() && grid.interior_pole(cell).is_some()) {
        return if grid.interior_pole(cell).is_some() {
            ProjectedCell::Pole
        } else {
            ProjectedCell::Failed
        };
    }
    let ring = &mut ring[..n];
    let mut shifts = [0.0; 3];
    let mut copies = 1;
    if let Some((west, east)) = longitude_bounds {
        let first = ring[0].0;
        let mut left = first;
        let mut right = first;
        for (lon, _) in ring.iter_mut() {
            *lon = first + ((*lon - first + 540.0).rem_euclid(360.0) - 180.0);
            left = left.min(*lon);
            right = right.max(*lon);
        }
        // Anchor at the tile's longitude convention. On a single globe the
        // ring and its immediate neighbours cover both possible seam pieces.
        let centre_shift = (((west + east) * 0.5 - first) / 360.0).round() * 360.0;
        copies = 0;
        for shift in [centre_shift - 360.0, centre_shift, centre_shift + 360.0] {
            if right + shift > west && left + shift < east {
                shifts[copies] = shift;
                copies += 1;
            }
        }
    }
    for (copy_idx, &shift) in shifts[..copies].iter().enumerate() {
        for (point, &(lon, lat)) in rings[copy_idx][..n].iter_mut().zip(ring.iter()) {
            let Some(pixel) = to_pixel_raw(tile, lon + shift, lat) else {
                return if grid.interior_pole(cell).is_some() {
                    ProjectedCell::Pole
                } else {
                    ProjectedCell::Failed
                };
            };
            *point = pixel;
        }
    }
    ProjectedCell::Polygons {
        count: copies,
        len: n,
    }
}

/// Visit the cell polygon on each longitude branch touching the raster. A
/// straddling cell uses the same accumulator for the pieces at its two edges.
fn visit_cell_polygons(
    grid: GridKind,
    cell: u64,
    tile: &TileDescriptor,
    longitude_bounds: Option<(f64, f64)>,
    mut visit: impl FnMut(&[(f64, f64)]) -> bool,
) -> bool {
    let mut rings = [[(0.0, 0.0); 10]; 3];
    let ProjectedCell::Polygons { count, len } =
        project_cell_polygons(grid, cell, tile, longitude_bounds, &mut rings)
    else {
        return false;
    };
    for ring in &rings[..count] {
        if !visit(&ring[..len]) {
            return false;
        }
    }
    true
}

/// The highest tile index from `from` on whose footprint may overlap `cell`.
fn last_overlapping_tile(mosaic: &MosaicReader, grid: GridKind, cell: u64, from: usize) -> usize {
    let Some([west, south, east, north]) = grid.wgs84_bbox(cell) else {
        return mosaic.tiles.len() - 1;
    };
    let mut last = from;
    for (idx, tile) in mosaic.tiles.iter().enumerate().skip(from + 1) {
        let [t_west, t_south, t_east, t_north] =
            tile.crs_transformer.rejection_bounds(tile.bounds_wgs84);
        let lat_overlap = south <= t_north && north >= t_south;
        // Boxes that cross the antimeridian are taken to overlap in longitude.
        let lon_overlap = west > east || t_west > t_east || (west <= t_east && east >= t_west);
        if lat_overlap && lon_overlap {
            last = idx;
        }
    }
    last
}

impl TransferStream<super::OverlayPartial> {
    pub(crate) fn partitioned_overlay(
        mosaic: Arc<MosaicReader>,
        config: &MultiResolutionConfig,
    ) -> Result<Self> {
        config.validate()?;
        crate::aggregator::multi_horizon::controller::check_bands_exist(&mosaic, config)?;
        Self::build(mosaic, config, vec![config.band], true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Native samples reduced as a span and then calibrated must agree with
    /// calibrating each sample first, even when the offset cancels the raw
    /// magnitude: the raw sum of these four values cannot represent the +2.
    #[test]
    fn native_span_calibration_keeps_differences_the_offset_exposes() {
        let samples = tiff::decoder::DecodingResult::F64(vec![1e16, 1e16, 1e16, 1e16 + 2.0]);
        let scaling = BandScaling {
            scale: 1.0,
            offset: -1e16,
        };
        let mut native = ContinuousAccumulator::empty(false);
        let valid =
            native.add_band_native_unit_slice(0, BorrowedSamples::from(&samples), None, scaling);
        let mut per_sample = ContinuousAccumulator::empty(false);
        for raw in [1e16, 1e16, 1e16, 1e16 + 2.0] {
            per_sample.update(scaling.apply(raw));
        }
        assert_eq!(valid, 4);
        assert_eq!(per_sample.sum, 2.0, "the per-sample reference is exact");
        assert_eq!(native.count, per_sample.count);
        assert_eq!(native.sum, per_sample.sum);
        assert_eq!((native.min, native.max), (per_sample.min, per_sample.max));
        assert!((native.m2 - per_sample.m2).abs() <= 1e-12 * per_sample.m2.max(1.0));
    }
    use crate::raster::mosaic::OverlapRule;
    use crate::raster::GeoTiffStreamReader;
    use std::collections::BTreeMap;

    #[test]
    fn projected_final_tile_finishes_cells_at_their_last_row() {
        use tiff::encoder::{colortype, TiffEncoder};
        use tiff::tags::Tag;
        let make_tile = |start: u32| {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
            let mut image = encoder.new_image::<colortype::Gray32Float>(16, 64).unwrap();
            let tags = image.encoder();
            tags.write_tag(
                Tag::GeoKeyDirectoryTag,
                &[1u16, 1, 0, 1, 3072, 0, 1, 2154][..],
            )
            .unwrap();
            tags.write_tag(Tag::ModelPixelScaleTag, &[100.0f64, 100.0, 0.0][..])
                .unwrap();
            tags.write_tag(
                Tag::ModelTiepointTag,
                &[
                    0.0f64,
                    0.0,
                    0.0,
                    700_000.0 + 100.0 * f64::from(start),
                    6_600_000.0,
                    0.0,
                ][..],
            )
            .unwrap();
            let pixels: Vec<_> = (0..64)
                .flat_map(|r| (0..16).map(move |c| (start + c + r) as f32))
                .collect();
            image.write_data(&pixels).unwrap();
            file
        };
        let left = make_tile(0);
        let right = make_tile(16);
        let paths = [left.path().to_owned(), right.path().to_owned()];
        let mosaic = Arc::new(MosaicReader::open(&paths, None, None, OverlapRule::First).unwrap());
        let mut config = MultiResolutionConfig::single(9);
        config.transfer = TransferMode::OVERLAY;
        let scan =
            || TransferStream::<ContinuousAccumulator>::new(mosaic.clone(), &config).unwrap();
        let drain = |mut s: TransferStream<ContinuousAccumulator>| {
            let mut cells = BTreeMap::new();
            while s
                .drain_into(128, |_, _, id, a| {
                    assert!(cells.insert(id, a).is_none());
                })
                .unwrap()
                > 0
            {}
            cells
        };
        let reference = drain(scan());
        let mut split = scan();
        let last = split.blocks.pop().unwrap();
        assert_eq!((last.tile, last.rows.clone()), (1, 0..64));
        // Split the final tile explicitly so this small regression is independent
        // of the production planner's target block size.
        for rows in [0..32, 32..64] {
            split.blocks.push(Block {
                bbox: block_bbox(split.grid, 9, &mosaic.tiles[1], &rows, &last.cols, true),
                last_of_tile: rows.end == 64,
                rows,
                ..last.clone()
            });
        }
        split.advance_until_completed(1).unwrap();
        assert_eq!(split.next_block, 2);
        assert!(!split.output.is_empty());
        assert!(split
            .pending
            .values()
            .any(|p| p.end_row.is_some_and(|r| r > 32) && p.touched));
        let actual = drain(split);
        assert!(reference.keys().eq(actual.keys()));
        for (id, a) in actual {
            let want = &reference[&id];
            assert!((a.count - want.count).abs() < 1e-8);
            assert!((a.sum - want.sum).abs() < 1e-7);
        }
    }

    /// A geographic (EPSG:4326) float raster with square-or-not pixels.
    fn geographic_raster(
        width: u32,
        height: u32,
        origin: (f64, f64),
        pixel: (f64, f64),
        value: impl Fn(u32, u32) -> f32,
    ) -> tempfile::NamedTempFile {
        use tiff::encoder::{colortype, TiffEncoder};
        use tiff::tags::Tag;
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
        let mut image = encoder
            .new_image::<colortype::Gray32Float>(width, height)
            .unwrap();
        let tags = image.encoder();
        tags.write_tag(
            Tag::GeoKeyDirectoryTag,
            &[1u16, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326][..],
        )
        .unwrap();
        tags.write_tag(Tag::ModelPixelScaleTag, &[pixel.0, pixel.1, 0.0][..])
            .unwrap();
        tags.write_tag(
            Tag::ModelTiepointTag,
            &[0.0f64, 0.0, 0.0, origin.0, origin.1, 0.0][..],
        )
        .unwrap();
        let pixels: Vec<_> = (0..height)
            .flat_map(|r| (0..width).map(move |c| (r, c)))
            .map(|(r, c)| value(c, r))
            .collect();
        image.write_data(&pixels).unwrap();
        file
    }

    /// Cells, plus the worker batches run and cells retried serially.
    fn overlay_scan(
        file: &tempfile::NamedTempFile,
        grid: GridKind,
        resolution: u8,
        workers: Option<usize>,
        mass_preserve: bool,
    ) -> (BTreeMap<u64, ContinuousAccumulator>, usize, usize) {
        overlay_scan_in(file, grid, resolution, workers, None, mass_preserve)
    }

    fn overlay_scan_in(
        file: &tempfile::NamedTempFile,
        grid: GridKind,
        resolution: u8,
        workers: Option<usize>,
        bbox: Option<[f64; 4]>,
        mass_preserve: bool,
    ) -> (BTreeMap<u64, ContinuousAccumulator>, usize, usize) {
        let mosaic = Arc::new(
            MosaicReader::open(&[file.path().to_owned()], None, None, OverlapRule::First).unwrap(),
        );
        let mut config = MultiResolutionConfig::single(resolution).with_grid(grid);
        config.bbox = bbox;
        config.transfer = TransferMode::Overlay {
            weighted: mass_preserve,
            mass_preserve,
        };
        config.aggregation_workers = workers;
        let mut stream = TransferStream::<ContinuousAccumulator>::new(mosaic, &config).unwrap();
        let mut cells = BTreeMap::new();
        while stream
            .drain_into(128, |_, _, id, a| {
                assert!(cells.insert(id, a).is_none());
            })
            .unwrap()
            > 0
        {}
        (cells, stream.overlap_batches, stream.overlap_retries)
    }

    #[test]
    fn overlay_workers_clip_cell_batches_and_hand_oversized_cells_back() {
        // Fine cells: pixels a few times smaller than a cell, so every batch
        // of cells is handled in parallel with zero retries.
        let fine = geographic_raster(60, 48, (-122.45, 37.80), (0.001, 0.001), |c, r| {
            (1 + (3 * c + r) % 23) as f32
        });
        let (serial, serial_batches, _) = overlay_scan(&fine, GridKind::H3, 10, Some(1), false);
        let (parallel, batches, retries) = overlay_scan(&fine, GridKind::H3, 10, Some(4), false);
        let (ambient, ambient_batches, _) = overlay_scan(&fine, GridKind::H3, 10, None, false);
        assert!(serial.len() > 128);
        assert_eq!(serial_batches, 0, "one worker must run serially");
        assert!(batches > 0, "four workers never ran a direct batch");
        assert!(ambient_batches > 0, "ambient default never ran in parallel");
        assert_eq!(retries, 0);
        assert_eq!(parallel, serial);
        assert_eq!(ambient, serial);

        // Narrow columns and tall rows: direct parallel overlay handles oversized cells
        // natively with zero retries, while mass_preserve prepared overlaps still retry.
        let narrow = geographic_raster(2048, 8, (-122.45, 37.80), (0.000002, 0.25), |c, r| {
            (1 + (c + 3 * r) % 31) as f32
        });
        let (serial_direct, _, _) = overlay_scan(&narrow, GridKind::H3, 8, Some(1), false);
        let (par_direct, batches_direct, retries_direct) =
            overlay_scan(&narrow, GridKind::H3, 8, Some(4), false);
        assert!(serial_direct
            .values()
            .any(|a| a.count > MAX_PREPARED_OVERLAPS as f64));
        assert!(batches_direct > 0);
        assert_eq!(retries_direct, 0);
        assert_eq!(par_direct, serial_direct);

        let (serial_mp, _, _) = overlay_scan(&narrow, GridKind::H3, 8, Some(1), true);
        let (par_mp, batches_mp, retries_mp) =
            overlay_scan(&narrow, GridKind::H3, 8, Some(4), true);
        assert!(batches_mp > 0, "four workers never prepared a batch");
        assert!(retries_mp > 0, "no oversized cell was handed back");
        assert_eq!(par_mp, serial_mp);
    }

    #[test]
    fn parallel_direct_overlay_matches_serial_with_large_cells_and_all_nodata_tiles() {
        use tiff::encoder::{colortype, TiffEncoder};
        use tiff::tags::Tag;

        // Strips: strips 0..4 are all NoData, strips 4..12 mix valid and NoData, at res 6
        // where each H3 cell covers > 512 pixels (previously excluded from parallel overlay).
        let (w, h, rps) = (128u32, 96u32, 8u32);
        let nodata = -9999.0f32;
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
        let mut image = encoder.new_image::<colortype::Gray32Float>(w, h).unwrap();
        image.rows_per_strip(rps).unwrap();
        let tags = image.encoder();
        tags.write_tag(
            Tag::GeoKeyDirectoryTag,
            &[1u16, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326][..],
        )
        .unwrap();
        tags.write_tag(Tag::ModelPixelScaleTag, &[0.002f64, 0.002, 0.0][..])
            .unwrap();
        tags.write_tag(
            Tag::ModelTiepointTag,
            &[0.0f64, 0.0, 0.0, -122.5, 37.9, 0.0][..],
        )
        .unwrap();
        tags.write_tag(Tag::Unknown(42113), "-9999").unwrap();
        let pixels: Vec<f32> = (0..h)
            .flat_map(|r| {
                (0..w).map(move |c| {
                    if r < 32 || (r + c) % 7 == 0 {
                        nodata
                    } else {
                        ((r * 5 + c) % 13 + 1) as f32
                    }
                })
            })
            .collect();
        image.write_data(&pixels).unwrap();

        let mosaic = Arc::new(
            MosaicReader::open(&[file.path().to_owned()], None, None, OverlapRule::First).unwrap(),
        );

        let drain_cont = |workers: Option<usize>, emit: bool, cov: Option<f64>, quantiles: bool| {
            let mut cfg = MultiResolutionConfig::new(vec![6, 7]);
            cfg.transfer = TransferMode::OVERLAY;
            cfg.aggregation_workers = workers;
            cfg.valid_coverage_threshold = cov;
            if emit {
                cfg.nodata_policy = NodataPolicy::Emit;
            }
            if quantiles {
                cfg.quantiles = vec![
                    crate::aggregator::multi_horizon::QuantileTarget::Percentile(0.5, "p50".into()),
                ];
            }
            let mut stream =
                TransferStream::<ContinuousAccumulator>::new(mosaic.clone(), &cfg).unwrap();
            let mut out = BTreeMap::new();
            while stream
                .drain_into(128, |_, res, id, acc| {
                    assert!(out.insert((res, id), acc).is_none());
                })
                .unwrap()
                > 0
            {}
            (out, stream.overlap_batches, stream.overlap_retries)
        };

        for (emit, cov, quantiles) in [
            (false, None, false),
            (false, Some(0.5), false),
            (true, None, false),
            (false, None, true),
        ] {
            let (s1, b1, _) = drain_cont(Some(1), emit, cov, quantiles);
            let (s4, b4, r4) = drain_cont(Some(4), emit, cov, quantiles);
            let (sa, ba, _) = drain_cont(None, emit, cov, quantiles);
            assert!(!s1.is_empty());
            assert_eq!(b1, 0);
            assert!(b4 > 0, "emit={emit} cov={cov:?} quantiles={quantiles}");
            assert!(ba > 0);
            assert_eq!(r4, 0);
            assert_eq!(s4, s1, "emit={emit} cov={cov:?} quantiles={quantiles}");
            assert_eq!(sa, s1, "emit={emit} cov={cov:?} quantiles={quantiles}");
        }

        // Categorical + OverlayPartial (checks NEEDS_COVER retains cover over AllNoData strips).
        let drain_part = |workers: Option<usize>| {
            let mut cfg = MultiResolutionConfig::new(vec![6, 7]);
            cfg.transfer = TransferMode::OVERLAY;
            cfg.aggregation_workers = workers;
            let mut stream =
                TransferStream::<super::super::OverlayPartial>::new(mosaic.clone(), &cfg).unwrap();
            let mut out = BTreeMap::new();
            while stream
                .drain_into(128, |_, res, id, part| {
                    assert!(out.insert((res, id), (part.covered, part.stats)).is_none());
                })
                .unwrap()
                > 0
            {}
            (out, stream.overlap_batches)
        };
        let (p1, pb1) = drain_part(Some(1));
        let (p4, pb4) = drain_part(Some(4));
        assert_eq!(pb1, 0);
        assert!(pb4 > 0);
        assert!(p1.values().any(|(cov, st)| *cov > 0.0 && st.count == 0.0));
        assert_eq!(p4, p1);

        let drain_cat = |workers: Option<usize>| {
            let mut cfg = MultiResolutionConfig::new(vec![6, 7]);
            cfg.transfer = TransferMode::Overlay {
                weighted: false,
                mass_preserve: false,
            };
            cfg.aggregation_workers = workers;
            let mut stream =
                TransferStream::<CategoricalAccumulator>::new(mosaic.clone(), &cfg).unwrap();
            let mut out = BTreeMap::new();
            while stream
                .drain_into(128, |_, res, id, acc| {
                    assert!(out.insert((res, id), acc).is_none());
                })
                .unwrap()
                > 0
            {}
            (out, stream.overlap_batches)
        };
        let (c1, cb1) = drain_cat(Some(1));
        let (c4, cb4) = drain_cat(Some(4));
        assert_eq!(cb1, 0);
        assert!(cb4 > 0);
        assert_eq!(c4, c1);
    }

    fn geographic_multi_strip_raster(
        width: u32,
        height: u32,
        rows_per_strip: u32,
        nodata: f32,
        sparse_strips: &[usize],
    ) -> tempfile::NamedTempFile {
        use tiff::encoder::{colortype, TiffEncoder};
        use tiff::tags::Tag;

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
        let mut image = encoder
            .new_image::<colortype::Gray32Float>(width, height)
            .unwrap();
        image.rows_per_strip(rows_per_strip).unwrap();
        let tags = image.encoder();
        tags.write_tag(
            Tag::GeoKeyDirectoryTag,
            &[1u16, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326][..],
        )
        .unwrap();
        tags.write_tag(Tag::ModelPixelScaleTag, &[0.002f64, 0.002, 0.0][..])
            .unwrap();
        tags.write_tag(
            Tag::ModelTiepointTag,
            &[0.0f64, 0.0, 0.0, -122.5, 37.85, 0.0][..],
        )
        .unwrap();
        tags.write_tag(Tag::Unknown(42113), &format!("{nodata}")[..])
            .unwrap();
        let pixels: Vec<f32> = (0..height)
            .flat_map(|r| {
                let strip = (r / rows_per_strip) as usize;
                let is_sparse = sparse_strips.contains(&strip);
                (0..width).map(move |c| {
                    if is_sparse || (r + c) % 19 == 0 {
                        nodata
                    } else {
                        ((r * 3 + c) % 17 + 1) as f32
                    }
                })
            })
            .collect();
        image.write_data(&pixels).unwrap();
        file
    }

    #[test]
    fn plan_run_chunk_misses_skips_sparse_and_deduplicates_overlaps() {
        let file = geographic_multi_strip_raster(16, 24, 2, -9999.0, &[1, 4]);
        let mut reader = GeoTiffStreamReader::open(file.path()).unwrap();
        let info = Arc::make_mut(reader.chunk_info.as_mut().unwrap());
        for s in [1usize, 4] {
            Arc::make_mut(&mut info.chunk_offsets)[s] = 0;
            Arc::make_mut(&mut info.chunk_bytes)[s] = 0;
        }
        let tile = TileDescriptor::new(0, reader, None).unwrap();
        let bbox = Some([-122.5, 37.8, -122.4, 37.85]);

        // Adjacent overlay blocks within strip 0 (0..1, 1..2) followed by strip 1 (sparse),
        // strip 2 (4..6), plus None bbox block.
        let blocks = vec![
            Block {
                resolution: 8,
                tile: 0,
                rows: 0..1,
                cols: 0..16,
                bbox,
                last_of_tile: false,
            },
            Block {
                resolution: 8,
                tile: 0,
                rows: 1..2,
                cols: 0..16,
                bbox,
                last_of_tile: false,
            },
            Block {
                resolution: 8,
                tile: 0,
                rows: 2..4,
                cols: 0..16,
                bbox,
                last_of_tile: false,
            },
            Block {
                resolution: 8,
                tile: 0,
                rows: 4..6,
                cols: 0..16,
                bbox: None,
                last_of_tile: false,
            },
            Block {
                resolution: 8,
                tile: 0,
                rows: 6..10,
                cols: 0..16,
                bbox,
                last_of_tile: true,
            },
        ];
        let (misses, covered) = plan_run_chunk_misses(&tile, &blocks, &[1], TransferMode::OVERLAY);
        assert_eq!(covered, 5);
        // Strip 0 missed once on block 0, hit on block 1; strip 1 sparse; strip 2 skipped (bbox None);
        // strips 3 and 4 missed on block 4, with strip 4 sparse -> [0, 3].
        assert_eq!(misses, vec![0, 3]);
    }

    #[test]
    fn prefetched_overlay_and_interpolate_match_single_worker() {
        let file = geographic_multi_strip_raster(48, 64, 2, -9999.0, &[3, 11]);
        let mut reader = GeoTiffStreamReader::open(file.path()).unwrap();
        let info = Arc::make_mut(reader.chunk_info.as_mut().unwrap());
        for s in [3usize, 11] {
            Arc::make_mut(&mut info.chunk_offsets)[s] = 0;
            Arc::make_mut(&mut info.chunk_bytes)[s] = 0;
        }
        let mosaic = Arc::new(MosaicReader::from_single_reader(reader, None, None).unwrap());

        for mode in [
            TransferMode::OVERLAY,
            TransferMode::Overlay {
                weighted: true,
                mass_preserve: true,
            },
            TransferMode::Interpolate(Interpolation::Bilinear),
        ] {
            let mut single_cfg = MultiResolutionConfig::new(vec![6, 7]);
            single_cfg.transfer = mode;
            single_cfg.decode_workers = 1;

            let mut multi_cfg = single_cfg.clone();
            multi_cfg.decode_workers = 4;

            let drain_cont = |cfg: &MultiResolutionConfig| {
                let mut stream =
                    TransferStream::<ContinuousAccumulator>::new(mosaic.clone(), cfg).unwrap();
                let mut out = BTreeMap::new();
                while stream
                    .drain_into(128, |_, res, id, acc| {
                        assert!(out.insert((res, id), acc).is_none());
                    })
                    .unwrap()
                    > 0
                {}
                out
            };
            let single = drain_cont(&single_cfg);
            let multi = drain_cont(&multi_cfg);
            assert!(!single.is_empty());
            assert_eq!(
                single.keys().collect::<Vec<_>>(),
                multi.keys().collect::<Vec<_>>()
            );
            for (k, a) in single {
                let b = &multi[&k];
                assert!((a.count - b.count).abs() < 1e-12, "{mode:?} {k:?}");
                assert!((a.sum - b.sum).abs() < 1e-12, "{mode:?} {k:?}");
                assert!((a.m2 - b.m2).abs() < 1e-12, "{mode:?} {k:?}");
            }
        }

        // Categorical overlay across multiple chunks and resolutions.
        let mut cat_single = MultiResolutionConfig::new(vec![6, 7]);
        cat_single.transfer = TransferMode::Overlay {
            weighted: false,
            mass_preserve: false,
        };
        cat_single.decode_workers = 1;
        let mut cat_multi = cat_single.clone();
        cat_multi.decode_workers = 4;
        let drain_cat = |cfg: &MultiResolutionConfig| {
            let mut stream =
                TransferStream::<CategoricalAccumulator>::new(mosaic.clone(), cfg).unwrap();
            let mut out = BTreeMap::new();
            while stream
                .drain_into(128, |_, res, id, acc| {
                    assert!(out.insert((res, id), acc).is_none());
                })
                .unwrap()
                > 0
            {}
            out
        };
        let cat1 = drain_cat(&cat_single);
        let cat4 = drain_cat(&cat_multi);
        assert!(!cat1.is_empty());
        assert_eq!(cat1, cat4);
    }

    #[test]
    fn parallel_interpolate_matches_serial() {
        let file = geographic_multi_strip_raster(64, 64, 16, -9999.0, &[]);
        let reader = GeoTiffStreamReader::open(file.path()).unwrap();
        let mosaic = Arc::new(MosaicReader::from_single_reader(reader, None, None).unwrap());

        for method in [Interpolation::Bilinear, Interpolation::Bicubic] {
            let drain_cont = |workers: Option<usize>| {
                let mut cfg = MultiResolutionConfig::new(vec![9]);
                cfg.transfer = TransferMode::Interpolate(method);
                cfg.aggregation_workers = workers;
                let mut stream =
                    TransferStream::<ContinuousAccumulator>::new(mosaic.clone(), &cfg).unwrap();
                let mut out = BTreeMap::new();
                while stream
                    .drain_into(256, |_, res, id, acc| {
                        assert!(out.insert((res, id), acc).is_none());
                    })
                    .unwrap()
                    > 0
                {}
                (out, stream.overlap_batches)
            };
            let (s1, b1) = drain_cont(Some(1));
            let (s4, b4) = drain_cont(Some(4));
            let (s_amb, b_amb) = drain_cont(None);
            assert_eq!(b1, 0);
            assert!(b4 > 0, "{method:?}");
            assert!(b_amb > 0, "{method:?}");
            assert!(!s1.is_empty());
            assert_eq!(s4, s1, "{method:?} dedicated parallel != serial");
            assert_eq!(s_amb, s1, "{method:?} ambient parallel != serial");
        }

        let drain_cat = |workers: Option<usize>| {
            let mut cfg = MultiResolutionConfig::new(vec![9]);
            cfg.transfer = TransferMode::Interpolate(Interpolation::Nearest);
            cfg.aggregation_workers = workers;
            let mut stream =
                TransferStream::<CategoricalAccumulator>::new(mosaic.clone(), &cfg).unwrap();
            let mut out = BTreeMap::new();
            while stream
                .drain_into(256, |_, res, id, acc| {
                    assert!(out.insert((res, id), acc).is_none());
                })
                .unwrap()
                > 0
            {}
            (out, stream.overlap_batches)
        };
        let (c1, cb1) = drain_cat(Some(1));
        let (c4, cb4) = drain_cat(Some(4));
        let (camb, cb_amb) = drain_cat(None);
        assert_eq!(cb1, 0);
        assert!(cb4 > 0);
        assert!(cb_amb > 0);
        assert!(!c1.is_empty());
        assert_eq!(c4, c1);
        assert_eq!(camb, c1);
    }

    #[test]
    fn a5_cells_in_bbox_handles_metre_scale_strips_at_fine_resolutions() {
        // About 4 km wide and 5 m tall at the equator. The fill guard's spherical cap
        // is far larger than the strip, so the slices must be narrower than 0.05 degrees.
        let res = 24;
        let bbox = [0.0, 0.0, 0.036, 0.000_045];
        let cells = cells_in_bbox(GridKind::A5, bbox, res, ContainmentMode::ContainsCentroid)
            .expect("a strip that fits the cell budget must not be rejected");
        let area_m2 =
            0.036_f64.to_radians() * 6_371_007.2 * 0.000_045_f64.to_radians() * 6_371_007.2;
        let expected = area_m2 / GridKind::A5.area_m2(res);
        let ratio = cells.len() as f64 / expected;
        assert!(
            (0.8..1.2).contains(&ratio),
            "{} cells, expected about {expected:.0}",
            cells.len()
        );
    }

    #[test]
    fn a5_cells_in_bbox_covers_wide_thin_strip() {
        let res = 10;
        for bbox in [
            [-125.0, 39.8, -67.0, 40.0],
            [-125.0, -40.0, -67.0, -39.8],
            [175.0, 39.8, -175.0, 40.0],
        ] {
            let covered = cells_in_bbox(GridKind::A5, bbox, res, ContainmentMode::Covers).unwrap();
            let set: FxHashSet<u64> = covered.iter().copied().collect();
            assert!(!set.is_empty(), "empty cover for {bbox:?}");

            let [west, south, east, north] = bbox;
            let spans = if west <= east {
                vec![(west, east)]
            } else {
                vec![(west, 180.0), (-180.0, east)]
            };
            let mut sampled = 0usize;
            for (sw, se) in spans {
                for i in 0..=200 {
                    let lon = sw + (se - sw) * f64::from(i) / 200.0;
                    for j in 0..=10 {
                        let lat = south + (north - south) * f64::from(j) / 10.0;
                        let cell = GridKind::A5.cell_at(lat, lon, res).unwrap();
                        assert!(
                            set.contains(&cell),
                            "missing A5 cell {cell:#x} at ({lon}, {lat}) for bbox {bbox:?}"
                        );
                        sampled += 1;
                    }
                }
            }
            assert!(sampled > 2000);
        }

        // ContainsCentroid must also retain cells along the full width of a thin strip,
        // without bowing past the strip's southern chord.
        let wide_strip = [-125.0, 39.8, -67.0, 40.0];
        let centered = cells_in_bbox(
            GridKind::A5,
            wide_strip,
            res,
            ContainmentMode::ContainsCentroid,
        )
        .unwrap();
        let center_set: FxHashSet<u64> = centered.iter().copied().collect();
        let cover_all =
            cells_in_bbox(GridKind::A5, wide_strip, res, ContainmentMode::Covers).unwrap();
        let mut checked_in = 0usize;
        for cell in cover_all {
            if let Some((lat, lon)) = GridKind::A5.center_lat_lng(cell) {
                let lon = wrap_lon_180(lon);
                if (wide_strip[0]..=wide_strip[2]).contains(&lon)
                    && (wide_strip[1]..=wide_strip[3]).contains(&lat)
                {
                    assert!(
                        center_set.contains(&cell),
                        "ContainsCentroid missed interior cell {cell:#x} at ({lon}, {lat})"
                    );
                    checked_in += 1;
                }
            }
        }
        assert!(
            checked_in > 50,
            "expected >50 interior CONUS cells, got {checked_in}"
        );
    }

    #[test]
    fn polar_cells_honour_the_bbox_in_direct_overlay() {
        // The cell holding the pole is assigned whole pixels by centre. A bbox must
        // still drop the pixels outside it, as it does for every other cell.
        let (west, north, step) = (-10.0, 90.0, 0.05);
        let value = |c: u32, r: u32| (1 + (7 * c + 3 * r) % 41) as f32;
        let raster = geographic_raster(400, 300, (west, north), (step, step), value);
        let cut = [west - 1.0, 70.0, 0.0, 90.0];
        let (clipped, _, _) = overlay_scan_in(&raster, GridKind::H3, 3, None, Some(cut), false);
        let masked = geographic_raster(400, 300, (west, north), (step, step), |c, r| {
            if c < 200 {
                value(c, r)
            } else {
                f32::NAN
            }
        });
        let (expected, _, _) = overlay_scan(&masked, GridKind::H3, 3, None, false);
        let pole = GridKind::H3.cell_at(89.99, -5.0, 3).unwrap();
        assert!(GridKind::H3.interior_pole(pole).is_some(), "pole cell");
        let (got, want) = (&clipped[&pole], &expected[&pole]);
        assert_eq!(got.count, want.count);
        assert_eq!(got.sum, want.sum);
    }

    #[test]
    fn bbox_selection_keeps_interior_runs_on_the_slice_path() {
        // About 50 pixels across an H3 res-8 cell, so interiors form long runs.
        let (west, north, step) = (-122.45, 37.80, 0.0002);
        let value = |c: u32, r: u32| (1 + (7 * c + 3 * r) % 41) as f32;
        let raster = geographic_raster(240, 160, (west, north), (step, step), value);
        let (unbounded, _, _) = overlay_scan(&raster, GridKind::H3, 8, Some(1), false);
        assert!(!unbounded.is_empty());

        // A bbox enclosing the raster selects every pixel: identical results.
        let all = [west - 1.0, north - 1.0, west + 1.0, north + 1.0];
        let (enclosed, _, _) = overlay_scan_in(&raster, GridKind::H3, 8, Some(1), Some(all), false);
        assert_eq!(enclosed, unbounded);

        // A bbox cutting through cells must match NoData outside it.
        let cut_col = 101;
        let cut = [
            west - 1.0,
            north - 1.0,
            west + cut_col as f64 * step,
            north + 1.0,
        ];
        let (clipped, _, _) = overlay_scan_in(&raster, GridKind::H3, 8, Some(1), Some(cut), false);
        let masked = geographic_raster(240, 160, (west, north), (step, step), |c, r| {
            if c < cut_col {
                value(c, r)
            } else {
                f32::NAN
            }
        });
        let (expected, _, _) = overlay_scan(&masked, GridKind::H3, 8, Some(1), false);
        let mut compared = 0;
        for (id, got) in &clipped {
            let Some(want) = expected.get(id) else {
                continue;
            };
            compared += 1;
            assert_eq!(got.count, want.count, "cell {id:x}");
            assert_eq!((got.min, got.max), (want.min, want.max), "cell {id:x}");
            assert!(
                (got.sum - want.sum).abs() <= 1e-9 * want.sum.abs(),
                "cell {id:x}"
            );
            assert!(
                (got.m2 - want.m2).abs() <= 1e-9 * want.m2.abs().max(1.0),
                "cell {id:x}"
            );
        }
        assert!(compared > 10, "only {compared} cells compared");
    }
}
