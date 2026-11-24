//! The streaming driver of the `interpolate` and `overlay` transfer modes.

mod edges;
mod ground_area;
pub(crate) use ground_area::ground_area_overlay_tile;
pub use ground_area::scan_ground_area_overlay;

use rayon::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::categorical::CategoricalAccumulator;
use crate::aggregator::multi_horizon::coordinates::is_point_in_bbox;
use crate::aggregator::multi_horizon::MultiResolutionConfig;
use crate::aggregator::output_policy::NodataPolicy;
use crate::aggregator::remap::CategoryRemapper;
use crate::crs::transformer::CrsTransformer;
use crate::error::{DggsStreamError, Result};
use crate::grid::{wrap_lon_180, GridKind};
use crate::raster::mosaic::{MosaicReader, TileDescriptor};
use crate::scalar_ops::{grid_polygon_to_cells_wkb_into, ContainmentMode, PolygonToCellsError};

use super::clip::visit_overlaps;
use super::interp::{interpolate, Interpolation};
use super::window::{BandSelection, ChunkCache, RowWindow};
use super::TransferMode;

/// Values of one window: enough for wide rasters without holding much more than a
/// chunk band.
const MAX_WINDOW_VALUES: usize = 4 << 20;
/// Cells listed per block, so the cell list stays small next to the window.
const TARGET_BLOCK_CELLS: f64 = 150_000.0;
// Limit the speculative overlap lists to 128 × 1024 (pixel, area) pairs.
// Leave room for irregular shapes around the estimated mean cell area.
const OVERLAP_BATCH_CELLS: usize = 128;
const MAX_PREPARED_OVERLAPS: usize = 1024;
const MAX_MEAN_PIXELS_PER_CELL: f64 = 512.0;

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

    fn empty(_track_quantiles: bool) -> Self {
        Self::default()
    }

    fn add(&mut self, value: f64, weight: f64) {
        self.update_weighted(value.round() as i64, weight);
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

/// Streams `(resolution, cell, accumulator)` for the `interpolate` and `overlay` modes.
pub struct TransferStream<A: TransferAccumulator> {
    mosaic: Arc<MosaicReader>,
    grid: GridKind,
    mode: TransferMode,
    overlap_pool: Option<Arc<rayon::ThreadPool>>,
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
            overlap_pool: config
                .aggregation_workers
                .filter(|&n| n > 1 && matches!(mode, TransferMode::Overlay { .. }))
                .map(crate::aggregator::multi_horizon::controller::shared_aggregation_pool)
                .transpose()?,
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
        let block = self.blocks[self.next_block].clone();
        self.next_block += 1;
        let result = self.process(&block);
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

    /// Read a window of `tile`, timed as decoding when a profile is attached.
    fn load_window(
        &mut self,
        tile: &TileDescriptor,
        rows: Range<u32>,
        cols: Range<u32>,
    ) -> Result<RowWindow> {
        use crate::run_profile::{Counter, Phase, PhaseTimer};
        let timer = self.profile.as_ref().map(|_| PhaseTimer::start());
        let read_width = cols.len() as u64;
        let window = RowWindow::load_bands(
            tile,
            &self.bands,
            self.selection,
            rows,
            cols,
            &mut self.cache,
            self.profile.as_deref(),
        )?;
        if let (Some(profile), Some(timer)) = (&self.profile, timer) {
            timer.stop(profile, Phase::Decode);
            let pixels = read_width * window.rows().len() as u64;
            profile.count(Counter::DecodedBytes, pixels * 8 * self.bands.len() as u64);
        }
        Ok(window)
    }

    fn process(&mut self, block: &Block) -> Result<()> {
        use crate::run_profile::{Counter, Phase, PhaseTimer};
        let timer = self.profile.as_ref().map(|_| PhaseTimer::start());
        let outcome = self.process_block(block);
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

    fn process_block(&mut self, block: &Block) -> Result<()> {
        if self.cache_tile != block.tile {
            self.cache.clear();
            self.cache_tile = block.tile;
        }
        match block.bbox {
            None => self.record_failures(1)?,
            Some(bbox) => match self.mode {
                TransferMode::Interpolate(method) => self.interpolate_block(block, bbox, method)?,
                TransferMode::Overlay {
                    weighted,
                    mass_preserve,
                } => self.overlay_block(block, bbox, weighted, mass_preserve)?,
                TransferMode::AssignCenters => {}
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
        let mut failures = 0;
        for cell in cells {
            let Some((lat, lng)) = self.grid.center_lat_lng(cell) else {
                continue;
            };
            if !is_point_in_bbox(lng, lat, self.bbox) {
                continue;
            }
            let Some((col, row)) = to_pixel(tile, lng, lat) else {
                failures += 1;
                continue;
            };
            // A cell belongs to the block holding its centre, so it is emitted once.
            let in_block = col >= f64::from(block.cols.start)
                && col < f64::from(block.cols.end)
                && row >= f64::from(block.rows.start)
                && row < f64::from(block.rows.end);
            if !in_block || !mosaic.is_point_owned_by(block.tile, lng, lat) {
                continue;
            }
            let mut acc = A::empty(self.track_quantiles);
            if let Some(value) = interpolate(&window, method, col, row).and_then(|v| {
                Self::mapped(self.histogram_bins.as_deref(), self.remapper.as_deref(), v)
            }) {
                acc.add(value, 1.0);
            }
            if (acc.has_samples() || self.emit_nodata) && acc.passes(&self.filters) {
                self.output.push_back((block.resolution, cell, acc));
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
        let mut overlaps: Vec<(u32, f64)> = Vec::new();
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
        let mut selected = (self.bbox.is_some() || several_tiles)
            .then(|| vec![None; band_width * block.rows.len()]);
        let mut contribute = |pending: &mut Pending<A>,
                              coverage: Option<&mut BandCoverage>,
                              pixel: u32,
                              area: f64,
                              weight: f64| {
            let index = pixel as usize;
            let col = (index % band_width) as i64;
            let row = rows.0 + (index / band_width) as i64;
            if !block.cols.contains(&(col as u32)) {
                return;
            }
            if let Some(cache) = &mut selected {
                let inside = cache[index].get_or_insert_with(|| {
                    let (x, y) = gt.pixel_to_coord(col as f64 + 0.5, row as f64 + 0.5);
                    tile.crs_transformer
                        .transform_point(x, y)
                        .is_ok_and(|(lon, lat)| {
                            is_point_in_bbox(wrap_lon_180(lon), lat, self.bbox)
                                && (!several_tiles
                                    || mosaic.is_point_owned_by(block.tile, lon, lat))
                        })
                });
                if !*inside {
                    return;
                }
            }
            pending.touched = true;
            pending.acc.cover(area);
            let mut coverage = coverage;
            if let Some(c) = coverage.as_mut() {
                c.covered_area += area;
            }
            for slot in 0..self.bands.len() {
                if let Some(sample) = window.band_value(slot, index) {
                    if let Some(c) = coverage.as_mut() {
                        c.valid_area[slot] += area;
                    }
                    if let Some(value) = Self::mapped(
                        self.histogram_bins.as_deref(),
                        self.remapper.as_deref(),
                        sample,
                    ) {
                        pending.acc.add_band(slot, value, weight);
                    }
                }
            }
        };
        let geometry = OverlayGeometry {
            grid: self.grid,
            tile,
            longitude_bounds,
            rows,
            width,
        };
        // Installing work while the caller is a Rayon worker could steal an
        // unrelated query that waits for the caller's engine lock.
        let pool = self.overlap_pool.as_ref().filter(|_| {
            !coarse
                && cells.len() >= OVERLAP_BATCH_CELLS / 2
                && rayon::current_thread_index().is_none()
                && crate::resolution::pixel_ground_area_m2(tile).is_ok_and(|area| {
                    self.grid.area_m2(block.resolution) <= MAX_MEAN_PIXELS_PER_CELL * area
                })
        });
        let mut prepared = Vec::<(Option<i64>, Vec<(u32, f64)>)>::new();
        for (i, &cell) in cells.iter().enumerate() {
            // Keep the serial reducer in original cell and pixel order.
            let slot = i % OVERLAP_BATCH_CELLS;
            if let Some(pool) = pool {
                if slot == 0 {
                    #[cfg(test)]
                    {
                        self.overlap_batches += 1;
                    }
                    let batch = &cells[i..cells.len().min(i + OVERLAP_BATCH_CELLS)];
                    prepared.resize_with(batch.len(), Default::default);
                    pool.install(|| {
                        prepared
                            .par_iter_mut()
                            .zip(batch)
                            .for_each(|((end, pixels), &id)| {
                                pixels.clear();
                                *end = geometry.clip(id, pixels, MAX_PREPARED_OVERLAPS);
                            })
                    });
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
                    if pool.is_some() {
                        self.overlap_retries += 1;
                    }
                    geometry.clip(cell, &mut overlaps, usize::MAX)
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
                        overlaps.push((pixel as u32, 1.0));
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
            let track_quantiles = self.track_quantiles;
            let grid = self.grid;
            let pending = self.pending.entry(cell).or_insert_with(|| Pending {
                acc: A::empty_bands(track_quantiles, self.bands.len()),
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
                for &(pixel, area) in &overlaps[before..] {
                    totals[pixel as usize] += area;
                }
                cell_ends.push((cell, overlaps.len()));
            } else {
                let mut coverage = self.coverage_filter.as_mut().map(|filter| {
                    filter.cells.entry(cell).or_insert_with(|| BandCoverage {
                        valid_area: vec![0.0; self.bands.len()],
                        covered_area: 0.0,
                    })
                });
                for (pixel, area) in overlaps.drain(..) {
                    contribute(
                        pending,
                        coverage.as_deref_mut(),
                        pixel,
                        area,
                        if weighted { area } else { 1.0 },
                    );
                }
            }
        }
        let mut start = 0;
        for (cell, end) in cell_ends {
            let pending = self
                .pending
                .get_mut(&cell)
                .expect("cell remains pending until block completion");
            for &(pixel, area) in &overlaps[start..end] {
                contribute(pending, None, pixel, area, area / totals[pixel as usize]);
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
fn plan_tile(
    grid: GridKind,
    mode: TransferMode,
    resolution: u8,
    tile_idx: usize,
    tile: &TileDescriptor,
    band_count: usize,
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
    let window_rows = ((MAX_WINDOW_VALUES / band_count / width as usize) as u32)
        .saturating_sub(margin)
        .max(1);
    // Whole chunk rows where the budgets allow, so chunks are not decoded twice.
    let chunk_height = tile.reader.chunk_layout.chunk_height.max(1);
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
        let row_end = (row + rows).min(height);
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

/// Above this cell diameter bound (`GridSpec::max_cell_diameter_km`, about four times a
/// typical edge) `overlay` assigns whole pixels to cells by centre: the edges of such
/// cells bend by more than a kilometre from the straight lines the polygon clipping
/// assumes, and a pixel is negligible next to the cell.
const COARSE_CELL_DIAMETER_KM: f64 = 500.0;

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
    let mut cells = Vec::new();
    let mut part = Vec::new();
    for (span_west, span_east) in spans {
        let slices = ((span_east - span_west) / MAX_FILL_LON_SPAN)
            .ceil()
            .max(1.0) as u32;
        for slice in 0..slices {
            let w = span_west + (span_east - span_west) * f64::from(slice) / f64::from(slices);
            let e = span_west + (span_east - span_west) * f64::from(slice + 1) / f64::from(slices);
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
            let wkb = rectangle_wkb(w, south, e, north);
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

/// WKB of the lon/lat rectangle `[west, south, east, north]`, counter-clockwise.
fn rectangle_wkb(west: f64, south: f64, east: f64, north: f64) -> Vec<u8> {
    let ring = [
        (west, south),
        (east, south),
        (east, north),
        (west, north),
        (west, south),
    ];
    let mut wkb = Vec::with_capacity(13 + 16 * ring.len());
    wkb.push(1u8);
    wkb.extend(3u32.to_le_bytes());
    wkb.extend(1u32.to_le_bytes());
    wkb.extend((ring.len() as u32).to_le_bytes());
    for (x, y) in ring {
        wkb.extend(x.to_le_bytes());
        wkb.extend(y.to_le_bytes());
    }
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

/// Two seam pieces can touch the same source pixel (e.g. a one-column
/// global raster). Combine its weight before counting it or updating variance.
fn merge_seam_overlaps(overlaps: &mut Vec<(u32, f64)>, start: usize) {
    overlaps[start..].sort_unstable_by_key(|&(pixel, _)| pixel);
    let mut end = start;
    for i in start..overlaps.len() {
        let entry = overlaps[i];
        if end > start && overlaps[end - 1].0 == entry.0 {
            overlaps[end - 1].1 += entry.1;
        } else {
            overlaps[end] = entry;
            end += 1;
        }
    }
    overlaps.truncate(end);
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
    fn clip(&self, cell: u64, overlaps: &mut Vec<(u32, f64)>, limit: usize) -> Option<i64> {
        let start = overlaps.len();
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
                visit_overlaps::<true>(polygon, self.rows, self.width, |c, r, area| {
                    if area > 1e-12 {
                        if overlaps.len() == limit {
                            return false;
                        }
                        let pixel = (r - self.rows.0) as usize * self.width as usize + c as usize;
                        overlaps.push((pixel as u32, area));
                    }
                    true
                })
            },
        );
        if !completed {
            return None;
        }
        if pieces > 1 {
            merge_seam_overlaps(overlaps, start);
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

/// Visit the cell polygon on each longitude branch touching the raster. A
/// straddling cell uses the same accumulator for the pieces at its two edges.
fn visit_cell_polygons(
    grid: GridKind,
    cell: u64,
    tile: &TileDescriptor,
    longitude_bounds: Option<(f64, f64)>,
    mut visit: impl FnMut(&[(f64, f64)]) -> bool,
) -> bool {
    let mut ring = [(0.0, 0.0); 10];
    let n = grid.boundary_lng_lat(cell, &mut ring);
    if n < 3 || (longitude_bounds.is_some() && grid.interior_pole(cell).is_some()) {
        return false;
    }
    // A pole vertex has been expanded into a longitude cap. `trace` keeps the sides
    // touching the pole straight and traces the rest, so a polar cell follows the
    // same curve as its neighbour along a shared side.
    let mut traced;
    let ring = if edges::corners_suffice(grid, &ring[..n], tile) {
        &mut ring[..n]
    } else {
        match edges::trace(grid, cell, &ring[..n], tile, longitude_bounds) {
            None => return false,
            Some(edges::Traced::Corners) => &mut ring[..n],
            Some(edges::Traced::Ring(points)) => {
                traced = points;
                traced.as_mut_slice()
            }
        }
    };
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
    let mut inline = [(0.0, 0.0); 10];
    let mut extra;
    let pixels = if ring.len() <= inline.len() {
        &mut inline[..ring.len()]
    } else {
        extra = vec![(0.0, 0.0); ring.len()];
        extra.as_mut_slice()
    };
    for &shift in &shifts[..copies] {
        for (point, &(lon, lat)) in pixels.iter_mut().zip(ring.iter()) {
            let Some(pixel) = to_pixel_raw(tile, lon + shift, lat) else {
                return false;
            };
            *point = pixel;
        }
        if !visit(pixels) {
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
    use crate::raster::mosaic::OverlapRule;
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
    ) -> (BTreeMap<u64, ContinuousAccumulator>, usize, usize) {
        let mosaic = Arc::new(
            MosaicReader::open(&[file.path().to_owned()], None, None, OverlapRule::First).unwrap(),
        );
        let mut config = MultiResolutionConfig::single(resolution).with_grid(grid);
        config.transfer = TransferMode::Overlay {
            weighted: false,
            mass_preserve: false,
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
        // of cells is prepared by workers and none is oversized.
        let fine = geographic_raster(60, 48, (-122.45, 37.80), (0.001, 0.001), |c, r| {
            (1 + (3 * c + r) % 23) as f32
        });
        let (serial, serial_batches, _) = overlay_scan(&fine, GridKind::H3, 10, Some(1));
        let (parallel, batches, retries) = overlay_scan(&fine, GridKind::H3, 10, Some(4));
        assert!(serial.len() > 128);
        assert_eq!(serial_batches, 0, "one worker must clip serially");
        assert!(batches > 0, "four workers never prepared a batch");
        assert_eq!(retries, 0);
        assert_eq!(parallel, serial);

        // Narrow columns and tall rows: the mean cell area passes the worker
        // gate, yet a cell crosses more than MAX_PREPARED_OVERLAPS pixels.
        let narrow = geographic_raster(2048, 8, (-122.45, 37.80), (0.000002, 0.25), |c, r| {
            (1 + (c + 3 * r) % 31) as f32
        });
        let (serial, _, _) = overlay_scan(&narrow, GridKind::H3, 8, Some(1));
        let (parallel, batches, retries) = overlay_scan(&narrow, GridKind::H3, 8, Some(4));
        assert!(serial
            .values()
            .any(|a| a.count > MAX_PREPARED_OVERLAPS as f64));
        assert!(batches > 0, "four workers never prepared a batch");
        assert!(retries > 0, "no oversized cell was handed back");
        assert_eq!(parallel, serial);
    }
}
