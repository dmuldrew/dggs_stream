//! The streaming driver of the `sample` and `overlay` transfer modes.

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::aggregator::accumulator::H3Accumulator;
use crate::aggregator::categorical::CategoricalAccumulator;
use crate::aggregator::multi_horizon::coordinates::is_point_in_bbox;
use crate::aggregator::multi_horizon::MultiResolutionConfig;
use crate::aggregator::output_policy::NodataPolicy;
use crate::aggregator::remap::CategoryRemapper;
use crate::crs::transformer::CrsTransformer;
use crate::error::{RasterH3Error, Result};
use crate::grid::{wrap_lon_180, GridKind};
use crate::raster::mosaic::{MosaicReader, TileDescriptor};
use crate::scalar_ops::{grid_polygon_to_cells_wkb_into, ContainmentMode, PolygonToCellsError};

use super::clip::pixel_overlaps;
use super::interp::{interpolate, SampleMethod};
use super::window::{BandSelection, ChunkCache, RowWindow};
use super::TransferMode;

/// Values of one window: enough for wide rasters without holding much more than a
/// chunk band.
const MAX_WINDOW_VALUES: usize = 4 << 20;
/// Cells listed per block, so the cell list stays small next to the window.
const TARGET_BLOCK_CELLS: f64 = 150_000.0;
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

impl TransferAccumulator for H3Accumulator {
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
    /// Past this row (exclusive) of tile `last_tile` nothing more overlaps the cell.
    end_row: i64,
    /// The last tile whose footprint the cell may overlap.
    last_tile: usize,
}

/// Streams `(resolution, cell, accumulator)` for the `sample` and `overlay` modes.
pub struct TransferStream<A: TransferAccumulator> {
    mosaic: Arc<MosaicReader>,
    grid: GridKind,
    mode: TransferMode,
    selection: BandSelection,
    emit_nodata: bool,
    bbox: Option<[f64; 4]>,
    track_quantiles: bool,
    filters: Filters,
    remapper: Option<Arc<CategoryRemapper>>,
    blocks: Vec<Block>,
    /// `horizon[i]`: the northernmost latitude any cell of blocks `i..` can reach.
    horizon: Vec<f64>,
    next_block: usize,
    cache: ChunkCache,
    cache_tile: usize,
    pending: HashMap<u64, Pending<A>>,
    output: VecDeque<(u8, u64, A)>,
    transform_failures: u64,
    failure_counter: Option<Arc<AtomicU64>>,
    strict_transforms: bool,
    /// Runtime telemetry, updated once per block.
    profile: Option<Arc<crate::run_profile::RunProfile>>,
}

impl<A: TransferAccumulator> TransferStream<A> {
    /// A stream over `mosaic` for the non-default `config.transfer`.
    pub fn new(mosaic: Arc<MosaicReader>, config: &MultiResolutionConfig) -> Result<Self> {
        let mode = config.transfer;
        let invalid = |message: &str| {
            Err(RasterH3Error::InvalidParameter(format!(
                "transfer '{}' {message}",
                mode.as_str()
            )))
        };
        match mode {
            TransferMode::AssignCenters => {
                return invalid("is the default scan, not a transfer stream")
            }
            TransferMode::Sample(method) if A::CATEGORICAL && method != SampleMethod::Nearest => {
                return invalid(
                    "of class codes needs sample_method 'nearest': classes cannot be \
                     interpolated",
                )
            }
            _ => {}
        }
        if config.partition.is_some() {
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
                plan_tile(grid, mode, resolution, tile_idx, tile, &mut blocks);
            }
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
            selection: BandSelection {
                band: config.band,
                nodata: config.custom_nodata,
                apply_scale_offset: config.apply_scale_offset && !A::CATEGORICAL,
            },
            emit_nodata: config.nodata_policy == NodataPolicy::Emit,
            bbox: config.bbox,
            track_quantiles: config.track_quantiles(),
            filters: Filters {
                min_count: config.min_count,
                min_mean: config.min_mean,
                max_mean: config.max_mean,
                min_majority_fraction: config.min_majority_fraction,
            },
            remapper: config.remapper.clone(),
            blocks,
            horizon,
            next_block: 0,
            cache: ChunkCache::default(),
            cache_tile: usize::MAX,
            pending: HashMap::new(),
            output: VecDeque::new(),
            transform_failures: 0,
            failure_counter: config.transform_failure_counter.clone(),
            strict_transforms: config.strict_transforms,
            profile: config.profile.clone(),
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

    /// Hand up to `max_rows` finished cells to `consumer`; `0` means the end.
    pub fn drain_into(
        &mut self,
        max_rows: usize,
        mut consumer: impl FnMut(usize, u8, u64, A),
    ) -> Result<usize> {
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
            let block = self.blocks[self.next_block].clone();
            self.next_block += 1;
            self.process(&block)?;
        }
        Ok(drained)
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
            return Err(RasterH3Error::TransformFailures(self.transform_failures));
        }
        Ok(())
    }

    /// Read a window of `tile`, timed as decoding when a profile is attached.
    fn load_window(&mut self, tile: &TileDescriptor, rows: Range<u32>) -> Result<RowWindow> {
        use crate::run_profile::{Counter, Phase, PhaseTimer};
        let timer = self.profile.as_ref().map(|_| PhaseTimer::start());
        let window = RowWindow::load(tile, self.selection, rows, &mut self.cache)?;
        if let (Some(profile), Some(timer)) = (&self.profile, timer) {
            timer.stop(profile, Phase::Decode);
            let pixels = u64::from(window.width()) * window.rows().len() as u64;
            profile.count(Counter::DecodedBytes, pixels * 8);
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
                TransferMode::Sample(method) => self.sample_block(block, bbox, method)?,
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
    fn mapped(&self, value: f64) -> Option<f64> {
        match &self.remapper {
            Some(remapper) if A::CATEGORICAL => {
                remapper.remap(value.round() as i64).map(|c| c as f64)
            }
            _ => Some(value),
        }
    }

    fn sample_block(&mut self, block: &Block, bbox: [f64; 4], method: SampleMethod) -> Result<()> {
        let mosaic = Arc::clone(&self.mosaic);
        let tile = &mosaic.tiles[block.tile];
        let margin = method.margin();
        let rows = block.rows.start.saturating_sub(margin)..block.rows.end.saturating_add(margin);
        let window = self.load_window(tile, rows)?;
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
            if let Some(value) = interpolate(&window, method, col, row).and_then(|v| self.mapped(v))
            {
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
        let window = self.load_window(tile, block.rows.clone())?;
        let width = i64::from(window.width());
        let rows = (i64::from(block.rows.start), i64::from(block.rows.end));
        let band_width = window.width() as usize;
        let cells = cells_in_bbox(self.grid, bbox, block.resolution, ContainmentMode::Covers)?;
        let several_tiles = mosaic.tiles.len() > 1;
        let gt = &tile.reader.metadata.geotransform;

        // `(cell slot, pixel in band, weight)` of every overlap, and each pixel's total.
        let mut overlaps: Vec<(u32, u32, f64)> = Vec::new();
        let mut totals = if mass_preserve {
            vec![0.0f64; band_width * block.rows.len()]
        } else {
            Vec::new()
        };
        let mut slots: Vec<u64> = Vec::new();
        let mut polygon = Vec::with_capacity(10);
        let mut failures = 0;
        for cell in cells {
            let Some((lat, lng)) = self.grid.center_lat_lng(cell) else {
                continue;
            };
            if !is_point_in_bbox(lng, lat, self.bbox) {
                continue;
            }
            let slot = slots.len() as u32;
            let before = overlaps.len();
            let end_row;
            if cell_polygon_in_pixels(self.grid, cell, tile, &mut polygon) {
                end_row = polygon
                    .iter()
                    .fold(f64::NEG_INFINITY, |m, p| m.max(p.1))
                    .ceil() as i64;
                pixel_overlaps(&polygon, rows, width, |c, r, area| {
                    if area > 1e-12 {
                        let pixel = (r - rows.0) as usize * band_width + c as usize;
                        overlaps.push((slot, pixel as u32, if weighted { area } else { 1.0 }));
                    }
                });
            } else if self.grid.interior_pole(cell).is_some() {
                // A cell holding a pole has no polygon in a lon/lat raster: it takes the
                // pixels whose centres it holds.
                end_row = i64::from(tile.reader.metadata.height);
                for r in rows.0..rows.1 {
                    for c in 0..width {
                        let (x, y) = gt.pixel_to_coord(c as f64 + 0.5, r as f64 + 0.5);
                        let inside = tile.crs_transformer.transform_point(x, y).ok().is_some_and(
                            |(lon, lat)| {
                                self.grid.cell_at(lat, wrap_lon_180(lon), block.resolution)
                                    == Some(cell)
                            },
                        );
                        if inside {
                            let pixel = (r - rows.0) as usize * band_width + c as usize;
                            overlaps.push((slot, pixel as u32, 1.0));
                        }
                    }
                }
            } else {
                failures += 1;
                continue;
            }
            if overlaps.len() == before && !self.pending.contains_key(&cell) {
                continue;
            }
            slots.push(cell);
            let last_tile = if several_tiles {
                last_overlapping_tile(&mosaic, self.grid, cell, block.tile)
            } else {
                block.tile
            };
            let track_quantiles = self.track_quantiles;
            let pending = self.pending.entry(cell).or_insert_with(|| Pending {
                acc: A::empty(track_quantiles),
                touched: false,
                end_row,
                last_tile,
            });
            pending.end_row = end_row;
            pending.touched |= overlaps.len() > before;
        }

        // In a mosaic a pixel counts only in the tile that owns it.
        if several_tiles {
            overlaps.retain(|&(_, pixel, _)| {
                let (c, r) = (pixel as usize % band_width, pixel as usize / band_width);
                let (x, y) = gt.pixel_to_coord(c as f64 + 0.5, r as f64 + rows.0 as f64 + 0.5);
                tile.crs_transformer
                    .transform_point(x, y)
                    .is_ok_and(|(lon, lat)| mosaic.is_point_owned_by(block.tile, lon, lat))
            });
        }
        if mass_preserve {
            for &(_, pixel, weight) in &overlaps {
                totals[pixel as usize] += weight;
            }
        }
        for (slot, pixel, weight) in overlaps {
            let (c, r) = (pixel as usize % band_width, pixel as usize / band_width);
            let Some(value) = window
                .get(c as i64, r as i64 + rows.0)
                .and_then(|v| self.mapped(v))
            else {
                continue;
            };
            let weight = if mass_preserve {
                weight / totals[pixel as usize]
            } else {
                weight
            };
            if let Some(pending) = self.pending.get_mut(&slots[slot as usize]) {
                pending.acc.add(value, weight);
            }
        }
        self.record_failures(failures)
    }

    /// Move the cells no later pixel can reach from `pending` to the output.
    fn finish_cells(&mut self, block: &Block) {
        if self.pending.is_empty() {
            return;
        }
        let end = i64::from(block.rows.end);
        let last_block_of_resolution = self
            .blocks
            .get(self.next_block)
            .is_none_or(|next| next.resolution != block.resolution);
        let mut done: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                last_block_of_resolution
                    || (p.last_tile == block.tile && (p.end_row <= end || block.last_of_tile))
                    || p.last_tile < block.tile
            })
            .map(|(cell, _)| *cell)
            .collect();
        // Deterministic output order within a band.
        done.sort_unstable();
        for cell in done {
            if let Some(pending) = self.pending.remove(&cell) {
                let keep = pending.acc.has_samples() || (self.emit_nodata && pending.touched);
                if keep && pending.acc.passes(&self.filters) {
                    self.output.push_back((block.resolution, cell, pending.acc));
                }
            }
        }
    }
}

/// Cut one tile into blocks for `mode` at `resolution`.
fn plan_tile(
    grid: GridKind,
    mode: TransferMode,
    resolution: u8,
    tile_idx: usize,
    tile: &TileDescriptor,
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
        TransferMode::Sample(method) => 2 * method.margin(),
        _ => 0,
    };
    let window_rows = ((MAX_WINDOW_VALUES / width as usize) as u32)
        .saturating_sub(margin)
        .max(1);
    // Whole chunk rows where the budgets allow, so chunks are not decoded twice.
    let chunk_height = tile.reader.chunk_layout.chunk_height.max(1);
    let mut rows = ((block_pixels / f64::from(width)).floor() as u32).clamp(1, window_rows);
    if rows > chunk_height {
        rows -= rows % chunk_height;
    }
    let overlay = matches!(mode, TransferMode::Overlay { .. });
    // A sample block may also be narrower than the raster; an overlay band is not, as
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
            let bbox = block_bbox(grid, resolution, tile, &block_rows, &block_cols, overlay);
            blocks.push(Block {
                resolution,
                tile: tile_idx,
                rows: block_rows,
                cols: block_cols,
                bbox,
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
    for &(lon, lat) in &points {
        south = south.min(lat);
        north = north.max(lat);
        // Longitude relative to the centre, the short way round.
        let offset = (lon - centre_lon + 540.0).rem_euclid(360.0) - 180.0;
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
            let wkb = rectangle_wkb(w, south, e, north);
            match grid_polygon_to_cells_wkb_into(grid, &wkb, i64::from(resolution), mode, &mut part)
            {
                Ok(()) => cells.extend_from_slice(&part),
                Err(PolygonToCellsError::TooManyCells { estimate, limit }) => {
                    return Err(RasterH3Error::InvalidParameter(format!(
                        "transfer: a block of the raster holds about {estimate} cells at \
                         resolution {resolution} (limit {limit}); use a coarser resolution"
                    )))
                }
                Err(e) => {
                    return Err(RasterH3Error::StreamFailed(format!(
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

/// Write the boundary of `cell` in the pixel coordinates of `tile` to `out`. `false`
/// if the cell has no usable polygon there: a vertex could not be located, or the cell
/// holds a pole of a lon/lat raster.
fn cell_polygon_in_pixels(
    grid: GridKind,
    cell: u64,
    tile: &TileDescriptor,
    out: &mut Vec<(f64, f64)>,
) -> bool {
    out.clear();
    let mut ring = [(0.0, 0.0); 10];
    let n = grid.boundary_lng_lat(cell, &mut ring);
    if n < 3 {
        return false;
    }
    let geographic = matches!(tile.crs_transformer, CrsTransformer::Wgs84Identity);
    if geographic && grid.interior_pole(cell).is_some() {
        return false;
    }
    let first_lon = ring[0].0;
    for &(lon, lat) in &ring[..n] {
        // In a lon/lat raster the ring must not jump at the antimeridian.
        let lon = if geographic {
            first_lon + ((lon - first_lon + 540.0).rem_euclid(360.0) - 180.0)
        } else {
            lon
        };
        match to_pixel_raw(tile, lon, lat) {
            Some(point) => out.push(point),
            None => return false,
        }
    }
    if geographic {
        // Bring the ring onto the raster's columns if it sits a whole turn away.
        let width = f64::from(tile.reader.metadata.width);
        let turn = (360.0 / tile.reader.metadata.geotransform.a).abs();
        if turn.is_finite() && turn > 0.0 {
            let centre = out.iter().map(|p| p.0).sum::<f64>() / out.len() as f64;
            let shift = if centre < 0.0 && centre + turn < width + turn / 2.0 {
                turn
            } else if centre >= width && centre - turn > -turn / 2.0 {
                -turn
            } else {
                0.0
            };
            if shift != 0.0 && (0.0..width).contains(&(centre + shift)) {
                for point in out.iter_mut() {
                    point.0 += shift;
                }
            }
        }
    }
    true
}

/// The highest tile index from `from` on whose footprint may overlap `cell`.
fn last_overlapping_tile(mosaic: &MosaicReader, grid: GridKind, cell: u64, from: usize) -> usize {
    let Some([west, south, east, north]) = grid.wgs84_bbox(cell) else {
        return from;
    };
    let mut last = from;
    for (idx, tile) in mosaic.tiles.iter().enumerate().skip(from + 1) {
        let [t_west, t_south, t_east, t_north] = tile.bounds_wgs84;
        let lat_overlap = south <= t_north && north >= t_south;
        // Boxes that cross the antimeridian are taken to overlap in longitude.
        let lon_overlap = west > east || t_west > t_east || (west <= t_east && east >= t_west);
        if lat_overlap && lon_overlap {
            last = idx;
        }
    }
    last
}
