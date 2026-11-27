//! Physical-area overlay for a single north-up WGS84 raster.
use super::{
    cells_in_bbox, pad_block_extent, periodic_longitude_bounds, plan_tile, project_cell_polygons,
    raw_block_extent, Block, ContainmentMode, OverlapParallelism, ProjectedCell, ProjectedRings,
    RawBlockExtent, COARSE_CELL_DIAMETER_KM, DIRECT_BATCH_CELLS, MIN_PARALLEL_DIRECT_CELLS,
};
use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::area_accumulator::AreaAccumulator;
use crate::aggregator::multi_horizon::{
    area_continuous::validate_ground_area_raster, coordinates::is_point_in_bbox,
    MultiResolutionConfig,
};
use crate::aggregator::{NodataPolicy, SamplingPattern};
use crate::error::{DggsStreamError, Result};
use crate::grid::{wrap_lon_180, GridKind};
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::geotransform::GeoTransform;
use crate::raster::ground_area::wgs84_pixel_area;
use crate::raster::mosaic::TileDescriptor;
use crate::raster::prefetch::PrefetchedChunkReader;
use crate::run_profile::{Counter, Phase, PhaseTimer};
use crate::transfer::clip::{visit_ground_area_spans_into, ClipScratch};
use crate::transfer::window::{chunk_is_absent, BandSelection, ChunkCache, RowWindow};
use crate::transfer::TransferMode;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::collections::BTreeMap;
use std::ops::Range;

/// Stream (resolution, cell ID, statistics) from the adaptively traced cell
/// polygons used by weighted overlay. Set `config.transfer = TransferMode::OVERLAY`.
/// Pixel count is the sum of planar pixel fractions. Retained area and the
/// coverage threshold use ellipsoidal square metres. For densities per m² the
/// weighted sum is the integrated quantity. A bbox selects whole pixels by center.
/// Coarse resolutions and pole-containing candidate cells requiring the ordinary
/// overlay's center fallback are unsupported.
///
/// All resolutions share one pass, and cells are yielded in completion order, so
/// resolutions interleave. Take a prefix only if any subset of completed cells
/// will do.
pub fn scan_ground_area_overlay(
    reader: GeoTiffStreamReader,
    config: &MultiResolutionConfig,
) -> Result<impl Iterator<Item = Result<(u8, u64, AreaAccumulator)>> + Send + 'static> {
    let tile = TileDescriptor::new(0, reader, config.custom_crs.as_deref())?;
    ground_area_overlay_tile(tile, config)
}

pub(crate) fn ground_area_overlay_tile(
    tile: TileDescriptor,
    config: &MultiResolutionConfig,
) -> Result<impl Iterator<Item = Result<(u8, u64, AreaAccumulator)>> + Send + 'static> {
    config.validate()?;
    validate_ground_area_raster(&tile.reader, config)?;
    if config.transfer != TransferMode::OVERLAY
        || config.nodata_policy != NodataPolicy::Omit
        || config.sampling != SamplingPattern::center()
        || config.track_quantiles()
        || config.histogram.is_some()
        || config.spectral_formula.is_some()
        || config.remapper.is_some()
        || config.should_compact_h3_children()
        || config.min_majority_fraction.is_some()
    {
        return Err(DggsStreamError::InvalidParameter("ground-area overlay requires weighted continuous overlay with omitted NoData and no mass normalization, supersampling, compaction, quantiles, histogram or formula".into()));
    }
    let meta = &tile.reader.metadata;
    let gt = &meta.geotransform;
    let mut columns = Vec::<Range<u32>>::new();
    let mut rows = 0..meta.height;
    if let Some(bbox) = config.bbox {
        // The transform is north-up. Latitude decreases monotonically and
        // wrapped longitude can select two disjoint runs across the seam.
        let latitude = |r: u32| gt.pixel_to_coord(0.0, f64::from(r) + 0.5).1;
        let mut selected = rows.clone().filter(|&r| {
            let lat = latitude(r);
            lat >= bbox[1] && lat <= bbox[3]
        });
        rows = selected.next().map_or(0..0, |first| {
            first..selected.next_back().unwrap_or(first) + 1
        });
        for c in 0..meta.width {
            let lon = gt.pixel_to_coord(f64::from(c) + 0.5, 0.0).0;
            if is_point_in_bbox(wrap_lon_180(lon), bbox[1], Some(bbox)) {
                match columns.last_mut() {
                    Some(run) if run.end == c => run.end += 1,
                    _ => columns.push(c..c + 1),
                }
            }
        }
    } else {
        columns.push(0..meta.width);
    }
    let parallelism = OverlapParallelism::from_workers(config.aggregation_workers)?;
    let layout = tile.reader.chunk_layout;
    let chunk_cols_across = if columns.len() == 1 {
        let c = &columns[0];
        if c.is_empty() {
            0
        } else {
            (c.end.div_ceil(layout.chunk_width) - c.start / layout.chunk_width) as usize
        }
    } else {
        layout.chunks_across as usize
    };
    let one_row_native_bytes = chunk_cols_across.saturating_mul(tile.reader.decoded_chunk_bytes());
    let allow_chunk_row_window = one_row_native_bytes <= config.decode_budget_bytes;
    for &res in &config.resolutions {
        if config.grid.spec().max_cell_diameter_km(res) > COARSE_CELL_DIAMETER_KM {
            return Err(DggsStreamError::InvalidParameter("ground-area overlay requires a valid resolution with polygon clipping (cell diameter bound at most 500 km)".into()));
        }
    }
    let mut blocks = Vec::new();
    if !config.resolutions.is_empty() && !rows.is_empty() && !columns.is_empty() {
        let finest_res = config.resolutions.iter().copied().max().unwrap_or(0);
        let mut bands = Vec::new();
        plan_tile(
            config.grid,
            config.transfer,
            finest_res,
            finest_res,
            &config.sampling,
            false,
            0,
            &tile,
            1,
            allow_chunk_row_window,
            &mut bands,
        );
        for mut block in bands {
            block.rows = block.rows.start.max(rows.start)..block.rows.end.min(rows.end);
            if columns.len() == 1 {
                block.cols = columns[0].clone();
            }
            block.last_of_tile = false;
            if !block.rows.is_empty() && !block.cols.is_empty() {
                blocks.push(block);
            }
        }
        if let Some(last) = blocks.last_mut() {
            last.last_of_tile = true;
        }
    }
    let config = config.clone();
    let mut cache = ChunkCache::default();
    let mut prefetcher: Option<PrefetchedChunkReader> = None;
    let mut prefetch_initialized = false;
    let mut pending: Vec<BTreeMap<u64, Cell>> = (0..config.resolutions.len())
        .map(|_| BTreeMap::new())
        .collect();
    let mut ready = Vec::new().into_iter();
    let mut next_block = 0usize;
    let mut failed = false;
    Ok(std::iter::from_fn(move || {
        while !failed {
            if let Some(row) = ready.next() {
                return Some(Ok(row));
            }
            let block = blocks.get(next_block)?.clone();
            if !prefetch_initialized {
                prefetch_initialized = true;
                let layout = tile.reader.chunk_layout;
                let height = tile.reader.metadata.height;
                let max_chunk_rows = blocks
                    .iter()
                    .filter_map(|b| {
                        let r = b.rows.start.min(height)..b.rows.end.min(height);
                        (!r.is_empty()).then(|| {
                            ((r.end - 1) / layout.chunk_height - r.start / layout.chunk_height + 1)
                                as usize
                        })
                    })
                    .max()
                    .unwrap_or(0);
                let max_window_bytes = max_chunk_rows
                    .saturating_mul(chunk_cols_across)
                    .saturating_mul(tile.reader.decoded_chunk_bytes());
                // Masked windows widen samples to f64, so the native-width estimate
                // understates them: keep the byte-limited cache for those rasters.
                let native_window = !tile.reader.metadata.has_internal_mask();
                if native_window && max_window_bytes <= config.decode_budget_bytes {
                    cache = ChunkCache::default();
                    let misses = plan_chunk_misses(&tile, &blocks);
                    if !misses.is_empty()
                        && (tile.reader.is_remote()
                            || (config.decode_workers > 1 && misses.len() > 2))
                    {
                        let chunk_bytes = tile.reader.decoded_chunk_bytes().max(1);
                        let budget_chunks = (config.decode_budget_bytes / chunk_bytes).max(16);
                        let target_lookahead = chunk_cols_across
                            .max(config.prefetch_chunks)
                            .max(config.decode_workers * 4)
                            .min(512);
                        let queue_cap = target_lookahead
                            .min(budget_chunks)
                            .min(misses.len())
                            .max(16);
                        prefetcher = Some(PrefetchedChunkReader::spawn_with_profile(
                            tile.reader.clone(),
                            misses,
                            queue_cap,
                            config.decode_workers,
                            config.fetch_workers,
                            config.profile.clone(),
                        ));
                    }
                } else {
                    cache = ChunkCache::with_budget(config.decode_budget_bytes);
                }
            }
            next_block += 1;
            let mut run = || {
                scan_block(
                    &tile,
                    &config,
                    &block,
                    &columns,
                    &mut cache,
                    prefetcher.as_ref(),
                    &mut pending,
                    &parallelism,
                )
            };
            let result = match &config.profile {
                Some(profile) => {
                    profile.count(Counter::Windows, 1);
                    profile.count(
                        Counter::PixelsRead,
                        block.rows.len() as u64 * block.cols.len() as u64,
                    );
                    let result = profile.time(Phase::Kernel, run);
                    // Decoded chunks held by the window cache only. A prefetcher
                    // runs only when the whole window fits the budget, and its
                    // queue (capped in chunks) is not counted; the window's own
                    // copy of its rows is bounded by the window size instead.
                    profile.peak(Counter::PeakDecodedBytes, cache.peak_bytes() as u64);
                    result
                }
                None => run(),
            };
            match result {
                Ok(rows) => ready = rows.into_iter(),
                Err(err) => {
                    failed = true;
                    return Some(Err(err));
                }
            }
        }
        None
    }))
}

fn plan_chunk_misses(tile: &TileDescriptor, run_blocks: &[Block]) -> Vec<u32> {
    let meta = &tile.reader.metadata;
    let (width, height) = (meta.width, meta.height);
    let layout = tile.reader.chunk_layout;
    let alpha_index = meta
        .alpha_sample
        .map(usize::from)
        .filter(|&a| a < usize::from(meta.samples_per_pixel.max(1)));
    let mut cached: FxHashSet<u32> = FxHashSet::default();
    let mut misses = Vec::new();
    for block in run_blocks {
        let rows = block.rows.start.min(height)..block.rows.end.min(height);
        let cols = block.cols.start.min(width)..block.cols.end.min(width);
        if rows.is_empty() || cols.is_empty() || width == 0 {
            continue;
        }
        let chunk_rows = rows.start / layout.chunk_height..=(rows.end - 1) / layout.chunk_height;
        let chunk_cols = cols.start / layout.chunk_width..cols.end.div_ceil(layout.chunk_width);
        cached.retain(|&index| {
            let chunk_row = index / layout.chunks_across;
            let chunk_col = index % layout.chunks_across;
            chunk_rows.contains(&chunk_row) && chunk_cols.contains(&chunk_col)
        });
        for chunk_row in chunk_rows {
            for chunk_col in chunk_cols.clone() {
                let index = chunk_row * layout.chunks_across + chunk_col;
                let bounds = layout.get_chunk_bounds(index, width, height);
                if bounds.width == 0 || bounds.height == 0 {
                    continue;
                }
                if chunk_is_absent(&tile.reader, index, &[0], alpha_index) {
                    continue;
                }
                if cached.insert(index) {
                    misses.push(index);
                }
            }
        }
    }
    misses
}

#[derive(Default)]
struct Cell {
    stats: AreaAccumulator,
    covered_m2: f64,
    end_row: i64,
}

struct GroundAreaCtx<'a> {
    grid: GridKind,
    tile: &'a TileDescriptor,
    window: &'a RowWindow,
    gt: &'a GeoTransform,
    longitude: Option<(f64, f64)>,
    rows: (i64, i64),
    cols: Range<u32>,
    width: i64,
    row_areas: &'a [f64],
    bbox: Option<[f64; 4]>,
    need_coverage: bool,
}

#[derive(Default)]
struct GroundAreaWorkerScratch {
    clip: ClipScratch,
    seam_pixels: Vec<((i64, i64), f64, f64)>,
}

#[derive(Default)]
enum GroundAreaOutcome {
    #[default]
    Empty,
    NoDataOnly {
        end_row: i64,
    },
    Accumulated {
        end_row: i64,
        stats: AreaAccumulator,
        covered_m2: f64,
    },
    Invalid(DggsStreamError),
}

fn clip_and_accumulate_ground_area_cell(
    id: u64,
    ctx: &GroundAreaCtx<'_>,
    scratch: &mut GroundAreaWorkerScratch,
) -> GroundAreaOutcome {
    let mut rings = ProjectedRings::default();
    let copies = match project_cell_polygons(ctx.grid, id, ctx.tile, ctx.longitude, &mut rings) {
        ProjectedCell::Polygons { count } => count,
        ProjectedCell::Pole | ProjectedCell::Failed => {
            return GroundAreaOutcome::Invalid(DggsStreamError::InvalidParameter(format!(
                    "ground-area overlay cannot represent the boundary of cell {id:x} in longitude/latitude"
                )));
        }
    };
    if copies == 0 {
        return GroundAreaOutcome::Empty;
    }

    let mut last_row = f64::NEG_INFINITY;
    let mut any_data = false;
    let check_data = !ctx.need_coverage;
    for copy in 0..copies {
        let poly = rings.ring(copy);
        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        for &(x, y) in poly {
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
        return GroundAreaOutcome::NoDataOnly { end_row };
    }

    let mut stats = AreaAccumulator::default();
    let mut covered_m2 = 0.0f64;
    let mut touched = false;

    if copies == 1 {
        let mut span_error: Option<DggsStreamError> = None;
        let completed = visit_ground_area_spans_into(
            rings.ring(0),
            ctx.rows,
            ctx.width,
            ctx.gt,
            ctx.row_areas,
            &mut scratch.clip,
            |r, c0, c1, fraction, m2| {
                let c0 = c0.max(i64::from(ctx.cols.start));
                let c1 = c1.min(i64::from(ctx.cols.end));
                if c0 >= c1 {
                    return true;
                }
                touched = true;
                let row_off = (r - ctx.rows.0) as usize;
                if fraction == 1.0 && (c1 - c0) >= 4 && ctx.bbox.is_none() {
                    covered_m2 += (c1 - c0) as f64 * m2;
                    let mut unit = ContinuousAccumulator::default();
                    let valid = ctx.window.accumulate_unit_span(
                        0,
                        row_off,
                        c0 as usize,
                        c1 as usize,
                        &mut unit,
                    );
                    if valid > 0 {
                        if let Err(err) = stats.merge_unit_span(&unit, m2) {
                            span_error = Some(err);
                            return false;
                        }
                        if !stats.is_finite() {
                            span_error = Some(DggsStreamError::InvalidParameter(
                                "overflow in ground-area overlay statistics".into(),
                            ));
                            return false;
                        }
                    }
                    return true;
                }
                for c in c0..c1 {
                    if let Some(bbox) = ctx.bbox {
                        let (lon, lat) = ctx.gt.pixel_to_coord(c as f64 + 0.5, r as f64 + 0.5);
                        if !is_point_in_bbox(wrap_lon_180(lon), lat, Some(bbox)) {
                            continue;
                        }
                    }
                    covered_m2 += m2;
                    if let Some(value) = ctx.window.band_value_at(0, row_off, c as usize) {
                        if let Err(err) = stats.update_overlap(value, fraction.min(1.0), m2) {
                            span_error = Some(err);
                            return false;
                        }
                        if !stats.is_finite() {
                            span_error = Some(DggsStreamError::InvalidParameter(
                                "overflow in ground-area overlay statistics".into(),
                            ));
                            return false;
                        }
                    }
                }
                true
            },
        );
        if let Some(err) = span_error {
            return GroundAreaOutcome::Invalid(err);
        }
        if !completed {
            return GroundAreaOutcome::Invalid(DggsStreamError::InvalidParameter(format!(
                "ground-area overlay cannot represent the boundary of cell {id:x} in longitude/latitude"
            )));
        }
    } else {
        scratch.seam_pixels.clear();
        for copy in 0..copies {
            let completed = visit_ground_area_spans_into(
                rings.ring(copy),
                ctx.rows,
                ctx.width,
                ctx.gt,
                ctx.row_areas,
                &mut scratch.clip,
                |r, c0, c1, fraction, m2| {
                    for c in c0..c1 {
                        scratch.seam_pixels.push(((r, c), fraction, m2));
                    }
                    true
                },
            );
            if !completed {
                return GroundAreaOutcome::Invalid(DggsStreamError::InvalidParameter(format!(
                    "ground-area overlay cannot represent the boundary of cell {id:x} in longitude/latitude"
                )));
            }
        }
        if !scratch.seam_pixels.is_empty() {
            scratch.seam_pixels.sort_unstable_by_key(|p| p.0);
            let mut iter = scratch.seam_pixels.drain(..).peekable();
            while let Some(((r, c), mut fraction, mut m2)) = iter.next() {
                while let Some((_, f, a)) = iter.next_if(|p| p.0 == (r, c)) {
                    fraction += f;
                    m2 += a;
                }
                if let Some(bbox) = ctx.bbox {
                    let (lon, lat) = ctx.gt.pixel_to_coord(c as f64 + 0.5, r as f64 + 0.5);
                    if !is_point_in_bbox(wrap_lon_180(lon), lat, Some(bbox)) {
                        continue;
                    }
                }
                touched = true;
                covered_m2 += m2;
                let row_off = (r - ctx.rows.0) as usize;
                if let Some(value) = ctx.window.band_value_at(0, row_off, c as usize) {
                    if let Err(err) = stats.update_overlap(value, fraction.min(1.0), m2) {
                        return GroundAreaOutcome::Invalid(err);
                    }
                    if !stats.is_finite() {
                        return GroundAreaOutcome::Invalid(DggsStreamError::InvalidParameter(
                            "overflow in ground-area overlay statistics".into(),
                        ));
                    }
                }
            }
        }
    }

    if !touched || (!ctx.need_coverage && stats.retained_area_m2() == 0.0) {
        return GroundAreaOutcome::NoDataOnly { end_row };
    }
    GroundAreaOutcome::Accumulated {
        end_row,
        stats,
        covered_m2,
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_block(
    tile: &TileDescriptor,
    config: &MultiResolutionConfig,
    block: &Block,
    columns: &[Range<u32>],
    cache: &mut ChunkCache,
    prefetcher: Option<&PrefetchedChunkReader>,
    pending_by_res: &mut [BTreeMap<u64, Cell>],
    parallelism: &OverlapParallelism,
) -> Result<Vec<(u8, u64, AreaAccumulator)>> {
    let decode = prefetcher
        .is_none()
        .then(|| config.profile.as_ref().map(|_| PhaseTimer::start()))
        .flatten();
    let window = RowWindow::load_bands(
        tile,
        &[1],
        BandSelection {
            band: 1,
            nodata: config.custom_nodata,
            apply_scale_offset: config.apply_scale_offset,
        },
        block.rows.clone(),
        block.cols.clone(),
        cache,
        prefetcher,
        config.profile.as_deref(),
    )?;
    if let (Some(profile), Some(timer)) = (&config.profile, decode) {
        timer.stop(profile, Phase::Decode);
    }
    let gt = &tile.reader.metadata.geotransform;
    let row_areas: Vec<_> = block
        .rows
        .clone()
        .map(|r| wgs84_pixel_area(gt, 0, r as usize))
        .collect::<Result<_>>()?;
    let rows = (i64::from(block.rows.start), i64::from(block.rows.end));
    let longitude = periodic_longitude_bounds(tile);
    let raw_extents: Vec<RawBlockExtent> = columns
        .iter()
        .map(|cols| {
            raw_block_extent(tile, &block.rows, cols).ok_or_else(|| {
                DggsStreamError::InvalidParameter("invalid WGS84 overlay extent".into())
            })
        })
        .collect::<Result<_>>()?;

    let ctx = GroundAreaCtx {
        grid: config.grid,
        tile,
        window: &window,
        gt,
        longitude,
        rows,
        cols: block.cols.clone(),
        width: i64::from(window.width()),
        row_areas: &row_areas,
        bbox: config.bbox,
        need_coverage: config.valid_coverage_threshold.is_some_and(|t| t > 0.0),
    };

    let mut ready = Vec::new();
    let mut outcomes: Vec<GroundAreaOutcome> = Vec::new();
    for (res_idx, &resolution) in config.resolutions.iter().enumerate() {
        let mut candidates = Vec::new();
        for ext in &raw_extents {
            let bbox = pad_block_extent(ext, config.grid, resolution, true);
            candidates.extend(cells_in_bbox(
                config.grid,
                bbox,
                resolution,
                ContainmentMode::Covers,
            )?);
        }
        if raw_extents.len() > 1 {
            candidates.sort_unstable();
            candidates.dedup();
        }

        let pending = &mut pending_by_res[res_idx];
        let mut apply_outcome = |id: u64, outcome: GroundAreaOutcome| -> Result<()> {
            match outcome {
                GroundAreaOutcome::Empty => {}
                GroundAreaOutcome::NoDataOnly { end_row } => {
                    if let Some(cell) = pending.get_mut(&id) {
                        cell.end_row = end_row;
                    }
                }
                GroundAreaOutcome::Invalid(err) => return Err(err),
                GroundAreaOutcome::Accumulated {
                    end_row,
                    stats,
                    covered_m2,
                } => {
                    let cell = pending.entry(id).or_default();
                    cell.end_row = end_row;
                    cell.covered_m2 += covered_m2;
                    cell.stats.merge(&stats);
                    if !cell.stats.is_finite() {
                        return Err(DggsStreamError::InvalidParameter(
                            "overflow in ground-area overlay statistics".into(),
                        ));
                    }
                }
            }
            Ok(())
        };

        if parallelism.should_run_parallel(candidates.len(), MIN_PARALLEL_DIRECT_CELLS) {
            for batch in candidates.chunks(DIRECT_BATCH_CELLS) {
                outcomes.resize_with(batch.len(), GroundAreaOutcome::default);
                parallelism.install(|| {
                    outcomes.par_iter_mut().zip(batch).for_each_init(
                        GroundAreaWorkerScratch::default,
                        |scratch, (out, &id)| {
                            *out = clip_and_accumulate_ground_area_cell(id, &ctx, scratch);
                        },
                    );
                });
                for (&id, outcome) in batch.iter().zip(outcomes.drain(..)) {
                    apply_outcome(id, outcome)?;
                }
            }
        } else {
            let mut scratch = GroundAreaWorkerScratch::default();
            for &id in &candidates {
                apply_outcome(
                    id,
                    clip_and_accumulate_ground_area_cell(id, &ctx, &mut scratch),
                )?;
            }
        }

        pending.retain(|id, cell| {
            if cell.end_row > rows.1 && !block.last_of_tile {
                return true;
            }
            let a = &cell.stats;
            if a.retained_area_m2() > 0.0
                && config
                    .valid_coverage_threshold
                    .is_none_or(|t| a.retained_area_m2() >= t * cell.covered_m2)
                && config.min_count.is_none_or(|n| a.pixel_count() >= n)
                && config.min_mean.is_none_or(|n| a.area_stats().mean() >= n)
                && config.max_mean.is_none_or(|n| a.area_stats().mean() <= n)
            {
                ready.push((resolution, *id, std::mem::take(&mut cell.stats)));
            }
            false
        });
    }
    Ok(ready)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cells_crossing_a_window_boundary_are_emitted_once() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/internal-mask.tif");
        let tile = TileDescriptor::new(0, GeoTiffStreamReader::open(path).unwrap(), None).unwrap();
        let mut config = MultiResolutionConfig::single(10);
        config.transfer = TransferMode::OVERLAY;
        let width = tile.reader.metadata.width;
        let height = tile.reader.metadata.height;
        let run = |ends: &[u32]| {
            let mut cells = BTreeMap::new();
            let mut pending = vec![BTreeMap::new()];
            let mut cache = ChunkCache::with_budget(256);
            let mut start = 0;
            for &end in ends {
                let rows = start..end;
                let cols = 0..width;
                let block = Block {
                    tile: 0,
                    rows: rows.clone(),
                    cols: cols.clone(),
                    extent: raw_block_extent(&tile, &rows, &cols),
                    last_of_tile: end == height,
                };
                for (_, id, stats) in scan_block(
                    &tile,
                    &config,
                    &block,
                    std::slice::from_ref(&(0..width)),
                    &mut cache,
                    None,
                    &mut pending,
                    &OverlapParallelism::Serial,
                )
                .unwrap()
                {
                    assert!(cells.insert(id, stats).is_none());
                }
                if end < height {
                    assert!(!pending[0].is_empty());
                }
                start = end;
            }
            assert!(pending[0].is_empty());
            cells
        };
        let split = run(&[8, height]);
        let whole = run(&[height]);
        assert_eq!(
            split.keys().collect::<Vec<_>>(),
            whole.keys().collect::<Vec<_>>()
        );
        for (id, a) in &split {
            let b = &whole[id];
            assert!(
                (a.pixel_count() - b.pixel_count()).abs() <= 1e-12,
                "{id:x} pixel_count"
            );
            assert!(
                (a.retained_area_m2() - b.retained_area_m2()).abs()
                    <= 1e-12 * b.retained_area_m2().max(1.0),
                "{id:x} area"
            );
            assert!(
                (a.area_weighted_sum() - b.area_weighted_sum()).abs()
                    <= 1e-12 * b.area_weighted_sum().abs().max(1.0),
                "{id:x} sum"
            );
            let (sa, sb) = (a.area_stats(), b.area_stats());
            assert_eq!(sa.min, sb.min, "{id:x} min");
            assert_eq!(sa.max, sb.max, "{id:x} max");
            assert!(
                (sa.m2 - sb.m2).abs() <= 1e-12 * sb.m2.abs().max(1.0),
                "{id:x} m2"
            );
            assert!(
                (sa.sum_w2 - sb.sum_w2).abs() <= 1e-12 * sb.sum_w2.abs().max(1.0),
                "{id:x} sum_w2"
            );
        }
    }

    #[test]
    fn parallel_ground_area_overlay_matches_serial_and_skips_nodata() {
        use tiff::encoder::{colortype, TiffEncoder};
        use tiff::tags::Tag;

        let nodata = -9999.0f32;
        let (width, height) = (64u32, 64u32);
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = TiffEncoder::new(file.as_file()).unwrap();
        let mut image = encoder
            .new_image::<colortype::Gray32Float>(width, height)
            .unwrap();
        image.rows_per_strip(16).unwrap();
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
        tags.write_tag(Tag::Unknown(42113), "-9999").unwrap();
        let pixels: Vec<f32> = (0..height)
            .flat_map(|r| {
                // Leave strip 1 (rows 16..32) all-NoData to exercise AllNoData skip across window boundaries.
                let all_nd = (16..32).contains(&r);
                (0..width).map(move |c| {
                    if all_nd {
                        nodata
                    } else {
                        ((r * 5 + c) % 23 + 1) as f32
                    }
                })
            })
            .collect();
        image.write_data(&pixels).unwrap();

        let run = |workers: Option<usize>| {
            let reader = GeoTiffStreamReader::open(file.path()).unwrap();
            let mut cfg = MultiResolutionConfig::single(9);
            cfg.transfer = TransferMode::OVERLAY;
            cfg.aggregation_workers = workers;
            let mut cells = BTreeMap::new();
            for row in scan_ground_area_overlay(reader, &cfg).unwrap() {
                let (res, id, acc) = row.unwrap();
                assert!(cells.insert((res, id), acc).is_none());
            }
            cells
        };

        let serial = run(Some(1));
        let dedicated = run(Some(4));
        let ambient = run(None);
        assert!(!serial.is_empty());
        for other in [&dedicated, &ambient] {
            assert_eq!(other.len(), serial.len());
            for (k, a) in &serial {
                let b = &other[k];
                assert!((a.pixel_count() - b.pixel_count()).abs() < 1e-12);
                assert!((a.retained_area_m2() - b.retained_area_m2()).abs() < 1e-6);
                assert!((a.area_weighted_sum() - b.area_weighted_sum()).abs() < 1e-6);
                assert!((a.area_stats().variance() - b.area_stats().variance()).abs() < 1e-10);
            }
        }
    }
}
