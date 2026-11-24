//! Physical-area overlay for a single north-up WGS84 raster.
use super::{
    block_bbox, cells_in_bbox, periodic_longitude_bounds, plan_tile, visit_cell_polygons, Block,
    ContainmentMode, COARSE_CELL_DIAMETER_KM,
};
use crate::aggregator::area_accumulator::AreaAccumulator;
use crate::aggregator::multi_horizon::{
    area_continuous::validate_ground_area_raster, coordinates::is_point_in_bbox,
    MultiResolutionConfig,
};
use crate::aggregator::{NodataPolicy, SamplingPattern};
use crate::error::{DggsStreamError, Result};
use crate::grid::wrap_lon_180;
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::ground_area::wgs84_pixel_area;
use crate::raster::mosaic::TileDescriptor;
use crate::raster::prefetch::PrefetchedChunkReader;
use crate::run_profile::{Counter, Phase, PhaseTimer};
use crate::transfer::clip::{visit_ground_area_spans_into, ClipScratch};
use crate::transfer::window::{chunk_is_absent, BandSelection, ChunkCache, RowWindow};
use crate::transfer::TransferMode;
use rustc_hash::FxHashSet;
use std::collections::BTreeMap;
use std::ops::Range;

/// Stream (resolution, cell ID, statistics) from the straight-edged cell
/// polygons used by weighted overlay. Set `config.transfer = TransferMode::OVERLAY`.
/// Pixel count is the sum of planar pixel fractions. Retained area and the
/// coverage threshold use ellipsoidal square metres. For densities per m² the
/// weighted sum is the integrated quantity. A bbox selects whole pixels by center.
/// Coarse resolutions and pole-containing candidate cells requiring the ordinary
/// overlay's center fallback are unsupported.
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
    let mut blocks = Vec::new();
    for &res in &config.resolutions {
        if config.grid.spec().max_cell_diameter_km(res) > COARSE_CELL_DIAMETER_KM {
            return Err(DggsStreamError::InvalidParameter("ground-area overlay requires a valid resolution with polygon clipping (cell diameter bound at most 500 km)".into()));
        }
        if !rows.is_empty() && !columns.is_empty() {
            let mut bands = Vec::new();
            plan_tile(config.grid, config.transfer, res, 0, &tile, 1, &mut bands);
            for mut block in bands {
                block.rows = block.rows.start.max(rows.start)..block.rows.end.min(rows.end);
                if !block.rows.is_empty() {
                    block.last_of_tile = block.rows.end == rows.end;
                    blocks.push(block);
                }
            }
        }
    }
    let config = config.clone();
    let mut cache = ChunkCache::default();
    let mut prefetcher: Option<PrefetchedChunkReader> = None;
    let mut current_resolution: Option<u8> = None;
    let mut pending = BTreeMap::new();
    let mut ready = Vec::new().into_iter();
    let mut next_block = 0usize;
    let mut failed = false;
    Ok(std::iter::from_fn(move || {
        while !failed {
            if let Some(row) = ready.next() {
                return Some(Ok(row));
            }
            let block = blocks.get(next_block)?.clone();
            if current_resolution != Some(block.resolution) {
                cache.clear_and_recycle(prefetcher.as_ref());
                prefetcher = None;
                current_resolution = Some(block.resolution);
                let run_end = next_block
                    + blocks[next_block..]
                        .iter()
                        .take_while(|b| b.resolution == block.resolution)
                        .count();
                let layout = tile.reader.chunk_layout;
                let height = tile.reader.metadata.height;
                let max_chunk_rows = blocks[next_block..run_end]
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
                    .saturating_mul(layout.chunks_across as usize)
                    .saturating_mul(tile.reader.decoded_chunk_bytes());
                if max_window_bytes <= config.decode_budget_bytes {
                    cache = ChunkCache::default();
                    let misses = plan_full_width_chunk_misses(&tile, &blocks[next_block..run_end]);
                    if !misses.is_empty()
                        && (tile.reader.is_remote()
                            || (config.decode_workers > 1 && misses.len() > 2))
                    {
                        let chunk_bytes = tile.reader.decoded_chunk_bytes().max(1);
                        let budget_chunks = (config.decode_budget_bytes / chunk_bytes).max(16);
                        let target_lookahead = (layout.chunks_across as usize)
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
                )
            };
            let result = match &config.profile {
                Some(profile) => {
                    profile.count(Counter::Windows, 1);
                    profile.count(
                        Counter::PixelsRead,
                        block.rows.len() as u64 * block.cols.len() as u64,
                    );
                    profile.time(Phase::Kernel, run)
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

fn plan_full_width_chunk_misses(tile: &TileDescriptor, run_blocks: &[Block]) -> Vec<u32> {
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
        if rows.is_empty() || width == 0 {
            continue;
        }
        let chunk_rows = rows.start / layout.chunk_height..=(rows.end - 1) / layout.chunk_height;
        cached.retain(|&index| chunk_rows.contains(&(index / layout.chunks_across)));
        for chunk_row in chunk_rows {
            for chunk_col in 0..layout.chunks_across {
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

fn scan_block(
    tile: &TileDescriptor,
    config: &MultiResolutionConfig,
    block: &Block,
    columns: &[Range<u32>],
    cache: &mut ChunkCache,
    prefetcher: Option<&PrefetchedChunkReader>,
    pending: &mut BTreeMap<u64, Cell>,
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
        // Not a partitioned scan: decode the tile's full width, as before windows
        // could be limited to a partition's columns.
        0..tile.reader.metadata.width,
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
    let mut overlaps = Vec::new();
    let mut clip_scratch = ClipScratch::default();
    let mut candidates = Vec::new();
    for cols in columns {
        let bbox = block_bbox(config.grid, block.resolution, tile, &block.rows, cols, true)
            .ok_or_else(|| {
                DggsStreamError::InvalidParameter("invalid WGS84 overlay extent".into())
            })?;
        candidates.extend(cells_in_bbox(
            config.grid,
            bbox,
            block.resolution,
            ContainmentMode::Covers,
        )?);
    }
    if columns.len() > 1 {
        candidates.sort_unstable();
        candidates.dedup();
    }
    for id in candidates {
        let mut end = i64::MIN;
        let mut pieces = 0;
        let completed = visit_cell_polygons(config.grid, id, tile, longitude, |polygon| {
            pieces += 1;
            for &(_, y) in polygon {
                end = end.max(y.ceil() as i64);
            }
            visit_ground_area_spans_into(
                polygon,
                rows,
                i64::from(window.width()),
                gt,
                &row_areas,
                &mut clip_scratch,
                |r, c0, c1, fraction, m2| {
                    for c in c0..c1 {
                        overlaps.push(((r, c), fraction, m2));
                    }
                    true
                },
            )
        });
        if !completed {
            return Err(DggsStreamError::InvalidParameter(format!("ground-area overlay cannot represent the boundary of cell {id:x} in longitude/latitude")));
        }
        if overlaps.is_empty() {
            continue;
        }
        // Longitude copies can intersect the same pixel. Count one observation
        // per pixel for the accumulator's squared weights.
        if pieces > 1 {
            overlaps.sort_unstable_by_key(|p| p.0);
        }
        let cell = pending.entry(id).or_default();
        cell.end_row = end;
        let mut pixels = overlaps.drain(..).peekable();
        while let Some(((r, c), mut fraction, mut m2)) = pixels.next() {
            while let Some((_, f, a)) = pixels.next_if(|p| p.0 == (r, c)) {
                fraction += f;
                m2 += a;
            }
            if let Some(bbox) = config.bbox {
                let (lon, lat) = gt.pixel_to_coord(c as f64 + 0.5, r as f64 + 0.5);
                if !is_point_in_bbox(wrap_lon_180(lon), lat, Some(bbox)) {
                    continue;
                }
            }
            cell.covered_m2 += m2;
            if let Some(value) = window.get(c, r) {
                cell.stats.update_overlap(value, fraction.min(1.0), m2)?;
                if !cell.stats.is_finite() {
                    return Err(DggsStreamError::InvalidParameter(
                        "overflow in ground-area overlay statistics".into(),
                    ));
                }
            }
        }
    }
    let mut ready = Vec::new();
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
            ready.push((block.resolution, *id, std::mem::take(&mut cell.stats)));
        }
        false
    });
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
            let mut pending = BTreeMap::new();
            let mut cache = ChunkCache::with_budget(256);
            let mut start = 0;
            for &end in ends {
                let block = Block {
                    resolution: 10,
                    tile: 0,
                    rows: start..end,
                    cols: 0..width,
                    bbox: None,
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
                )
                .unwrap()
                {
                    assert!(cells.insert(id, stats).is_none());
                }
                if end < height {
                    assert!(!pending.is_empty());
                }
                start = end;
            }
            assert!(pending.is_empty());
            cells
        };
        assert_eq!(run(&[8, height]), run(&[height]));
    }
}
