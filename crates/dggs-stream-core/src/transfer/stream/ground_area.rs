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
use crate::run_profile::{Counter, Phase, PhaseTimer};
use crate::transfer::clip::{
    merge_overlapping_runs, visit_ground_area_spans_into, ClipScratch, PixelRun,
};
use crate::transfer::window::{BandSelection, ChunkCache, RowWindow};
use crate::transfer::TransferMode;
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
    let mut cache = ChunkCache::with_budget(config.decode_budget_bytes);
    let mut pending = BTreeMap::new();
    let mut ready = Vec::new().into_iter();
    let mut blocks = blocks.into_iter();
    let mut failed = false;
    Ok(std::iter::from_fn(move || {
        while !failed {
            if let Some(row) = ready.next() {
                return Some(Ok(row));
            }
            let block = blocks.next()?;
            let mut run = || scan_block(&tile, &config, &block, &columns, &mut cache, &mut pending);
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
    pending: &mut BTreeMap<u64, Cell>,
) -> Result<Vec<(u8, u64, AreaAccumulator)>> {
    let decode = config.profile.as_ref().map(|_| PhaseTimer::start());
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
        config.profile.as_deref(),
    )?;
    if let (Some(profile), Some(timer)) = (&config.profile, decode) {
        timer.stop(profile, Phase::Decode);
        profile.count(
            Counter::DecodedBytes,
            block.rows.len() as u64 * u64::from(window.width()) * 8,
        );
    }
    let gt = &tile.reader.metadata.geotransform;
    let row_areas: Vec<_> = block
        .rows
        .clone()
        .map(|r| wgs84_pixel_area(gt, 0, r as usize))
        .collect::<Result<_>>()?;
    let rows = (i64::from(block.rows.start), i64::from(block.rows.end));
    let width = u64::from(window.width());
    let longitude = periodic_longitude_bounds(tile);
    let mut overlaps: Vec<GroundRun> = Vec::new();
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
                    overlaps.push(GroundRun {
                        pixel: (r - rows.0) as u64 * width + c0 as u64,
                        len: (c1 - c0) as u64,
                        fraction,
                        m2,
                    });
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
            merge_overlapping_runs(&mut overlaps, 0);
        }
        let cell = pending.entry(id).or_default();
        cell.end_row = end;
        for run in overlaps.drain(..) {
            let r = rows.0 + (run.pixel / width) as i64;
            let c0 = (run.pixel % width) as i64;
            for c in c0..c0 + run.len as i64 {
                if let Some(bbox) = config.bbox {
                    let (lon, lat) = gt.pixel_to_coord(c as f64 + 0.5, r as f64 + 0.5);
                    if !is_point_in_bbox(wrap_lon_180(lon), lat, Some(bbox)) {
                        continue;
                    }
                }
                cell.covered_m2 += run.m2;
                if let Some(value) = window.get(c, r) {
                    cell.stats
                        .update_overlap(value, run.fraction.min(1.0), run.m2)?;
                    if !cell.stats.is_finite() {
                        return Err(DggsStreamError::InvalidParameter(
                            "overflow in ground-area overlay statistics".into(),
                        ));
                    }
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

/// A horizontal run of pixels, indexed row-major within the block, sharing one
/// pixel fraction and ellipsoidal overlap area.
#[derive(Clone, Copy)]
struct GroundRun {
    pixel: u64,
    len: u64,
    fraction: f64,
    m2: f64,
}

impl PixelRun for GroundRun {
    fn start(&self) -> u64 {
        self.pixel
    }
    fn end(&self) -> u64 {
        self.pixel + self.len
    }
    fn with_range(self, start: u64, end: u64) -> Self {
        Self {
            pixel: start,
            len: end - start,
            ..self
        }
    }
    fn add_payload(self, other: Self) -> Self {
        Self {
            fraction: self.fraction + other.fraction,
            m2: self.m2 + other.m2,
            ..self
        }
    }
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
