//! Physical-area overlay for a single north-up WGS84 raster.
use super::{
    cells_in_bbox, periodic_longitude_bounds, plan_tile, visit_cell_polygons, Block,
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
use crate::raster::ground_area::{clipped_pixel_area, wgs84_pixel_area};
use crate::raster::mosaic::TileDescriptor;
use crate::transfer::clip::visit_pixel_shapes;
use crate::transfer::window::{BandSelection, ChunkCache, RowWindow};
use crate::transfer::TransferMode;
use std::collections::BTreeMap;

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
    config.validate()?;
    validate_ground_area_raster(&reader, config)?;
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
    let tile = TileDescriptor::new(0, reader, config.custom_crs.as_deref())?;
    let mut blocks = Vec::new();
    for &res in &config.resolutions {
        if config.grid.spec().max_cell_diameter_km(res) > COARSE_CELL_DIAMETER_KM {
            return Err(DggsStreamError::InvalidParameter("ground-area overlay requires a valid resolution with polygon clipping (cell diameter bound at most 500 km)".into()));
        }
        plan_tile(config.grid, config.transfer, res, 0, &tile, 1, &mut blocks);
    }
    let config = config.clone();
    let mut cache = ChunkCache::default();
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
            match scan_block(&tile, &config, &block, &mut cache, &mut pending) {
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
    cache: &mut ChunkCache,
    pending: &mut BTreeMap<u64, Cell>,
) -> Result<Vec<(u8, u64, AreaAccumulator)>> {
    let bbox = block
        .bbox
        .ok_or_else(|| DggsStreamError::InvalidParameter("invalid WGS84 overlay extent".into()))?;
    let window = RowWindow::load(
        tile,
        BandSelection {
            band: 1,
            nodata: config.custom_nodata,
            apply_scale_offset: config.apply_scale_offset,
        },
        block.rows.clone(),
        cache,
    )?;
    let gt = &tile.reader.metadata.geotransform;
    let row_areas: Vec<_> = block
        .rows
        .clone()
        .map(|r| wgs84_pixel_area(gt, 0, r as usize))
        .collect::<Result<_>>()?;
    let rows = (i64::from(block.rows.start), i64::from(block.rows.end));
    let longitude = periodic_longitude_bounds(tile);
    let mut overlaps = Vec::new();
    for id in cells_in_bbox(config.grid, bbox, block.resolution, ContainmentMode::Covers)? {
        let mut end = i64::MIN;
        let mut pieces = 0;
        let completed = visit_cell_polygons(config.grid, id, tile, longitude, |polygon| {
            pieces += 1;
            for &(_, y) in polygon {
                end = end.max(y.ceil() as i64);
            }
            visit_pixel_shapes::<true>(polygon, rows, i64::from(window.width()), |c, r, shape| {
                let (fraction, m2) = shape.map_or_else(
                    || (1.0, row_areas[(r - rows.0) as usize]),
                    |p| clipped_pixel_area(gt, p),
                );
                if fraction > 0.0 && m2 > 0.0 {
                    overlaps.push(((r, c), fraction, m2));
                }
                true
            })
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
