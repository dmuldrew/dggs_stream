//! Grid-agnostic resolution selection: explicit resolutions, or `auto`, the resolution
//! whose cells are closest in area to the raster's pixels.
//!
//! The grids number their resolutions on unrelated scales (H3 `0..=15`, A5 and S2
//! `0..=30`, HEALPix `0..=29`), so "resolution 9" means cells of very different sizes
//! from one grid to the next. `auto` asks for the same thing on every grid: cells about
//! the size of a pixel, on whichever side is closer. `auto-finer` and `auto-coarser`
//! select the closest size on the requested side, including equality. An offset asks for a rank:
//! `auto+N` is the N-th resolution whose cells are smaller than a pixel and `auto-N` the
//! N-th whose cells are at least a pixel, counting from the one closest to the pixel
//! (see [`directional_resolution`](crate::resolution::directional_resolution)).

use std::path::PathBuf;

use crate::error::{DggsStreamError, Result};
use crate::grid::GridKind;
use crate::raster::mosaic::{MosaicReader, OverlapRule, TileDescriptor};
use crate::scalar_ops::try_parse_resolutions_str_for;

const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257_223_563;
const WGS84_E2: f64 = WGS84_F * (2.0 - WGS84_F);

/// The resolutions a scan asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionSpec {
    /// These resolutions (sorted, without repeats).
    Fixed(Vec<u8>),
    /// The closest mean cell area on the requested side, including equality.
    Directional(ResolutionDirection),
    /// With `offset` 0, the resolution whose mean cell area best matches the raster's
    /// ground pixel area. With `offset` N > 0, the N-th resolution whose cells are
    /// smaller than a pixel; with -N, the N-th whose cells are at least a pixel; both
    /// counted from the one closest to the pixel ([`directional_resolution`]).
    Auto {
        /// 0 for the area match; otherwise the side (sign) and rank (magnitude).
        offset: i32,
    },
}

/// Inclusive bounds on the mean cell area for a named auto selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionDirection {
    /// Largest cell area at most the pixel area.
    Finer,
    /// Smallest cell area at least the pixel area.
    Coarser,
}

impl ResolutionDirection {
    fn selector(self) -> &'static str {
        match self {
            Self::Finer => "auto-finer",
            Self::Coarser => "auto-coarser",
        }
    }

    fn resolve(self, grid: GridKind, pixel_area: f64) -> Result<u8> {
        let finest = grid.max_resolution();
        let mut levels = 0..=finest;
        let selected = match self {
            Self::Finer => levels.find(|&r| grid.area_m2(r) <= pixel_area),
            Self::Coarser => levels.rfind(|&r| grid.area_m2(r) >= pixel_area),
        };
        selected.ok_or_else(|| DggsStreamError::InvalidParameter(format!(
            "{} cannot match pixel area {pixel_area:.4e} m² on {}: mean cell areas range from {:.4e} to {:.4e} m²",
            self.selector(), grid.label(), grid.area_m2(finest), grid.area_m2(0)
        )))
    }
}

impl ResolutionSpec {
    /// The area match itself.
    pub const AUTO: Self = Self::Auto { offset: 0 };

    /// Largest offset accepted by [`Self::parse`]. A rank past the grid's range stops at
    /// its finest or coarsest resolution, so no grid needs more.
    pub const MAX_OFFSET: i32 = 64;

    /// Parse `auto`, `auto-finer`, `auto-coarser`, `auto+N` / `auto-N` (ASCII case-insensitive, spaces allowed around
    /// the sign), or a comma- or space-separated list of resolutions of `grid`.
    pub fn parse(grid: GridKind, text: &str) -> Result<Self> {
        let trimmed = text.trim();
        if let Some(auto) = Self::parse_auto(trimmed) {
            return auto;
        }
        try_parse_resolutions_str_for(grid, trimmed)
            .map(Self::Fixed)
            .ok_or_else(|| {
                DggsStreamError::InvalidParameter(format!(
                    "Invalid resolutions '{text}': expected 'auto', 'auto-finer', 'auto-coarser', 'auto+N', 'auto-N' or {} \
                     resolutions between 0 and {}, e.g. 7,8",
                    grid.label(),
                    grid.max_resolution()
                ))
            })
    }

    /// `Some` if `text` starts with `auto`: the parsed spec, or why it is malformed.
    /// `None` if it is not an `auto` form at all.
    pub fn parse_auto(text: &str) -> Option<Result<Self>> {
        let trimmed = text.trim();
        let head = trimmed.get(..4)?;
        if !head.eq_ignore_ascii_case("auto") {
            return None;
        }
        let rest = trimmed[4..].trim();
        if rest.is_empty() {
            return Some(Ok(Self::AUTO));
        }
        for direction in [ResolutionDirection::Finer, ResolutionDirection::Coarser] {
            if trimmed.eq_ignore_ascii_case(direction.selector()) {
                return Some(Ok(Self::Directional(direction)));
            }
        }
        let bad = || {
            DggsStreamError::InvalidParameter(format!(
                "Invalid resolution '{text}': expected 'auto', 'auto-finer', 'auto-coarser', 'auto+N' or 'auto-N' with N \
                 at most {}",
                Self::MAX_OFFSET
            ))
        };
        let (sign, digits) = match rest.as_bytes()[0] {
            b'+' => (1, rest[1..].trim()),
            b'-' => (-1, rest[1..].trim()),
            _ => return Some(Err(bad())),
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Some(Err(bad()));
        }
        Some(match digits.parse::<i32>() {
            Ok(n) if n <= Self::MAX_OFFSET => Ok(Self::Auto { offset: sign * n }),
            _ => Err(bad()),
        })
    }

    /// Whether the resolution depends on the raster.
    pub fn is_auto(&self) -> bool {
        !matches!(self, Self::Fixed(_))
    }

    /// The resolutions to scan `mosaic` at on `grid`.
    pub fn resolve(&self, grid: GridKind, mosaic: &MosaicReader) -> Result<Vec<u8>> {
        match self {
            Self::Fixed(resolutions) => Ok(resolutions.clone()),
            Self::Directional(direction) => Ok(vec![
                direction.resolve(grid, mosaic_pixel_ground_area_m2(mosaic)?)?
            ]),
            Self::Auto { offset } => Ok(vec![auto_resolution(grid, mosaic, *offset)?]),
        }
    }

    /// [`Self::resolve`] for raster sources that are not open yet. An `auto` spec opens
    /// them (headers only) with `custom_crs`, as a scan would.
    pub fn resolve_paths(
        &self,
        grid: GridKind,
        paths: &[PathBuf],
        custom_crs: Option<&str>,
    ) -> Result<Vec<u8>> {
        self.resolve_paths_with(grid, paths, custom_crs, None)
    }

    /// [`Self::resolve_paths`] reading the rasters through `remote_source_factory` (a
    /// host file-system layer) when one is given.
    pub fn resolve_paths_with(
        &self,
        grid: GridKind,
        paths: &[PathBuf],
        custom_crs: Option<&str>,
        remote_source_factory: Option<&crate::raster::http_range::RemoteSourceFactory>,
    ) -> Result<Vec<u8>> {
        match self {
            Self::Fixed(resolutions) => Ok(resolutions.clone()),
            Self::Auto { .. } | Self::Directional(_) => {
                let mosaic = MosaicReader::open_with_remote_source_factory(
                    paths,
                    None,
                    custom_crs,
                    OverlapRule::default(),
                    remote_source_factory,
                )?;
                self.resolve(grid, &mosaic)
            }
        }
    }
}

impl std::fmt::Display for ResolutionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed(resolutions) => {
                for (i, r) in resolutions.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{r}")?;
                }
                Ok(())
            }
            Self::Directional(direction) => f.write_str(direction.selector()),
            Self::Auto { offset: 0 } => f.write_str("auto"),
            Self::Auto { offset } => write!(f, "auto{offset:+}"),
        }
    }
}

/// The resolution of `grid` whose mean cell area is closest to `area_m2`, comparing by
/// ratio (so a cell twice the area is as far off as one half the area). A non-finite or
/// non-positive area gives the finest resolution.
pub fn area_matched_resolution(grid: GridKind, area_m2: f64) -> u8 {
    let finest = grid.max_resolution();
    if !(area_m2.is_finite() && area_m2 > 0.0) {
        return finest;
    }
    (0..=finest)
        .min_by(|&a, &b| {
            let off = |r: u8| (grid.area_m2(r) / area_m2).ln().abs();
            off(a).total_cmp(&off(b))
        })
        .unwrap_or(finest)
}

/// The resolution `auto+N` / `auto-N` (`offset` = ±N) picks for a ground pixel area of
/// `pixel_area_m2`, and `auto` itself for `offset` 0.
///
/// A resolution is on the *finer* side of the pixel when its mean cell area divided by
/// `pixel_area_m2` is below [`crate::transfer::AUTO_INTERPOLATE_MAX_RATIO`] (1), and on
/// the *coarser* side otherwise, so a cell exactly the size of a pixel counts as coarser.
/// That is the comparison `transfer = "auto"` makes, so without a bbox `auto+N` always
/// gives cells `auto` transfer interpolates (continuous scans) and `auto-N` cells it
/// overlays.
///
/// - `offset` 0: [`area_matched_resolution`], the closest resolution on either side.
/// - `offset` N > 0: the N-th finer-side resolution, counting from the coarsest of them
///   (the closest to the pixel). `auto+1` is the same resolution as `auto` when `auto`
///   is already finer than the pixel, rather than a step past it.
/// - `offset` -N: the N-th coarser-side resolution, counting from the finest of them.
///   `auto-1` is `auto` when `auto` already holds at least a pixel.
/// - An N that runs past the grid's finest (or coarsest) resolution stops there, which
///   is still on the requested side.
/// - When the requested side has no resolution at all (`auto+N` on pixels no larger than
///   the grid's finest cells, `auto-N` on pixels larger than its coarsest), the result
///   is an error rather than a resolution on the wrong side.
///
/// A non-finite or non-positive `pixel_area_m2` is an error for `offset` other than 0.
pub fn directional_resolution(grid: GridKind, pixel_area_m2: f64, offset: i32) -> Result<u8> {
    if offset == 0 {
        return Ok(area_matched_resolution(grid, pixel_area_m2));
    }
    if !(pixel_area_m2.is_finite() && pixel_area_m2 > 0.0) {
        return Err(DggsStreamError::InvalidParameter(format!(
            "cannot choose a resolution automatically: invalid ground pixel area \
             {pixel_area_m2} m²"
        )));
    }
    let finest = grid.max_resolution();
    let finer =
        |r: u8| grid.area_m2(r) / pixel_area_m2 < crate::transfer::AUTO_INTERPOLATE_MAX_RATIO;
    let rank = offset.unsigned_abs() - 1;
    if offset > 0 {
        let closest = (0..=finest).find(|&r| finer(r)).ok_or_else(|| {
            DggsStreamError::InvalidParameter(format!(
                "auto{offset:+} needs {} cells smaller than the raster's pixels \
                 ({pixel_area_m2:.4e} m²), but the finest resolution, {finest}, averages \
                 {:.4e} m²; use 'auto' or an explicit resolution",
                grid.label(),
                grid.area_m2(finest)
            ))
        })?;
        Ok((u32::from(closest) + rank).min(u32::from(finest)) as u8)
    } else {
        let closest = (0..=finest).rev().find(|&r| !finer(r)).ok_or_else(|| {
            DggsStreamError::InvalidParameter(format!(
                "auto{offset:+} needs {} cells at least as large as the raster's pixels \
                 ({pixel_area_m2:.4e} m²), but resolution 0 averages {:.4e} m²; use 'auto' \
                 or an explicit resolution",
                grid.label(),
                grid.area_m2(0)
            ))
        })?;
        Ok(u32::from(closest).saturating_sub(rank) as u8)
    }
}

/// `resolution` moved `offset` steps (positive is finer), clamped to `grid`'s range.
/// `auto+N` / `auto-N` no longer step from `auto`; see [`directional_resolution`].
pub fn offset_resolution(grid: GridKind, resolution: u8, offset: i32) -> u8 {
    (i32::from(resolution) + offset).clamp(0, i32::from(grid.max_resolution())) as u8
}

/// Area in square metres of a small quadrilateral given by its corners in WGS84
/// `(lon, lat)` degrees, in ring order. The corners are laid out on the ellipsoid's
/// local tangent plane at their mean latitude, which is accurate to a few parts in a
/// million for footprints up to tens of kilometres.
pub fn lnglat_quad_area_m2(corners: &[(f64, f64); 4]) -> f64 {
    let mean_lat = corners.iter().map(|c| c.1).sum::<f64>() / 4.0;
    let (sin, cos) = mean_lat.to_radians().sin_cos();
    let w = 1.0 - WGS84_E2 * sin * sin;
    // Radii of curvature: along the meridian and along the parallel.
    let meridian = WGS84_A * (1.0 - WGS84_E2) / (w * w.sqrt());
    let parallel = WGS84_A / w.sqrt() * cos;
    let lon0 = corners[0].0;
    let local = corners.map(|(lon, lat)| {
        // The shorter way round from the first corner, so the antimeridian is no seam.
        let dlon = (lon - lon0 + 540.0).rem_euclid(360.0) - 180.0;
        (
            dlon.to_radians() * parallel,
            (lat - mean_lat).to_radians() * meridian,
        )
    });
    let mut twice_area = 0.0;
    for i in 0..4 {
        let (x0, y0) = local[i];
        let (x1, y1) = local[(i + 1) % 4];
        twice_area += x0 * y1 - x1 * y0;
    }
    0.5 * twice_area.abs()
}

/// Ground area in square metres of the pixel at the centre of `tile`: its four corners
/// taken through the raster's geotransform and CRS to WGS84.
///
/// The centre pixel stands for the raster. In a projection whose scale varies across the
/// raster (Web Mercator over a tall extent) pixels elsewhere cover somewhat more or less
/// ground.
pub fn pixel_ground_area_m2(tile: &TileDescriptor) -> Result<f64> {
    let meta = &tile.reader.metadata;
    let (col, row) = (f64::from(meta.width / 2), f64::from(meta.height / 2));
    let mut corners = [(0.0, 0.0); 4];
    for (corner, (dc, dr)) in
        corners
            .iter_mut()
            .zip([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
    {
        let (x, y) = meta.geotransform.pixel_to_coord(col + dc, row + dr);
        *corner = tile.crs_transformer.transform_point(x, y)?;
    }
    let area = lnglat_quad_area_m2(&corners);
    if area.is_finite() && area > 0.0 {
        Ok(area)
    } else {
        Err(DggsStreamError::InvalidParameter(format!(
            "cannot measure the ground size of a pixel of {}: its centre pixel has no area \
             in WGS84 (check the raster's geotransform and CRS); give an explicit resolution",
            tile.file_path.display()
        )))
    }
}

/// Ground pixel area of `tile` at the centre of `bbox` (or at the tile's own centre
/// when `bbox` is `None` or lies outside the tile's valid coordinate mapping).
pub fn tile_pixel_ground_area_m2_in_bbox(
    tile: &TileDescriptor,
    bbox: Option<[f64; 4]>,
) -> Result<f64> {
    if let Some([west, south, east, north]) = bbox {
        let lat = 0.5 * (south + north);
        let lon = if west <= east {
            0.5 * (west + east)
        } else {
            crate::grid::wrap_lon_180(west + 0.5 * ((east + 360.0) - west))
        };
        let meta = &tile.reader.metadata;
        if let Ok((x, y)) = tile.crs_transformer.wgs84_to_source(lon, lat) {
            if let Some((col_f, row_f)) = meta.geotransform.coord_to_pixel(x, y) {
                if col_f.is_finite() && row_f.is_finite() && meta.width > 0 && meta.height > 0 {
                    let col = col_f.floor().clamp(0.0, f64::from(meta.width - 1));
                    let row = row_f.floor().clamp(0.0, f64::from(meta.height - 1));
                    let mut corners = [(0.0, 0.0); 4];
                    let mut ok = true;
                    for (corner, (dc, dr)) in
                        corners
                            .iter_mut()
                            .zip([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
                    {
                        let (px, py) = meta.geotransform.pixel_to_coord(col + dc, row + dr);
                        match tile.crs_transformer.transform_point(px, py) {
                            Ok(pt) => *corner = pt,
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        let area = lnglat_quad_area_m2(&corners);
                        if area.is_finite() && area > 0.0 {
                            return Ok(area);
                        }
                    }
                }
            }
        }
    }
    pixel_ground_area_m2(tile)
}

/// Ground pixel area of a mosaic, in square metres: that of its finest tile, so `auto`
/// keeps the detail of the highest-resolution source.
pub fn mosaic_pixel_ground_area_m2(mosaic: &MosaicReader) -> Result<f64> {
    mosaic_pixel_ground_area_m2_in_bbox(mosaic, None)
}

/// [`mosaic_pixel_ground_area_m2`] sampled near the centre of `bbox` when provided, so
/// regional scans over projected rasters see local pixel sizes.
pub fn mosaic_pixel_ground_area_m2_in_bbox(
    mosaic: &MosaicReader,
    bbox: Option<[f64; 4]>,
) -> Result<f64> {
    let mut finest: Option<f64> = None;
    let mut first_error = None;
    for tile in &mosaic.tiles {
        match tile_pixel_ground_area_m2_in_bbox(tile, bbox) {
            Ok(area) => finest = Some(finest.map_or(area, |f| f.min(area))),
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }
    match (finest, first_error) {
        (Some(area), _) => Ok(area),
        (None, Some(e)) => Err(e),
        (None, None) => Err(DggsStreamError::InvalidParameter(
            "cannot choose a resolution automatically: the mosaic has no tiles".into(),
        )),
    }
}

/// The `auto` (`offset` 0) or `auto±N` resolution of `mosaic` on `grid`, from its ground
/// pixel area ([`mosaic_pixel_ground_area_m2`]); see [`directional_resolution`].
pub fn auto_resolution(grid: GridKind, mosaic: &MosaicReader, offset: i32) -> Result<u8> {
    let area = mosaic_pixel_ground_area_m2(mosaic)?;
    directional_resolution(grid, area, offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_auto_forms_and_lists() {
        let auto = |offset| ResolutionSpec::Auto { offset };
        for grid in GridKind::ALL {
            assert_eq!(ResolutionSpec::parse(grid, "auto").unwrap(), auto(0));
            assert_eq!(ResolutionSpec::parse(grid, " AUTO ").unwrap(), auto(0));
            assert_eq!(ResolutionSpec::parse(grid, "auto+1").unwrap(), auto(1));
            assert_eq!(ResolutionSpec::parse(grid, "Auto - 2").unwrap(), auto(-2));
            assert_eq!(ResolutionSpec::parse(grid, "auto+0").unwrap(), auto(0));
            assert_eq!(ResolutionSpec::parse(grid, "auto+64").unwrap(), auto(64));
            for bad in [
                "auto+",
                "auto1",
                "auto+1.5",
                "auto+-1",
                "auto+65",
                "automatic",
                "auto+x",
            ] {
                let err = ResolutionSpec::parse(grid, bad).unwrap_err().to_string();
                assert!(err.contains("auto"), "{bad}: {err}");
            }
            let finest = grid.max_resolution();
            assert_eq!(
                ResolutionSpec::parse(grid, &format!("3, {finest} 3")).unwrap(),
                ResolutionSpec::Fixed(vec![3, finest])
            );
            let err = ResolutionSpec::parse(grid, &(finest + 1).to_string()).unwrap_err();
            assert!(err.to_string().contains(&format!("between 0 and {finest}")));
            assert!(ResolutionSpec::parse(grid, "").is_err());
            for spec in [
                auto(0),
                ResolutionSpec::Directional(ResolutionDirection::Finer),
                ResolutionSpec::Directional(ResolutionDirection::Coarser),
                auto(3),
                auto(-1),
                ResolutionSpec::Fixed(vec![2, 5]),
            ] {
                assert_eq!(
                    ResolutionSpec::parse(grid, &spec.to_string()).unwrap(),
                    spec
                );
            }
        }
        assert_eq!(ResolutionSpec::parse_auto("8").map(|r| r.is_ok()), None);
        assert!(ResolutionSpec::AUTO.is_auto());
        assert!(!ResolutionSpec::Fixed(vec![1]).is_auto());
    }

    #[test]
    fn area_match_is_the_nearest_resolution_in_ratio() {
        for grid in GridKind::ALL {
            let finest = grid.max_resolution();
            for res in 0..=finest {
                let area = grid.area_m2(res);
                assert_eq!(area_matched_resolution(grid, area), res, "{grid} {res}");
                // Just short of the geometric midpoint to the next resolution still
                // matches this one; just past it matches the next.
                if res < finest {
                    let midpoint = (area * grid.area_m2(res + 1)).sqrt();
                    assert_eq!(area_matched_resolution(grid, midpoint * 1.001), res);
                    assert_eq!(area_matched_resolution(grid, midpoint * 0.999), res + 1);
                }
            }
            assert_eq!(area_matched_resolution(grid, 1e30), 0);
            assert_eq!(area_matched_resolution(grid, 1e-30), finest);
            for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
                assert_eq!(area_matched_resolution(grid, bad), finest);
            }
            assert_eq!(offset_resolution(grid, 5, 2), 7);
            assert_eq!(offset_resolution(grid, 5, -9), 0);
            assert_eq!(offset_resolution(grid, 5, 64), finest);
        }
    }

    #[test]
    fn named_directions_include_exact_matches_and_bracket_pixel_area() {
        use ResolutionDirection::{Coarser, Finer};
        for grid in GridKind::ALL {
            let finest = grid.max_resolution();
            for r in 0..=finest {
                let area = grid.area_m2(r);
                assert_eq!(Finer.resolve(grid, area).unwrap(), r);
                assert_eq!(Coarser.resolve(grid, area).unwrap(), r);
                if r < finest {
                    let between = (area * grid.area_m2(r + 1)).sqrt();
                    assert_eq!(Finer.resolve(grid, between).unwrap(), r + 1);
                    assert_eq!(Coarser.resolve(grid, between).unwrap(), r);
                }
            }
            let small = grid.area_m2(finest) / 2.0;
            let large = grid.area_m2(0) * 2.0;
            assert!(Finer.resolve(grid, small).is_err());
            assert_eq!(Coarser.resolve(grid, small).unwrap(), finest);
            assert!(Coarser.resolve(grid, large).is_err());
            assert_eq!(Finer.resolve(grid, large).unwrap(), 0);
        }
    }

    #[test]
    fn directional_offsets_pick_the_requested_side_of_the_pixel() {
        for grid in GridKind::ALL {
            let finest = grid.max_resolution();
            let dir = |area: f64, offset: i32| directional_resolution(grid, area, offset);
            for res in 1..finest {
                let (coarse, fine) = (grid.area_m2(res), grid.area_m2(res + 1));
                // A pixel strictly between two resolutions: +1 is the finer neighbour and
                // -1 the coarser, whichever of them `auto` picks.
                for area in [fine * 1.001, (coarse * fine).sqrt(), coarse * 0.999] {
                    assert_eq!(dir(area, 1).unwrap(), res + 1, "{grid} {res} {area}");
                    assert_eq!(dir(area, -1).unwrap(), res, "{grid} {res} {area}");
                    assert_eq!(dir(area, 2).unwrap(), (res + 2).min(finest), "{grid} {res}");
                    assert_eq!(dir(area, -2).unwrap(), res - 1, "{grid} {res}");
                    let auto = dir(area, 0).unwrap();
                    assert_eq!(auto, area_matched_resolution(grid, area));
                    assert!(auto == res || auto == res + 1, "{grid} {res}");
                }
                // `auto` already finer than the pixel: +1 stays there instead of
                // overshooting; already coarser: -1 stays there.
                assert_eq!(dir(fine * 1.001, 0).unwrap(), res + 1);
                assert_eq!(dir(fine * 1.001, 1).unwrap(), res + 1);
                assert_eq!(dir(coarse * 0.999, 0).unwrap(), res);
                assert_eq!(dir(coarse * 0.999, -1).unwrap(), res);
                // A cell exactly the size of a pixel is on the coarser side, as for
                // `transfer = "auto"`, which overlays it.
                assert_eq!(dir(coarse, 0).unwrap(), res);
                assert_eq!(dir(coarse, -1).unwrap(), res);
                assert_eq!(dir(coarse, 1).unwrap(), res + 1);
            }
            // Ranks past the grid's range stop at its limits, still on the requested side.
            let mid = grid.area_m2(finest / 2) * 0.999;
            assert_eq!(dir(mid, 64).unwrap(), finest, "{grid}");
            assert_eq!(dir(mid, -64).unwrap(), 0, "{grid}");
            // No resolution on the requested side is an error, not the wrong side.
            for area in [grid.area_m2(finest), 1e-30] {
                let err = dir(area, 1).unwrap_err().to_string();
                assert!(
                    err.contains("auto+1") && err.contains("smaller"),
                    "{grid}: {err}"
                );
                assert_eq!(dir(area, -1).unwrap(), finest, "{grid}");
            }
            let err = dir(1e30, -1).unwrap_err().to_string();
            assert!(
                err.contains("auto-1") && err.contains("at least"),
                "{grid}: {err}"
            );
            assert_eq!(dir(1e30, 1).unwrap(), 0, "{grid}");
            for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
                assert!(
                    dir(bad, 1).is_err() && dir(bad, -1).is_err(),
                    "{grid} {bad}"
                );
                assert_eq!(dir(bad, 0).unwrap(), finest, "{grid} {bad}");
            }
        }
    }

    #[test]
    fn quad_area_matches_the_ellipsoidal_rectangle() {
        use crate::raster::ground_area::wgs84_rectangle_area;
        for (lon, lat, size) in [
            (-122.45, 37.8, 0.001),
            (10.0, 0.0, 0.01),
            (179.9995, -45.0, 0.001),
            (30.0, 70.0, 0.0003),
            (-60.0, -80.0, 0.05),
        ] {
            let (w, e, s, n) = (lon, lon + size, lat - size, lat);
            let wrap = |x: f64| if x > 180.0 { x - 360.0 } else { x };
            let quad = [(w, n), (wrap(e), n), (wrap(e), s), (w, s)];
            let exact = wgs84_rectangle_area(w, s, wrap(e), n).unwrap();
            let got = lnglat_quad_area_m2(&quad);
            assert!(
                (got / exact - 1.0).abs() < 2e-4,
                "({lon}, {lat}) {size}: {got} vs {exact}"
            );
            // Orientation does not matter.
            let mut reversed = quad;
            reversed.reverse();
            assert!((lnglat_quad_area_m2(&reversed) / got - 1.0).abs() < 1e-12);
        }
        assert_eq!(lnglat_quad_area_m2(&[(1.0, 1.0); 4]), 0.0);
    }
}
