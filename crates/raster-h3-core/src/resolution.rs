//! Grid-agnostic resolution selection: explicit resolutions, or `auto`, the resolution
//! whose cells are closest in area to the raster's pixels.
//!
//! The grids number their resolutions on unrelated scales (H3 `0..=15`, A5 and S2
//! `0..=30`, HEALPix `0..=29`), so "resolution 9" means cells of very different sizes
//! from one grid to the next. `auto` asks for the same thing on every grid: cells about
//! the size of a pixel. An offset moves from there in whole resolution steps
//! (`auto+1` is one step finer, `auto-2` two steps coarser).

use std::path::PathBuf;

use crate::error::{RasterH3Error, Result};
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
    /// The resolution whose mean cell area best matches the raster's ground pixel area,
    /// moved `offset` resolution steps finer (positive) or coarser (negative) and
    /// clamped to the grid's range.
    Auto {
        /// Resolution steps away from the area match.
        offset: i32,
    },
}

impl ResolutionSpec {
    /// The area match itself.
    pub const AUTO: Self = Self::Auto { offset: 0 };

    /// Largest offset accepted by [`Self::parse`]; any larger one clamps the same way.
    pub const MAX_OFFSET: i32 = 64;

    /// Parse `auto`, `auto+N` / `auto-N` (ASCII case-insensitive, spaces allowed around
    /// the sign), or a comma- or space-separated list of resolutions of `grid`.
    pub fn parse(grid: GridKind, text: &str) -> Result<Self> {
        let trimmed = text.trim();
        if let Some(auto) = Self::parse_auto(trimmed) {
            return auto;
        }
        try_parse_resolutions_str_for(grid, trimmed)
            .map(Self::Fixed)
            .ok_or_else(|| {
                RasterH3Error::InvalidParameter(format!(
                    "Invalid resolutions '{text}': expected 'auto', 'auto+N', 'auto-N' or {} \
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
        let bad = || {
            RasterH3Error::InvalidParameter(format!(
                "Invalid resolution '{text}': expected 'auto', 'auto+N' or 'auto-N' with N \
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
        matches!(self, Self::Auto { .. })
    }

    /// The resolutions to scan `mosaic` at on `grid`.
    pub fn resolve(&self, grid: GridKind, mosaic: &MosaicReader) -> Result<Vec<u8>> {
        match self {
            Self::Fixed(resolutions) => Ok(resolutions.clone()),
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
            Self::Auto { .. } => {
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

/// `resolution` moved `offset` steps (positive is finer), clamped to `grid`'s range.
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
        Err(RasterH3Error::InvalidParameter(format!(
            "cannot measure the ground size of a pixel of {}: its centre pixel has no area \
             in WGS84 (check the raster's geotransform and CRS); give an explicit resolution",
            tile.file_path.display()
        )))
    }
}

/// Ground pixel area of a mosaic, in square metres: that of its finest tile, so `auto`
/// keeps the detail of the highest-resolution source.
pub fn mosaic_pixel_ground_area_m2(mosaic: &MosaicReader) -> Result<f64> {
    let mut finest: Option<f64> = None;
    let mut first_error = None;
    for tile in &mosaic.tiles {
        match pixel_ground_area_m2(tile) {
            Ok(area) => finest = Some(finest.map_or(area, |f| f.min(area))),
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }
    match (finest, first_error) {
        (Some(area), _) => Ok(area),
        (None, Some(e)) => Err(e),
        (None, None) => Err(RasterH3Error::InvalidParameter(
            "cannot choose a resolution automatically: the mosaic has no tiles".into(),
        )),
    }
}

/// The `auto` resolution of `mosaic` on `grid`: [`area_matched_resolution`] of its ground
/// pixel area, moved by `offset` and clamped to the grid's range.
pub fn auto_resolution(grid: GridKind, mosaic: &MosaicReader, offset: i32) -> Result<u8> {
    let area = mosaic_pixel_ground_area_m2(mosaic)?;
    Ok(offset_resolution(
        grid,
        area_matched_resolution(grid, area),
        offset,
    ))
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
