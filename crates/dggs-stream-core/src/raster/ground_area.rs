//! Ellipsoidal ground areas for north-up WGS84 pixels and clipped longitude/latitude polygons.

use crate::error::{DggsStreamError, Result};
use crate::raster::geotransform::GeoTransform;

const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257_223_563;
const WGS84_E2: f64 = WGS84_F * (2.0 - WGS84_F);

/// Latitude overshoot past a pole, in degrees, treated as rounding error in a
/// pixel edge computed from the geotransform (about 0.1 mm on the ground).
const POLE_TOLERANCE_DEG: f64 = 1e-9;

fn invalid(message: &str) -> DggsStreamError {
    DggsStreamError::InvalidParameter(format!("invalid WGS84 footprint: {message}"))
}

/// Area in square metres of a WGS84 rectangle bounded by meridians and parallels.
///
/// Longitude and latitude are degrees. Longitudes must lie in [-180, 180]; an
/// east longitude smaller than west denotes a crossing of the antimeridian.
/// A full longitude band is expressed as west = -180 and east = 180. Latitude
/// must satisfy -90 <= south < north <= 90. Empty and ambiguous zero-width
/// rectangles are rejected. The footprint edges follow parallels, not geodesics.
pub fn wgs84_rectangle_area(west: f64, south: f64, east: f64, north: f64) -> Result<f64> {
    if ![west, south, east, north].iter().all(|v| v.is_finite()) {
        return Err(invalid("coordinates must be finite"));
    }
    if !(-180.0..=180.0).contains(&west) || !(-180.0..=180.0).contains(&east) {
        return Err(invalid("longitude must be in [-180, 180]"));
    }
    if south < -90.0 || north > 90.0 || south >= north {
        return Err(invalid(
            "latitude bounds must satisfy -90 <= south < north <= 90",
        ));
    }
    let mut width = east - west;
    if width < 0.0 {
        width += 360.0;
    }
    if width <= 0.0 {
        return Err(invalid("longitude width must be positive"));
    }

    // Integrate a^2(1-e^2) cos(phi)/(1-e^2 sin^2(phi))^2 over
    // latitude and longitude. Express the antiderivative difference using
    // sin(north)-sin(south) so small pixels do not subtract near-equal areas.
    let span = (north - south).to_radians();
    let midpoint = (north + south) * 0.5;
    let cos_mid = if midpoint > 45.0 {
        (((90.0 - north) + (90.0 - south)) * 0.5).to_radians().sin()
    } else if midpoint < -45.0 {
        (((90.0 + north) + (90.0 + south)) * 0.5).to_radians().sin()
    } else {
        midpoint.to_radians().cos()
    };
    let sin_difference = 2.0 * cos_mid * (span * 0.5).sin();
    let u = south.to_radians().sin();
    let v = north.to_radians().sin();
    let cross = 1.0 - WGS84_E2 * u * v;
    let e = WGS84_E2.sqrt();
    let t = e * sin_difference / cross;
    let atanh_ratio = if t == 0.0 { 1.0 } else { t.atanh() / t };
    let rational = (1.0 + WGS84_E2 * u * v) / ((1.0 - WGS84_E2 * u * u) * (1.0 - WGS84_E2 * v * v));
    let integral = 0.5 * sin_difference * (rational + atanh_ratio / cross);
    Ok(WGS84_A * WGS84_A * (1.0 - WGS84_E2) * width.to_radians() * integral)
}

/// Area of a pixel from a north-up, axis-aligned WGS84 geographic transform.
///
/// The caller must establish that the raster CRS is WGS84. Longitudes may use
/// a 0..360 representation, but pixel widths must be in (0, 360] degrees.
/// Rotated, sheared, west-up and south-up transforms are rejected.
pub fn wgs84_pixel_area(gt: &GeoTransform, col: usize, row: usize) -> Result<f64> {
    if ![gt.a, gt.b, gt.c0, gt.d, gt.e, gt.f0]
        .iter()
        .all(|v| v.is_finite())
        || gt.b != 0.0
        || gt.d != 0.0
        || !(gt.a > 0.0 && gt.a <= 360.0)
        || gt.e >= 0.0
    {
        return Err(invalid(
            "expected a finite north-up, axis-aligned transform",
        ));
    }
    let west_unwrapped = gt.c0 + col as f64 * gt.a;
    // Both edges come straight from their row indices, so the error of one
    // edge is not carried into the next.
    let north = gt.f0 + row as f64 * gt.e;
    let south = gt.f0 + (row as f64 + 1.0) * gt.e;
    if !west_unwrapped.is_finite() || !north.is_finite() || !south.is_finite() {
        return Err(invalid("pixel coordinates are not finite"));
    }
    // A raster that nominally ends at a pole can overshoot it by rounding.
    let snap = |lat: f64| {
        if (90.0..=90.0 + POLE_TOLERANCE_DEG).contains(&lat) {
            90.0
        } else if (-90.0 - POLE_TOLERANCE_DEG..=-90.0).contains(&lat) {
            -90.0
        } else {
            lat
        }
    };
    let (north, south) = (snap(north), snap(south));
    if gt.a == 360.0 {
        return wgs84_rectangle_area(-180.0, south, 180.0, north);
    }
    let west = (west_unwrapped + 180.0).rem_euclid(360.0) - 180.0;
    let east_unwrapped = west + gt.a;
    let east = if east_unwrapped > 180.0 {
        east_unwrapped - 360.0
    } else {
        east_unwrapped
    };
    wgs84_rectangle_area(west, south, east, north)
}

/// Ellipsoidal area integrals along a directed edge segment ascending from
/// `y0` to `y1` (`y0 < y1`, in pixel-row coordinates) with local column coordinates
/// `rx0` at `y0` and `rx1` at `y1` (in `[0, 1]` relative to pixel column `c`).
///
/// Returns `(local_m2, full_m2)` where:
/// - `local_m2` is the ellipsoidal area inside `[c, c + 1] x [y0, y1]` to the
///   right of the segment (`∫_{y0}^{y1} (1 - rx(y)) * w(y) dy`), and
/// - `full_m2` is the ellipsoidal area of the unit-width strip `[c, c + 1] x [y0, y1]`
///   (`∫_{y0}^{y1} w(y) dy`).
pub(crate) fn segment_ellipsoidal_integrals(
    gt: &GeoTransform,
    rx0: f64,
    y0: f64,
    rx1: f64,
    y1: f64,
) -> (f64, f64) {
    let dy = y1 - y0;
    if dy <= 0.0 || !dy.is_finite() {
        return (0.0, 0.0);
    }
    let drx = rx1 - rx0;
    // Four-point Gauss-Legendre quadrature on segments of at most five degrees,
    // matching the per-polygon reference used in tests.
    let n = ((dy * gt.e).abs() / 5.0).ceil().max(1.0) as usize;
    let half = 0.5 / n as f64;
    let mut local = 0.0;
    let mut full = 0.0;
    for i in 0..n {
        let mid = (2 * i + 1) as f64 * half;
        for (node, weight) in [
            (0.3399810435848563, 0.6521451548625461),
            (0.8611363115940526, 0.3478548451374538),
        ] {
            for t in [mid - half * node, mid + half * node] {
                let lat =
                    gt.e.mul_add(dy.mul_add(t, y0), gt.f0)
                        .to_radians()
                        .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2);
                let sin_lat = lat.sin();
                let d = 1.0 - WGS84_E2 * sin_lat * sin_lat;
                let w = dy * half * weight * lat.cos() / (d * d);
                let rx = drx.mul_add(t, rx0).clamp(0.0, 1.0);
                full += w;
                local += (1.0 - rx) * w;
            }
        }
    }
    let scale = WGS84_A.powi(2) * (1.0 - WGS84_E2) * gt.a.to_radians() * (-gt.e).to_radians();
    ((local * scale).max(0.0), (full * scale).max(0.0))
}

/// Test-only reference: per-polygon Green's-theorem areas the span clipper is
/// checked against.
#[cfg(test)]
pub(crate) mod reference {
    use super::{GeoTransform, WGS84_A, WGS84_E2};

    /// Pixel fraction and ellipsoidal area of a clipped, straight-edged polygon in
    /// pixel coordinates. The caller has validated a north-up EPSG:4326 transform.
    /// Green's theorem gives both areas as an edge integral of (x - origin) dy,
    /// with an additional latitude-dependent surface density for the physical area.
    pub(crate) fn clipped_pixel_area(gt: &GeoTransform, polygon: &[(f64, f64)]) -> (f64, f64) {
        let Some(&(origin, _)) = polygon.first() else {
            return (0.0, 0.0);
        };
        let mut previous = polygon[polygon.len() - 1];
        let mut fraction = 0.0;
        let mut area = 0.0;
        for &(x1, y1) in polygon {
            let (x0, y0) = previous;
            previous = (x1, y1);
            let dy = y1 - y0;
            if dy == 0.0 {
                continue;
            }
            let left = x0 - origin;
            let dx = x1 - x0;
            fraction += (left + 0.5 * dx) * dy;
            // Four-point Gauss-Legendre quadrature on segments of at most five
            // degrees. Longitude varies linearly along each clipped edge.
            let n = ((dy * gt.e).abs() / 5.0).ceil().max(1.0) as usize;
            let half = 0.5 / n as f64;
            for i in 0..n {
                let mid = (2 * i + 1) as f64 * half;
                for (node, weight) in [
                    (0.3399810435848563, 0.6521451548625461),
                    (0.8611363115940526, 0.3478548451374538),
                ] {
                    for t in [mid - half * node, mid + half * node] {
                        let lat = gt.e.mul_add(dy.mul_add(t, y0), gt.f0).to_radians();
                        let d = 1.0 - WGS84_E2 * lat.sin().powi(2);
                        area += dy * half * weight * dx.mul_add(t, left) * lat.cos() / (d * d);
                    }
                }
            }
        }
        let scale = WGS84_A.powi(2) * (1.0 - WGS84_E2) * gt.a.to_radians() * (-gt.e).to_radians();
        (fraction.abs().min(1.0), area.abs() * scale)
    }
}

#[cfg(test)]
mod tests {
    use super::reference::clipped_pixel_area;
    use super::*;

    #[test]
    fn partial_triangle_area_matches_latitude_strip_integration() {
        for north in [5.0, 85.0] {
            let gt = GeoTransform::from_gdal_array([179.0, 2.0, 0.0, north, 0.0, -10.0]);
            // The diagonal splits the pixel into two equal planar fractions.
            let triangle = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)];
            let (fraction, measured) = clipped_pixel_area(&gt, &triangle);
            assert_eq!(fraction, 0.5);
            // Independent Simpson integration of horizontal strip width times
            // the meridional and prime-vertical radii of curvature.
            let n = 1024;
            let expected: f64 = (0..=n)
                .map(|i| {
                    let t = f64::from(i) / f64::from(n);
                    let phi = (north - 10.0 * t).to_radians();
                    let q = 1.0 - WGS84_E2 * phi.sin().powi(2);
                    let meridian = WGS84_A * (1.0 - WGS84_E2) / q.powf(1.5);
                    let parallel = WGS84_A * phi.cos() / q.sqrt();
                    let weight = if i == 0 || i == n {
                        1.0
                    } else if i % 2 == 0 {
                        2.0
                    } else {
                        4.0
                    };
                    weight * (2.0 * (1.0 - t)).to_radians() * parallel * meridian
                })
                .sum::<f64>()
                * 10.0_f64.to_radians()
                / f64::from(3 * n);
            assert!((measured / expected - 1.0).abs() < 2e-13);
            let (_, other) = clipped_pixel_area(&gt, &[(1.0, 1.0), (1.0, 0.0), (0.0, 1.0)]);
            let full = wgs84_pixel_area(&gt, 0, 0).unwrap();
            assert!(((measured + other) / full - 1.0).abs() < 2e-13);
            if north > 80.0 {
                assert!(measured < 0.45 * full);
            }
        }
    }

    #[test]
    fn pixel_edges_tolerate_rounding_at_the_poles_only() {
        // 0.05° rows from 90°N: accumulating one step at a time from row 3599
        // gives a southern edge of -90.00000000000001.
        let global = GeoTransform::from_gdal_array([-180.0, 0.05, 0.0, 90.0, 0.0, -0.05]);
        assert!(wgs84_pixel_area(&global, 7199, 3599).unwrap() > 0.0);
        assert!(wgs84_pixel_area(&global, 0, 0).unwrap() > 0.0);
        // Non-binary steps whose last edge rounds just past the South Pole.
        for (step, rows) in [(0.15, 2usize), (0.01, 2), (0.03, 7)] {
            let north = -90.0 + rows as f64 * step;
            let gt = GeoTransform::from_gdal_array([0.0, step, 0.0, north, 0.0, -step]);
            let area = wgs84_pixel_area(&gt, 0, rows - 1).unwrap();
            let edge = north + (rows - 1) as f64 * -step;
            let expected = wgs84_rectangle_area(0.0, -90.0, step, edge).unwrap();
            assert!(
                (area - expected).abs() <= expected * 1e-9,
                "{step}: {area} vs {expected}"
            );
        }
        // A footprint materially beyond a pole is still rejected.
        let past_south = GeoTransform::from_gdal_array([0.0, 0.1, 0.0, -89.95, 0.0, -0.1]);
        assert!(wgs84_pixel_area(&past_south, 0, 0).is_err());
        let past_north = GeoTransform::from_gdal_array([0.0, 0.1, 0.0, 90.05, 0.0, -0.1]);
        assert!(wgs84_pixel_area(&past_north, 0, 0).is_err());
    }

    #[test]
    fn ellipsoidal_area_matches_high_precision_reference() {
        // Independent 70-digit mpmath evaluation of the ellipsoidal surface
        // integral with WGS84 a=6378137 and 1/f=298.257223563.
        let cases = [
            (0.0, 0.0, 1.0, 1.0, 12_308_463_893.975_353),
            (0.0, 59.0, 1.0, 60.0, 6_309_805_669.030_524),
            (0.0, 89.0, 1.0, 90.0, 108_866_681.636_207_67),
            (179.5, -1.0, -179.5, 1.0, 24_616_927_787.950_706),
            (-180.0, -90.0, 180.0, 90.0, 510_065_621_724_088.5),
            (
                0.0,
                89.999_999,
                0.000_001,
                90.0,
                1.088_696_687_240_833_9e-10,
            ),
        ];
        for (west, south, east, north, expected) in cases {
            let actual = wgs84_rectangle_area(west, south, east, north).unwrap();
            assert!(
                (actual - expected).abs() <= expected * 1e-8,
                "{west} {south} {east} {north}: {actual} vs {expected}"
            );
        }
    }

    #[test]
    fn pixel_area_wraps_longitudes_and_rejects_unsupported_geometry() {
        let mut gt = GeoTransform::from_gdal_array([179.5, 1.0, 0.0, 1.0, 0.0, -2.0]);
        let expected = wgs84_rectangle_area(179.5, -1.0, -179.5, 1.0).unwrap();
        assert!((wgs84_pixel_area(&gt, 0, 0).unwrap() - expected).abs() < 1e-5);
        gt.c0 = -180.5;
        assert!((wgs84_pixel_area(&gt, 0, 0).unwrap() - expected).abs() < 1e-5);
        gt.b = 0.01;
        assert!(wgs84_pixel_area(&gt, 0, 0).is_err());
        gt.b = 0.0;
        gt.e = 1.0;
        assert!(wgs84_pixel_area(&gt, 0, 0).is_err());
    }

    #[test]
    fn rejects_invalid_bounds_and_preserves_area_additivity() {
        for (west, south, east, north) in [
            (0.0, -91.0, 1.0, 0.0),
            (0.0, 1.0, 1.0, 1.0),
            (0.0, 0.0, 0.0, 1.0),
            (180.0, 0.0, -180.0, 1.0),
            (f64::NAN, 0.0, 1.0, 1.0),
        ] {
            assert!(wgs84_rectangle_area(west, south, east, north).is_err());
        }
        let whole = wgs84_rectangle_area(-1.0, 10.0, 1.0, 12.0).unwrap();
        let parts = wgs84_rectangle_area(-1.0, 10.0, 1.0, 11.0).unwrap()
            + wgs84_rectangle_area(-1.0, 11.0, 1.0, 12.0).unwrap();
        assert!((whole - parts).abs() < whole * 1e-14);
    }

    #[test]
    fn segment_ellipsoidal_integrals_match_clipped_pixel_and_strip() {
        for north in [5.0, 85.0] {
            let gt = GeoTransform::from_gdal_array([179.0, 2.0, 0.0, north, 0.0, -10.0]);
            let full = wgs84_pixel_area(&gt, 0, 0).unwrap();
            // Triangle (0,0)-(1,0)-(0,1): only edge (1,0)->(0,1) slopes across the pixel,
            // and the left edge (0,1)->(0,0) has rx=0 so right-of-segment area is `full` minus
            // the area to the right of (1,0)->(0,1).
            let (diag_local, strip) = segment_ellipsoidal_integrals(&gt, 1.0, 0.0, 0.0, 1.0);
            assert!((strip / full - 1.0).abs() < 1e-12);
            let triangle_m2 = strip - diag_local;
            let (_, expected) = clipped_pixel_area(&gt, &[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]);
            assert!((triangle_m2 / expected - 1.0).abs() < 1e-13);

            // Sub-row segment with positive slope.
            let (sub_local, sub_strip) = segment_ellipsoidal_integrals(&gt, 0.25, 0.2, 0.75, 0.8);
            let (_, expected_right) =
                clipped_pixel_area(&gt, &[(0.25, 0.2), (1.0, 0.2), (1.0, 0.8), (0.75, 0.8)]);
            let (_, expected_left) =
                clipped_pixel_area(&gt, &[(0.0, 0.2), (0.25, 0.2), (0.75, 0.8), (0.0, 0.8)]);
            assert!((sub_local / expected_right - 1.0).abs() < 1e-13);
            assert!(((sub_strip - sub_local) / expected_left - 1.0).abs() < 1e-13);
        }
    }
}
