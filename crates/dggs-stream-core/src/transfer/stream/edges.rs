//! Sample a grid side in its native geometry and measure its curvature after
//! projection into raster pixels.
use super::{to_pixel_raw, GridKind, TileDescriptor};
use crate::grid::{healpix_decode, wrap_lon_180};

type Point = (f64, f64);
const TOLERANCE: f64 = 1.0 / 64.0;
const MAX_DEPTH: u8 = 12;

/// Cheap upper bound for small great-circle edges on an axis-aligned geographic
/// raster. With unit angular speed, |lat''| <= tan(m) and |lon''| <=
/// 2 tan(m)/cos(m), where m bounds absolute latitude on the arc. Linear
/// interpolation of a function with bounded second derivative errs by at most
/// length²/8 times that bound. Other edge models and CRSs use sampled tracing.
pub(super) fn corners_suffice(grid: GridKind, corners: &[Point], tile: &TileDescriptor) -> bool {
    use crate::crs::transformer::CrsTransformer;
    let gt = &tile.reader.metadata.geotransform;
    if !grid.has_geodesic_edges()
        || gt.b != 0.0
        || gt.d != 0.0
        || !matches!(tile.crs_transformer, CrsTransformer::Wgs84Identity)
    {
        return false;
    }
    let mut length = 0.0_f64;
    let mut latitude = 0.0_f64;
    let mut prev = corners[corners.len() - 1];
    for &p in corners {
        // A meridian plus a parallel gives an upper bound on arc length.
        length = length.max((p.1 - prev.1).abs() + wrap_lon_180(p.0 - prev.0).abs());
        latitude = latitude.max(p.1.abs());
        prev = p;
    }
    let m = (latitude + length * 0.5).to_radians();
    if m >= std::f64::consts::FRAC_PI_2 {
        return false;
    }
    let bound = length.to_radians().powi(2) * m.tan() / 8.0;
    bound.to_degrees() * (2.0 / (m.cos() * gt.a)).hypot(1.0 / gt.e) < TOLERANCE
}

/// Emit unclosed longitude/latitude vertices. Periodic rasters measure error
/// on the continuous longitude branch nearest the tile, before seam splitting.
pub(super) fn trace(
    grid: GridKind,
    cell: u64,
    corners: &[Point],
    tile: &TileDescriptor,
    longitude_bounds: Option<(f64, f64)>,
) -> Option<Vec<Point>> {
    let anchor = corners[0].0;
    let shift = longitude_bounds.map(|(w, e)| (((w + e) * 0.5 - anchor) / 360.0).round() * 360.0);
    let project = |(lon, lat): Point| {
        let lon = shift.map_or_else(
            || wrap_lon_180(lon),
            |s| anchor + wrap_lon_180(lon - anchor) + s,
        );
        to_pixel_raw(tile, lon, lat)
    };
    let mut result = Vec::with_capacity(corners.len());
    let mut side = |i: usize, sample: &mut dyn FnMut(f64) -> Option<Point>| -> Option<()> {
        let a = corners[i];
        let b = corners[(i + 1) % corners.len()];
        refine(
            &mut result,
            &project,
            sample,
            Sample {
                t: 0.0,
                lng_lat: a,
                pixel: project(a)?,
            },
            Sample {
                t: 1.0,
                lng_lat: b,
                pixel: project(b)?,
            },
            0,
        )
    };
    match grid {
        GridKind::H3 | GridKind::S2 => {
            for i in 0..corners.len() {
                let vector = |(lon, lat): Point| {
                    let (s, c) = lat.to_radians().sin_cos();
                    let (y, x) = lon.to_radians().sin_cos();
                    [c * x, c * y, s]
                };
                let a = vector(corners[i]);
                let b = vector(corners[(i + 1) % corners.len()]);
                side(i, &mut |t| {
                    let [x, y, z] = std::array::from_fn(|k| a[k] + t * (b[k] - a[k]));
                    Some((y.atan2(x).to_degrees(), z.atan2(x.hypot(y)).to_degrees()))
                })?;
            }
        }
        GridKind::Healpix => {
            let (depth, pixel) = healpix_decode(cell)?;
            let vertices = healpix::get(depth).projected_vertices(pixel);
            for i in 0..4 {
                let (a, b) = (vertices[i], vertices[(i + 1) % 4]);
                side(i, &mut |t| {
                    let p = healpix::proj::unproj(
                        (a.0 + t * (b.0 - a.0)).rem_euclid(8.0),
                        a.1 + t * (b.1 - a.1),
                    );
                    Some((p.lon.to_degrees(), p.lat.to_degrees()))
                })?;
            }
        }
        GridKind::A5 => {
            use a5::{
                coordinate_systems::Face,
                core::{
                    cell::get_pentagon, coordinate_transforms::to_lon_lat,
                    serialization::deserialize,
                },
                projections::dodecahedron::DodecahedronProjection,
            };
            let id = deserialize(cell).ok()?;
            let pentagon = get_pentagon(&id).ok()?;
            let vertices = pentagon.get_vertices_vec();
            let n = vertices.len();
            if n != corners.len() {
                return None;
            }
            for i in 0..n {
                // cell_to_boundary reverses the face winding on output.
                let a = vertices[n - 1 - i];
                let b = vertices[n - 1 - (i + 1) % n];
                side(i, &mut |t| {
                    let face = Face::new(a.x() + t * (b.x() - a.x()), a.y() + t * (b.y() - a.y()));
                    let p = to_lon_lat(
                        DodecahedronProjection::get_thread_local()
                            .inverse(face, id.origin_id)
                            .ok()?,
                    );
                    Some((p.longitude(), p.latitude()))
                })?;
            }
        }
    }
    for point in &mut result {
        point.0 = wrap_lon_180(point.0);
    }
    Some(result)
}

#[derive(Clone, Copy)]
struct Sample {
    t: f64,
    lng_lat: Point,
    pixel: Point,
}

fn refine(
    out: &mut Vec<Point>,
    project: &impl Fn(Point) -> Option<Point>,
    sample: &mut dyn FnMut(f64) -> Option<Point>,
    a: Sample,
    b: Sample,
    depth: u8,
) -> Option<()> {
    let parameter = |fraction: f64| a.t + fraction * (b.t - a.t);
    let mid = parameter(0.5);
    let p = sample(mid)?;
    let pixel = project(p)?;
    let deviates = |q: Point| {
        let (dx, dy) = (b.pixel.0 - a.pixel.0, b.pixel.1 - a.pixel.1);
        let (x, y) = (q.0 - a.pixel.0, q.1 - a.pixel.1);
        let length2 = dx * dx + dy * dy;
        let t = if length2 == 0.0 {
            0.0
        } else {
            ((x * dx + y * dy) / length2).clamp(0.0, 1.0)
        };
        (x - t * dx).hypot(y - t * dy) > TOLERANCE
    };
    // Quarter points also catch an inflection whose midpoint lies on the chord.
    if depth < MAX_DEPTH
        && (deviates(pixel)
            || deviates(project(sample(parameter(0.25))?)?)
            || deviates(project(sample(parameter(0.75))?)?))
    {
        let m = Sample {
            t: mid,
            lng_lat: p,
            pixel,
        };
        refine(out, project, sample, a, m, depth + 1)?;
        refine(out, project, sample, m, b, depth + 1)?;
    } else {
        out.push(a.lng_lat);
    }
    Some(())
}
