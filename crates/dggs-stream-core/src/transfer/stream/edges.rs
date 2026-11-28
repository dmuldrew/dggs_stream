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

/// The outcome of tracing a ring.
pub(super) enum Traced {
    /// No side deviates from its chord: use the corners as they are, without a copy.
    Corners,
    /// The corners with extra vertices where sides curve.
    Ring(Vec<Point>),
}

/// A ring vertex on a pole. `write_lng_lat_ring` expands a pole vertex into two
/// ring points on the pole at the neighbouring vertices' meridians.
fn at_pole(p: Point) -> bool {
    p.1.abs() >= 90.0 - 1e-9
}

/// The native vertex each ring point comes from, and the native vertex count. The
/// two ring points of an expanded pole vertex share one index.
fn native_indices(ring: &[Point]) -> ([usize; 10], usize) {
    let mut index = [0; 10];
    let (mut native, mut i) = (0, 0);
    while i < ring.len() {
        index[i] = native;
        if at_pole(ring[i]) && ring.get(i + 1).is_some_and(|&p| at_pole(p)) {
            index[i + 1] = native;
            i += 1;
        }
        native += 1;
        i += 1;
    }
    (index, native)
}

/// Memoize exact face-space samples shared by adjacent A5 sides within one
/// serial overlay block. Keep distinct faces and floating
/// point coordinates separate even where edges nearly coincide.
pub(super) type EdgeSamples = rustc_hash::FxHashMap<(u8, u64, u64), Point>;

const MAX_CACHED_EDGE_SAMPLES: usize = 4096;

/// Select the A5 inverse projection at compile time so uncached scans keep
/// their direct call in the edge tracing loop.
pub(super) trait A5EdgeSampler {
    fn sample(&mut self, origin: u8, face: a5::coordinate_systems::Face) -> Option<Point>;
}

impl A5EdgeSampler for () {
    #[inline]
    fn sample(&mut self, origin: u8, face: a5::coordinate_systems::Face) -> Option<Point> {
        use a5::{
            core::coordinate_transforms::to_lon_lat,
            projections::dodecahedron::DodecahedronProjection,
        };
        let p = to_lon_lat(
            DodecahedronProjection::get_thread_local()
                .inverse(face, origin)
                .ok()?,
        );
        Some((p.longitude(), p.latitude()))
    }
}

impl A5EdgeSampler for EdgeSamples {
    #[inline]
    fn sample(&mut self, origin: u8, face: a5::coordinate_systems::Face) -> Option<Point> {
        let key = (origin, face.x().to_bits(), face.y().to_bits());
        if let Some(&point) = self.get(&key) {
            return Some(point);
        }
        let point = ().sample(origin, face)?;
        if self.len() == MAX_CACHED_EDGE_SAMPLES {
            self.clear();
        }
        self.insert(key, point);
        Some(point)
    }
}

/// Emit unclosed longitude/latitude vertices. Periodic rasters measure error
/// on the continuous longitude branch nearest the tile, before seam splitting.
///
/// Sides that touch a pole stay straight: a side from a vertex to the pole is a
/// meridian and the cap between the two pole points runs along the pole, so both
/// are straight in a north-up raster, and a neighbour across them is polar too and
/// keeps the same line. Every other side is traced, so a polar cell and its
/// non-polar neighbour follow the same curve along their shared side.
pub(super) fn trace(
    grid: GridKind,
    cell: u64,
    corners: &[Point],
    tile: &TileDescriptor,
    longitude_bounds: Option<(f64, f64)>,
    edge_samples: &mut impl A5EdgeSampler,
) -> Option<Traced> {
    let anchor = corners[0].0;
    let shift = longitude_bounds.map(|(w, e)| (((w + e) * 0.5 - anchor) / 360.0).round() * 360.0);
    let project = |(lon, lat): Point| {
        let lon = shift.map_or_else(
            || wrap_lon_180(lon),
            |s| anchor + wrap_lon_180(lon - anchor) + s,
        );
        to_pixel_raw(tile, lon, lat)
    };
    let n = corners.len();
    let (native_of, native_count) = native_indices(corners);
    // Stays unallocated until a side needs extra vertices. The sides before it were
    // straight and each contributed its start corner, which is backfilled.
    let mut result: Option<Vec<Point>> = None;
    let mut side = |i: usize, sample: &mut dyn FnMut(f64) -> Option<Point>| -> Option<()> {
        let (start, end) = (corners[i], corners[(i + 1) % n]);
        if at_pole(start) || at_pole(end) {
            if let Some(out) = result.as_mut() {
                out.push(start);
            }
            return Some(());
        }
        let a = Sample {
            t: 0.0,
            lng_lat: start,
            pixel: project(start)?,
        };
        let b = Sample {
            t: 1.0,
            lng_lat: end,
            pixel: project(end)?,
        };
        match split_point(&project, sample, a, b, 0)? {
            None => {
                if let Some(out) = result.as_mut() {
                    out.push(start);
                }
            }
            Some(mid) => {
                let out = result.get_or_insert_with(|| {
                    let mut out = Vec::with_capacity(2 * n);
                    out.extend_from_slice(&corners[..i]);
                    out
                });
                refine(out, &project, sample, a, mid, 1)?;
                refine(out, &project, sample, mid, b, 1)?;
            }
        }
        Some(())
    };
    match grid {
        GridKind::H3 | GridKind::S2 => {
            for i in 0..n {
                let vector = |(lon, lat): Point| {
                    let (s, c) = lat.to_radians().sin_cos();
                    let (y, x) = lon.to_radians().sin_cos();
                    [c * x, c * y, s]
                };
                let a = vector(corners[i]);
                let b = vector(corners[(i + 1) % n]);
                side(i, &mut |t| {
                    let [x, y, z] = std::array::from_fn(|k| a[k] + t * (b[k] - a[k]));
                    Some((y.atan2(x).to_degrees(), z.atan2(x.hypot(y)).to_degrees()))
                })?;
            }
        }
        GridKind::Healpix => {
            let (depth, pixel) = healpix_decode(cell)?;
            let vertices = healpix::get(depth).projected_vertices(pixel);
            if native_count != vertices.len() {
                return None;
            }
            for i in 0..n {
                let (a, b) = (vertices[native_of[i]], vertices[native_of[(i + 1) % n]]);
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
                core::{cell::get_pentagon, serialization::deserialize},
            };
            let id = deserialize(cell).ok()?;
            let pentagon = get_pentagon(&id).ok()?;
            let vertices = pentagon.get_vertices_vec();
            let m = vertices.len();
            if native_count != m {
                return None;
            }
            for i in 0..n {
                // cell_to_boundary reverses the face winding on output.
                let a = vertices[m - 1 - native_of[i]];
                let b = vertices[m - 1 - native_of[(i + 1) % n]];
                side(i, &mut |t| {
                    let face = Face::new(a.x() + t * (b.x() - a.x()), a.y() + t * (b.y() - a.y()));
                    edge_samples.sample(id.origin_id, face)
                })?;
            }
        }
    }
    let Some(mut ring) = result else {
        return Some(Traced::Corners);
    };
    for point in &mut ring {
        point.0 = wrap_lon_180(point.0);
    }
    Some(Traced::Ring(ring))
}

#[derive(Clone, Copy)]
struct Sample {
    t: f64,
    lng_lat: Point,
    pixel: Point,
}

/// The midpoint sample of side `a`-`b` when the side must be split because the chord
/// misses the curve by more than the tolerance, `None` when the chord is close enough.
/// The outer `None` reports a sample that can't be projected.
fn split_point(
    project: &impl Fn(Point) -> Option<Point>,
    sample: &mut dyn FnMut(f64) -> Option<Point>,
    a: Sample,
    b: Sample,
    depth: u8,
) -> Option<Option<Sample>> {
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
    let split = depth < MAX_DEPTH
        && (deviates(pixel)
            || deviates(project(sample(parameter(0.25))?)?)
            || deviates(project(sample(parameter(0.75))?)?));
    Some(split.then_some(Sample {
        t: mid,
        lng_lat: p,
        pixel,
    }))
}

fn refine(
    out: &mut Vec<Point>,
    project: &impl Fn(Point) -> Option<Point>,
    sample: &mut dyn FnMut(f64) -> Option<Point>,
    a: Sample,
    b: Sample,
    depth: u8,
) -> Option<()> {
    if let Some(m) = split_point(project, sample, a, b, depth)? {
        refine(out, project, sample, a, m, depth + 1)?;
        refine(out, project, sample, m, b, depth + 1)
    } else {
        out.push(a.lng_lat);
        Some(())
    }
}
