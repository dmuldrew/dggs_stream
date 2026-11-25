//! Polygon fill for grids whose cells nest exactly inside their parents (S2, HEALPix).
//!
//! The fill walks the cell hierarchy from the base cells, descending only into cells
//! whose padded lon/lat box overlaps the polygon's box, and tests the cells at the
//! target resolution against the polygon. Tests are planar in lon/lat degrees, the
//! same model the cell geometry is published in (WKB, GeoParquet, vector tiles).

use geo_types::{MultiPolygon, Polygon};

use crate::grid::GridKind;
use crate::scalar_ops::{ContainmentMode, PolygonToCellsError, H3_POLYGON_TO_CELLS_MAX_CELLS};

/// `[west, south, east, north]` in degrees; `west..east` may leave `[-180, 180]`.
type LonLatBox = [f64; 4];

/// Even-odd / intersection slab index along latitude for rings with many edges.
struct EdgeSlabIndex {
    y_min: f64,
    inv_step: f64,
    edge_start_slab: Vec<u16>,
    slabs: Vec<Vec<u32>>,
}

impl EdgeSlabIndex {
    fn try_build(points: &[(f64, f64)], y_min: f64, y_max: f64) -> Option<Self> {
        let n = points.len();
        if n < 32
            || y_max <= y_min
            || !y_min.is_finite()
            || !y_max.is_finite()
            || n > u32::MAX as usize
        {
            return None;
        }
        let num_slabs = (n / 4).clamp(16, 512);
        let inv_step = (num_slabs as f64) / (y_max - y_min);
        if !inv_step.is_finite() {
            return None;
        }
        let mut index = Self {
            y_min,
            inv_step,
            edge_start_slab: Vec::with_capacity(n),
            slabs: vec![Vec::new(); num_slabs],
        };
        for i in 0..n {
            let y1 = points[i].1;
            let y2 = points[(i + 1) % n].1;
            let (y_lo, y_hi) = if y1 <= y2 { (y1, y2) } else { (y2, y1) };
            let s0 = index.slab_of(y_lo);
            let s1 = index.slab_of(y_hi);
            index.edge_start_slab.push(s0 as u16);
            for slab in &mut index.slabs[s0..=s1] {
                slab.push(i as u32);
            }
        }
        Some(index)
    }

    #[inline]
    fn slab_of(&self, y: f64) -> usize {
        let idx = ((y - self.y_min) * self.inv_step).floor();
        if idx <= 0.0 {
            0
        } else {
            (idx as usize).min(self.slabs.len() - 1)
        }
    }

    #[inline]
    fn slab(&self, y: f64) -> &[u32] {
        &self.slabs[self.slab_of(y)]
    }

    fn visit_range(&self, y_lo: f64, y_hi: f64, mut f: impl FnMut(usize) -> bool) -> bool {
        if y_hi < y_lo {
            return false;
        }
        let s0 = self.slab_of(y_lo);
        let s1 = self.slab_of(y_hi);
        for s in s0..=s1 {
            for &edge in &self.slabs[s] {
                let idx = edge as usize;
                // Edges spanning multiple slabs appear in s0..=s1; test once on entry.
                if (s == s0 || usize::from(self.edge_start_slab[idx]) == s) && f(idx) {
                    return true;
                }
            }
        }
        false
    }
}

struct Ring {
    /// Unclosed vertices, `(lon, lat)` in degrees.
    points: Vec<(f64, f64)>,
    bbox: LonLatBox,
    slabs: Option<EdgeSlabIndex>,
}

impl Ring {
    fn from_points(points: Vec<(f64, f64)>, indexed: bool) -> Self {
        let bbox = bbox_of(&points);
        let slabs = if indexed {
            EdgeSlabIndex::try_build(&points, bbox[1], bbox[3])
        } else {
            None
        };
        Self {
            points,
            bbox,
            slabs,
        }
    }

    fn new(coords: impl Iterator<Item = (f64, f64)>) -> Result<Self, PolygonToCellsError> {
        let mut points: Vec<(f64, f64)> = coords.collect();
        if points.iter().any(|p| !p.0.is_finite() || !p.1.is_finite()) {
            return Err(PolygonToCellsError::InvalidGeometry(
                "non-finite coordinates",
            ));
        }
        if points.iter().any(|p| !(-90.0..=90.0).contains(&p.1)) {
            return Err(PolygonToCellsError::InvalidGeometry(
                "latitude outside [-90, 90]",
            ));
        }
        if points.len() > 1 && points.first() == points.last() {
            points.pop();
        }
        Ok(Self::from_points(points, true))
    }

    #[inline]
    fn edge(&self, idx: usize) -> ((f64, f64), (f64, f64)) {
        let n = self.points.len();
        (self.points[idx], self.points[(idx + 1) % n])
    }

    fn edges(&self) -> impl Iterator<Item = ((f64, f64), (f64, f64))> + '_ {
        let n = self.points.len();
        (0..n).map(move |i| (self.points[i], self.points[(i + 1) % n]))
    }

    fn shift_lon(&mut self, delta: f64) {
        if delta == 0.0 {
            return;
        }
        for point in &mut self.points {
            point.0 += delta;
        }
        self.bbox[0] += delta;
        self.bbox[2] += delta;
    }

    /// Even-odd ray cast.
    fn contains(&self, (x, y): (f64, f64)) -> bool {
        if y < self.bbox[1]
            || y >= self.bbox[3]
            || x < self.bbox[0] - 1e-9
            || x > self.bbox[2] + 1e-9
        {
            return false;
        }
        let mut inside = false;
        let test_edge = |(x1, y1): (f64, f64), (x2, y2): (f64, f64)| -> bool {
            (y1 > y) != (y2 > y) && x < x1 + (y - y1) / (y2 - y1) * (x2 - x1)
        };
        if let Some(slabs) = &self.slabs {
            for &idx in slabs.slab(y) {
                let (a, b) = self.edge(idx as usize);
                if test_edge(a, b) {
                    inside = !inside;
                }
            }
        } else {
            for (a, b) in self.edges() {
                if test_edge(a, b) {
                    inside = !inside;
                }
            }
        }
        inside
    }

    /// Whether any edge of this ring intersects or has a vertex inside closed box `b`.
    fn intersects_box(&self, b: &LonLatBox) -> bool {
        if !boxes_overlap(&self.bbox, b) {
            return false;
        }
        if let Some(slabs) = &self.slabs {
            slabs.visit_range(b[1], b[3], |idx| {
                let (p, q) = self.edge(idx);
                segment_intersects_box(p, q, b)
            })
        } else {
            self.edges().any(|(p, q)| segment_intersects_box(p, q, b))
        }
    }

    /// Whether any edge of this ring crosses any edge of `cell`.
    fn crosses_ring(&self, cell: &Ring) -> bool {
        if !boxes_overlap(&self.bbox, &cell.bbox) {
            return false;
        }
        let edge_crosses = |a: (f64, f64), b: (f64, f64)| -> bool {
            let eb = [a.0.min(b.0), a.1.min(b.1), a.0.max(b.0), a.1.max(b.1)];
            boxes_overlap(&eb, &cell.bbox) && cell.edges().any(|(c, d)| segments_cross(a, b, c, d))
        };
        if let Some(slabs) = &self.slabs {
            slabs.visit_range(cell.bbox[1], cell.bbox[3], |idx| {
                let (a, b) = self.edge(idx);
                edge_crosses(a, b)
            })
        } else {
            self.edges().any(|(a, b)| edge_crosses(a, b))
        }
    }
}

fn segment_intersects_box(a: (f64, f64), b: (f64, f64), bx: &LonLatBox) -> bool {
    if a.0.max(b.0) < bx[0] || a.0.min(b.0) > bx[2] || a.1.max(b.1) < bx[1] || a.1.min(b.1) > bx[3]
    {
        return false;
    }
    let in_box = |p: (f64, f64)| p.0 >= bx[0] && p.0 <= bx[2] && p.1 >= bx[1] && p.1 <= bx[3];
    if in_box(a) || in_box(b) {
        return true;
    }
    let sw = (bx[0], bx[1]);
    let se = (bx[2], bx[1]);
    let ne = (bx[2], bx[3]);
    let nw = (bx[0], bx[3]);
    segments_cross(a, b, sw, se)
        || segments_cross(a, b, se, ne)
        || segments_cross(a, b, ne, nw)
        || segments_cross(a, b, nw, sw)
}

struct Area {
    exterior: Ring,
    holes: Vec<Ring>,
    bbox: LonLatBox,
}

impl Area {
    fn new(polygon: &Polygon<f64>) -> Result<Option<Self>, PolygonToCellsError> {
        let exterior = Ring::new(polygon.exterior().coords().map(|c| (c.x, c.y)))?;
        if exterior.points.len() < 3 {
            return Ok(None);
        }
        let holes = polygon
            .interiors()
            .iter()
            .map(|ring| Ring::new(ring.coords().map(|c| (c.x, c.y))))
            .collect::<Result<Vec<_>, _>>()?;
        let bbox = exterior.bbox;
        Ok(Some(Self {
            exterior,
            holes,
            bbox,
        }))
    }

    fn from_rect(rect: [f64; 4]) -> Result<Option<Self>, PolygonToCellsError> {
        if !rect.iter().all(|v| v.is_finite()) {
            return Err(PolygonToCellsError::InvalidGeometry(
                "non-finite coordinates",
            ));
        }
        if !(-90.0..=90.0).contains(&rect[1]) || !(-90.0..=90.0).contains(&rect[3]) {
            return Err(PolygonToCellsError::InvalidGeometry(
                "latitude outside [-90, 90]",
            ));
        }
        if rect[2] <= rect[0] || rect[3] <= rect[1] {
            return Ok(None);
        }
        let exterior = Ring::from_points(
            vec![
                (rect[0], rect[1]),
                (rect[2], rect[1]),
                (rect[2], rect[3]),
                (rect[0], rect[3]),
            ],
            false,
        );
        let bbox = exterior.bbox;
        Ok(Some(Self {
            exterior,
            holes: Vec::new(),
            bbox,
        }))
    }

    fn rings(&self) -> impl Iterator<Item = &Ring> {
        std::iter::once(&self.exterior).chain(&self.holes)
    }

    fn contains(&self, p: (f64, f64)) -> bool {
        self.exterior.contains(p) && !self.holes.iter().any(|hole| hole.contains(p))
    }

    /// Whether every point in closed rectangle `b` lies strictly inside `self`.
    fn contains_box(&self, b: &LonLatBox) -> bool {
        if b[0] < self.bbox[0] || b[2] > self.bbox[2] || b[1] < self.bbox[1] || b[3] > self.bbox[3]
        {
            return false;
        }
        for corner in [(b[0], b[1]), (b[2], b[1]), (b[2], b[3]), (b[0], b[3])] {
            if !self.contains(corner) {
                return false;
            }
        }
        !self.exterior.intersects_box(b) && !self.holes.iter().any(|hole| hole.intersects_box(b))
    }

    /// Whether any polygon edge crosses any edge of `cell`.
    fn boundary_crosses(&self, cell: &Ring) -> bool {
        self.rings().any(|ring| ring.crosses_ring(cell))
    }
}

fn bbox_of(points: &[(f64, f64)]) -> LonLatBox {
    let mut b = [
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    ];
    for &(x, y) in points {
        b[0] = b[0].min(x);
        b[1] = b[1].min(y);
        b[2] = b[2].max(x);
        b[3] = b[3].max(y);
    }
    b
}

fn boxes_overlap(a: &LonLatBox, b: &LonLatBox) -> bool {
    a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]
}

fn orient(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> f64 {
    (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
}

/// Proper crossing or touching of segments `ab` and `cd`.
fn segments_cross(a: (f64, f64), b: (f64, f64), c: (f64, f64), d: (f64, f64)) -> bool {
    let (d1, d2) = (orient(c, d, a), orient(c, d, b));
    let (d3, d4) = (orient(a, b, c), orient(a, b, d));
    if (d1 > 0.0) != (d2 > 0.0) && (d3 > 0.0) != (d4 > 0.0) && d1 != d2 && d3 != d4 {
        return true;
    }
    let on = |p: (f64, f64), q: (f64, f64), r: (f64, f64)| {
        r.0 >= p.0.min(q.0) && r.0 <= p.0.max(q.0) && r.1 >= p.1.min(q.1) && r.1 <= p.1.max(q.1)
    };
    (d1 == 0.0 && on(c, d, a))
        || (d2 == 0.0 && on(c, d, b))
        || (d3 == 0.0 && on(a, b, c))
        || (d4 == 0.0 && on(a, b, d))
}

/// Segments per cell side: coarse cells have strongly curved sides in lon/lat, so they
/// are traced; from level 5 (cells under about 3 degrees) the corners suffice.
fn side_segments(level: u8) -> u32 {
    1 << 5u8.saturating_sub(level)
}

/// A cell's boundary in lon/lat degrees, counter-clockwise, sides traced along their
/// true curve with `segments` steps each.
fn traced_boundary(grid: GridKind, cell: u64, segments: u32) -> Option<Vec<(f64, f64)>> {
    match grid {
        GridKind::S2 => {
            let id = s2::cellid::CellID(cell);
            if !id.is_valid() {
                return None;
            }
            let corners = s2::cell::Cell::from(id).vertices();
            let mut points = Vec::with_capacity(4 * segments as usize);
            for side in 0..4 {
                let (a, b) = (corners[side].0, corners[(side + 1) % 4].0);
                for step in 0..segments {
                    // A chord point pushed back to the sphere stays on the great circle.
                    let t = f64::from(step) / f64::from(segments);
                    let (x, y, z) = (
                        a.x + (b.x - a.x) * t,
                        a.y + (b.y - a.y) * t,
                        a.z + (b.z - a.z) * t,
                    );
                    points.push((y.atan2(x).to_degrees(), z.atan2(x.hypot(y)).to_degrees()));
                }
            }
            Some(points)
        }
        GridKind::Healpix => {
            let (depth, pixel) = crate::grid::healpix_decode(cell)?;
            let path = healpix::get(depth).path_along_cell_edge(
                pixel,
                healpix::dir::Cardinal::S,
                false,
                segments,
            );
            Some(
                path.iter()
                    .map(|p| (p.lon.to_degrees(), p.lat.to_degrees()))
                    .collect(),
            )
        }
        GridKind::H3 | GridKind::A5 => None,
    }
}

/// A cell's lon/lat ring for planar tests: sides traced, a pole vertex spread along the
/// pole, a pole in the interior closed over the pole, and longitudes unwrapped so
/// consecutive vertices are within 180 degrees.
fn cell_ring(grid: GridKind, cell: u64, level: u8) -> Option<Ring> {
    let traced = traced_boundary(grid, cell, side_segments(level))?;
    let n = traced.len();
    let mut points = Vec::with_capacity(n + 2);
    for i in 0..n {
        let (lon, lat) = traced[i];
        if lat.abs() >= 90.0 - 1e-9 {
            // A pole has no longitude: run along it between the neighbours' meridians.
            points.push((traced[(i + n - 1) % n].0, lat.clamp(-90.0, 90.0)));
            points.push((traced[(i + 1) % n].0, lat.clamp(-90.0, 90.0)));
        } else {
            points.push((lon, lat));
        }
    }
    for i in 1..points.len() {
        while points[i].0 - points[i - 1].0 > 180.0 {
            points[i].0 -= 360.0;
        }
        while points[i].0 - points[i - 1].0 < -180.0 {
            points[i].0 += 360.0;
        }
    }
    if let Some(north) = grid.interior_pole(cell) {
        // The boundary circles the pole once. The traced sides stop short of the first
        // vertex, so finish the turn at that vertex (one turn on from where the ring
        // started) and only then close the ring over the pole; closing straight from
        // the last sample would leave out the wedge between it and the first vertex.
        let pole = if north { 90.0 } else { -90.0 };
        let (first_lon, first_lat) = points[0];
        let last = points[points.len() - 1].0;
        let mut end = first_lon;
        while end - last > 180.0 {
            end -= 360.0;
        }
        while end - last < -180.0 {
            end += 360.0;
        }
        points.push((end, first_lat));
        points.push((end, pole));
        points.push((first_lon, pole));
    }
    Some(Ring::from_points(points, false))
}

/// A box that holds every point of the cell and of its descendants. Edges are curves
/// in lon/lat, so the vertex box is padded by half the longest edge's lon/lat length
/// (no point of an edge is further than that from an endpoint), with the longitude
/// pad stretched by the convergence of the meridians.
fn padded_box(ring: &Ring) -> LonLatBox {
    let mut b = ring.bbox;
    let pad = ring
        .edges()
        .map(|(p, q)| 0.5 * ((p.0 - q.0).abs() + (p.1 - q.1).abs()))
        .fold(0.0, f64::max);
    b[1] = (b[1] - pad).max(-90.0);
    b[3] = (b[3] + pad).min(90.0);
    let max_abs_lat = b[1].abs().max(b[3].abs());
    if max_abs_lat >= 85.0 {
        b[0] = -540.0;
        b[2] = 540.0;
    } else {
        let lon_pad = pad / max_abs_lat.to_radians().cos();
        b[0] -= lon_pad;
        b[2] += lon_pad;
    }
    b
}

/// Whole turns to try when laying a cell's ring over the polygon.
const TURNS: [f64; 3] = [0.0, -360.0, 360.0];

fn shifted(b: &LonLatBox, turn: f64) -> LonLatBox {
    [b[0] + turn, b[1], b[2] + turn, b[3]]
}

fn too_many(estimate: usize) -> PolygonToCellsError {
    PolygonToCellsError::TooManyCells {
        estimate,
        limit: H3_POLYGON_TO_CELLS_MAX_CELLS,
    }
}

/// Cells of `grid` at `res` whose area could fall inside `bbox`, as an upper estimate.
fn estimate_cells(grid: GridKind, res: u8, bbox: &LonLatBox) -> usize {
    const EARTH_RADIUS_M: f64 = 6_371_007.2;
    let lon_span = (bbox[2] - bbox[0]).clamp(0.0, 360.0).to_radians();
    let band = bbox[3].to_radians().sin() - bbox[1].to_radians().sin();
    let area = lon_span * band.max(0.0) * EARTH_RADIUS_M * EARTH_RADIUS_M;
    // Cells along the box's edge are visited too.
    let cell_area = grid.area_m2(res);
    // sqrt(area) undercounts the perimeter of long, thin boxes, even tending
    // to zero for a line that crosses millions of cells.
    let height = (bbox[3] - bbox[1]).to_radians() * EARTH_RADIUS_M;
    let nearest_equator = if bbox[1] <= 0.0 && bbox[3] >= 0.0 {
        0.0
    } else {
        bbox[1].abs().min(bbox[3].abs())
    };
    let width = lon_span * nearest_equator.to_radians().cos() * EARTH_RADIUS_M;
    (area / cell_area + 2.0 * (width + height) / cell_area.sqrt() + 16.0).ceil() as usize
}

struct FillScratch {
    base: Vec<u64>,
    children: Vec<u64>,
    stack: Vec<(u64, u8)>,
}

impl FillScratch {
    fn new(grid: GridKind) -> Self {
        let mut base = Vec::with_capacity(usize::from(grid.spec().base_cell_count));
        grid.base_cells_into(&mut base);
        Self {
            base,
            children: Vec::with_capacity(4),
            stack: Vec::new(),
        }
    }
}

/// Emit all descendant cells of `cell` (at `level < target_res`) directly at `target_res`.
fn emit_descendants_into(
    grid: GridKind,
    cell: u64,
    level: u8,
    target_res: u8,
    out: &mut Vec<u64>,
) -> Result<(), PolygonToCellsError> {
    let diff = target_res.saturating_sub(level);
    let Some(shift) = u32::from(diff).checked_mul(2) else {
        return Err(too_many(usize::MAX));
    };
    let Some(count) = 1usize.checked_shl(shift) else {
        return Err(too_many(usize::MAX));
    };
    let new_len = out.len().saturating_add(count);
    if new_len > H3_POLYGON_TO_CELLS_MAX_CELLS {
        return Err(too_many(new_len));
    }
    out.reserve(count);
    match grid {
        GridKind::S2 => {
            let id = s2::cellid::CellID(cell);
            let lvl = u64::from(target_res);
            let mut cur = id.child_begin_at_level(lvl);
            let end = id.child_end_at_level(lvl);
            while cur != end {
                out.push(cur.0);
                cur = cur.next();
            }
        }
        GridKind::Healpix => {
            let Some((_, pixel)) = crate::grid::healpix_decode(cell) else {
                return Err(PolygonToCellsError::InvalidGeometry("invalid healpix cell"));
            };
            let start = pixel << shift;
            for offset in 0..(count as u64) {
                out.push(crate::grid::healpix_encode(target_res, start + offset));
            }
        }
        GridKind::H3 | GridKind::A5 => {}
    }
    Ok(())
}

fn fill_nested_grid_area(
    grid: GridKind,
    area: &Area,
    res: u8,
    mode: ContainmentMode,
    scratch: &mut FillScratch,
    out: &mut Vec<u64>,
    mut short_circuits: Option<&mut usize>,
) -> Result<(), PolygonToCellsError> {
    let estimate = out
        .len()
        .saturating_add(estimate_cells(grid, res, &area.bbox));
    if estimate > H3_POLYGON_TO_CELLS_MAX_CELLS {
        return Err(too_many(estimate));
    }
    scratch.stack.clear();
    scratch
        .stack
        .extend(scratch.base.iter().map(|&cell| (cell, 0u8)));
    while let Some((cell, level)) = scratch.stack.pop() {
        let Some(mut ring) = cell_ring(grid, cell, level) else {
            continue;
        };
        if level < res {
            // A base cell can hold a pole or span most longitudes: always descend.
            let padded = padded_box(&ring);
            if level == 0
                || TURNS
                    .iter()
                    .any(|&turn| boxes_overlap(&shifted(&padded, turn), &area.bbox))
            {
                if level >= 1
                    && grid.interior_pole(cell).is_none()
                    && (padded[2] - padded[0]) < 180.0
                    && padded[1].abs().max(padded[3].abs()) < 85.0
                    && TURNS
                        .iter()
                        .any(|&turn| area.contains_box(&shifted(&padded, turn)))
                {
                    emit_descendants_into(grid, cell, level, res, out)?;
                    if let Some(count) = short_circuits.as_deref_mut() {
                        *count += 1;
                    }
                    continue;
                }
                grid.children_into(cell, level + 1, &mut scratch.children);
                scratch
                    .stack
                    .extend(scratch.children.iter().map(|&child| (child, level + 1)));
            }
            continue;
        }
        // Test the ring on every turn that lays it over the polygon.
        let tight = ring.bbox;
        let mut turned = 0.0;
        let mut matches = false;
        for turn in TURNS {
            if !boxes_overlap(&shifted(&tight, turn), &area.bbox) {
                continue;
            }
            ring.shift_lon(turn - turned);
            turned = turn;
            if cell_matches(grid, cell, &ring, area, mode) {
                matches = true;
                break;
            }
        }
        if matches {
            if out.len() >= H3_POLYGON_TO_CELLS_MAX_CELLS {
                return Err(too_many(out.len() + 1));
            }
            out.push(cell);
        }
    }
    Ok(())
}

/// Fill `multi` with cells of `grid` (S2 or HEALPix) at `res`, appending to `out`.
pub(crate) fn fill_nested_grid(
    grid: GridKind,
    multi: &MultiPolygon<f64>,
    res: u8,
    mode: ContainmentMode,
    out: &mut Vec<u64>,
) -> Result<(), PolygonToCellsError> {
    let mut scratch = FillScratch::new(grid);
    for polygon in &multi.0 {
        let Some(area) = Area::new(polygon)? else {
            continue;
        };
        fill_nested_grid_area(grid, &area, res, mode, &mut scratch, out, None)?;
    }
    out.sort_unstable();
    out.dedup();
    Ok(())
}

/// Fill axis-aligned lon/lat box `[west, south, east, north]` directly without WKB overhead.
pub(crate) fn fill_nested_grid_rect(
    grid: GridKind,
    rect: [f64; 4],
    res: u8,
    mode: ContainmentMode,
    out: &mut Vec<u64>,
) -> Result<(), PolygonToCellsError> {
    out.clear();
    let Some(area) = Area::from_rect(rect)? else {
        return Ok(());
    };
    let mut scratch = FillScratch::new(grid);
    fill_nested_grid_area(grid, &area, res, mode, &mut scratch, out, None)?;
    out.sort_unstable();
    out.dedup();
    Ok(())
}

fn cell_matches(
    grid: GridKind,
    cell: u64,
    ring: &Ring,
    area: &Area,
    mode: ContainmentMode,
) -> bool {
    match mode {
        ContainmentMode::ContainsCentroid => {
            let Some((lat, mut lon)) = grid.center_lat_lng(cell) else {
                return false;
            };
            // Bring the centre onto the same turn as the (shifted) ring.
            let reference = ring.points[0].0;
            while lon - reference > 180.0 {
                lon -= 360.0;
            }
            while lon - reference < -180.0 {
                lon += 360.0;
            }
            area.contains((lon, lat))
        }
        ContainmentMode::ContainsBoundary => {
            ring.points.iter().all(|&p| area.contains(p))
                && !area.boundary_crosses(ring)
                // A hole lying wholly inside the cell.
                && !area.holes.iter().any(|hole| ring.contains(hole.points[0]))
        }
        // `IntersectsBoundary`, `Covers` and any mode h3o adds later: any overlap,
        // including a polygon that lies wholly inside the cell.
        _ => {
            ring.points.iter().any(|&p| area.contains(p))
                || ring.contains(area.exterior.points[0])
                || area.boundary_crosses(ring)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{Coord, LineString, Polygon};

    fn naive_ring_contains(points: &[(f64, f64)], (x, y): (f64, f64)) -> bool {
        let mut inside = false;
        let n = points.len();
        for i in 0..n {
            let (x1, y1) = points[i];
            let (x2, y2) = points[(i + 1) % n];
            if (y1 > y) != (y2 > y) && x < x1 + (y - y1) / (y2 - y1) * (x2 - x1) {
                inside = !inside;
            }
        }
        inside
    }

    fn naive_area_contains(area: &Area, p: (f64, f64)) -> bool {
        naive_ring_contains(&area.exterior.points, p)
            && !area
                .holes
                .iter()
                .any(|hole| naive_ring_contains(&hole.points, p))
    }

    fn naive_boundary_crosses(area: &Area, cell: &Ring) -> bool {
        area.rings().any(|ring| {
            ring.edges()
                .any(|(a, b)| cell.edges().any(|(c, d)| segments_cross(a, b, c, d)))
        })
    }

    fn naive_cell_matches(
        grid: GridKind,
        cell: u64,
        ring: &Ring,
        area: &Area,
        mode: ContainmentMode,
    ) -> bool {
        match mode {
            ContainmentMode::ContainsCentroid => {
                let Some((lat, mut lon)) = grid.center_lat_lng(cell) else {
                    return false;
                };
                let reference = ring.points[0].0;
                while lon - reference > 180.0 {
                    lon -= 360.0;
                }
                while lon - reference < -180.0 {
                    lon += 360.0;
                }
                naive_area_contains(area, (lon, lat))
            }
            ContainmentMode::ContainsBoundary => {
                ring.points.iter().all(|&p| naive_area_contains(area, p))
                    && !naive_boundary_crosses(area, ring)
                    && !area
                        .holes
                        .iter()
                        .any(|hole| naive_ring_contains(&ring.points, hole.points[0]))
            }
            _ => {
                ring.points.iter().any(|&p| naive_area_contains(area, p))
                    || naive_ring_contains(&ring.points, area.exterior.points[0])
                    || naive_boundary_crosses(area, ring)
            }
        }
    }

    fn fill_nested_grid_naive(
        grid: GridKind,
        multi: &MultiPolygon<f64>,
        res: u8,
        mode: ContainmentMode,
    ) -> Result<Vec<u64>, PolygonToCellsError> {
        let mut out = Vec::new();
        let mut base = Vec::with_capacity(usize::from(grid.spec().base_cell_count));
        grid.base_cells_into(&mut base);
        let mut children = Vec::with_capacity(4);
        let mut stack = Vec::new();
        for polygon in &multi.0 {
            let Some(area) = Area::new(polygon)? else {
                continue;
            };
            let estimate = out
                .len()
                .saturating_add(estimate_cells(grid, res, &area.bbox));
            if estimate > H3_POLYGON_TO_CELLS_MAX_CELLS {
                return Err(too_many(estimate));
            }
            stack.clear();
            stack.extend(base.iter().map(|&cell| (cell, 0u8)));
            while let Some((cell, level)) = stack.pop() {
                let Some(mut ring) = cell_ring(grid, cell, level) else {
                    continue;
                };
                if level < res {
                    let padded = padded_box(&ring);
                    if level == 0
                        || TURNS
                            .iter()
                            .any(|&turn| boxes_overlap(&shifted(&padded, turn), &area.bbox))
                    {
                        grid.children_into(cell, level + 1, &mut children);
                        stack.extend(children.iter().map(|&child| (child, level + 1)));
                    }
                    continue;
                }
                let tight = bbox_of(&ring.points);
                let mut turned = 0.0;
                let mut matches = false;
                for turn in TURNS {
                    if !boxes_overlap(&shifted(&tight, turn), &area.bbox) {
                        continue;
                    }
                    for point in &mut ring.points {
                        point.0 += turn - turned;
                    }
                    turned = turn;
                    if naive_cell_matches(grid, cell, &ring, &area, mode) {
                        matches = true;
                        break;
                    }
                }
                if matches {
                    if out.len() >= H3_POLYGON_TO_CELLS_MAX_CELLS {
                        return Err(too_many(out.len() + 1));
                    }
                    out.push(cell);
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    fn ring_circle(cx: f64, cy: f64, radius: f64, vertices: usize) -> LineString<f64> {
        let mut coords: Vec<Coord<f64>> = (0..vertices)
            .map(|i| {
                let theta = 2.0 * std::f64::consts::PI * (i as f64) / (vertices as f64);
                Coord {
                    x: cx + radius * theta.cos(),
                    y: cy + radius * theta.sin(),
                }
            })
            .collect();
        coords.push(coords[0]);
        LineString::new(coords)
    }

    fn wavy_multi_polygon_with_holes() -> MultiPolygon<f64> {
        let vertices = 256;
        let mut ext_coords: Vec<Coord<f64>> = (0..vertices)
            .map(|i| {
                let theta = 2.0 * std::f64::consts::PI * (i as f64) / (vertices as f64);
                let r = 5.5 + 1.8 * (5.0 * theta).sin() + 0.7 * (11.0 * theta).cos();
                Coord {
                    x: 10.0 + r * theta.cos(),
                    y: 20.0 + r * theta.sin(),
                }
            })
            .collect();
        ext_coords.push(ext_coords[0]);
        let hole_medium = ring_circle(11.0, 20.5, 1.1, 48);
        // Pinhole well inside a single interior coarse cell so short-circuit must check holes.
        let hole_subcell = ring_circle(8.5, 19.0, 0.02, 16);
        MultiPolygon::new(vec![Polygon::new(
            LineString::new(ext_coords),
            vec![hole_medium, hole_subcell],
        )])
    }

    fn c_shaped_polygon() -> MultiPolygon<f64> {
        let exterior = LineString::from(vec![
            (-15.0, -5.0),
            (-5.0, -5.0),
            (-5.0, -2.0),
            (-11.0, -2.0),
            (-11.0, 2.0),
            (-5.0, 2.0),
            (-5.0, 5.0),
            (-15.0, 5.0),
            (-15.0, -5.0),
        ]);
        MultiPolygon::new(vec![Polygon::new(exterior, vec![])])
    }

    fn rect_polygon(w: f64, s: f64, e: f64, n: f64) -> MultiPolygon<f64> {
        let exterior = LineString::from(vec![(w, s), (e, s), (e, n), (w, n), (w, s)]);
        MultiPolygon::new(vec![Polygon::new(exterior, vec![])])
    }

    #[test]
    fn optimized_fill_matches_naive_across_grids_modes_and_hard_geometries() {
        let modes = [
            ContainmentMode::ContainsCentroid,
            ContainmentMode::ContainsBoundary,
            ContainmentMode::IntersectsBoundary,
            ContainmentMode::Covers,
        ];
        let cases: &[(&str, MultiPolygon<f64>, u8)] = &[
            ("wavy_256_holes", wavy_multi_polygon_with_holes(), 6),
            ("c_shaped", c_shaped_polygon(), 5),
            (
                "antimeridian_pos",
                rect_polygon(177.0, 10.0, 183.0, 15.0),
                5,
            ),
            (
                "antimeridian_neg",
                rect_polygon(-183.0, -15.0, -177.0, -10.0),
                5,
            ),
            ("polar_cap", rect_polygon(-180.0, 84.0, 180.0, 90.0), 4),
        ];

        for grid in [GridKind::S2, GridKind::Healpix] {
            for (name, poly, res) in cases {
                for mode in modes {
                    let expected = fill_nested_grid_naive(grid, poly, *res, mode).unwrap();
                    let mut actual = Vec::new();
                    fill_nested_grid(grid, poly, *res, mode, &mut actual).unwrap();
                    assert_eq!(
                        actual, expected,
                        "mismatch for {grid:?} case={name} res={res} mode={mode:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn interior_short_circuit_triggers_and_rect_matches_polygon_fill() {
        let rect = [-6.0, -6.0, 6.0, 6.0];
        let poly = rect_polygon(rect[0], rect[1], rect[2], rect[3]);
        for grid in [GridKind::S2, GridKind::Healpix] {
            let area = Area::new(&poly.0[0]).unwrap().unwrap();
            let mut scratch = FillScratch::new(grid);
            let mut area_out = Vec::new();
            let mut short_circuits = 0usize;
            fill_nested_grid_area(
                grid,
                &area,
                6,
                ContainmentMode::ContainsCentroid,
                &mut scratch,
                &mut area_out,
                Some(&mut short_circuits),
            )
            .unwrap();
            area_out.sort_unstable();
            area_out.dedup();
            assert!(
                short_circuits > 0,
                "expected interior short-circuits for {grid:?}"
            );

            let mut rect_out = vec![123u64];
            fill_nested_grid_rect(
                grid,
                rect,
                6,
                ContainmentMode::ContainsCentroid,
                &mut rect_out,
            )
            .unwrap();
            assert_eq!(rect_out, area_out);
        }
    }

    #[test]
    fn emit_descendants_rejects_oversized_jump() {
        let mut s2_base = Vec::new();
        GridKind::S2.base_cells_into(&mut s2_base);
        let s2_child = {
            let mut c = Vec::new();
            GridKind::S2.children_into(s2_base[0], 1, &mut c);
            c[0]
        };
        let mut out = Vec::new();
        let err = emit_descendants_into(GridKind::S2, s2_child, 1, 20, &mut out).unwrap_err();
        assert!(matches!(err, PolygonToCellsError::TooManyCells { .. }));
        assert!(out.is_empty());

        let mut hp_base = Vec::new();
        GridKind::Healpix.base_cells_into(&mut hp_base);
        let mut hp_children = Vec::new();
        GridKind::Healpix.children_into(hp_base[0], 1, &mut hp_children);
        let err =
            emit_descendants_into(GridKind::Healpix, hp_children[0], 1, 20, &mut out).unwrap_err();
        assert!(matches!(err, PolygonToCellsError::TooManyCells { .. }));
        assert!(out.is_empty());
    }
}
