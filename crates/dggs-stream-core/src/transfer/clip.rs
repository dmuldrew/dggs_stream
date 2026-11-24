//! Exact overlap areas between a polygon and the unit squares of a pixel grid.
//!
//! Uses an Edge-DDA grid traversal combined with Green's-theorem signed trapezoid
//! accumulation and a left-to-right row prefix sum. Interior pixels are emitted as
//! horizontal `(row, col_start, col_end, area)` spans rather than per-pixel clips.

use crate::raster::geotransform::GeoTransform;
use crate::raster::ground_area::segment_ellipsoidal_integrals;

/// Overlaps at or below this fraction of a pixel are not reported: the prefix-sum
/// sweep cannot tell them from floating-point cancellation noise (a few ulps of 1.0
/// per pixel crossed). Coverage within this much of 1.0 is snapped to a full pixel.
/// The effect on any total is below `1e-14` of a pixel per boundary pixel, and
/// ordinary overlay already ignores overlaps under `1e-12`.
const AREA_EPS: f64 = 1e-14;

/// Maximum rows or columns per packed `u64` `(rel_row << 32) | rel_col` chunk.
const MAX_PACKED_DIM: i64 = u32::MAX as i64;

/// Rows with at most this many events are ordered by insertion sort after bucketing.
const INSERTION_SORT_MAX: usize = 16;

/// Below this many events a single comparison sort beats bucketing by row.
const ROW_BUCKET_MIN_EVENTS: usize = 256;

/// Unsigned area of a polygon given by its vertices in order (shoelace formula).
///
/// Shifts coordinates relative to the first vertex to avoid catastrophic
/// cancellation when a small cell sits at large raster coordinates.
pub fn polygon_area(points: &[(f64, f64)]) -> f64 {
    0.5 * signed_twice_area(points).abs()
}

/// Twice the signed area of `points` relative to `points[0]` (positive for CCW
/// polygons in standard Cartesian `(x, y)` coordinates).
#[inline]
fn signed_twice_area(points: &[(f64, f64)]) -> f64 {
    let n = points.len();
    if n < 3 {
        return 0.0;
    }
    let (ox, oy) = points[0];
    let mut twice = 0.0;
    for i in 1..n - 1 {
        let (x0, y0) = (points[i].0 - ox, points[i].1 - oy);
        let (x1, y1) = (points[i + 1].0 - ox, points[i + 1].1 - oy);
        twice += x0 * y1 - x1 * y0;
    }
    twice
}

#[derive(Clone, Copy)]
struct CellEvent {
    key: u64,
    local: f64,
    carry: f64,
}

#[derive(Clone, Copy)]
struct GroundCellEvent {
    key: u64,
    local_frac: f64,
    carry_frac: f64,
    local_m2: f64,
    carry_m2: f64,
}

/// Reusable scratch buffers for Edge-DDA polygon clipping across cells.
#[derive(Default)]
pub(super) struct ClipScratch {
    events: Vec<CellEvent>,
    row_carry: Vec<f64>,
    ground_events: Vec<GroundCellEvent>,
    ground_row_carry: Vec<(f64, f64)>,
    /// Destination of the row-bucketing pass; swapped with the event buffer.
    sorted_events: Vec<CellEvent>,
    sorted_ground_events: Vec<GroundCellEvent>,
    row_offsets: Vec<usize>,
    /// Most edge events one visit may buffer; `None` is unbounded.
    event_limit: Option<usize>,
}

impl ClipScratch {
    /// Cap the edge events a visit buffers before sorting. A visit that would exceed
    /// `limit` stops without calling the visitor and returns `false`, exactly as if
    /// the visitor had declined, so callers that bound their own output also bound
    /// this scratch. `None` removes the cap.
    pub(super) fn set_event_limit(&mut self, limit: Option<usize>) {
        self.event_limit = limit;
    }
}

/// Order `events` by `(row, col)` key: a counting sort into row buckets, then a
/// per-row sort. The DDA emits few events per row, so for large cells the per-row
/// pass is a short insertion sort instead of a full `O(E log E)` comparison sort;
/// small cells keep the single comparison sort, which is cheaper at their size.
fn sort_events_by_key<E: DdaEvent>(
    events: &mut Vec<E>,
    sorted: &mut Vec<E>,
    row_offsets: &mut Vec<usize>,
    num_rows: usize,
) {
    if events.len() < ROW_BUCKET_MIN_EVENTS {
        events.sort_unstable_by_key(|e| e.key());
        return;
    }
    row_offsets.clear();
    row_offsets.resize(num_rows + 1, 0);
    for e in events.iter() {
        row_offsets[(e.key() >> 32) as usize + 1] += 1;
    }
    for r in 0..num_rows {
        row_offsets[r + 1] += row_offsets[r];
    }
    // `sorted` is fully overwritten below; seed its length with copies.
    sorted.clear();
    sorted.extend_from_slice(events);
    for &e in events.iter() {
        let slot = &mut row_offsets[(e.key() >> 32) as usize];
        sorted[*slot] = e;
        *slot += 1;
    }
    // `row_offsets[r]` now holds the end of row `r` (= start of row `r + 1`).
    let mut start = 0;
    for &end in &row_offsets[..num_rows] {
        let row = &mut sorted[start..end];
        if row.len() <= INSERTION_SORT_MAX {
            insertion_sort_by_key(row);
        } else {
            row.sort_unstable_by_key(|e| e.key());
        }
        start = end;
    }
    std::mem::swap(events, sorted);
}

#[inline]
fn insertion_sort_by_key<E: DdaEvent>(items: &mut [E]) {
    for i in 1..items.len() {
        let item = items[i];
        let key = item.key();
        let mut j = i;
        while j > 0 && items[j - 1].key() > key {
            items[j] = items[j - 1];
            j -= 1;
        }
        items[j] = item;
    }
}

trait DdaEvent: Copy {
    type Acc: Copy + Default;
    fn key(self) -> u64;
    fn local(self) -> Self::Acc;
    fn carry(self) -> Self::Acc;
    fn add(a: Self::Acc, b: Self::Acc) -> Self::Acc;
}

impl DdaEvent for CellEvent {
    type Acc = f64;
    #[inline(always)]
    fn key(self) -> u64 {
        self.key
    }
    #[inline(always)]
    fn local(self) -> f64 {
        self.local
    }
    #[inline(always)]
    fn carry(self) -> f64 {
        self.carry
    }
    #[inline(always)]
    fn add(a: f64, b: f64) -> f64 {
        a + b
    }
}

impl DdaEvent for GroundCellEvent {
    type Acc = (f64, f64);
    #[inline(always)]
    fn key(self) -> u64 {
        self.key
    }
    #[inline(always)]
    fn local(self) -> (f64, f64) {
        (self.local_frac, self.local_m2)
    }
    #[inline(always)]
    fn carry(self) -> (f64, f64) {
        (self.carry_frac, self.carry_m2)
    }
    #[inline(always)]
    fn add(a: (f64, f64), b: (f64, f64)) -> (f64, f64) {
        (a.0 + b.0, a.1 + b.1)
    }
}

#[derive(Clone, Copy)]
struct PreparedClip {
    first_row: i64,
    last_row: i64,
    first_col: i64,
    last_col: i64,
    min_x: f64,
    orient_sign: f64,
}

impl PreparedClip {
    #[inline]
    fn with_bounds(&self, row_start: i64, row_end: i64, col_start: i64, col_end: i64) -> Self {
        Self {
            first_row: row_start,
            last_row: row_end,
            first_col: col_start,
            last_col: col_end,
            min_x: self.min_x,
            orient_sign: self.orient_sign,
        }
    }

    #[inline]
    fn has_left_clip(&self) -> bool {
        self.min_x <= self.first_col as f64
    }
}

/// Bounds of the pixel window `polygon` can touch, or `None` when nothing can be
/// visited: fewer than three vertices, a non-finite vertex, zero signed area, or a
/// polygon outside the window.
fn prepare_clip(polygon: &[(f64, f64)], rows: (i64, i64), width: i64) -> Option<PreparedClip> {
    if polygon.len() < 3 || rows.0 >= rows.1 || width <= 0 {
        return None;
    }
    let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
    for &(x, y) in polygon {
        // The public contract is to visit nothing, in every build profile.
        if !(x.is_finite() && y.is_finite()) {
            return None;
        }
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }
    if max_x <= 0.0 || min_x >= width as f64 || max_y <= rows.0 as f64 || min_y >= rows.1 as f64 {
        return None;
    }
    let twice_area = signed_twice_area(polygon);
    if !twice_area.is_finite() || twice_area == 0.0 {
        return None;
    }
    let first_row = (min_y.floor() as i64).max(rows.0);
    let last_row = (max_y.ceil() as i64).min(rows.1);
    let first_col = (min_x.floor() as i64).max(0);
    let last_col = (max_x.ceil() as i64).min(width);
    if first_row >= last_row || first_col >= last_col {
        return None;
    }
    Some(PreparedClip {
        first_row,
        last_row,
        first_col,
        last_col,
        min_x,
        orient_sign: if twice_area > 0.0 { 1.0 } else { -1.0 },
    })
}

/// Run `f` on sub-windows of `prep` whose relative row and column indices fit the
/// packed `u64` event key. Stream callers address rasters with `u32` dimensions and
/// always get a single chunk; only the public [`pixel_overlaps`], which accepts any
/// `i64` width, can need more than one.
fn for_each_packed_chunk(prep: &PreparedClip, mut f: impl FnMut(&PreparedClip) -> bool) -> bool {
    let mut r0 = prep.first_row;
    while r0 < prep.last_row {
        let r1 = r0.saturating_add(MAX_PACKED_DIM).min(prep.last_row);
        let mut c0 = prep.first_col;
        while c0 < prep.last_col {
            let c1 = c0.saturating_add(MAX_PACKED_DIM).min(prep.last_col);
            if !f(&prep.with_bounds(r0, r1, c0, c1)) {
                return false;
            }
            c0 = c1;
        }
        r0 = r1;
    }
    true
}

#[inline(always)]
fn make_key(rel_row: usize, rel_col: usize) -> u64 {
    ((rel_row as u64) << 32) | (rel_col as u64)
}

#[inline(always)]
fn snap_unit(area: f64) -> f64 {
    if area >= 1.0 - AREA_EPS {
        1.0
    } else {
        area
    }
}

struct LeftPiece {
    rel_row: usize,
    r: i64,
    ry_a: f64,
    ry_b: f64,
    dir: f64,
    dh: f64,
}

struct ColPiece {
    rel_row: usize,
    rel_col: usize,
    r: i64,
    ry_a: f64,
    ry_b: f64,
    rx_a: f64,
    rx_b: f64,
    dir: f64,
    dh: f64,
}

/// Traverse every polygon edge across `[prep.first_row, prep.last_row) x [prep.first_col, prep.last_col)`
/// using canonical bottom-to-top DDA stepping.
///
/// Returns `false` as soon as `on_col` returns `false`, abandoning the traversal.
#[inline(always)]
fn walk_polygon_dda(
    polygon: &[(f64, f64)],
    prep: &PreparedClip,
    mut on_left: impl FnMut(LeftPiece),
    mut on_col: impl FnMut(ColPiece) -> bool,
) -> bool {
    let first_row = prep.first_row;
    let last_row = prep.last_row;
    let first_col = prep.first_col;
    let last_col = prep.last_col;
    let first_col_f = first_col as f64;
    let last_col_f = last_col as f64;
    let n = polygon.len();

    for i in 0..n {
        let p0 = polygon[i];
        let p1 = polygon[(i + 1) % n];
        if p0.1 == p1.1 {
            continue;
        }
        // Canonicalize bottom-to-top (`ya < yb`) so shared edges between neighboring
        // cells compute bit-identical crossings with exact opposite `dir`.
        let (xa, ya, xb, yb, dir) = if p0.1 < p1.1 {
            (p0.0, p0.1, p1.0, p1.1, prep.orient_sign)
        } else {
            (p1.0, p1.1, p0.0, p0.1, -prep.orient_sign)
        };
        let r_start = (ya.floor() as i64).max(first_row);
        let r_end = (yb.ceil() as i64).min(last_row);
        if r_start >= r_end {
            continue;
        }
        let dx = xb - xa;
        let dy_edge = yb - ya;
        let (x_min_edge, x_max_edge) = if xa <= xb { (xa, xb) } else { (xb, xa) };
        let x_at_y = |y: f64| -> f64 {
            if y <= ya {
                xa
            } else if y >= yb {
                xb
            } else {
                let t = (y - ya) / dy_edge;
                (xa + t * dx).clamp(x_min_edge, x_max_edge)
            }
        };

        let mut y_cur = ya.max(r_start as f64);
        let mut x_cur = x_at_y(y_cur);

        for r in r_start..r_end {
            let r_f = r as f64;
            let y_next = yb.min((r + 1) as f64);
            let x_next = x_at_y(y_next);
            let ry0 = (y_cur - r_f).clamp(0.0, 1.0);
            let ry1 = (y_next - r_f).clamp(0.0, 1.0);
            let row_h = ry1 - ry0;
            if row_h <= 0.0 {
                y_cur = y_next;
                x_cur = x_next;
                continue;
            }
            let rel_row = (r - first_row) as usize;
            let (x_lo, x_hi) = if x_cur <= x_next {
                (x_cur, x_next)
            } else {
                (x_next, x_cur)
            };

            if x_hi <= first_col_f {
                on_left(LeftPiece {
                    rel_row,
                    r,
                    ry_a: ry0,
                    ry_b: ry1,
                    dir,
                    dh: row_h,
                });
            } else if x_lo < last_col_f {
                let c0 = x_lo.floor() as i64;
                if c0 >= first_col && x_hi <= (c0 + 1) as f64 {
                    let cf = c0 as f64;
                    if !on_col(ColPiece {
                        rel_row,
                        rel_col: (c0 - first_col) as usize,
                        r,
                        ry_a: ry0,
                        ry_b: ry1,
                        rx_a: (x_cur - cf).clamp(0.0, 1.0),
                        rx_b: (x_next - cf).clamp(0.0, 1.0),
                        dir,
                        dh: row_h,
                    }) {
                        return false;
                    }
                } else {
                    let x_span = x_hi - x_lo;
                    if x_span > 0.0 {
                        let h_per_x = row_h / x_span;
                        let pos_slope = x_cur < x_next;
                        let mut x_prev = x_lo.max(first_col_f);
                        let mut h_prev = if x_lo < first_col_f {
                            let h_clip = ((first_col_f - x_lo) * h_per_x).min(row_h);
                            if h_clip > 0.0 {
                                let (left_rya, left_ryb) = if pos_slope {
                                    (ry0, (ry0 + h_clip).min(ry1))
                                } else {
                                    ((ry1 - h_clip).max(ry0), ry1)
                                };
                                on_left(LeftPiece {
                                    rel_row,
                                    r,
                                    ry_a: left_rya,
                                    ry_b: left_ryb,
                                    dir,
                                    dh: h_clip,
                                });
                            }
                            h_clip
                        } else {
                            0.0
                        };
                        let c_start = (x_prev.floor() as i64).max(first_col);
                        let c_end = (x_hi.ceil() as i64).min(last_col);
                        for c in c_start..c_end {
                            let cf = c as f64;
                            let x_next_c = x_hi.min((c + 1) as f64);
                            let h_next = if x_next_c >= x_hi {
                                row_h
                            } else {
                                ((x_next_c - x_lo) * h_per_x).min(row_h)
                            };
                            let dh = h_next - h_prev;
                            if dh > 0.0 {
                                let rx_prev = (x_prev - cf).clamp(0.0, 1.0);
                                let rx_next = (x_next_c - cf).clamp(0.0, 1.0);
                                let (ry_a, ry_b, rx_a, rx_b) = if pos_slope {
                                    (
                                        (ry0 + h_prev).clamp(ry0, ry1),
                                        (ry0 + h_next).clamp(ry0, ry1),
                                        rx_prev,
                                        rx_next,
                                    )
                                } else {
                                    (
                                        (ry1 - h_next).clamp(ry0, ry1),
                                        (ry1 - h_prev).clamp(ry0, ry1),
                                        rx_next,
                                        rx_prev,
                                    )
                                };
                                if !on_col(ColPiece {
                                    rel_row,
                                    rel_col: (c - first_col) as usize,
                                    r,
                                    ry_a,
                                    ry_b,
                                    rx_a,
                                    rx_b,
                                    dir,
                                    dh,
                                }) {
                                    return false;
                                }
                            }
                            x_prev = x_next_c;
                            h_prev = h_next;
                        }
                    }
                }
            }

            y_cur = y_next;
            x_cur = x_next;
        }
    }
    true
}

#[inline(always)]
fn sweep_single_row<E: DdaEvent>(
    r: i64,
    first_col: i64,
    last_col: i64,
    mut cover: E::Acc,
    row_events: &[E],
    emit_span: &mut impl FnMut(i64, i64, i64, E::Acc) -> bool,
) -> bool {
    let mut prev_col = first_col;
    let mut i = 0;
    while i < row_events.len() {
        let key = row_events[i].key();
        let col = first_col + (key & 0xffff_ffff) as i64;
        let mut local = row_events[i].local();
        let mut carry = row_events[i].carry();
        i += 1;
        while i < row_events.len() && row_events[i].key() == key {
            local = E::add(local, row_events[i].local());
            carry = E::add(carry, row_events[i].carry());
            i += 1;
        }
        if prev_col < col && !emit_span(r, prev_col, col, cover) {
            return false;
        }
        let area = E::add(cover, local);
        cover = E::add(cover, carry);
        if !emit_span(r, col, col + 1, area) {
            return false;
        }
        prev_col = col + 1;
    }
    prev_col >= last_col || emit_span(r, prev_col, last_col, cover)
}

#[inline(always)]
fn sweep_dda_events<E: DdaEvent>(
    prep: &PreparedClip,
    events: &[E],
    row_carry: &[E::Acc],
    mut emit_span: impl FnMut(i64, i64, i64, E::Acc) -> bool,
) -> bool {
    if prep.has_left_clip() {
        let num_rows = (prep.last_row - prep.first_row) as usize;
        let mut ev_idx = 0;
        for (rel_row, &init_cover) in row_carry.iter().enumerate().take(num_rows) {
            let start = ev_idx;
            while ev_idx < events.len() && (events[ev_idx].key() >> 32) as usize == rel_row {
                ev_idx += 1;
            }
            let r = prep.first_row + rel_row as i64;
            if !sweep_single_row(
                r,
                prep.first_col,
                prep.last_col,
                init_cover,
                &events[start..ev_idx],
                &mut emit_span,
            ) {
                return false;
            }
        }
    } else {
        let mut ev_idx = 0;
        while ev_idx < events.len() {
            let rel_row = (events[ev_idx].key() >> 32) as usize;
            let start = ev_idx;
            ev_idx += 1;
            while ev_idx < events.len() && (events[ev_idx].key() >> 32) as usize == rel_row {
                ev_idx += 1;
            }
            let r = prep.first_row + rel_row as i64;
            if !sweep_single_row(
                r,
                prep.first_col,
                prep.last_col,
                E::Acc::default(),
                &events[start..ev_idx],
                &mut emit_span,
            ) {
                return false;
            }
        }
    }
    true
}

fn visit_overlap_spans_chunk(
    polygon: &[(f64, f64)],
    prep: &PreparedClip,
    scratch: &mut ClipScratch,
    visit_span: &mut impl FnMut(i64, i64, i64, f64) -> bool,
) -> bool {
    scratch.events.clear();
    if prep.has_left_clip() {
        let num_rows = (prep.last_row - prep.first_row) as usize;
        scratch.row_carry.clear();
        scratch.row_carry.resize(num_rows, 0.0);
    }

    let row_carry = &mut scratch.row_carry;
    let events = &mut scratch.events;
    let event_limit = scratch.event_limit.unwrap_or(usize::MAX);
    let walked = walk_polygon_dda(
        polygon,
        prep,
        |left| {
            row_carry[left.rel_row] -= left.dir * left.dh;
        },
        |col| {
            let dy = col.dir * col.dh;
            events.push(CellEvent {
                key: make_key(col.rel_row, col.rel_col),
                local: (0.5 * (col.rx_a + col.rx_b) - 1.0) * dy,
                carry: -dy,
            });
            events.len() <= event_limit
        },
    );
    if !walked {
        return false;
    }

    sort_events_by_key(
        &mut scratch.events,
        &mut scratch.sorted_events,
        &mut scratch.row_offsets,
        (prep.last_row - prep.first_row) as usize,
    );
    sweep_dda_events(
        prep,
        &scratch.events,
        &scratch.row_carry,
        |r, c0, c1, acc| {
            if acc > AREA_EPS {
                visit_span(r, c0, c1, snap_unit(acc))
            } else {
                true
            }
        },
    )
}

/// Visit every pixel and horizontal interior span covered by `polygon` within
/// `0 <= c < width` and `rows.0 <= r < rows.1`.
///
/// `visit(row, col_start, col_end, area)` is called in row-major order for
/// contiguous uniform-coverage runs `[col_start, col_end)` (`col_start < col_end`),
/// including single boundary pixels (`col_end == col_start + 1`).
/// Returns `false` immediately if `visit` returns `false`.
pub(super) fn visit_overlap_spans_into(
    polygon: &[(f64, f64)],
    rows: (i64, i64),
    width: i64,
    scratch: &mut ClipScratch,
    mut visit: impl FnMut(i64, i64, i64, f64) -> bool,
) -> bool {
    let Some(prep) = prepare_clip(polygon, rows, width) else {
        return true;
    };
    for_each_packed_chunk(&prep, |chunk| {
        visit_overlap_spans_chunk(polygon, chunk, scratch, &mut visit)
    })
}

/// Visit every pixel overlapped by `polygon` with `rows.0 <= r < rows.1` and
/// `0 <= c < width` and area `1e-14 < area <= 1`, as `visit(row, col, area)` (the same
/// row-first order as the span visitors). Returns `false` immediately when `visit`
/// declines an overlap.
pub(super) fn visit_overlaps(
    polygon: &[(f64, f64)],
    rows: (i64, i64),
    width: i64,
    scratch: &mut ClipScratch,
    mut visit: impl FnMut(i64, i64, f64) -> bool,
) -> bool {
    visit_overlap_spans_into(polygon, rows, width, scratch, |r, c0, c1, area| {
        for c in c0..c1 {
            if !visit(r, c, area) {
                return false;
            }
        }
        true
    })
}

/// For a polygon in pixel coordinates (pixel `(c, r)` is the unit square
/// `[c, c + 1] x [r, r + 1]`), call `visit(c, r, area)` for every pixel with
/// `rows.0 <= r < rows.1` and `0 <= c < width` that the polygon overlaps, with the
/// overlapped area. The polygon may be convex or concave, in either orientation; it
/// must not self-intersect.
///
/// Reported areas satisfy `1e-14 < area <= 1`: an overlap of at most `1e-14` of a pixel
/// is indistinguishable from rounding noise in the sweep and is skipped. Polygons with
/// fewer than three vertices, a non-finite vertex, or zero area visit nothing.
pub fn pixel_overlaps(
    polygon: &[(f64, f64)],
    rows: (i64, i64),
    width: i64,
    mut visit: impl FnMut(i64, i64, f64),
) {
    let mut scratch = ClipScratch::default();
    visit_overlaps(polygon, rows, width, &mut scratch, |r, c, area| {
        visit(c, r, area);
        true
    });
}

#[allow(clippy::too_many_arguments)]
fn visit_ground_area_spans_chunk(
    polygon: &[(f64, f64)],
    prep: &PreparedClip,
    rows_start: i64,
    gt: &GeoTransform,
    row_areas: &[f64],
    scratch: &mut ClipScratch,
    visit_span: &mut impl FnMut(i64, i64, i64, f64, f64) -> bool,
) -> bool {
    scratch.ground_events.clear();
    if prep.has_left_clip() {
        let num_rows = (prep.last_row - prep.first_row) as usize;
        scratch.ground_row_carry.clear();
        scratch.ground_row_carry.resize(num_rows, (0.0, 0.0));
    }

    let ground_row_carry = &mut scratch.ground_row_carry;
    let ground_events = &mut scratch.ground_events;
    let event_limit = scratch.event_limit.unwrap_or(usize::MAX);
    let walked = walk_polygon_dda(
        polygon,
        prep,
        |left| {
            let full_m2 = if left.ry_a == 0.0 && left.ry_b == 1.0 {
                row_areas[(left.r - rows_start) as usize]
            } else {
                let r_f = left.r as f64;
                segment_ellipsoidal_integrals(gt, 0.0, r_f + left.ry_a, 0.0, r_f + left.ry_b).1
            };
            let entry = &mut ground_row_carry[left.rel_row];
            entry.0 -= left.dir * left.dh;
            entry.1 -= left.dir * full_m2;
        },
        |col| {
            let r_f = col.r as f64;
            let (seg_local_m2, seg_full_m2) = segment_ellipsoidal_integrals(
                gt,
                col.rx_a,
                r_f + col.ry_a,
                col.rx_b,
                r_f + col.ry_b,
            );
            let dy = col.dir * col.dh;
            ground_events.push(GroundCellEvent {
                key: make_key(col.rel_row, col.rel_col),
                local_frac: (0.5 * (col.rx_a + col.rx_b) - 1.0) * dy,
                carry_frac: -dy,
                local_m2: -col.dir * seg_local_m2,
                carry_m2: -col.dir * seg_full_m2,
            });
            ground_events.len() <= event_limit
        },
    );
    if !walked {
        return false;
    }

    sort_events_by_key(
        &mut scratch.ground_events,
        &mut scratch.sorted_ground_events,
        &mut scratch.row_offsets,
        (prep.last_row - prep.first_row) as usize,
    );
    sweep_dda_events(
        prep,
        &scratch.ground_events,
        &scratch.ground_row_carry,
        |r, c0, c1, (frac, m2)| {
            let row_area = row_areas[(r - rows_start) as usize];
            if frac >= 1.0 - AREA_EPS {
                visit_span(r, c0, c1, 1.0, row_area)
            } else if frac > AREA_EPS && m2 > row_area * AREA_EPS {
                visit_span(r, c0, c1, frac, m2.min(row_area))
            } else {
                true
            }
        },
    )
}

/// Visit every pixel and horizontal interior span covered by `polygon` on a north-up
/// WGS84 raster, yielding `(row, col_start, col_end, fraction, ellipsoidal_m2)`
/// without per-pixel polygon clipping.
#[allow(clippy::too_many_arguments)]
pub(super) fn visit_ground_area_spans_into(
    polygon: &[(f64, f64)],
    rows: (i64, i64),
    width: i64,
    gt: &GeoTransform,
    row_areas: &[f64],
    scratch: &mut ClipScratch,
    mut visit: impl FnMut(i64, i64, i64, f64, f64) -> bool,
) -> bool {
    let Some(prep) = prepare_clip(polygon, rows, width) else {
        return true;
    };
    debug_assert_eq!(row_areas.len(), (rows.1 - rows.0) as usize);
    for_each_packed_chunk(&prep, |chunk| {
        visit_ground_area_spans_chunk(polygon, chunk, rows.0, gt, row_areas, scratch, &mut visit)
    })
}

/// A horizontal run of pixels `[start, end)` in a row-major linear pixel index,
/// carrying an additive per-pixel payload (e.g. overlap area).
pub(super) trait PixelRun: Copy {
    fn start(&self) -> u64;
    fn end(&self) -> u64;
    /// The same payload over `[start, end)`.
    fn with_range(self, start: u64, end: u64) -> Self;
    /// Payload of a pixel covered by both `self` and `other`.
    fn add_payload(self, other: Self) -> Self;
}

/// Make `runs[from..]` disjoint: wherever runs overlap (pieces of one cell split at
/// the antimeridian can reach the same pixel), each pixel appears once with the
/// summed payload. Runs are left sorted by start. Disjoint runs keep their full
/// length, and nothing is expanded to single pixels, so the result holds at most
/// `2 * n - 1` runs for `n` inputs.
///
/// Runs never span rows, so splitting at run endpoints never yields a cross-row run.
pub(super) fn merge_overlapping_runs<R: PixelRun>(runs: &mut Vec<R>, from: usize) {
    runs[from..].sort_by_key(PixelRun::start);
    let mut max_end = 0;
    let overlapping = runs[from..].iter().any(|run| {
        let overlaps = run.start() < max_end;
        max_end = max_end.max(run.end());
        overlaps
    });
    if !overlapping {
        return;
    }
    let sorted: Vec<R> = runs.drain(from..).collect();
    let mut bounds: Vec<u64> = sorted.iter().flat_map(|r| [r.start(), r.end()]).collect();
    bounds.sort_unstable();
    bounds.dedup();
    // Runs of one piece are disjoint, so at most `pieces` runs are active at once.
    let mut active: Vec<R> = Vec::new();
    let mut next = 0;
    for window in bounds.windows(2) {
        let (lo, hi) = (window[0], window[1]);
        active.retain(|run| run.end() > lo);
        while next < sorted.len() && sorted[next].start() == lo {
            active.push(sorted[next]);
            next += 1;
        }
        if let Some((&first, rest)) = active.split_first() {
            let merged = rest.iter().fold(first, |acc, &run| acc.add_payload(run));
            runs.push(merged.with_range(lo, hi));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raster::ground_area::reference::clipped_pixel_area;
    use crate::raster::ground_area::wgs84_pixel_area;
    use proptest::prelude::*;

    fn clip_half_plane_ref(
        input: &[(f64, f64)],
        out: &mut Vec<(f64, f64)>,
        vertical: bool,
        bound: f64,
        keep_greater: bool,
    ) {
        out.clear();
        let n = input.len();
        if n == 0 {
            return;
        }
        let coordinate = |p: (f64, f64)| if vertical { p.0 } else { p.1 };
        let inside = |p: (f64, f64)| {
            if keep_greater {
                coordinate(p) >= bound
            } else {
                coordinate(p) <= bound
            }
        };
        let mut previous = input[n - 1];
        let mut previous_inside = inside(previous);
        for &current in input {
            let current_inside = inside(current);
            if current_inside != previous_inside {
                let (a, b) = (coordinate(previous), coordinate(current));
                let t = (bound - a) / (b - a);
                let crossing = if vertical {
                    (bound, previous.1 + t * (current.1 - previous.1))
                } else {
                    (previous.0 + t * (current.0 - previous.0), bound)
                };
                out.push(crossing);
            }
            if current_inside {
                out.push(current);
            }
            previous = current;
            previous_inside = current_inside;
        }
    }

    fn reference_pixel_shapes(
        polygon: &[(f64, f64)],
        rows: (i64, i64),
        width: i64,
        mut visit: impl FnMut(i64, i64, &[(f64, f64)]),
    ) {
        if polygon.len() < 3 {
            return;
        }
        let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
        for &(_, y) in polygon {
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        if !(min_y.is_finite() && max_y.is_finite()) {
            return;
        }
        let first_row = (min_y.floor() as i64).max(rows.0);
        let last_row = (max_y.ceil() as i64).min(rows.1);
        let (mut below, mut slab, mut right, mut cell) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for r in first_row..last_row {
            clip_half_plane_ref(polygon, &mut below, false, r as f64, true);
            clip_half_plane_ref(&below, &mut slab, false, (r + 1) as f64, false);
            if slab.len() < 3 {
                continue;
            }
            let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
            for &(x, _) in &slab {
                min_x = min_x.min(x);
                max_x = max_x.max(x);
            }
            let first_col = (min_x.floor() as i64).max(0);
            let last_col = (max_x.ceil() as i64).min(width);
            for c in first_col..last_col {
                clip_half_plane_ref(&slab, &mut right, true, c as f64, true);
                clip_half_plane_ref(&right, &mut cell, true, (c + 1) as f64, false);
                if cell.len() >= 3 {
                    visit(c, r, &cell);
                }
            }
        }
    }

    fn reference_pixel_overlaps(
        polygon: &[(f64, f64)],
        rows: (i64, i64),
        width: i64,
    ) -> std::collections::BTreeMap<(i64, i64), f64> {
        let mut out = std::collections::BTreeMap::new();
        reference_pixel_shapes(polygon, rows, width, |c, r, cell| {
            let area = polygon_area(cell);
            if area > 1e-13 {
                out.insert((c, r), area.min(1.0));
            }
        });
        out
    }

    fn overlaps(polygon: &[(f64, f64)], rows: (i64, i64), width: i64) -> Vec<(i64, i64, f64)> {
        let mut out = Vec::new();
        pixel_overlaps(polygon, rows, width, |c, r, a| out.push((c, r, a)));
        out
    }

    fn assert_matches_reference(shape: &[(f64, f64)], rows: (i64, i64), width: i64, tol: f64) {
        let got_vec = overlaps(shape, rows, width);
        let mut got = std::collections::BTreeMap::new();
        for &(c, r, a) in &got_vec {
            assert!(a > 0.0 && a <= 1.0, "out of range ({c}, {r}): {a}");
            assert!(
                got.insert((c, r), a).is_none(),
                "duplicate pixel ({c}, {r})"
            );
        }
        let expected = reference_pixel_overlaps(shape, rows, width);
        for (&key, &want) in &expected {
            let actual = got.get(&key).copied().unwrap_or(0.0);
            assert!(
                (actual - want).abs() <= tol,
                "pixel {key:?}: actual={actual} want={want}"
            );
        }
        for (&key, &actual) in &got {
            let want = expected.get(&key).copied().unwrap_or(0.0);
            assert!(
                (actual - want).abs() <= tol,
                "extra pixel {key:?}: actual={actual} want={want}"
            );
        }
    }

    #[test]
    fn stopping_at_a_boundary_or_interior_overlap_ends_the_walk() {
        let rect = [(0.5, 0.0), (4095.5, 0.0), (4095.5, 4.0), (0.5, 4.0)];
        let mut scratch = ClipScratch::default();
        for stop_at in [1, 1025] {
            let mut visits = 0;
            assert!(!visit_overlaps(
                &rect,
                (0, 4),
                4096,
                &mut scratch,
                |_, _, _| {
                    visits += 1;
                    visits < stop_at
                }
            ));
            assert_eq!(visits, stop_at);
        }
        // Span callback emits contiguous interior runs rather than per-pixel callbacks.
        let mut span_runs = 0usize;
        let mut covered_pixels = 0i64;
        assert!(visit_overlap_spans_into(
            &rect,
            (0, 4),
            4096,
            &mut scratch,
            |_, c0, c1, area| {
                if c1 - c0 > 4 {
                    span_runs += 1;
                    assert_eq!(area, 1.0);
                }
                covered_pixels += c1 - c0;
                true
            }
        ));
        assert_eq!(span_runs, 4);
        assert_eq!(covered_pixels, 4 * 4096);
    }

    #[test]
    fn event_limit_stops_the_walk_before_buffering_the_whole_boundary() {
        // A wide, thin diamond crosses many columns in each of a few rows.
        let diamond = [(0.5, 20.5), (2000.5, 16.5), (4000.5, 20.5), (2000.5, 24.5)];
        let mut scratch = ClipScratch::default();
        let mut visits = 0;
        scratch.set_event_limit(Some(10));
        assert!(!visit_overlap_spans_into(
            &diamond,
            (0, 40),
            4100,
            &mut scratch,
            |_, _, _, _| {
                visits += 1;
                true
            }
        ));
        assert_eq!(visits, 0, "the visitor must not run on a truncated walk");
        assert!(
            scratch.events.len() <= 11,
            "{} events",
            scratch.events.len()
        );

        // Lifting the cap visits the same polygon in full.
        scratch.set_event_limit(None);
        let mut covered = 0.0;
        assert!(visit_overlap_spans_into(
            &diamond,
            (0, 40),
            4100,
            &mut scratch,
            |_, c0, c1, area| {
                covered += area * (c1 - c0) as f64;
                true
            }
        ));
        assert!((covered - polygon_area(&diamond)).abs() < 1e-6);
        assert!(scratch.events.len() > 1000);
    }

    #[test]
    fn non_finite_vertices_visit_nothing_in_every_build_profile() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for polygon in [
                [(0.0, 0.0), (1.0, 0.0), (bad, 1.0)],
                [(0.0, 0.0), (1.0, 0.0), (1.0, bad)],
                [(bad, bad), (1.0, 0.0), (0.0, 1.0)],
            ] {
                let mut visits = 0;
                pixel_overlaps(&polygon, (0, 1), 1, |_, _, _| visits += 1);
                assert_eq!(visits, 0, "{polygon:?}");
                let mut scratch = ClipScratch::default();
                assert!(visit_overlap_spans_into(
                    &polygon,
                    (0, 1),
                    1,
                    &mut scratch,
                    |_, _, _, _| {
                        visits += 1;
                        true
                    }
                ));
                assert_eq!(visits, 0, "{polygon:?}");
            }
        }
    }

    #[test]
    fn tiny_overlaps_above_the_documented_floor_are_reported() {
        let square = |side: f64| {
            [
                (0.5, 0.5),
                (0.5 + side, 0.5),
                (0.5 + side, 0.5 + side),
                (0.5, 0.5 + side),
            ]
        };
        // 2^-40 of a pixel, far below the 1e-12 cutoff of ordinary overlay but above
        // the clipper's floor.
        let side = 2f64.powi(-20);
        let got = overlaps(&square(side), (0, 2), 2);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!((got[0].0, got[0].1), (0, 0));
        assert!((got[0].2 / (side * side) - 1.0).abs() < 1e-3, "{got:?}");
        // 2^-48 is under the floor and is documented as skipped.
        assert!(overlaps(&square(2f64.powi(-24)), (0, 2), 2).is_empty());
    }

    #[test]
    fn interior_weights_match_individual_column_clips() {
        let shapes = [
            vec![(-4.25, 1.25), (24.5, 2.25), (20.0, 11.5), (-1.0, 9.5)],
            // A U has two disconnected spans in the same row.
            vec![
                (0., 0.),
                (20., 0.),
                (20., 12.),
                (14., 12.),
                (14., 3.75),
                (6., 3.75),
                (6., 12.),
                (0., 12.),
            ],
            vec![(-1.5, 2.1), (20.5, 2.2), (20.5, 2.3)],
            vec![(2., 2.), (22., 2.), (22., 8.), (2., 8.)],
            // Enclosing the full column & row window.
            vec![(-2.0, -2.0), (25.0, -2.0), (25.0, 20.0), (-2.0, 20.0)],
            // Diamond whose vertices sit strictly inside rows & graze pixel corners.
            vec![(9.0, 2.5), (14.5, 7.0), (9.0, 11.5), (3.5, 7.0)],
        ];
        for mut shape in shapes {
            for _ in 0..2 {
                for rows in [(0, 15), (3, 7)] {
                    assert_matches_reference(&shape, rows, 18, 1e-12);
                }
                shape.reverse();
            }
        }
    }

    #[test]
    fn areas_of_simple_shapes() {
        assert_eq!(
            polygon_area(&[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0), (0.0, 3.0)]),
            12.0
        );
        assert_eq!(
            polygon_area(&[(0.0, 0.0), (0.0, 3.0), (4.0, 3.0), (4.0, 0.0)]),
            12.0
        );
        assert_eq!(polygon_area(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0)]), 6.0);
        assert_eq!(polygon_area(&[(0.0, 0.0), (1.0, 1.0)]), 0.0);
        // Large coordinate offset still retains sub-pixel accuracy.
        let shifted = [
            (150_000.25, 100_000.5),
            (150_004.25, 100_000.5),
            (150_004.25, 100_003.5),
            (150_000.25, 100_003.5),
        ];
        assert!((polygon_area(&shifted) - 12.0).abs() < 1e-12);
    }

    #[test]
    fn an_aligned_rectangle_covers_whole_and_part_pixels() {
        // x in [0.5, 2.5], y in [1.0, 2.25].
        let rect = [(0.5, 1.0), (2.5, 1.0), (2.5, 2.25), (0.5, 2.25)];
        let got = overlaps(&rect, (0, 10), 10);
        let expected = [
            (0, 1, 0.5),
            (1, 1, 1.0),
            (2, 1, 0.5),
            (0, 2, 0.125),
            (1, 2, 0.25),
            (2, 2, 0.125),
        ];
        assert_eq!(got.len(), expected.len());
        for ((c, r, a), (ec, er, ea)) in got.iter().zip(expected) {
            assert_eq!((*c, *r), (ec, er));
            assert!((a - ea).abs() < 1e-12);
        }
    }

    #[test]
    fn overlaps_sum_to_the_polygon_area_and_respect_the_window() {
        let shapes: [&[(f64, f64)]; 4] = [
            // A hexagon.
            &[
                (3.2, 1.1),
                (6.7, 0.9),
                (8.4, 3.8),
                (6.9, 6.6),
                (3.1, 6.9),
                (1.3, 4.0),
            ],
            // A thin sliver.
            &[(0.2, 0.3), (9.7, 5.1), (9.6, 5.3)],
            // A concave arrow, clockwise.
            &[(1.0, 1.0), (5.0, 4.0), (1.0, 7.0), (2.5, 4.0)],
            // Smaller than a pixel.
            &[(4.2, 4.2), (4.6, 4.3), (4.4, 4.7)],
        ];
        for shape in shapes {
            let all = overlaps(shape, (0, 20), 20);
            let total: f64 = all.iter().map(|o| o.2).sum();
            assert!((total - polygon_area(shape)).abs() < 1e-9, "{shape:?}");
            assert!(all.iter().all(|o| o.2 > 0.0 && o.2 <= 1.0));
            // No pixel is reported twice.
            let mut seen: Vec<(i64, i64)> = all.iter().map(|o| (o.0, o.1)).collect();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), all.len());
            // Two halves of the rows partition the whole, bit-for-bit.
            let top = overlaps(shape, (0, 4), 20);
            let bottom = overlaps(shape, (4, 20), 20);
            let mut split = top.clone();
            split.extend_from_slice(&bottom);
            assert_eq!(split, all);
            assert!(top.iter().all(|o| o.1 < 4) && bottom.iter().all(|o| o.1 >= 4));
            // A narrower raster keeps only its columns.
            let narrow = overlaps(shape, (0, 20), 5);
            let kept: Vec<_> = all.iter().filter(|o| o.0 < 5).collect();
            assert_eq!(narrow.len(), kept.len());
        }
        // Outside the raster entirely.
        assert!(overlaps(&[(-5.0, -5.0), (-1.0, -5.0), (-1.0, -1.0)], (0, 10), 10).is_empty());
        assert!(overlaps(&[(1.0, 1.0), (2.0, 2.0)], (0, 10), 10).is_empty());
    }

    #[test]
    fn neighbouring_polygons_partition_each_pixel() {
        // Two polygons sharing an edge cover the square [1, 7] x [1, 7] between them.
        let left = [(1.0, 1.0), (4.3, 1.0), (2.9, 7.0), (1.0, 7.0)];
        let right = [(4.3, 1.0), (7.0, 1.0), (7.0, 7.0), (2.9, 7.0)];
        let mut coverage = std::collections::BTreeMap::new();
        for polygon in [&left[..], &right[..]] {
            pixel_overlaps(polygon, (0, 10), 10, |c, r, a| {
                *coverage.entry((c, r)).or_insert(0.0) += a;
            });
        }
        assert_eq!(coverage.len(), 36);
        for ((c, r), area) in coverage {
            assert!((area - 1.0).abs() < 1e-13, "pixel ({c}, {r}): {area}");
        }
    }

    #[test]
    fn large_coordinate_cell_and_vertex_rows_match_reference() {
        let hex: Vec<(f64, f64)> = (0..6)
            .map(|k| {
                let a = 0.41 + std::f64::consts::FRAC_PI_3 * f64::from(k);
                (150_012.3 + 5.7 * a.cos(), 100_008.7 + 5.7 * a.sin())
            })
            .collect();
        assert_matches_reference(&hex, (100_000, 100_020), 150_025, 1e-10);
        let mut reversed = hex.clone();
        reversed.reverse();
        assert_matches_reference(&reversed, (100_000, 100_020), 150_025, 1e-10);
    }

    #[test]
    fn ground_area_spans_match_sutherland_hodgman_at_low_and_high_latitudes() {
        let polygons = [
            // Rotated hexagon
            (0..6)
                .map(|k| {
                    let a = 0.29 + std::f64::consts::FRAC_PI_3 * f64::from(k);
                    (5.4 + 3.6 * a.cos(), 5.3 + 3.6 * a.sin())
                })
                .collect::<Vec<_>>(),
            // Negatively sloped shallow triangle + left/right clipping
            vec![(-1.3, 2.2), (12.4, 2.7), (4.1, 8.9)],
            // Positively sloped pentagon with vertex rows and concave indent
            vec![(1.2, 1.3), (8.7, 2.4), (9.3, 6.8), (5.0, 4.9), (0.8, 7.6)],
        ];
        for north in [5.0, 85.0] {
            let gt = GeoTransform::from_gdal_array([-10.0, 0.5, 0.0, north, 0.0, -0.4]);
            let (width, height) = (10i64, 10i64);
            let row_areas: Vec<_> = (0..height as usize)
                .map(|r| wgs84_pixel_area(&gt, 0, r).unwrap())
                .collect();
            let mut scratch = ClipScratch::default();
            for mut poly in polygons.clone() {
                for _ in 0..2 {
                    let mut actual = std::collections::BTreeMap::new();
                    visit_ground_area_spans_into(
                        &poly,
                        (0, height),
                        width,
                        &gt,
                        &row_areas,
                        &mut scratch,
                        |r, c0, c1, frac, m2| {
                            for c in c0..c1 {
                                assert!(actual.insert((c, r), (frac, m2)).is_none());
                            }
                            true
                        },
                    );

                    // Split row window check (0..5 and 5..10 must match 0..10)
                    let mut split_actual = std::collections::BTreeMap::new();
                    for band in [0..5, 5..height] {
                        visit_ground_area_spans_into(
                            &poly,
                            (band.start, band.end),
                            width,
                            &gt,
                            &row_areas[band.start as usize..band.end as usize],
                            &mut scratch,
                            |r, c0, c1, frac, m2| {
                                for c in c0..c1 {
                                    assert!(split_actual.insert((c, r), (frac, m2)).is_none());
                                }
                                true
                            },
                        );
                    }
                    assert_eq!(actual, split_actual);

                    let mut expected = std::collections::BTreeMap::new();
                    reference_pixel_shapes(&poly, (0, height), width, |c, r, cell| {
                        let (frac, m2) = clipped_pixel_area(&gt, cell);
                        if frac > 1e-12 && m2 > row_areas[r as usize] * 1e-12 {
                            expected.insert((c, r), (frac, m2));
                        }
                    });
                    for (&(c, r), &(want_f, want_m2)) in &expected {
                        let (got_f, got_m2) = actual.get(&(c, r)).copied().unwrap_or((0.0, 0.0));
                        assert!((got_f - want_f).abs() < 1e-12, "({c},{r}) frac");
                        assert!(
                            (got_m2 - want_m2).abs() <= want_m2.max(row_areas[r as usize]) * 1e-12,
                            "({c},{r}) m2: {got_m2} vs {want_m2}"
                        );
                    }
                    for (&(c, r), &(got_f, got_m2)) in &actual {
                        if let Some(&(want_f, want_m2)) = expected.get(&(c, r)) {
                            assert!((got_f - want_f).abs() < 1e-12);
                            assert!(
                                (got_m2 - want_m2).abs()
                                    <= want_m2.max(row_areas[r as usize]) * 1e-12
                            );
                        } else {
                            assert!(got_f <= 1e-12, "extra pixel ({c},{r}): {got_f}");
                            assert!(got_m2 <= row_areas[r as usize] * 1e-12);
                        }
                    }
                    poly.reverse();
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn random_ngons_match_sutherland_hodgman(
            cx in -2.0f64..20.0,
            cy in -2.0f64..20.0,
            rad in 0.2f64..10.0,
            turn in 0.0f64..std::f64::consts::TAU,
            sides in 4usize..=9,
            clockwise in any::<bool>(),
            squash in 0.3f64..1.8,
        ) {
            let mut poly: Vec<(f64, f64)> = (0..sides)
                .map(|k| {
                    let a = turn + std::f64::consts::TAU * (k as f64) / (sides as f64);
                    (cx + rad * a.cos(), cy + rad * squash * a.sin())
                })
                .collect();
            if clockwise {
                poly.reverse();
            }
            assert_matches_reference(&poly, (0, 18), 18, 1e-12);
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestRun(u64, u64, f64);

    impl PixelRun for TestRun {
        fn start(&self) -> u64 {
            self.0
        }
        fn end(&self) -> u64 {
            self.1
        }
        fn with_range(self, start: u64, end: u64) -> Self {
            TestRun(start, end, self.2)
        }
        fn add_payload(self, other: Self) -> Self {
            TestRun(self.0, self.1, self.2 + other.2)
        }
    }

    #[test]
    fn merging_runs_sums_only_shared_pixels_and_keeps_spans() {
        // Disjoint runs are only sorted.
        let mut runs = vec![TestRun(20, 30, 1.0), TestRun(0, 10, 0.5)];
        merge_overlapping_runs(&mut runs, 0);
        assert_eq!(runs, [TestRun(0, 10, 0.5), TestRun(20, 30, 1.0)]);

        // Two seam pieces share pixels 8..12; a prefix before `from` is untouched.
        let mut runs = vec![
            TestRun(100, 101, 0.1),
            TestRun(10, 20, 0.25),
            TestRun(0, 12, 0.5),
            TestRun(8, 9, 0.125),
        ];
        merge_overlapping_runs(&mut runs, 1);
        assert_eq!(
            runs,
            [
                TestRun(100, 101, 0.1),
                TestRun(0, 8, 0.5),
                TestRun(8, 9, 0.625),
                TestRun(9, 10, 0.5),
                TestRun(10, 12, 0.75),
                TestRun(12, 20, 0.25),
            ]
        );
        // Pixel totals are preserved.
        let total: f64 = runs[1..].iter().map(|r| (r.1 - r.0) as f64 * r.2).sum();
        assert!((total - (10.0 * 0.25 + 12.0 * 0.5 + 0.125)).abs() < 1e-12);
    }
}
