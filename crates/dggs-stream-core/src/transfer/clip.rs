//! Exact overlap areas between a polygon and the unit squares of a pixel grid.

/// Unsigned area of a polygon given by its vertices in order (shoelace formula).
pub fn polygon_area(points: &[(f64, f64)]) -> f64 {
    let n = points.len();
    if n < 3 {
        return 0.0;
    }
    let mut twice = 0.0;
    for i in 0..n {
        let (x0, y0) = points[i];
        let (x1, y1) = points[(i + 1) % n];
        twice += x0 * y1 - x1 * y0;
    }
    0.5 * twice.abs()
}

/// Clip `input` to the half-plane where `coordinate(p) * sign >= bound * sign`
/// (Sutherland-Hodgman against one axis-aligned line), writing the result to `out`.
fn clip_half_plane(
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

/// Whole columns inside a four-sided slab spanning the full pixel row. The left
/// and right edges are linear, so their most restrictive x values occur at
/// the ends. More complex slabs (including disconnected concave slices) and
/// partial-height rows go through the ordinary column clipper.
fn interior_columns(slab: &[(f64, f64)], row: i64) -> std::ops::Range<i64> {
    let &[a, b, c, d] = slab else { return 0..0 };
    let (a, b, c, d) = if a.1 == b.1 {
        (a, b, c, d)
    } else {
        (b, c, d, a)
    };
    let bottom = row as f64;
    let top = (row + 1) as f64;
    if a.1 != b.1 || c.1 != d.1 || !((a.1 == bottom && c.1 == top) || (c.1 == bottom && a.1 == top))
    {
        return 0..0;
    }
    let left = a.0.min(b.0).max(c.0.min(d.0));
    let right = a.0.max(b.0).min(c.0.max(d.0));
    left.ceil() as i64..right.floor() as i64
}

/// For a polygon in pixel coordinates (pixel `(c, r)` is the unit square
/// `[c, c + 1] x [r, r + 1]`), call `visit(c, r, area)` for every pixel with
/// `rows.0 <= r < rows.1` and `0 <= c < width` that the polygon overlaps, with the
/// overlapped area (`0 < area <= 1`). The polygon may be convex or concave, in either
/// orientation; it must not self-intersect.
///
/// The polygon is cut into row slabs. Boundary columns are clipped individually.
pub fn pixel_overlaps(
    polygon: &[(f64, f64)],
    rows: (i64, i64),
    width: i64,
    mut visit: impl FnMut(i64, i64, f64),
) {
    visit_overlaps::<true>(polygon, rows, width, |c, r, area| {
        visit(c, r, area);
        true
    });
}

/// Return false immediately when the visitor declines an overlap.
pub(super) fn visit_overlaps<const SKIP_INTERIOR: bool>(
    polygon: &[(f64, f64)],
    rows: (i64, i64),
    width: i64,
    mut visit: impl FnMut(i64, i64, f64) -> bool,
) -> bool {
    if polygon.len() < 3 {
        return true;
    }
    let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
    for &(_, y) in polygon {
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }
    if !(min_y.is_finite() && max_y.is_finite()) {
        return true;
    }
    let first_row = (min_y.floor() as i64).max(rows.0);
    let last_row = (max_y.ceil() as i64).min(rows.1);
    let (mut below, mut slab, mut right, mut cell) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for r in first_row..last_row {
        clip_half_plane(polygon, &mut below, false, r as f64, true);
        clip_half_plane(&below, &mut slab, false, (r + 1) as f64, false);
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
        let interior = if SKIP_INTERIOR && last_col.saturating_sub(first_col) >= 4 {
            interior_columns(&slab, r)
        } else {
            0..0
        };
        for c in first_col..last_col {
            if interior.contains(&c) {
                if !visit(c, r, 1.0) {
                    return false;
                }
                continue;
            }
            clip_half_plane(&slab, &mut right, true, c as f64, true);
            clip_half_plane(&right, &mut cell, true, (c + 1) as f64, false);
            let area = polygon_area(&cell);
            if area > 0.0 && !visit(c, r, area.min(1.0)) {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlaps(polygon: &[(f64, f64)], rows: (i64, i64), width: i64) -> Vec<(i64, i64, f64)> {
        let mut out = Vec::new();
        pixel_overlaps(polygon, rows, width, |c, r, a| out.push((c, r, a)));
        out
    }

    #[test]
    fn stopping_at_a_boundary_or_interior_overlap_ends_the_walk() {
        let rect = [(0.5, 0.0), (4095.5, 0.0), (4095.5, 4.0), (0.5, 4.0)];
        for stop_at in [1, 1025] {
            let mut visits = 0;
            assert!(!visit_overlaps::<true>(&rect, (0, 4), 4096, |_, _, _| {
                visits += 1;
                visits < stop_at
            }));
            assert_eq!(visits, stop_at);
        }
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
        ];
        for mut shape in shapes {
            for _ in 0..2 {
                for rows in [(0, 15), (3, 7)] {
                    let mut reference = Vec::new();
                    visit_overlaps::<false>(&shape, rows, 18, |c, r, a| {
                        reference.push((c, r, a));
                        true
                    });
                    assert_eq!(overlaps(&shape, rows, 18), reference);
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
            // Two halves of the rows partition the whole.
            let top = overlaps(shape, (0, 4), 20);
            let bottom = overlaps(shape, (4, 20), 20);
            assert_eq!(top.len() + bottom.len(), all.len());
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
            assert!((area - 1.0).abs() < 1e-12, "pixel ({c}, {r}): {area}");
        }
    }
}
