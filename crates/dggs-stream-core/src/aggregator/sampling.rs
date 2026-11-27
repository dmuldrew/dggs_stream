//! Quadrature offsets in `[0, 1]^2` used both for sub-pixel sampling
//! (`TransferMode::AssignCenters`) and area-preserving sub-cell polygon
//! sampling (`TransferMode::Interpolate`).

use serde::{Deserialize, Serialize};

use crate::error::{DggsStreamError, Result};

/// A single quadrature offset within the unit reference square `[0.0, 1.0]^2`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SamplePoint {
    pub dx: f64,     // X offset in [0.0, 1.0] (0.5 is center)
    pub dy: f64,     // Y offset in [0.0, 1.0] (0.5 is center)
    pub weight: f64, // Proportional weight (e.g. 0.2)
}

/// Collection of quadrature sample points and weights over `[0.0, 1.0]^2`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SamplingPattern {
    pub points: Vec<SamplePoint>,
}

impl Default for SamplingPattern {
    fn default() -> Self {
        Self::center()
    }
}

impl SamplingPattern {
    /// 1. Single-point centroid sampling (default: maximum raw throughput)
    pub fn center() -> Self {
        Self {
            points: vec![SamplePoint {
                dx: 0.5,
                dy: 0.5,
                weight: 1.0,
            }],
        }
    }

    /// 2. Rotated Grid Super-Sampling (RGSS / 4-point rotated: optimal anti-aliasing efficiency)
    ///    Rotates coordinates by ~26.6 degrees so no two points share the same X or Y axis.
    pub fn rgss() -> Self {
        Self {
            points: vec![
                SamplePoint {
                    dx: 0.375,
                    dy: 0.125,
                    weight: 0.25,
                },
                SamplePoint {
                    dx: 0.875,
                    dy: 0.375,
                    weight: 0.25,
                },
                SamplePoint {
                    dx: 0.125,
                    dy: 0.625,
                    weight: 0.25,
                },
                SamplePoint {
                    dx: 0.625,
                    dy: 0.875,
                    weight: 0.25,
                },
            ],
        }
    }

    /// 3. 5-point quincunx sampling (center + 4 diagonal corner insets)
    pub fn five_point() -> Self {
        Self {
            points: vec![
                SamplePoint {
                    dx: 0.5,
                    dy: 0.5,
                    weight: 0.2,
                },
                SamplePoint {
                    dx: 0.2,
                    dy: 0.2,
                    weight: 0.2,
                },
                SamplePoint {
                    dx: 0.8,
                    dy: 0.2,
                    weight: 0.2,
                },
                SamplePoint {
                    dx: 0.2,
                    dy: 0.8,
                    weight: 0.2,
                },
                SamplePoint {
                    dx: 0.8,
                    dy: 0.8,
                    weight: 0.2,
                },
            ],
        }
    }

    /// 4. Gaussian Center-Weighted 5-point sampling (Point Spread Function / optical sensor modeling)
    ///    Center has 50% weight; 4 cross points have 12.5% weight each.
    pub fn gaussian_five_point() -> Self {
        Self {
            points: vec![
                SamplePoint {
                    dx: 0.5,
                    dy: 0.5,
                    weight: 0.50,
                },
                SamplePoint {
                    dx: 0.2,
                    dy: 0.5,
                    weight: 0.125,
                },
                SamplePoint {
                    dx: 0.8,
                    dy: 0.5,
                    weight: 0.125,
                },
                SamplePoint {
                    dx: 0.5,
                    dy: 0.2,
                    weight: 0.125,
                },
                SamplePoint {
                    dx: 0.5,
                    dy: 0.8,
                    weight: 0.125,
                },
            ],
        }
    }

    /// 5. 7-point Hexagonal Lattice sampling (inscribed regular hexagon matching H3 geometry)
    pub fn hex_seven_point() -> Self {
        let r = 0.35; // Radius from pixel center
        let w = 1.0 / 7.0;
        let mut points = Vec::with_capacity(7);
        points.push(SamplePoint {
            dx: 0.5,
            dy: 0.5,
            weight: w,
        });

        for i in 0..6 {
            let angle = (i as f64) * std::f64::consts::PI / 3.0;
            let dx = 0.5 + r * angle.cos();
            let dy = 0.5 + r * angle.sin();
            points.push(SamplePoint { dx, dy, weight: w });
        }
        Self { points }
    }

    /// 6. 8-Rooks Stratified sampling (Latin Hypercube sampling: anti-clumping on diagonal edges)
    pub fn eight_rooks() -> Self {
        // Non-attacking rook permutation on 8x8 subgrid
        let rook_cols = [0, 4, 1, 5, 2, 6, 3, 7];
        let w = 1.0 / 8.0;
        let points = (0..8)
            .map(|row| {
                let col = rook_cols[row];
                let dx = (col as f64 + 0.5) / 8.0;
                let dy = (row as f64 + 0.5) / 8.0;
                SamplePoint { dx, dy, weight: w }
            })
            .collect();
        Self { points }
    }

    /// 7. 9-point regular 3x3 grid sampling
    pub fn nine_point() -> Self {
        let mut points = Vec::with_capacity(9);
        let offsets = [0.2, 0.5, 0.8];
        let w = 1.0 / 9.0;
        for &dy in &offsets {
            for &dx in &offsets {
                points.push(SamplePoint { dx, dy, weight: w });
            }
        }
        Self { points }
    }

    /// 8. 16-point regular 4x4 dense grid sampling (ideal for coarse pixel -> fine H3 cells)
    pub fn sixteen_point() -> Self {
        let mut points = Vec::with_capacity(16);
        let offsets = [0.125, 0.375, 0.625, 0.875];
        let w = 1.0 / 16.0;
        for &dy in &offsets {
            for &dx in &offsets {
                points.push(SamplePoint { dx, dy, weight: w });
            }
        }
        Self { points }
    }

    /// Parse a sampling preset, rejecting unrecognized names.
    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s.trim().to_lowercase().as_str() {
            "center" | "centre" | "1" => Self::center(),
            "rgss" | "rotated4" | "4point_rotated" | "rotated" | "4" => Self::rgss(),
            "5point" | "5_point" | "quincunx" | "5" => Self::five_point(),
            "gaussian" | "weighted5" | "psf" => Self::gaussian_five_point(),
            "hex" | "hex7" | "7point" | "7point_hex" | "7" => Self::hex_seven_point(),
            "8rooks" | "rooks8" | "stratified8" | "8" => Self::eight_rooks(),
            "9point" | "9_point" | "3x3" | "grid3x3" | "9" => Self::nine_point(),
            "16point" | "16_point" | "4x4" | "grid4x4" | "16" => Self::sixteen_point(),
            _ => return Err(DggsStreamError::InvalidParameter(format!(
                "Invalid sampling '{s}': expected one of center, 5point, gaussian, rgss, hex, 8rooks, 9point, 16point"
            ))),
        })
    }

    /// Whether this can use the unit-weight single-center fast path.
    #[inline(always)]
    pub fn is_single_point(&self) -> bool {
        self.points.len() == 1
            && self.points[0].dx == 0.5
            && self.points[0].dy == 0.5
            && self.points[0].weight == 1.0
    }

    /// Return a copy whose point weights sum to exactly `1.0` when added in order, as the
    /// accumulators add them, so a stencil whose samples are all valid reaches a count of
    /// `1.0` (e.g. passes an inclusive `min_count = 1`). The floating-point residual goes
    /// to the last weight; a stencil that loses samples still sums to less than one.
    pub fn normalized(&self) -> Self {
        let total: f64 = self.points.iter().map(|p| p.weight).sum();
        if !total.is_finite() || total <= 0.0 {
            return self.clone();
        }
        let inv = 1.0 / total;
        let mut points: Vec<SamplePoint> = self
            .points
            .iter()
            .map(|&p| SamplePoint {
                dx: p.dx,
                dy: p.dy,
                weight: p.weight * inv,
            })
            .collect();
        if let Some((last, head)) = points.split_last_mut() {
            let partial = head.iter().fold(0.0_f64, |sum, p| sum + p.weight);
            let mut weight = 1.0 - partial;
            for _ in 0..4 {
                let sum = partial + weight;
                if sum == 1.0 {
                    break;
                }
                weight = if sum < 1.0 {
                    weight.next_up()
                } else {
                    weight.next_down()
                };
            }
            if weight > 0.0 && partial + weight == 1.0 {
                last.weight = weight;
            }
        }
        Self { points }
    }

    /// Maximum normalized Chebyshev radius from `(0.5, 0.5)` across all sample points, in `[0.0, 1.0]`.
    pub fn max_radius(&self) -> f64 {
        self.points
            .iter()
            .map(|p| (2.0 * p.dx - 1.0).abs().max((2.0 * p.dy - 1.0).abs()))
            .fold(0.0_f64, f64::max)
    }
}

/// Constant-Jacobian, area-preserving map from `[0, 1]^2` quadrature offsets onto a
/// star-shaped polygon (`3..=10` vertices) in projected pixel coordinates `(col, row)`.
#[derive(Debug, Clone, Copy)]
pub struct CellPolygonSampler {
    center: (f64, f64),
    offsets: [(f64, f64); 10],
    cum_area: [f64; 11],
    len: usize,
}

impl CellPolygonSampler {
    /// Construct a sampler anchored at `center` over the unclosed polygon `ring` (`3..=10` vertices).
    ///
    /// Retries around the vertex centroid if `center` does not strictly see every edge positively
    /// (or negatively) under projection curvature/shear. Returns `None` if neither star center is valid.
    pub fn new(center: (f64, f64), ring: &[(f64, f64)]) -> Option<Self> {
        let n = ring.len();
        if !(3..=10).contains(&n)
            || !center.0.is_finite()
            || !center.1.is_finite()
            || ring.iter().any(|&(x, y)| !x.is_finite() || !y.is_finite())
        {
            return None;
        }
        if let Some(sampler) = Self::try_star(center, ring) {
            return Some(sampler);
        }
        let inv_n = 1.0 / (n as f64);
        let (mut sx, mut sy) = (0.0, 0.0);
        for &(x, y) in ring {
            sx += x;
            sy += y;
        }
        let centroid = (sx * inv_n, sy * inv_n);
        Self::try_star(centroid, ring)
    }

    fn try_star(star_center: (f64, f64), ring: &[(f64, f64)]) -> Option<Self> {
        let n = ring.len();
        let mut offsets = [(0.0, 0.0); 10];
        for (dst, &(vx, vy)) in offsets[..n].iter_mut().zip(ring) {
            *dst = (vx - star_center.0, vy - star_center.1);
        }
        let mut cum_area = [0.0; 11];
        let mut sign = 0i8;
        let mut total_twice = 0.0;
        for i in 0..n {
            let (ax, ay) = offsets[i];
            let (bx, by) = offsets[(i + 1) % n];
            let cross = ax * by - ay * bx;
            if !cross.is_finite() || cross == 0.0 {
                return None;
            }
            let s = if cross > 0.0 { 1 } else { -1 };
            if sign == 0 {
                sign = s;
            } else if sign != s {
                return None;
            }
            total_twice += cross.abs();
            cum_area[i + 1] = total_twice;
        }
        if !total_twice.is_finite() || total_twice <= 1e-16 {
            return None;
        }
        let inv_total = 1.0 / total_twice;
        for slot in &mut cum_area[1..n] {
            *slot *= inv_total;
        }
        cum_area[n] = 1.0;
        Some(Self {
            center: star_center,
            offsets,
            cum_area,
            len: n,
        })
    }

    /// Map a unit-square offset `(dx, dy)` in `[0, 1]^2` into projected pixel coordinates `(col, row)`.
    #[inline]
    pub fn sample(&self, dx: f64, dy: f64) -> (f64, f64) {
        let a = (2.0 * dx - 1.0).clamp(-1.0, 1.0);
        let b = (2.0 * dy - 1.0).clamp(-1.0, 1.0);
        if a == 0.0 && b == 0.0 {
            return self.center;
        }
        let (abs_a, abs_b) = (a.abs(), b.abs());
        let (r, mut s) = if abs_a >= abs_b {
            if a > 0.0 {
                (abs_a, (b / abs_a + 1.0) * 0.125)
            } else {
                (abs_a, 0.5 + (1.0 - b / abs_a) * 0.125)
            }
        } else if b > 0.0 {
            (abs_b, 0.25 + (1.0 - a / abs_b) * 0.125)
        } else {
            (abs_b, 0.75 + (a / abs_b + 1.0) * 0.125)
        };
        if !(0.0..1.0).contains(&s) {
            s = s.rem_euclid(1.0);
            if s >= 1.0 {
                s = 0.0;
            }
        }
        let mut idx = self.len - 1;
        for k in 0..self.len - 1 {
            if s < self.cum_area[k + 1] {
                idx = k;
                break;
            }
        }
        let span = (self.cum_area[idx + 1] - self.cum_area[idx]).max(1e-30);
        let t = ((s - self.cum_area[idx]) / span).clamp(0.0, 1.0);
        let u0 = self.offsets[idx];
        let u1 = self.offsets[(idx + 1) % self.len];
        (
            self.center.0 + r * (u0.0 + t * (u1.0 - u0.0)),
            self.center.1 + r * (u0.1 + t * (u1.1 - u0.1)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_sampling_presets_weights() {
        let presets = [
            ("center", SamplingPattern::center(), 1),
            ("rgss", SamplingPattern::rgss(), 4),
            ("5point", SamplingPattern::five_point(), 5),
            ("gaussian", SamplingPattern::gaussian_five_point(), 5),
            ("hex", SamplingPattern::hex_seven_point(), 7),
            ("8rooks", SamplingPattern::eight_rooks(), 8),
            ("9point", SamplingPattern::nine_point(), 9),
            ("16point", SamplingPattern::sixteen_point(), 16),
        ];

        for (name, pattern, expected_len) in presets {
            assert_eq!(
                pattern.points.len(),
                expected_len,
                "Failed len for {}",
                name
            );
            let sum_w: f64 = pattern.points.iter().map(|p| p.weight).sum();
            assert!(
                (sum_w - 1.0).abs() < 1e-9,
                "Weights did not sum to 1 for {}",
                name
            );
            for p in &pattern.points {
                assert!(p.dx >= 0.0 && p.dx <= 1.0, "dx out of bounds for {}", name);
                assert!(p.dy >= 0.0 && p.dy <= 1.0, "dy out of bounds for {}", name);
                assert!(
                    p.weight > 0.0 && p.weight <= 1.0,
                    "weight out of bounds for {}",
                    name
                );
            }
        }
    }

    #[test]
    fn test_sampling_parser() {
        assert_eq!(SamplingPattern::parse("rgss").unwrap().points.len(), 4);
        assert_eq!(SamplingPattern::parse("hex").unwrap().points.len(), 7);
        assert_eq!(SamplingPattern::parse("gaussian").unwrap().points.len(), 5);
        assert_eq!(SamplingPattern::parse("8rooks").unwrap().points.len(), 8);
        assert_eq!(SamplingPattern::parse("16point").unwrap().points.len(), 16);
        for name in ["unknown_preset", "", "centered", "rgsss"] {
            let error = SamplingPattern::parse(name).unwrap_err().to_string();
            assert!(error.contains("Invalid sampling"), "{error}");
        }
        for name in ["center", "centre", "1", " CENTER "] {
            assert!(SamplingPattern::parse(name).unwrap().is_single_point());
        }
        for (name, len) in [
            ("rotated4", 4),
            ("4point_rotated", 4),
            ("rotated", 4),
            ("4", 4),
            ("5point", 5),
            ("5_point", 5),
            ("quincunx", 5),
            ("5", 5),
            ("weighted5", 5),
            ("psf", 5),
            ("hex7", 7),
            ("7point", 7),
            ("7point_hex", 7),
            ("7", 7),
            ("rooks8", 8),
            ("stratified8", 8),
            ("8", 8),
            ("9_point", 9),
            ("3x3", 9),
            ("grid3x3", 9),
            ("9", 9),
            ("16_point", 16),
            ("4x4", 16),
            ("grid4x4", 16),
            ("16", 16),
        ] {
            assert_eq!(
                SamplingPattern::parse(name).unwrap().points.len(),
                len,
                "{name}"
            );
        }
    }

    fn point_in_polygon(pt: (f64, f64), ring: &[(f64, f64)]) -> bool {
        let mut inside = false;
        let n = ring.len();
        let mut j = n - 1;
        for i in 0..n {
            let (xi, yi) = ring[i];
            let (xj, yj) = ring[j];
            let cross_y = (yi > pt.1) != (yj > pt.1);
            if cross_y && pt.0 < (xj - xi) * (pt.1 - yi) / (yj - yi) + xi {
                inside = !inside;
            }
            j = i;
        }
        inside
    }

    #[test]
    fn test_cell_polygon_sampler_containment_and_center() {
        let triangle = [(0.0, 0.0), (4.0, 0.5), (1.2, 3.6)];
        let diamond_cw = [(10.0, 10.0), (12.0, 8.0), (14.0, 10.0), (12.0, 12.0)];
        let pentagon = [(2.0, 0.0), (4.2, 1.5), (3.4, 4.1), (0.6, 3.8), (-0.2, 1.4)];
        let hexagon = [
            (1.0, 0.0),
            (0.5, 0.8660254),
            (-0.5, 0.8660254),
            (-1.0, 0.0),
            (-0.5, -0.8660254),
            (0.5, -0.8660254),
        ];
        type ShapeSpec<'a> = (&'a str, (f64, f64), &'a [(f64, f64)]);
        let shapes: &[ShapeSpec<'_>] = &[
            ("triangle", (1.7, 1.3), &triangle),
            ("diamond_cw", (12.0, 10.0), &diamond_cw),
            ("pentagon", (2.0, 2.1), &pentagon),
            ("hexagon", (0.0, 0.0), &hexagon),
        ];
        let presets = [
            SamplingPattern::rgss(),
            SamplingPattern::five_point(),
            SamplingPattern::gaussian_five_point(),
            SamplingPattern::hex_seven_point(),
            SamplingPattern::eight_rooks(),
            SamplingPattern::nine_point(),
            SamplingPattern::sixteen_point(),
        ];

        for &(name, center, ring) in shapes {
            let sampler = CellPolygonSampler::new(center, ring).unwrap();
            assert_eq!(sampler.sample(0.5, 0.5), center, "{name} center");
            for preset in &presets {
                for sp in &preset.points {
                    let pt = sampler.sample(sp.dx, sp.dy);
                    assert!(
                        point_in_polygon(pt, ring),
                        "{name} missed preset point ({}, {}) -> {:?}",
                        sp.dx,
                        sp.dy,
                        pt
                    );
                }
            }
            for iy in 0..32 {
                let dy = (iy as f64 + 0.5) / 32.0;
                for ix in 0..32 {
                    let dx = (ix as f64 + 0.5) / 32.0;
                    let pt = sampler.sample(dx, dy);
                    assert!(
                        point_in_polygon(pt, ring),
                        "{name} missed grid point ({dx}, {dy}) -> {pt:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_cell_polygon_sampler_quadrature_moments_and_linearity() {
        // Irregular pentagon: verify uniform [0, 1]^2 quadrature integrates 1, x, y, x^2, x*y
        // to the exact analytic shoelace moments (confirms constant Jacobian).
        let pentagon = [(1.2, 0.4), (4.1, 1.3), (3.5, 4.2), (0.9, 3.7), (0.1, 1.9)];
        let sampler = CellPolygonSampler::new((2.0, 2.3), &pentagon).unwrap();

        let (mut area, mut ix, mut iy, mut ix2, mut ixy) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for j in 0..pentagon.len() {
            let (x0, y0) = pentagon[j];
            let (x1, y1) = pentagon[(j + 1) % pentagon.len()];
            let c = x0 * y1 - x1 * y0;
            area += 0.5 * c;
            ix += c * (x0 + x1) / 6.0;
            iy += c * (y0 + y1) / 6.0;
            ix2 += c * (x0 * x0 + x0 * x1 + x1 * x1) / 12.0;
            ixy += c * (x0 * y1 + 2.0 * x0 * y0 + 2.0 * x1 * y1 + x1 * y0) / 24.0;
        }
        let ex = ix / area;
        let ey = iy / area;
        let ex2 = ix2 / area;
        let exy = ixy / area;

        let n = 64;
        let w = 1.0 / ((n * n) as f64);
        let (mut sx, mut sy, mut sx2, mut sxy) = (0.0, 0.0, 0.0, 0.0);
        for r in 0..n {
            let dy = (r as f64 + 0.5) / (n as f64);
            for c in 0..n {
                let dx = (c as f64 + 0.5) / (n as f64);
                let (px, py) = sampler.sample(dx, dy);
                sx += w * px;
                sy += w * py;
                sx2 += w * px * px;
                sxy += w * px * py;
            }
        }
        assert!((sx - ex).abs() < 1e-3, "E[x] {sx} vs {ex}");
        assert!((sy - ey).abs() < 1e-3, "E[y] {sy} vs {ey}");
        assert!((sx2 - ex2).abs() < 5e-3, "E[x^2] {sx2} vs {ex2}");
        assert!((sxy - exy).abs() < 5e-3, "E[x*y] {sxy} vs {exy}");

        // Symmetric presets preserve linear functions exactly on centrally symmetric polygons.
        let quad = [(1.0, 2.0), (5.0, 3.0), (4.0, 7.0), (0.0, 6.0)];
        let q_center = (2.5, 4.5);
        let q_sampler = CellPolygonSampler::new(q_center, &quad).unwrap();
        let hex: Vec<(f64, f64)> = (0..6)
            .map(|k| {
                let a = (k as f64) * std::f64::consts::PI / 3.0;
                (3.0 + 2.0 * a.cos(), -1.5 + 2.0 * a.sin())
            })
            .collect();
        let h_center = (3.0, -1.5);
        let h_sampler = CellPolygonSampler::new(h_center, &hex).unwrap();
        let linear = |x: f64, y: f64| 3.5 * x - 1.75 * y + 12.0;

        for (label, sampler, (cx, cy)) in
            [("quad", q_sampler, q_center), ("hex", h_sampler, h_center)]
        {
            let exact = linear(cx, cy);
            for (pname, preset) in [
                ("rgss", SamplingPattern::rgss()),
                ("5point", SamplingPattern::five_point()),
                ("gaussian", SamplingPattern::gaussian_five_point()),
                ("8rooks", SamplingPattern::eight_rooks()),
                ("9point", SamplingPattern::nine_point()),
                ("16point", SamplingPattern::sixteen_point()),
            ] {
                let norm = preset.normalized();
                let est: f64 = norm
                    .points
                    .iter()
                    .map(|p| {
                        let (px, py) = sampler.sample(p.dx, p.dy);
                        p.weight * linear(px, py)
                    })
                    .sum();
                assert!(
                    (est - exact).abs() < 1e-11,
                    "{label} {pname}: est={est} exact={exact}"
                );
            }
        }
    }

    #[test]
    fn normalized_weights_sum_to_exactly_one_in_accumulation_order() {
        for name in [
            "center", "rgss", "5point", "gaussian", "hex", "8rooks", "9point", "16point",
        ] {
            let norm = SamplingPattern::parse(name).unwrap().normalized();
            assert!(norm.points.iter().all(|p| p.weight > 0.0), "{name}");
            // The accumulators start from the first weight and add the rest in order.
            let sum = norm.points.iter().fold(0.0_f64, |s, p| s + p.weight);
            assert_eq!(sum, 1.0, "{name}");
            // Losing a sample keeps the stencil below one.
            let partial = norm.points[..norm.points.len() - 1]
                .iter()
                .fold(0.0_f64, |s, p| s + p.weight);
            assert!(norm.points.len() == 1 || partial < 1.0, "{name}");
        }
        // Weights already summing to one within rounding (7 x 1/7) are corrected too.
        let hex = SamplingPattern::hex_seven_point();
        assert_ne!(hex.points.iter().fold(0.0_f64, |s, p| s + p.weight), 1.0);
    }
}
