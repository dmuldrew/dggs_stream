//! Interpolation of a raster band at fractional pixel positions.

use super::window::RowWindow;

/// The interpolation kernel of [`super::TransferMode::Sample`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SampleMethod {
    /// The value of the pixel holding the point.
    Nearest,
    /// Linear in each direction over the 2 x 2 nearest pixel centres.
    #[default]
    Bilinear,
    /// Catmull-Rom cubic convolution over 4 x 4 pixel centres. Exact for values that
    /// vary linearly or quadratically.
    Bicubic,
    /// Lanczos windowed sinc (a = 3) over 6 x 6 pixel centres, normalised.
    Lanczos,
}

impl SampleMethod {
    /// The accepted names, for error messages.
    pub const EXPECTED: &'static str = "'nearest', 'bilinear', 'bicubic' or 'lanczos'";

    /// Parse a method name (ASCII case-insensitive).
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "nearest" | "near" => Some(Self::Nearest),
            "bilinear" | "linear" => Some(Self::Bilinear),
            "bicubic" | "cubic" => Some(Self::Bicubic),
            "lanczos" | "lanczos3" => Some(Self::Lanczos),
            _ => None,
        }
    }

    /// The canonical name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nearest => "nearest",
            Self::Bilinear => "bilinear",
            Self::Bicubic => "bicubic",
            Self::Lanczos => "lanczos",
        }
    }

    /// Rows (and columns) of pixels the kernel reaches on each side of the pixel
    /// holding the point.
    pub const fn margin(self) -> u32 {
        match self {
            Self::Nearest => 0,
            Self::Bilinear => 1,
            Self::Bicubic => 2,
            Self::Lanczos => 3,
        }
    }
}

/// Catmull-Rom weights of the four taps around a point `t` in `[0, 1)` past the second.
fn cubic_weights(t: f64) -> [f64; 4] {
    let (t2, t3) = (t * t, t * t * t);
    [
        0.5 * (-t3 + 2.0 * t2 - t),
        0.5 * (3.0 * t3 - 5.0 * t2 + 2.0),
        0.5 * (-3.0 * t3 + 4.0 * t2 + t),
        0.5 * (t3 - t2),
    ]
}

/// Lanczos (a = 3) weights of the six taps around a point `t` in `[0, 1)` past the
/// third, normalised to sum to one.
fn lanczos_weights(t: f64) -> [f64; 6] {
    let kernel = |x: f64| {
        if x == 0.0 {
            1.0
        } else {
            let px = std::f64::consts::PI * x;
            3.0 * px.sin() * (px / 3.0).sin() / (px * px)
        }
    };
    let mut weights = [0.0; 6];
    let mut total = 0.0;
    for (k, w) in weights.iter_mut().enumerate() {
        *w = kernel(t - (k as f64 - 2.0));
        total += *w;
    }
    weights.map(|w| w / total)
}

/// The band value at pixel position `(col, row)` (pixel `(i, j)` covers
/// `[i, i + 1) x [j, j + 1)`, its centre at `(i + 0.5, j + 0.5)`), or `None` where the
/// raster has no value there.
///
/// Taps past the raster's edge repeat the edge pixel. Bilinear interpolation leaves out
/// NoData taps and renormalises; the wider kernels fall back to bilinear when any of
/// their taps is NoData, and bilinear to nothing when all four are.
pub fn interpolate(window: &RowWindow, method: SampleMethod, col: f64, row: f64) -> Option<f64> {
    let (width, height) = (i64::from(window.width()), i64::from(window.raster_height()));
    if !(col >= 0.0 && row >= 0.0 && col < width as f64 && row < height as f64) {
        return None;
    }
    let tap = |i: i64, j: i64| window.get(i.clamp(0, width - 1), j.clamp(0, height - 1));
    if method == SampleMethod::Nearest {
        return tap(col.floor() as i64, row.floor() as i64);
    }
    // Position relative to pixel centres.
    let (u, v) = (col - 0.5, row - 0.5);
    let (i0, j0) = (u.floor(), v.floor());
    let (fx, fy) = (u - i0, v - j0);
    let (i0, j0) = (i0 as i64, j0 as i64);

    let separable = |wx: &[f64], wy: &[f64], first: i64| -> Option<f64> {
        let mut value = 0.0;
        for (b, &wy) in wy.iter().enumerate() {
            for (a, &wx) in wx.iter().enumerate() {
                value += wx * wy * tap(i0 + first + a as i64, j0 + first + b as i64)?;
            }
        }
        Some(value)
    };
    let wide = match method {
        SampleMethod::Bicubic => separable(&cubic_weights(fx), &cubic_weights(fy), -1),
        SampleMethod::Lanczos => separable(&lanczos_weights(fx), &lanczos_weights(fy), -2),
        _ => None,
    };
    if wide.is_some() {
        return wide;
    }
    let (mut value, mut weight) = (0.0, 0.0);
    for (b, wy) in [(0, 1.0 - fy), (1, fy)] {
        for (a, wx) in [(0, 1.0 - fx), (1, fx)] {
            if let Some(v) = tap(i0 + a, j0 + b) {
                value += wx * wy * v;
                weight += wx * wy;
            }
        }
    }
    (weight > 1e-12).then(|| value / weight)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(width: u32, height: u32, value: impl Fn(u32, u32) -> f64) -> RowWindow {
        let values = (0..height)
            .flat_map(|r| (0..width).map(move |c| (c, r)))
            .map(|(c, r)| value(c, r))
            .collect();
        RowWindow::from_values(width, height, 0, values)
    }

    #[test]
    fn kernel_weights_sum_to_one() {
        for t in [0.0, 0.1, 0.5, 0.9, 0.999] {
            assert!((cubic_weights(t).iter().sum::<f64>() - 1.0).abs() < 1e-12);
            assert!((lanczos_weights(t).iter().sum::<f64>() - 1.0).abs() < 1e-12);
        }
        assert_eq!(cubic_weights(0.0), [0.0, 1.0, 0.0, 0.0]);
        let at_centre = lanczos_weights(0.0);
        assert!((at_centre[2] - 1.0).abs() < 1e-12);
    }

    #[test]
    fn every_method_returns_pixel_values_at_pixel_centres() {
        let w = window(9, 7, |c, r| {
            f64::from(c * c) + 3.0 * f64::from(r) - 0.25 * f64::from(c * r)
        });
        for method in [
            SampleMethod::Nearest,
            SampleMethod::Bilinear,
            SampleMethod::Bicubic,
            SampleMethod::Lanczos,
        ] {
            for r in 0..7u32 {
                for c in 0..9u32 {
                    let got =
                        interpolate(&w, method, f64::from(c) + 0.5, f64::from(r) + 0.5).unwrap();
                    assert!(
                        (got - w.get(i64::from(c), i64::from(r)).unwrap()).abs() < 1e-9,
                        "{method:?}"
                    );
                }
            }
            assert_eq!(interpolate(&w, method, -0.1, 3.0), None);
            assert_eq!(interpolate(&w, method, 9.0, 3.0), None);
            assert_eq!(interpolate(&w, method, 3.0, 7.0), None);
            assert_eq!(SampleMethod::parse(method.as_str()), Some(method));
        }
    }

    #[test]
    fn linear_fields_are_reproduced() {
        // value = 2x - 3y + 7 at pixel centres.
        let field = |x: f64, y: f64| 2.0 * x - 3.0 * y + 7.0;
        let w = window(16, 16, |c, r| field(f64::from(c) + 0.5, f64::from(r) + 0.5));
        let mut y = 3.05;
        while y < 13.0 {
            let mut x = 3.05;
            while x < 13.0 {
                let exact = field(x, y);
                let bilinear = interpolate(&w, SampleMethod::Bilinear, x, y).unwrap();
                let bicubic = interpolate(&w, SampleMethod::Bicubic, x, y).unwrap();
                let lanczos = interpolate(&w, SampleMethod::Lanczos, x, y).unwrap();
                assert!((bilinear - exact).abs() < 1e-10, "bilinear at ({x}, {y})");
                assert!((bicubic - exact).abs() < 1e-10, "bicubic at ({x}, {y})");
                // Lanczos ripples by a couple of percent of the slope on a ramp.
                assert!(
                    (lanczos - exact).abs() < 0.1,
                    "lanczos at ({x}, {y}): {lanczos} vs {exact}"
                );
                let nearest = interpolate(&w, SampleMethod::Nearest, x, y).unwrap();
                assert_eq!(nearest, field(x.floor() + 0.5, y.floor() + 0.5));
                x += 0.37;
            }
            y += 0.41;
        }
    }

    #[test]
    fn bicubic_reproduces_quadratics_and_beats_bilinear_on_smooth_fields() {
        let quadratic = |x: f64, y: f64| 0.5 * x * x - 0.25 * x * y + 0.125 * y * y;
        let w = window(20, 20, |c, r| {
            quadratic(f64::from(c) + 0.5, f64::from(r) + 0.5)
        });
        let smooth = |x: f64, y: f64| (0.35 * x).sin() * (0.3 * y).cos();
        let s = window(40, 40, |c, r| {
            smooth(f64::from(c) + 0.5, f64::from(r) + 0.5)
        });
        let (mut linear_err, mut cubic_err, mut lanczos_err) = (0.0f64, 0.0f64, 0.0f64);
        let mut y = 5.1;
        while y < 15.0 {
            let mut x = 5.1;
            while x < 15.0 {
                let got = interpolate(&w, SampleMethod::Bicubic, x, y).unwrap();
                assert!((got - quadratic(x, y)).abs() < 1e-9);
                let (sx, sy) = (x * 2.0, y * 2.0);
                let exact = smooth(sx, sy);
                let err = |m| (interpolate(&s, m, sx, sy).unwrap() - exact).abs();
                linear_err = linear_err.max(err(SampleMethod::Bilinear));
                cubic_err = cubic_err.max(err(SampleMethod::Bicubic));
                lanczos_err = lanczos_err.max(err(SampleMethod::Lanczos));
                x += 0.43;
            }
            y += 0.47;
        }
        assert!(cubic_err < linear_err / 3.0, "{cubic_err} vs {linear_err}");
        assert!(
            lanczos_err < linear_err / 3.0,
            "{lanczos_err} vs {linear_err}"
        );
    }

    #[test]
    fn nodata_taps_fall_back_and_edges_repeat() {
        // A NoData pixel at (4, 4).
        let w = window(9, 9, |c, r| if (c, r) == (4, 4) { f64::NAN } else { 10.0 });
        for method in [
            SampleMethod::Bilinear,
            SampleMethod::Bicubic,
            SampleMethod::Lanczos,
        ] {
            // Near the hole the remaining taps still give the constant.
            assert_eq!(interpolate(&w, method, 5.2, 5.2), Some(10.0));
            // Far from it the kernels are exact.
            assert!((interpolate(&w, method, 1.3, 7.6).unwrap() - 10.0).abs() < 1e-12);
            // Edge pixels repeat outward.
            assert!((interpolate(&w, method, 0.1, 0.1).unwrap() - 10.0).abs() < 1e-12);
            assert!((interpolate(&w, method, 8.9, 8.9).unwrap() - 10.0).abs() < 1e-12);
        }
        assert_eq!(interpolate(&w, SampleMethod::Nearest, 4.5, 4.5), None);
        // Dead centre of the hole, bilinear has only that tap.
        assert_eq!(interpolate(&w, SampleMethod::Bilinear, 4.5, 4.5), None);
        let all_nodata = window(4, 4, |_, _| f64::NAN);
        assert_eq!(
            interpolate(&all_nodata, SampleMethod::Bicubic, 2.0, 2.0),
            None
        );
    }
}
