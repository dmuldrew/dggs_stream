//! How raster pixels are transferred onto grid cells, independent of the grid.
//!
//! - [`TransferMode::AssignCenters`](crate::transfer::TransferMode::AssignCenters) (the default): each pixel (or sub-pixel sample) is
//!   indexed to the cell holding its centre and aggregated there. This is the
//!   single-pass horizon kernel in [`crate::aggregator::multi_horizon`]; nothing in
//!   this module runs for it.
//! - [`TransferMode::Interpolate`](crate::transfer::TransferMode::Interpolate): each cell whose centre lies on the raster takes the raster
//!   value interpolated at that centre. For cells finer than the pixels, where centre
//!   assignment leaves most cells empty.
//! - [`TransferMode::Overlay`](crate::transfer::TransferMode::Overlay): each cell aggregates the pixels it overlaps, weighted by
//!   the overlapped fraction of each pixel, computed exactly from the cell's polygon in
//!   pixel space. With `mass_preserve`, every pixel's weights are normalised to sum to
//!   one, so the cells' `sum` column adds up to the raster's total.
//! - [`TransferMode::Auto`](crate::transfer::TransferMode::Auto): resolves on the open raster from `cell_area / pixel_area`
//!   (`Interpolate` below 1:1, `AssignCenters` near 1:1, weighted `Overlay` at >= 16:1)
//!   while respecting scan options such as categorical mode, compaction, and coverage.
//!
//! Both non-default modes walk the raster in bands of rows, list the cells of each band
//! from the band's bounding box, and hold only that band's pixels and the cells still
//! open across band edges, so memory stays flat in the raster's height.

mod clip;
mod partial;
pub(crate) use partial::OverlayPartial;
pub(crate) use stream::Filters;
mod interp;
mod stream;
mod window;

pub use clip::{pixel_overlaps, polygon_area};
pub use interp::{interpolate, Interpolation};
pub(crate) use stream::ground_area_overlay_tile;
pub use stream::{scan_ground_area_overlay, TransferAccumulator, TransferStream};
pub use window::RowWindow;

use crate::aggregator::multi_horizon::MultiResolutionConfig;
use crate::error::{DggsStreamError, Result};
use crate::grid::GridKind;
use crate::histogram::HistogramWeight;
use crate::raster::mosaic::MosaicReader;

/// Maximum `cell_area / pixel_area` ratio for `transfer = "auto"` to pick cell-centre
/// interpolation (`Interpolate`): below 1:1, cells are smaller than pixels so centre
/// assignment leaves empty cells.
pub const AUTO_INTERPOLATE_MAX_RATIO: f64 = 1.0;

/// Minimum `cell_area / pixel_area` ratio for `transfer = "auto"` to pick area-weighted
/// `Overlay`: from 16:1 (roughly 4x linear pixel size) upward, fractional boundary
/// weights improve accuracy and polygon span clipping avoids per-pixel indexing.
pub const AUTO_OVERLAY_MIN_RATIO: f64 = 16.0;

/// Above this cell diameter bound (`GridSpec::max_cell_diameter_km`, about four times a
/// typical edge) `overlay` assigns whole pixels to cells by centre: the edges of such
/// cells bend by more than a kilometre from the straight lines the polygon clipping
/// assumes, and a pixel is negligible next to the cell.
pub(crate) const COARSE_CELL_DIAMETER_KM: f64 = 500.0;

/// How pixels are transferred onto cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransferMode {
    /// Aggregate each pixel into the cell holding its centre.
    #[default]
    AssignCenters,
    /// Give each cell the raster value interpolated at its centre.
    Interpolate(Interpolation),
    /// Aggregate, for each cell, the pixels it overlaps.
    Overlay {
        /// Weight each pixel by the fraction of it the cell covers (`true`, the
        /// default), or count every pixel the cell touches once (`false`).
        weighted: bool,
        /// Normalise each pixel's weights across cells to sum to one, so `sum` is
        /// conserved exactly.
        mass_preserve: bool,
    },
    /// Choose between [`Self::Interpolate`], [`Self::AssignCenters`], and
    /// [`Self::OVERLAY`] automatically from cell-to-pixel area ratio and scan options.
    Auto,
}

impl TransferMode {
    /// The accepted mode names, for error messages.
    pub const EXPECTED: &'static str = "'assign_centers', 'interpolate', 'overlay' or 'auto'";

    /// The area-weighted overlay.
    pub const OVERLAY: Self = Self::Overlay {
        weighted: true,
        mass_preserve: false,
    };

    /// Build a mode from the user-facing options: `transfer` (`assign_centers`,
    /// `interpolate`, `overlay`, `auto`; `None` is the default `assign_centers`),
    /// `interpolation` (`nearest`, `bilinear`, `bicubic`, `lanczos`; default `bilinear`),
    /// `overlay_weighted` (default `true`) and `mass_preserve` (default `false`). An
    /// option that does not belong to the chosen mode is an error.
    pub fn from_options(
        transfer: Option<&str>,
        interpolation: Option<&str>,
        overlay_weighted: Option<bool>,
        mass_preserve: Option<bool>,
    ) -> Result<Self> {
        let invalid = |message: String| DggsStreamError::InvalidParameter(message);
        let name = transfer.map(|t| t.trim().to_ascii_lowercase().replace('-', "_"));
        let mode = match name.as_deref() {
            None | Some("assign_centers" | "assign" | "centers" | "centres") => Self::AssignCenters,
            Some("auto") => Self::Auto,
            Some("interpolate" | "interpolation" | "resample") => {
                Self::Interpolate(match interpolation {
                    None => Interpolation::Bilinear,
                    Some(method) => Interpolation::parse(method).ok_or_else(|| {
                        invalid(format!(
                            "Invalid interpolation '{method}': expected {}",
                            Interpolation::EXPECTED
                        ))
                    })?,
                })
            }
            // The interpolation methods are accepted as transfer names too.
            Some(method) if Interpolation::parse(method).is_some() && interpolation.is_none() => {
                Self::Interpolate(Interpolation::parse(method).unwrap_or_default())
            }
            Some("overlay") => Self::Overlay {
                weighted: overlay_weighted.unwrap_or(true),
                mass_preserve: mass_preserve.unwrap_or(false),
            },
            // Not a name: `sampling` is the sub-pixel option of the default mode, and
            // the cell-centre mode is `interpolate`.
            Some("sample" | "sampling") => {
                return Err(invalid(format!(
                    "Invalid transfer '{}': use 'interpolate' to give each cell the value \
                     interpolated at its centre (the sub-pixel `sampling` option is separate \
                     and works with 'assign_centers'); expected {}",
                    name.as_deref().unwrap_or_default(),
                    Self::EXPECTED
                )))
            }
            Some(other) => {
                return Err(invalid(format!(
                    "Invalid transfer '{other}': expected {}",
                    Self::EXPECTED
                )))
            }
        };
        if interpolation.is_some() && !matches!(mode, Self::Interpolate(_)) {
            return Err(invalid("interpolation needs transfer 'interpolate'".into()));
        }
        if (overlay_weighted.is_some() || mass_preserve.is_some())
            && !matches!(mode, Self::Overlay { .. })
        {
            return Err(invalid(
                "overlay_weighted and mass_preserve need transfer 'overlay'".into(),
            ));
        }
        if let Self::Overlay {
            weighted: false,
            mass_preserve: true,
        } = mode
        {
            return Err(invalid(
                "mass_preserve needs area weights: it cannot be combined with \
                 overlay_weighted false"
                    .into(),
            ));
        }
        Ok(mode)
    }

    /// Whether this is the default centre assignment.
    pub fn is_assign_centers(&self) -> bool {
        matches!(self, Self::AssignCenters)
    }

    /// Whether this mode asks the engine to choose a concrete transfer mode automatically.
    pub fn is_auto(&self) -> bool {
        matches!(self, Self::Auto)
    }

    /// Resolve `Self::Auto` for a standard single-band continuous or categorical mosaic
    /// scan, returning concrete modes unchanged.
    pub fn resolve_for_mosaic(
        &self,
        mosaic: &MosaicReader,
        config: &MultiResolutionConfig,
        categorical: bool,
    ) -> Self {
        self.resolve_for_mosaic_with(mosaic, config, categorical, true)
    }

    /// Resolve `Self::Auto` for a `ground_area` scan (`Interpolate` is unsupported there,
    /// so resolution stays between [`Self::AssignCenters`] and [`Self::OVERLAY`]).
    pub fn resolve_for_ground_area(
        &self,
        mosaic: &MosaicReader,
        config: &MultiResolutionConfig,
    ) -> Self {
        self.resolve_for_mosaic_with(mosaic, config, false, false)
    }

    /// Resolve `Self::Auto` for a multi-band continuous scan (`Interpolate` is
    /// unsupported there, so resolution stays between [`Self::AssignCenters`] and
    /// [`Self::OVERLAY`]).
    pub fn resolve_for_multiband(
        &self,
        mosaic: &MosaicReader,
        config: &MultiResolutionConfig,
    ) -> Self {
        self.resolve_for_mosaic_with(mosaic, config, false, false)
    }

    fn resolve_for_mosaic_with(
        &self,
        mosaic: &MosaicReader,
        config: &MultiResolutionConfig,
        categorical: bool,
        allow_interpolate: bool,
    ) -> Self {
        if !self.is_auto() {
            return *self;
        }
        if config.should_compact_h3_children()
            || config.spectral_formula.is_some()
            || !config.sampling.is_single_point()
        {
            return Self::AssignCenters;
        }
        if config.valid_coverage_threshold.is_some()
            || config
                .histogram
                .as_ref()
                .is_some_and(|h| h.weight == HistogramWeight::Area)
        {
            return Self::OVERLAY;
        }
        if config.partition.is_some() {
            return Self::AssignCenters;
        }
        let Ok(pixel_area_m2) =
            crate::resolution::mosaic_pixel_ground_area_m2_in_bbox(mosaic, config.bbox)
        else {
            return Self::AssignCenters;
        };
        resolve_auto_transfer(
            config.grid,
            &config.resolutions,
            pixel_area_m2,
            categorical,
            allow_interpolate,
        )
    }

    /// `"assign_centers"`, `"interpolate"`, `"overlay"` or `"auto"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AssignCenters => "assign_centers",
            Self::Interpolate(_) => "interpolate",
            Self::Overlay { .. } => "overlay",
            Self::Auto => "auto",
        }
    }
}

/// Choose a concrete [`TransferMode`] for `grid` at `resolutions` given `pixel_area_m2`.
///
/// - When all resolutions have `cell_area / pixel_area < 1.0`, selects `Interpolate`
///   (`Nearest` when `categorical`, `Bilinear` when continuous) if `allow_interpolate`,
///   or falls back to `AssignCenters`.
/// - When all resolutions have `cell_area / pixel_area >= 16.0`, or span across `1.0`,
///   and every cell diameter bound is at most `500 km`, selects [`TransferMode::OVERLAY`].
/// - Otherwise (near `1:1` or coarse global cells), selects [`TransferMode::AssignCenters`].
pub fn resolve_auto_transfer(
    grid: GridKind,
    resolutions: &[u8],
    pixel_area_m2: f64,
    categorical: bool,
    allow_interpolate: bool,
) -> TransferMode {
    if resolutions.is_empty() || !pixel_area_m2.is_finite() || pixel_area_m2 <= 0.0 {
        return TransferMode::AssignCenters;
    }
    let spec = grid.spec();
    let any_coarse = resolutions
        .iter()
        .any(|&r| spec.max_cell_diameter_km(r) > COARSE_CELL_DIAMETER_KM);
    let mut min_ratio = f64::INFINITY;
    let mut max_ratio = 0.0f64;
    for &r in resolutions {
        let ratio = grid.area_m2(r) / pixel_area_m2;
        min_ratio = min_ratio.min(ratio);
        max_ratio = max_ratio.max(ratio);
    }
    let all_below_one = max_ratio < AUTO_INTERPOLATE_MAX_RATIO;
    let all_overlay = min_ratio >= AUTO_OVERLAY_MIN_RATIO;
    let spans_unit =
        min_ratio < AUTO_INTERPOLATE_MAX_RATIO && max_ratio >= AUTO_INTERPOLATE_MAX_RATIO;
    if all_below_one {
        if allow_interpolate {
            TransferMode::Interpolate(if categorical {
                Interpolation::Nearest
            } else {
                Interpolation::Bilinear
            })
        } else {
            TransferMode::AssignCenters
        }
    } else if (all_overlay || spans_unit) && !any_coarse {
        TransferMode::OVERLAY
    } else {
        TransferMode::AssignCenters
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_build_modes() {
        let build = TransferMode::from_options;
        assert_eq!(
            build(None, None, None, None).unwrap(),
            TransferMode::AssignCenters
        );
        assert_eq!(
            build(Some("Assign-Centers"), None, None, None).unwrap(),
            TransferMode::AssignCenters
        );
        assert_eq!(
            build(Some("interpolate"), None, None, None).unwrap(),
            TransferMode::Interpolate(Interpolation::Bilinear)
        );
        assert_eq!(
            build(Some("interpolate"), Some("Lanczos"), None, None).unwrap(),
            TransferMode::Interpolate(Interpolation::Lanczos)
        );
        assert_eq!(
            build(Some("bicubic"), None, None, None).unwrap(),
            TransferMode::Interpolate(Interpolation::Bicubic)
        );
        assert_eq!(
            build(Some("overlay"), None, None, None).unwrap(),
            TransferMode::OVERLAY
        );
        assert_eq!(
            build(Some("overlay"), None, Some(true), Some(true)).unwrap(),
            TransferMode::Overlay {
                weighted: true,
                mass_preserve: true
            }
        );
        assert_eq!(
            build(Some("overlay"), None, Some(false), None).unwrap(),
            TransferMode::Overlay {
                weighted: false,
                mass_preserve: false
            }
        );
        assert_eq!(
            build(Some("auto"), None, None, None).unwrap(),
            TransferMode::Auto
        );
        assert_eq!(
            build(Some("AUTO"), None, None, None).unwrap(),
            TransferMode::Auto
        );
        assert!(TransferMode::Auto.is_auto());
        assert!(!TransferMode::AssignCenters.is_auto());
        assert_eq!(TransferMode::Auto.as_str(), "auto");
        for bad in [
            build(Some("smear"), None, None, None),
            build(Some("interpolate"), Some("cubic9"), None, None),
            build(None, Some("bilinear"), None, None),
            build(Some("overlay"), Some("bilinear"), None, None),
            build(Some("auto"), Some("bilinear"), None, None),
            build(Some("auto"), None, Some(true), None),
            build(Some("auto"), None, None, Some(true)),
            build(Some("interpolate"), None, None, Some(true)),
            build(None, None, Some(true), None),
            build(Some("overlay"), None, Some(false), Some(true)),
        ] {
            assert!(bad.is_err());
        }
        assert_eq!(
            build(Some("Interpolation"), Some("nearest"), None, None).unwrap(),
            TransferMode::Interpolate(Interpolation::Nearest)
        );
        let err = build(Some("sample"), None, None, None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'interpolate'") && err.contains("sampling"),
            "{err}"
        );
        assert!(TransferMode::default().is_assign_centers());
        assert_eq!(
            TransferMode::Interpolate(Interpolation::Bicubic).as_str(),
            "interpolate"
        );
        assert_eq!(TransferMode::OVERLAY.as_str(), "overlay");
    }

    #[test]
    fn resolve_auto_transfer_thresholds() {
        for grid in GridKind::ALL {
            let res = 8u8.min(grid.max_resolution());
            let cell = grid.area_m2(res);

            // Below 1:1 chooses interpolate (Bilinear for continuous, Nearest for categorical),
            // or AssignCenters when interpolation is disallowed.
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell * 2.0, false, true),
                TransferMode::Interpolate(Interpolation::Bilinear)
            );
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell * 2.0, true, true),
                TransferMode::Interpolate(Interpolation::Nearest)
            );
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell * 2.0, false, false),
                TransferMode::AssignCenters
            );

            // Near 1:1 (1..16) chooses AssignCenters.
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell, false, true),
                TransferMode::AssignCenters
            );
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell / 10.0, false, true),
                TransferMode::AssignCenters
            );

            // At or above 16:1 chooses Overlay (unless cell exceeds 500 km diameter).
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell / 16.0, false, true),
                TransferMode::OVERLAY
            );
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell / 16.0, true, true),
                TransferMode::OVERLAY
            );
        }

        // Mixed resolutions spanning across 1:1 choose Overlay when below 500 km.
        let r10 = 10u8;
        let pixel_at_11 = GridKind::H3.area_m2(11);
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[r10, 12], pixel_at_11, false, true),
            TransferMode::OVERLAY
        );
        // Coarse global resolution (> 500 km diameter bound) falls back to AssignCenters.
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[0], 1e6, false, true),
            TransferMode::AssignCenters
        );
        // Degenerate or empty inputs fall back safely to AssignCenters.
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[], 100.0, false, true),
            TransferMode::AssignCenters
        );
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[8], 0.0, false, true),
            TransferMode::AssignCenters
        );
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[8], f64::NAN, false, true),
            TransferMode::AssignCenters
        );
    }
}
