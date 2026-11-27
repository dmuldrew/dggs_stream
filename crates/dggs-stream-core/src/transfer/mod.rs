//! How raster pixels are transferred onto grid cells, independent of the grid.
//!
//! - [`TransferMode::AssignCenters`](crate::transfer::TransferMode::AssignCenters) (the default): each pixel (or sub-pixel sample) is
//!   indexed to the cell holding its centre and aggregated there. This is the
//!   single-pass horizon kernel in [`crate::aggregator::multi_horizon`]; nothing in
//!   this module runs for it.
//! - [`TransferMode::Interpolate`](crate::transfer::TransferMode::Interpolate): each cell whose centre lies on the raster takes the raster
//!   value interpolated at its centre, or across a sub-cell quadrature [`SamplingPattern`](crate::aggregator::SamplingPattern)
//!   inside its projected cell polygon. For cells finer than or comparable to the pixels,
//!   where centre assignment leaves empty cells or misses intra-cell spread.
//! - [`TransferMode::Overlay`](crate::transfer::TransferMode::Overlay): each cell aggregates the pixels it overlaps, weighted by
//!   the overlapped fraction of each pixel, computed exactly from the cell's polygon in
//!   pixel space. With `mass_preserve`, every pixel's weights are normalised to sum to
//!   one, so the cells' `sum` column adds up to the raster's total.
//! - [`TransferMode::Auto`](crate::transfer::TransferMode::Auto): resolves on the open raster from `cell_area / pixel_area`
//!   (bilinear `Interpolate` for continuous scans below 1:1, weighted `Overlay` otherwise)
//!   while respecting scan options such as compaction, sampling and partitions, which
//!   need `AssignCenters`.
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

/// `cell_area / pixel_area` below which `transfer = "auto"` interpolates a continuous
/// scan at cell centres (bilinear): cells smaller than pixels would mostly hold no pixel
/// centre, and interpolation gives a smooth value per cell at a fraction of overlay's
/// cost. At and above it, and for categorical scans at every ratio, `auto` runs weighted
/// `Overlay`.
///
/// Centre assignment is never chosen by ratio. Measured with
/// `scripts/benchmark_transfer_modes.py` (see `docs/transfer-modes.md`), it was 2.2-3.9x
/// faster than overlay at about 1:1, where it left 9-26% of the cells empty and put their
/// areas 16-35% off, and still 1.7-2.7x faster at 16:1 on S2 and HEALPix; on H3 and A5
/// overlay was also faster from about 12:1.
pub const AUTO_INTERPOLATE_MAX_RATIO: f64 = 1.0;

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
            // Not a name: `sampling` is an option on `'assign_centers'` and `'interpolate'`,
            // while the interpolation transfer mode itself is `'interpolate'`.
            Some("sample" | "sampling") => {
                return Err(invalid(format!(
                    "Invalid transfer '{}': use 'interpolate' to interpolate at each cell \
                     (`sampling` is a separate option that works with 'assign_centers' and \
                     'interpolate'); expected {}",
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
        if config.should_compact_h3_children() || config.spectral_formula.is_some() {
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
        let resolve = if config.sampling.is_single_point() {
            resolve_auto_transfer
        } else {
            resolve_auto_transfer_sampled
        };
        resolve(
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

/// Whether an overlay scan with `mode` on `grid` may clip only its finest resolution and
/// roll those cells up into the coarser ones (strictly nested grids, weighted overlay
/// without mass preservation, categorical statistics). Engines that push resolution
/// predicates into a scan must not do so then: dropping the finest resolution changes
/// how the coarser cells are clipped.
pub fn nested_overlay_rollup(grid: GridKind, mode: TransferMode, categorical: bool) -> bool {
    categorical
        && matches!(grid, GridKind::S2 | GridKind::Healpix)
        && matches!(
            mode,
            TransferMode::Overlay {
                weighted: true,
                mass_preserve: false
            }
        )
}

/// Whether every requested resolution's mean cell is smaller than a pixel of
/// `pixel_area_m2` (`cell_area / pixel_area` below [`AUTO_INTERPOLATE_MAX_RATIO`]).
fn cells_finer_than_pixels(grid: GridKind, resolutions: &[u8], pixel_area_m2: f64) -> bool {
    resolutions
        .iter()
        .all(|&r| grid.area_m2(r) / pixel_area_m2 < AUTO_INTERPOLATE_MAX_RATIO)
}

/// [`resolve_auto_transfer`] for a scan with a non-`center` `sampling` preset, which
/// `Overlay` refuses, so `auto` chooses between the two modes that accept it:
///
/// - When every resolution's cells are finer than the pixels and `allow_interpolate`,
///   `Interpolate` with sub-cell samples (`Nearest` when `categorical`, `Bilinear`
///   otherwise). Centre assignment with sub-pixel samples leaves cells empty there
///   (`rgss` left 13–23% of cells empty at a quarter of a pixel per cell), and for
///   classes, nearest-neighbour sub-cell samples came 1.5–3x closer to overlay's class
///   shares than sub-pixel samples with the same preset.
/// - Otherwise `AssignCenters` with sub-pixel samples, which were as close to overlay as
///   sub-cell samples or closer for classes at every ratio from 1:1 measured, and closer
///   for continuous scans at 2:1.
///
/// Measured with `scripts/benchmark_transfer_modes.py` (see `docs/transfer-modes.md`).
pub fn resolve_auto_transfer_sampled(
    grid: GridKind,
    resolutions: &[u8],
    pixel_area_m2: f64,
    categorical: bool,
    allow_interpolate: bool,
) -> TransferMode {
    if resolutions.is_empty() || !pixel_area_m2.is_finite() || pixel_area_m2 <= 0.0 {
        return TransferMode::AssignCenters;
    }
    if allow_interpolate && cells_finer_than_pixels(grid, resolutions, pixel_area_m2) {
        TransferMode::Interpolate(if categorical {
            Interpolation::Nearest
        } else {
            Interpolation::Bilinear
        })
    } else {
        TransferMode::AssignCenters
    }
}

/// Choose a concrete [`TransferMode`] for `grid` at `resolutions` given `pixel_area_m2`.
///
/// - A continuous scan (`!categorical`) whose resolutions all have
///   `cell_area / pixel_area` below [`AUTO_INTERPOLATE_MAX_RATIO`] selects bilinear
///   `Interpolate` when `allow_interpolate`.
/// - Any resolution whose cell diameter bound is above `500 km` selects
///   [`TransferMode::AssignCenters`]: overlay takes whole pixels by centre there anyway.
/// - Everything else selects weighted [`TransferMode::OVERLAY`]: continuous scans at or
///   above 1:1 (or spanning it), categorical scans at every ratio (`Nearest` is only a
///   centre lookup, while overlay gives each class its share of the cell), and scans
///   that cannot interpolate (ground area, several bands) below 1:1.
/// - A missing or invalid pixel area selects [`TransferMode::AssignCenters`].
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
    if allow_interpolate
        && !categorical
        && cells_finer_than_pixels(grid, resolutions, pixel_area_m2)
    {
        TransferMode::Interpolate(Interpolation::Bilinear)
    } else if any_coarse {
        TransferMode::AssignCenters
    } else {
        TransferMode::OVERLAY
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

            // Continuous scans below 1:1 interpolate bilinearly.
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell * 2.0, false, true),
                TransferMode::Interpolate(Interpolation::Bilinear)
            );
            // Categorical scans overlay at every ratio, and so do scans that cannot
            // interpolate (ground area, several bands) below 1:1.
            for pixel in [cell * 2.0, cell, cell / 10.0, cell / 100.0] {
                assert_eq!(
                    resolve_auto_transfer(grid, &[res], pixel, true, true),
                    TransferMode::OVERLAY,
                    "{grid} categorical, cell/pixel {}",
                    cell / pixel
                );
            }
            assert_eq!(
                resolve_auto_transfer(grid, &[res], cell * 2.0, false, false),
                TransferMode::OVERLAY
            );

            // Continuous scans overlay from exactly 1:1 up; centre assignment is never
            // chosen by ratio.
            for pixel in [cell, cell / 1.5, cell / 10.0, cell / 16.0, cell / 1000.0] {
                assert_eq!(
                    resolve_auto_transfer(grid, &[res], pixel, false, true),
                    TransferMode::OVERLAY,
                    "{grid} continuous, cell/pixel {}",
                    cell / pixel
                );
            }
        }

        // Mixed resolutions spanning across 1:1 choose Overlay when below 500 km.
        let r10 = 10u8;
        let pixel_at_11 = GridKind::H3.area_m2(11);
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[r10, 12], pixel_at_11, false, true),
            TransferMode::OVERLAY
        );
        // Coarse global resolution (> 500 km diameter bound) falls back to AssignCenters,
        // for categorical scans too, and when it is mixed with fine resolutions.
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[0], 1e6, false, true),
            TransferMode::AssignCenters
        );
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[0], 1e6, true, true),
            TransferMode::AssignCenters
        );
        assert_eq!(
            resolve_auto_transfer(GridKind::H3, &[0, 8], 1e6, false, true),
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

    #[test]
    fn sampled_auto_interpolates_finer_cells_and_assigns_centres_otherwise() {
        for grid in GridKind::ALL {
            let res = 8u8.min(grid.max_resolution());
            let cell = grid.area_m2(res);
            // Cells finer than the pixels: sub-cell samples, nearest for classes.
            assert_eq!(
                resolve_auto_transfer_sampled(grid, &[res], cell * 2.0, false, true),
                TransferMode::Interpolate(Interpolation::Bilinear),
                "{grid}"
            );
            assert_eq!(
                resolve_auto_transfer_sampled(grid, &[res], cell * 2.0, true, true),
                TransferMode::Interpolate(Interpolation::Nearest),
                "{grid}"
            );
            // Scans that cannot interpolate fall back to sub-pixel samples.
            assert_eq!(
                resolve_auto_transfer_sampled(grid, &[res], cell * 2.0, true, false),
                TransferMode::AssignCenters,
                "{grid}"
            );
            // From 1:1 up, sub-pixel samples for both kinds; never overlay.
            for pixel in [cell, cell / 16.0, cell / 1000.0] {
                for categorical in [false, true] {
                    assert_eq!(
                        resolve_auto_transfer_sampled(grid, &[res], pixel, categorical, true),
                        TransferMode::AssignCenters,
                        "{grid} categorical={categorical} cell/pixel {}",
                        cell / pixel
                    );
                }
            }
        }
        // Mixed resolutions interpolate only when every one is finer than the pixels.
        let pixel_at_11 = GridKind::H3.area_m2(11);
        assert_eq!(
            resolve_auto_transfer_sampled(GridKind::H3, &[10, 12], pixel_at_11, true, true),
            TransferMode::AssignCenters
        );
        assert_eq!(
            resolve_auto_transfer_sampled(GridKind::H3, &[12, 13], pixel_at_11, true, true),
            TransferMode::Interpolate(Interpolation::Nearest)
        );
        assert_eq!(
            resolve_auto_transfer_sampled(GridKind::H3, &[8], f64::NAN, false, true),
            TransferMode::AssignCenters
        );
    }
}
