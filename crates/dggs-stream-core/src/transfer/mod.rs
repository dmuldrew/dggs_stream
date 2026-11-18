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
//!
//! Both non-default modes walk the raster in bands of rows, list the cells of each band
//! from the band's bounding box, and hold only that band's pixels and the cells still
//! open across band edges, so memory stays flat in the raster's height.

mod clip;
mod interp;
mod stream;
mod window;

pub use clip::{pixel_overlaps, polygon_area};
pub use interp::{interpolate, Interpolation};
pub use stream::{TransferAccumulator, TransferStream};
pub use window::RowWindow;

use crate::error::{DggsStreamError, Result};

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
}

impl TransferMode {
    /// The accepted mode names, for error messages.
    pub const EXPECTED: &'static str = "'assign_centers', 'interpolate' or 'overlay'";

    /// The area-weighted overlay.
    pub const OVERLAY: Self = Self::Overlay {
        weighted: true,
        mass_preserve: false,
    };

    /// Build a mode from the user-facing options: `transfer` (`assign_centers`,
    /// `interpolate`, `overlay`; `None` is the default), `interpolation` (`nearest`,
    /// `bilinear`, `bicubic`, `lanczos`; default `bilinear`), `overlay_weighted`
    /// (default `true`) and `mass_preserve` (default `false`). An option that does not
    /// belong to the chosen mode is an error.
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

    /// `"assign_centers"`, `"interpolate"` or `"overlay"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AssignCenters => "assign_centers",
            Self::Interpolate(_) => "interpolate",
            Self::Overlay { .. } => "overlay",
        }
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
        for bad in [
            build(Some("smear"), None, None, None),
            build(Some("interpolate"), Some("cubic9"), None, None),
            build(None, Some("bilinear"), None, None),
            build(Some("overlay"), Some("bilinear"), None, None),
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
}
