//! How raster pixels are transferred onto grid cells, independent of the grid.
//!
//! - [`TransferMode::AssignCenters`] (the default): each pixel (or sub-pixel sample) is
//!   indexed to the cell holding its centre and aggregated there. This is the
//!   single-pass horizon kernel in [`crate::aggregator::multi_horizon`]; nothing in
//!   this module runs for it.
//! - [`TransferMode::Sample`]: each cell whose centre lies on the raster takes the raster
//!   value interpolated at that centre. For cells finer than the pixels, where centre
//!   assignment leaves most cells empty.
//! - [`TransferMode::Overlay`]: each cell aggregates the pixels it overlaps, weighted by
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
pub use interp::{interpolate, SampleMethod};
pub use stream::{TransferAccumulator, TransferStream};
pub use window::RowWindow;

use crate::error::{RasterH3Error, Result};

/// How pixels are transferred onto cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransferMode {
    /// Aggregate each pixel into the cell holding its centre.
    #[default]
    AssignCenters,
    /// Give each cell the raster value interpolated at its centre.
    Sample(SampleMethod),
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
    pub const EXPECTED: &'static str = "'assign_centers', 'sample' or 'overlay'";

    /// The area-weighted overlay.
    pub const OVERLAY: Self = Self::Overlay {
        weighted: true,
        mass_preserve: false,
    };

    /// Build a mode from the user-facing options: `transfer` (`assign_centers`,
    /// `sample`, `overlay`; `None` is the default), `sample_method` (`nearest`,
    /// `bilinear`, `bicubic`, `lanczos`; default `bilinear`), `overlay_weighted`
    /// (default `true`) and `mass_preserve` (default `false`). An option that does not
    /// belong to the chosen mode is an error.
    pub fn from_options(
        transfer: Option<&str>,
        sample_method: Option<&str>,
        overlay_weighted: Option<bool>,
        mass_preserve: Option<bool>,
    ) -> Result<Self> {
        let invalid = |message: String| RasterH3Error::InvalidParameter(message);
        let name = transfer.map(|t| t.trim().to_ascii_lowercase().replace('-', "_"));
        let mode = match name.as_deref() {
            None | Some("assign_centers" | "assign" | "centers" | "centres") => Self::AssignCenters,
            Some("sample" | "resample") => Self::Sample(match sample_method {
                None => SampleMethod::Bilinear,
                Some(method) => SampleMethod::parse(method).ok_or_else(|| {
                    invalid(format!(
                        "Invalid sample_method '{method}': expected {}",
                        SampleMethod::EXPECTED
                    ))
                })?,
            }),
            // The interpolation methods are accepted as transfer names too.
            Some(method) if SampleMethod::parse(method).is_some() && sample_method.is_none() => {
                Self::Sample(SampleMethod::parse(method).unwrap_or_default())
            }
            Some("overlay") => Self::Overlay {
                weighted: overlay_weighted.unwrap_or(true),
                mass_preserve: mass_preserve.unwrap_or(false),
            },
            Some(other) => {
                return Err(invalid(format!(
                    "Invalid transfer '{other}': expected {}",
                    Self::EXPECTED
                )))
            }
        };
        if sample_method.is_some() && !matches!(mode, Self::Sample(_)) {
            return Err(invalid("sample_method needs transfer 'sample'".into()));
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

    /// `"assign_centers"`, `"sample"` or `"overlay"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AssignCenters => "assign_centers",
            Self::Sample(_) => "sample",
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
            build(Some("sample"), None, None, None).unwrap(),
            TransferMode::Sample(SampleMethod::Bilinear)
        );
        assert_eq!(
            build(Some("sample"), Some("Lanczos"), None, None).unwrap(),
            TransferMode::Sample(SampleMethod::Lanczos)
        );
        assert_eq!(
            build(Some("bicubic"), None, None, None).unwrap(),
            TransferMode::Sample(SampleMethod::Bicubic)
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
            build(Some("sample"), Some("cubic9"), None, None),
            build(None, Some("bilinear"), None, None),
            build(Some("overlay"), Some("bilinear"), None, None),
            build(Some("sample"), None, None, Some(true)),
            build(None, None, Some(true), None),
            build(Some("overlay"), None, Some(false), Some(true)),
        ] {
            assert!(bad.is_err());
        }
        assert!(TransferMode::default().is_assign_centers());
        assert_eq!(TransferMode::OVERLAY.as_str(), "overlay");
    }
}
