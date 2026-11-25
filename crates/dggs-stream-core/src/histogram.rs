//! One description of a per-cell histogram, for categorical rasters (a weight per exact
//! value) and continuous ones (a weight per bin).
//!
//! A histogram scan is a categorical scan: each pixel adds its weight to one class of its
//! cell. With [`HistogramBins::Values`] the class is the pixel's value; with bins it is
//! the index of the bin holding the value, so a binned histogram of a continuous raster
//! streams through the same single-pass kernel and holds one counter per occupied bin.
//!
//! The weight of a pixel is 1 ([`HistogramWeight::Count`]) or the area of the cell it
//! covers, in pixels ([`HistogramWeight::Area`], which runs the `overlay` transfer mode).
//! [`HistogramNormalize`] turns the weights of a cell into shares of their total, or into
//! shares per unit of value (a density, bins only).

use crate::aggregator::categorical::CategoricalAccumulator;
use crate::error::{DggsStreamError, Result};
use crate::transfer::TransferMode;

/// Most bins a histogram may have. Each occupied bin of each cell is a counter in the
/// horizon maps, so this bounds a cell's footprint.
pub const MAX_HISTOGRAM_BINS: usize = 4096;

/// What the classes of a histogram are.
#[derive(Debug, Clone, PartialEq)]
pub enum HistogramBins {
    /// One class per distinct pixel value (categorical rasters).
    Values,
    /// Bins between ascending edges: bin `i` holds `edges[i] <= v < edges[i + 1]`, and the
    /// last bin also holds its upper edge. Values outside the edges are not counted.
    Edges(Vec<f64>),
}

/// What a pixel adds to its class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HistogramWeight {
    /// 1 per pixel whose centre lies in the cell (or per sample of the transfer mode).
    #[default]
    Count,
    /// The area of the pixel inside the cell, in pixels (`overlay` transfer).
    Area,
}

/// How the weights of one cell are scaled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HistogramNormalize {
    /// Weights as accumulated.
    #[default]
    None,
    /// Weights divided by the cell's total, so they sum to 1.
    Fraction,
    /// Fractions divided by the bin's width, so the histogram integrates to 1.
    Density,
}

/// A per-cell histogram: its classes, the weight of a pixel and the scaling of the result.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSpec {
    pub bins: HistogramBins,
    pub weight: HistogramWeight,
    pub normalize: HistogramNormalize,
}

impl Default for HistogramSpec {
    fn default() -> Self {
        Self {
            bins: HistogramBins::Values,
            weight: HistogramWeight::Count,
            normalize: HistogramNormalize::None,
        }
    }
}

/// One class of one cell's histogram.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistogramEntry {
    /// The pixel value ([`HistogramBins::Values`]) or the bin's index.
    pub class: i64,
    /// The bin's edges; both are the value itself for [`HistogramBins::Values`].
    pub lower: f64,
    pub upper: f64,
    /// Count, area, fraction or density, as the spec asks.
    pub weight: f64,
}

impl HistogramBins {
    /// `count` equal bins from `min` to `max`.
    pub fn uniform(count: usize, min: f64, max: f64) -> Result<Self> {
        if count == 0 || count > MAX_HISTOGRAM_BINS {
            return Err(invalid(format!(
                "a histogram takes 1 to {MAX_HISTOGRAM_BINS} bins, got {count}"
            )));
        }
        if !(min.is_finite() && max.is_finite() && min < max) {
            return Err(invalid(format!(
                "histogram range must be finite with min < max, got {min}..{max}"
            )));
        }
        let width = (max - min) / count as f64;
        let mut edges: Vec<f64> = (0..count).map(|i| min + width * i as f64).collect();
        edges.push(max);
        Self::edges(edges)
    }

    /// Bins between explicit, strictly ascending, finite edges.
    pub fn edges(edges: Vec<f64>) -> Result<Self> {
        if edges.len() < 2 || edges.len() - 1 > MAX_HISTOGRAM_BINS {
            return Err(invalid(format!(
                "a histogram takes 2 to {} bin edges, got {}",
                MAX_HISTOGRAM_BINS + 1,
                edges.len()
            )));
        }
        if edges.iter().any(|e| !e.is_finite()) || edges.windows(2).any(|w| w[0] >= w[1]) {
            return Err(invalid(
                "histogram bin edges must be finite and strictly ascending".into(),
            ));
        }
        Ok(HistogramBins::Edges(edges))
    }

    /// Parse `values`, `N:min:max` (N equal bins) or `e0,e1,...` (explicit edges).
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if text.eq_ignore_ascii_case("values") || text.eq_ignore_ascii_case("categorical") {
            return Ok(HistogramBins::Values);
        }
        let number = |part: &str| {
            part.trim().parse::<f64>().map_err(|_| {
                invalid(format!(
                    "invalid histogram bins '{text}': expected `values`, `N:min:max` or \
                     comma-separated edges"
                ))
            })
        };
        if text.contains(':') {
            let parts: Vec<&str> = text.split(':').collect();
            let count = parts[0].trim().parse::<usize>().ok();
            return match (count, parts.len()) {
                (Some(count), 3) => Self::uniform(count, number(parts[1])?, number(parts[2])?),
                _ => Err(invalid(format!(
                    "invalid histogram bins '{text}': expected `N:min:max`"
                ))),
            };
        }
        Self::edges(text.split(',').map(number).collect::<Result<Vec<_>>>()?)
    }

    /// The number of bins, if the histogram is binned.
    pub fn bin_count(&self) -> Option<usize> {
        match self {
            HistogramBins::Values => None,
            HistogramBins::Edges(edges) => Some(edges.len() - 1),
        }
    }

    /// The bin of `value`, if it lies within the edges. `None` for [`Self::Values`] and
    /// for NaN.
    #[inline]
    pub fn bin_of(&self, value: f64) -> Option<i64> {
        let HistogramBins::Edges(edges) = self else {
            return None;
        };
        let (first, last) = (edges[0], edges[edges.len() - 1]);
        if !(value >= first && value <= last) {
            return None;
        }
        // Edges past `value`; the upper edge belongs to the last bin.
        let above = edges.partition_point(|&e| e <= value);
        Some((above.max(1).min(edges.len() - 1) - 1) as i64)
    }
}

impl HistogramWeight {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "count" => Ok(HistogramWeight::Count),
            "area" => Ok(HistogramWeight::Area),
            other => Err(invalid(format!(
                "invalid histogram weight '{other}': expected count or area"
            ))),
        }
    }
}

impl HistogramNormalize {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(HistogramNormalize::None),
            "fraction" => Ok(HistogramNormalize::Fraction),
            "density" => Ok(HistogramNormalize::Density),
            other => Err(invalid(format!(
                "invalid histogram normalization '{other}': expected none, fraction or density"
            ))),
        }
    }
}

impl HistogramSpec {
    /// The spec named by an engine's options, each optional: `histogram` (see
    /// [`HistogramBins::parse`]), `histogram_weight` and `histogram_normalize`. `None`
    /// when none is given.
    pub fn from_options(
        bins: Option<&str>,
        weight: Option<&str>,
        normalize: Option<&str>,
    ) -> Result<Option<Self>> {
        if bins.is_none() && weight.is_none() && normalize.is_none() {
            return Ok(None);
        }
        let spec = Self {
            bins: bins
                .map(HistogramBins::parse)
                .transpose()?
                .unwrap_or(HistogramBins::Values),
            weight: weight
                .map(HistogramWeight::parse)
                .transpose()?
                .unwrap_or_default(),
            normalize: normalize
                .map(HistogramNormalize::parse)
                .transpose()?
                .unwrap_or_default(),
        };
        spec.validate()?;
        Ok(Some(spec))
    }

    pub fn validate(&self) -> Result<()> {
        if self.normalize == HistogramNormalize::Density && self.bins == HistogramBins::Values {
            return Err(invalid(
                "histogram density needs bins: exact values have no width".into(),
            ));
        }
        Ok(())
    }

    /// Whether classes are bins rather than pixel values.
    pub fn is_binned(&self) -> bool {
        self.bins.bin_count().is_some()
    }

    /// The transfer mode a scan with this histogram runs: area weights need a weighted
    /// `overlay`, which is chosen when the scan asks for the default mode and refused for
    /// `interpolate` and for an unweighted overlay (which would weight each pixel 1).
    pub fn transfer_mode(&self, requested: TransferMode) -> Result<TransferMode> {
        match (self.weight, requested) {
            (HistogramWeight::Count, mode) => Ok(mode),
            (HistogramWeight::Area, TransferMode::AssignCenters | TransferMode::Auto) => {
                Ok(TransferMode::Overlay {
                    weighted: true,
                    mass_preserve: false,
                })
            }
            (HistogramWeight::Area, mode @ TransferMode::Overlay { weighted: true, .. }) => {
                Ok(mode)
            }
            (
                HistogramWeight::Area,
                TransferMode::Overlay {
                    weighted: false, ..
                },
            ) => Err(invalid(
                "histogram weight 'area' needs a weighted overlay (overlay_weighted=true)".into(),
            )),
            (HistogramWeight::Area, TransferMode::Interpolate(_)) => Err(invalid(
                "histogram weight 'area' needs transfer 'overlay', not 'interpolate'".into(),
            )),
        }
    }

    /// The histogram of one cell, ascending by class. A binned histogram lists every bin,
    /// empty ones with weight 0; a value histogram lists the values seen.
    pub fn entries(&self, accumulator: &CategoricalAccumulator) -> Vec<HistogramEntry> {
        let mut out = Vec::new();
        self.entries_into(accumulator, &mut out);
        out
    }

    /// [`Self::entries`] into a reused buffer.
    pub fn entries_into(
        &self,
        accumulator: &CategoricalAccumulator,
        out: &mut Vec<HistogramEntry>,
    ) {
        out.clear();
        match &self.bins {
            HistogramBins::Values => {
                accumulator.for_each_class(|class, weight| {
                    out.push(HistogramEntry {
                        class,
                        lower: class as f64,
                        upper: class as f64,
                        weight,
                    });
                });
                out.sort_unstable_by_key(|e| e.class);
            }
            HistogramBins::Edges(edges) => {
                out.extend(edges.windows(2).enumerate().map(|(i, w)| HistogramEntry {
                    class: i as i64,
                    lower: w[0],
                    upper: w[1],
                    weight: 0.0,
                }));
                accumulator.for_each_class(|class, weight| {
                    if let Some(entry) = usize::try_from(class).ok().and_then(|i| out.get_mut(i)) {
                        entry.weight += weight;
                    }
                });
            }
        }
        let total: f64 = out.iter().map(|e| e.weight).sum();
        match self.normalize {
            HistogramNormalize::None => {}
            _ if total <= 0.0 => {}
            HistogramNormalize::Fraction => out.iter_mut().for_each(|e| e.weight /= total),
            HistogramNormalize::Density => out
                .iter_mut()
                .for_each(|e| e.weight /= total * (e.upper - e.lower)),
        }
    }

    /// The histogram of one cell as JSON, for sinks without nested columns: an object of
    /// `"value": weight` for value histograms, and an array of `[lower, upper, weight]`
    /// for binned ones.
    pub fn json_into(
        &self,
        accumulator: &CategoricalAccumulator,
        scratch: &mut Vec<HistogramEntry>,
        out: &mut String,
    ) {
        use std::fmt::Write;
        self.entries_into(accumulator, scratch);
        out.clear();
        let binned = self.is_binned();
        out.push(if binned { '[' } else { '{' });
        for (i, entry) in scratch.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = if binned {
                write!(out, "[{},{},{}]", entry.lower, entry.upper, entry.weight)
            } else {
                write!(out, "\"{}\":{}", entry.class, entry.weight)
            };
        }
        out.push(if binned { ']' } else { '}' });
    }
}

fn invalid(message: String) -> DggsStreamError {
    DggsStreamError::InvalidParameter(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bins_parse_and_locate_values() {
        assert_eq!(
            HistogramBins::parse("values").unwrap(),
            HistogramBins::Values
        );
        let bins = HistogramBins::parse("4:0:8").unwrap();
        assert_eq!(bins, HistogramBins::Edges(vec![0.0, 2.0, 4.0, 6.0, 8.0]));
        assert_eq!(bins.bin_count(), Some(4));
        let located: Vec<_> = [-0.1, 0.0, 1.99, 2.0, 7.9, 8.0, 8.1, f64::NAN]
            .iter()
            .map(|&v| bins.bin_of(v))
            .collect();
        assert_eq!(
            located,
            [
                None,
                Some(0),
                Some(0),
                Some(1),
                Some(3),
                Some(3),
                None,
                None
            ]
        );
        let edges = HistogramBins::parse("0, 1, 10, 100").unwrap();
        assert_eq!(edges.bin_of(5.0), Some(1));
        assert_eq!(edges.bin_of(100.0), Some(2));
        for bad in [
            "", "0:1", "0:0:1", "3:1:1", "5", "1,1", "2,1", "a,b", "3:x:1", "9999:0:1",
        ] {
            assert!(HistogramBins::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn options_build_a_spec() {
        assert_eq!(HistogramSpec::from_options(None, None, None).unwrap(), None);
        let spec = HistogramSpec::from_options(Some("2:0:1"), Some("area"), Some("density"))
            .unwrap()
            .unwrap();
        assert!(spec.is_binned());
        assert_eq!(spec.weight, HistogramWeight::Area);
        assert_eq!(spec.normalize, HistogramNormalize::Density);
        assert_eq!(
            HistogramSpec::from_options(None, None, Some("fraction"))
                .unwrap()
                .unwrap()
                .bins,
            HistogramBins::Values
        );
        assert!(HistogramSpec::from_options(None, None, Some("density")).is_err());
        assert!(HistogramSpec::from_options(None, Some("mass"), None).is_err());
        assert!(HistogramSpec::from_options(None, None, Some("percent")).is_err());
    }

    #[test]
    fn area_weights_run_overlay() {
        use crate::transfer::Interpolation;
        let area = HistogramSpec {
            weight: HistogramWeight::Area,
            ..Default::default()
        };
        assert!(matches!(
            area.transfer_mode(TransferMode::AssignCenters).unwrap(),
            TransferMode::Overlay { weighted: true, .. }
        ));
        assert!(area
            .transfer_mode(TransferMode::Interpolate(Interpolation::Nearest))
            .is_err());
        let unweighted = TransferMode::Overlay {
            weighted: false,
            mass_preserve: false,
        };
        assert!(area.transfer_mode(unweighted).is_err());
        assert_eq!(
            HistogramSpec::default().transfer_mode(unweighted).unwrap(),
            unweighted
        );
        assert_eq!(
            HistogramSpec::default()
                .transfer_mode(TransferMode::AssignCenters)
                .unwrap(),
            TransferMode::AssignCenters
        );
    }

    #[test]
    fn entries_follow_the_spec() {
        let mut acc = CategoricalAccumulator::default();
        acc.update_weighted(7, 3.0);
        acc.update_weighted(2, 1.0);
        let values = HistogramSpec::default();
        let classes: Vec<_> = values
            .entries(&acc)
            .iter()
            .map(|e| (e.class, e.weight))
            .collect();
        assert_eq!(classes, [(2, 1.0), (7, 3.0)]);
        let mut json = String::new();
        values.json_into(&acc, &mut Vec::new(), &mut json);
        assert_eq!(json, "{\"2\":1,\"7\":3}");

        // Classes of a binned scan are bin indices.
        let mut acc = CategoricalAccumulator::default();
        acc.update_weighted(0, 1.0);
        acc.update_weighted(2, 3.0);
        let mut spec = HistogramSpec {
            bins: HistogramBins::parse("0,1,3,7").unwrap(),
            ..Default::default()
        };
        let weights = |spec: &HistogramSpec| -> Vec<f64> {
            spec.entries(&acc).iter().map(|e| e.weight).collect()
        };
        assert_eq!(weights(&spec), [1.0, 0.0, 3.0]);
        spec.normalize = HistogramNormalize::Fraction;
        assert_eq!(weights(&spec), [0.25, 0.0, 0.75]);
        spec.normalize = HistogramNormalize::Density;
        assert_eq!(weights(&spec), [0.25, 0.0, 0.1875]);
        let integral: f64 = spec
            .entries(&acc)
            .iter()
            .map(|e| e.weight * (e.upper - e.lower))
            .sum();
        assert!((integral - 1.0).abs() < 1e-12);
        spec.json_into(&acc, &mut Vec::new(), &mut json);
        assert_eq!(json, "[[0,1,0.25],[1,3,0],[3,7,0.1875]]");
        // A cell without samples stays all zero.
        assert_eq!(
            spec.entries(&CategoricalAccumulator::default())
                .iter()
                .map(|e| e.weight)
                .sum::<f64>(),
            0.0
        );
    }
}
