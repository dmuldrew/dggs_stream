//! What happens to NoData and to output precision, shared by every serializer.
//!
//! [`NodataPolicy`] decides whether cells that received only NoData samples are output;
//! [`OutputPolicy`] turns an accumulator into the values a row carries, applying the
//! NoData value and the decimal rounding in one place, so Arrow batches, Parquet, PMTiles,
//! DuckDB, PostgreSQL and the CLI agree.

use serde::{Deserialize, Serialize};

use crate::aggregator::accumulator::H3Accumulator;
use crate::aggregator::categorical::CategoricalAccumulator;

/// Whether cells whose samples are all NoData are output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodataPolicy {
    /// NoData samples are skipped and a cell with no valid sample is not output.
    #[default]
    Omit,
    /// Every cell a pixel falls in is output. NoData samples still add nothing to the
    /// statistics, so a cell with some valid samples reads as under `Omit`; a cell with
    /// none has `count` 0 and its statistics are the configured NoData value, or NULL.
    ///
    /// Chunks a sparse TIFF does not store hold no samples at all and are not output.
    Emit,
}

impl NodataPolicy {
    /// The accepted names, for error messages.
    pub const EXPECTED: &'static str = "'omit' or 'emit'";

    /// Parse `omit` or `emit` (ASCII case-insensitive).
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "omit" | "skip" => Some(Self::Omit),
            "emit" | "keep" => Some(Self::Emit),
            _ => None,
        }
    }

    /// `"omit"` or `"emit"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Omit => "omit",
            Self::Emit => "emit",
        }
    }
}

/// Most decimals [`OutputPolicy::decimals`] may ask for; an `f64` holds 15 to 17
/// significant digits, so more would change nothing.
pub const MAX_DECIMALS: u8 = 15;

/// How finished cells are turned into output values.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct OutputPolicy {
    /// Whether all-NoData cells are output.
    pub nodata: NodataPolicy,
    /// The value an all-NoData cell's statistics take under [`NodataPolicy::Emit`]
    /// (`mean`, `min`, `max`, quantiles; the class of a categorical cell, which must
    /// then be integral). `None` leaves them NULL.
    pub nodata_value: Option<f64>,
    /// Round floating-point statistics to this many decimals (half away from zero).
    /// `None` leaves them as computed. Counts and class codes are not rounded.
    pub decimals: Option<u8>,
}

/// The statistics of one continuous cell, ready to serialize.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContinuousStats {
    /// `None` for an all-NoData cell without a NoData value.
    pub mean: Option<f64>,
    /// `None` below two samples.
    pub stddev: Option<f64>,
    pub count: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub sum: f64,
}

/// The statistics of one categorical cell, ready to serialize.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CategoricalStats {
    /// `None` for an all-NoData cell without a NoData value.
    pub majority_class: Option<i64>,
    pub majority_fraction: f64,
    pub majority_count: f64,
    pub unique_classes: i64,
    pub total_count: f64,
    pub shannon_entropy: f64,
}

impl OutputPolicy {
    /// Whether all-NoData cells reach the output.
    #[inline]
    pub fn emits_nodata(&self) -> bool {
        self.nodata == NodataPolicy::Emit
    }

    /// `value` rounded to the configured decimals; non-finite values pass through.
    #[inline]
    pub fn round(&self, value: f64) -> f64 {
        match self.decimals {
            Some(decimals) if value.is_finite() => round_decimals(value, decimals),
            _ => value,
        }
    }

    /// [`Self::round`] of an optional value.
    #[inline]
    pub fn round_opt(&self, value: Option<f64>) -> Option<f64> {
        value.map(|v| self.round(v))
    }

    /// The value a statistic of an all-NoData cell takes: the NoData value (as given,
    /// not rounded), or `None`.
    #[inline]
    pub fn nodata_stat(&self) -> Option<f64> {
        self.nodata_value
    }

    /// The class of an all-NoData categorical cell: the NoData value if it is an
    /// integer, else `None`.
    #[inline]
    pub fn nodata_class(&self) -> Option<i64> {
        self.nodata_value
            .filter(|v| v.fract() == 0.0 && v.abs() < 9.0e15)
            .map(|v| v as i64)
    }

    /// A quantile (or other derived statistic) of a cell with `count` samples.
    #[inline]
    pub fn derived(&self, count: f64, value: f64) -> Option<f64> {
        if count > 0.0 {
            value.is_finite().then(|| self.round(value))
        } else {
            self.nodata_stat()
        }
    }

    /// The output values of a continuous cell.
    pub fn continuous(&self, acc: &H3Accumulator) -> ContinuousStats {
        if acc.count > 0.0 {
            let stddev = acc.stddev();
            ContinuousStats {
                mean: Some(self.round(acc.mean())),
                stddev: stddev.is_finite().then(|| self.round(stddev)),
                count: acc.count,
                min: Some(self.round(acc.min)),
                max: Some(self.round(acc.max)),
                sum: self.round(acc.sum),
            }
        } else {
            let value = self.nodata_stat();
            ContinuousStats {
                mean: value,
                stddev: None,
                count: 0.0,
                min: value,
                max: value,
                sum: 0.0,
            }
        }
    }

    /// The output values of a categorical cell.
    pub fn categorical(&self, acc: &CategoricalAccumulator) -> CategoricalStats {
        if acc.total_count > 0.0 {
            let (majority_class, majority_count, majority_fraction) = acc.majority();
            CategoricalStats {
                majority_class: Some(majority_class),
                majority_fraction: self.round(majority_fraction),
                majority_count,
                unique_classes: acc.unique_classes() as i64,
                total_count: acc.total_count,
                shannon_entropy: self.round(acc.shannon_entropy()),
            }
        } else {
            CategoricalStats {
                majority_class: self.nodata_class(),
                majority_fraction: 0.0,
                majority_count: 0.0,
                unique_classes: 0,
                total_count: 0.0,
                shannon_entropy: 0.0,
            }
        }
    }
}

/// `value` rounded to `decimals` decimal places, half away from zero. Values too large
/// for the scaled form to carry a fraction are returned unchanged.
#[inline]
pub fn round_decimals(value: f64, decimals: u8) -> f64 {
    const POW10: [f64; 16] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
    ];
    let scale = POW10[usize::from(decimals.min(MAX_DECIMALS))];
    let scaled = value * scale;
    if !scaled.is_finite() || scaled.abs() >= 4.5e15 {
        return value;
    }
    scaled.round() / scale
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_half_away_from_zero_and_leaves_the_rest() {
        assert_eq!(round_decimals(1.23456, 2), 1.23);
        assert_eq!(round_decimals(1.235, 0), 1.0);
        assert_eq!(round_decimals(2.5, 0), 3.0);
        assert_eq!(round_decimals(-2.5, 0), -3.0);
        assert_eq!(round_decimals(0.125, 2), 0.13);
        assert_eq!(round_decimals(-0.0004, 3), -0.0);
        assert_eq!(round_decimals(1e300, 3), 1e300);
        assert_eq!(round_decimals(123456789.5, 15), 123456789.5);
        assert_eq!(
            round_decimals(1.0 / 3.0, 200),
            round_decimals(1.0 / 3.0, 15)
        );
        let policy = OutputPolicy {
            decimals: Some(1),
            ..Default::default()
        };
        assert_eq!(policy.round(2.449), 2.4);
        assert!(policy.round(f64::NAN).is_nan());
        assert_eq!(policy.round(f64::INFINITY), f64::INFINITY);
        assert_eq!(OutputPolicy::default().round(2.449), 2.449);
        assert_eq!(policy.round_opt(None), None);
    }

    #[test]
    fn parses_policy_names() {
        assert_eq!(NodataPolicy::parse("omit"), Some(NodataPolicy::Omit));
        assert_eq!(NodataPolicy::parse(" EMIT "), Some(NodataPolicy::Emit));
        assert_eq!(NodataPolicy::parse("drop"), None);
        assert_eq!(NodataPolicy::default(), NodataPolicy::Omit);
        for policy in [NodataPolicy::Omit, NodataPolicy::Emit] {
            assert_eq!(NodataPolicy::parse(policy.as_str()), Some(policy));
        }
    }

    #[test]
    fn continuous_cells_with_and_without_samples() {
        let mut acc = H3Accumulator::default();
        for v in [1.0, 2.0, 4.0] {
            acc.update(v);
        }
        let plain = OutputPolicy::default().continuous(&acc);
        assert_eq!(plain.mean, Some(7.0 / 3.0));
        assert_eq!(
            (plain.min, plain.max, plain.count, plain.sum),
            (Some(1.0), Some(4.0), 3.0, 7.0)
        );
        let rounded = OutputPolicy {
            decimals: Some(2),
            ..Default::default()
        }
        .continuous(&acc);
        assert_eq!(rounded.mean, Some(2.33));
        assert_eq!(rounded.stddev, plain.stddev.map(|s| round_decimals(s, 2)));
        assert_eq!(rounded.count, 3.0);

        let empty = H3Accumulator::default();
        let null = OutputPolicy::default().continuous(&empty);
        assert_eq!(
            (null.mean, null.min, null.max, null.stddev),
            (None, None, None, None)
        );
        assert_eq!((null.count, null.sum), (0.0, 0.0));
        let sentinel = OutputPolicy {
            nodata: NodataPolicy::Emit,
            nodata_value: Some(-9999.5),
            decimals: Some(0),
        };
        let filled = sentinel.continuous(&empty);
        // The NoData value is written as given, not rounded.
        assert_eq!(
            (filled.mean, filled.min, filled.max),
            (Some(-9999.5), Some(-9999.5), Some(-9999.5))
        );
        assert_eq!(filled.stddev, None);
        assert_eq!(sentinel.derived(0.0, f64::NAN), Some(-9999.5));
        assert_eq!(sentinel.derived(2.0, 1.6), Some(2.0));
        assert_eq!(sentinel.nodata_class(), None);
    }

    #[test]
    fn categorical_cells_with_and_without_samples() {
        let mut acc = CategoricalAccumulator::default();
        for class in [3, 3, 7] {
            acc.update_weighted(class, 1.0);
        }
        let stats = OutputPolicy {
            decimals: Some(2),
            ..Default::default()
        }
        .categorical(&acc);
        assert_eq!(stats.majority_class, Some(3));
        assert_eq!(stats.majority_fraction, 0.67);
        assert_eq!(
            (
                stats.majority_count,
                stats.total_count,
                stats.unique_classes
            ),
            (2.0, 3.0, 2)
        );

        let empty = CategoricalAccumulator::default();
        assert_eq!(
            OutputPolicy::default().categorical(&empty).majority_class,
            None
        );
        let sentinel = OutputPolicy {
            nodata: NodataPolicy::Emit,
            nodata_value: Some(255.0),
            decimals: None,
        };
        let stats = sentinel.categorical(&empty);
        assert_eq!(stats.majority_class, Some(255));
        assert_eq!(
            (
                stats.total_count,
                stats.unique_classes,
                stats.majority_fraction
            ),
            (0.0, 0, 0.0)
        );
    }
}
