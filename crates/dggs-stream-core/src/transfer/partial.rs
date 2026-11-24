//! Mergeable state for ordinary continuous weighted overlay. The continuous
//! accumulator's count is valid area. Missing pixels only increase covered area.
use super::{Filters, TransferAccumulator};
use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::multi_horizon::{decode_partial_state, encode_partial_state};
use crate::error::{DggsStreamError, Result};

#[derive(Default)]
pub(crate) struct OverlayPartial {
    pub stats: ContinuousAccumulator,
    pub covered: f64,
}

impl TransferAccumulator for OverlayPartial {
    const CATEGORICAL: bool = false;
    const SUPPORTS_MERGE: bool = true;
    const NEEDS_COVER: bool = true;
    fn empty(quantiles: bool) -> Self {
        Self {
            stats: ContinuousAccumulator::empty(quantiles),
            covered: 0.0,
        }
    }
    fn merge_from(&mut self, other: &Self) {
        self.covered += other.covered;
        self.stats.merge(&other.stats);
    }
    fn cover(&mut self, area: f64) {
        self.covered += area;
    }
    fn add(&mut self, value: f64, weight: f64) {
        self.stats.add(value, weight);
    }
    fn add_band_unit_slice(&mut self, slot: usize, slice: &[f64]) -> usize {
        debug_assert_eq!(slot, 0);
        self.stats.add_band_unit_slice(0, slice)
    }
    fn add_band_native_unit_slice(
        &mut self,
        slot: usize,
        samples: crate::aggregator::multi_horizon::borrowed::BorrowedSamples<'_>,
        nodata: Option<f64>,
        scaling: crate::raster::metadata::BandScaling,
    ) -> usize {
        debug_assert_eq!(slot, 0);
        self.stats
            .add_band_native_unit_slice(0, samples, nodata, scaling)
    }
    fn has_samples(&self) -> bool {
        self.covered > 0.0
    }
    fn passes(&self, _: &Filters) -> bool {
        true
    }
}

impl OverlayPartial {
    // Versioned envelope around the standard continuous partial.
    const PREFIX: &'static [u8] = b"DGOV\x01";
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(Self::PREFIX)
    }
    pub fn encode(&self, minimum: f64) -> Vec<u8> {
        let mut bytes = Self::PREFIX.to_vec();
        bytes.extend_from_slice(&minimum.to_le_bytes());
        bytes.extend_from_slice(&self.covered.to_le_bytes());
        if self.stats.m2 < 0.0 {
            // Weighted updates keep two independently rounded means, so constant
            // samples can leave the variance numerator a few ULPs below zero. The true
            // value is zero (serial variance clamps it too), and the decoder rejects
            // negative state, so encode the clamped value.
            let mut stats = self.stats.clone();
            stats.m2 = 0.0;
            bytes.extend(encode_partial_state(&stats));
        } else {
            bytes.extend(encode_partial_state(&self.stats));
        }
        bytes
    }
    pub fn decode(bytes: &[u8], limit: usize) -> Result<(Self, f64)> {
        let invalid =
            || DggsStreamError::InvalidParameter("invalid weighted overlay partial state".into());
        let bytes = bytes.strip_prefix(Self::PREFIX).ok_or_else(invalid)?;
        let (minimum, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (covered, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let minimum = f64::from_le_bytes(*minimum);
        let covered = f64::from_le_bytes(*covered);
        if !(0.0..=1.0).contains(&minimum) || !covered.is_finite() || covered <= 0.0 {
            return Err(invalid());
        }
        Ok((
            Self {
                stats: decode_partial_state(bytes, limit)?,
                covered,
            },
            minimum,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_pixels_with_fractional_weights_round_trip() {
        // Two independently rounded running means straddle 7, which leaves the
        // variance numerator a few ULPs below zero.
        let mut partial = OverlayPartial::empty(false);
        for weight in [0.2871112485674534, 1.0] {
            partial.cover(weight);
            partial.add(7.0, weight);
        }
        assert!(
            partial.stats.m2 < 0.0,
            "the rounding case this test exists for"
        );
        let bytes = partial.encode(0.5);
        let (decoded, minimum) = OverlayPartial::decode(&bytes, 1 << 20).unwrap();
        assert_eq!(minimum, 0.5);
        assert_eq!(decoded.stats.m2, 0.0);
        assert_eq!(decoded.stats.variance(), 0.0);
        assert!((decoded.stats.mean() - 7.0).abs() < 1e-12);
    }
}
