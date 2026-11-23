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
    fn empty(quantiles: bool) -> Self {
        Self {
            stats: ContinuousAccumulator::empty(quantiles),
            covered: 0.0,
        }
    }
    fn cover(&mut self, area: f64) {
        self.covered += area;
    }
    fn add(&mut self, value: f64, weight: f64) {
        self.stats.add(value, weight);
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
        bytes.extend(encode_partial_state(&self.stats));
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
