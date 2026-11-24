//! Accumulator state for opt-in ground-area-weighted continuous statistics.

use std::io::{self, Read, Write};

use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::multi_horizon::partial::PartialState;
use crate::aggregator::multi_horizon::sharded_map::AccumulatorMerge;
use crate::aggregator::multi_horizon::spill::SpillAccumulator;
use crate::error::{DggsStreamError, Result};

/// Pixel contribution count and area-weighted continuous statistics for one cell.
///
/// `pixel_count` is the sum of retained pixel fractions. The underlying
/// accumulator's count is retained area in square metres and its sum is the
/// area-weighted value sum, with units of value times square metres.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AreaAccumulator {
    pixel_count: f64,
    area_stats: ContinuousAccumulator,
}

impl AreaAccumulator {
    /// Add one pixel's total retained fraction in a cell and its full area in m².
    ///
    /// All samples of one pixel assigned to this cell must be combined before
    /// calling this method, so the variance uses one observation per pixel.
    /// Invalid inputs leave the state unchanged.
    pub fn update_pixel(&mut self, value: f64, pixel_fraction: f64, area_m2: f64) -> Result<()> {
        self.update_overlap(value, pixel_fraction, pixel_fraction * area_m2)
    }

    /// Add the measured ground area of one pixel-cell intersection.
    pub(crate) fn update_overlap(
        &mut self,
        value: f64,
        pixel_fraction: f64,
        area: f64,
    ) -> Result<()> {
        if !value.is_finite()
            || !pixel_fraction.is_finite()
            || !(0.0 < pixel_fraction && pixel_fraction <= 1.0)
            || !area.is_finite()
            || area <= 0.0
            || !(self.pixel_count + pixel_fraction).is_finite()
            || !(self.area_stats.count + area).is_finite()
            || !(area * value).is_finite()
            || !(self.area_stats.sum + area * value).is_finite()
            || !(area * area).is_finite()
            || !(self.area_stats.sum_w2 + area * area).is_finite()
        {
            return Err(DggsStreamError::InvalidParameter(
                "invalid or overflowing area-weighted pixel contribution".into(),
            ));
        }
        self.pixel_count += pixel_fraction;
        self.area_stats.update_weighted(value, area);
        Ok(())
    }

    /// Sum of retained pixel fractions.
    pub fn pixel_count(&self) -> f64 {
        self.pixel_count
    }

    /// Retained ground area in square metres.
    pub fn retained_area_m2(&self) -> f64 {
        self.area_stats.count
    }

    /// Integral of pixel value over retained area, in value times square metres.
    pub fn area_weighted_sum(&self) -> f64 {
        self.area_stats.sum
    }

    /// Area-weighted statistics; `count` is retained area in square metres.
    pub fn area_stats(&self) -> &ContinuousAccumulator {
        &self.area_stats
    }

    /// Whether every statistic is finite. A merge of individually valid states
    /// can overflow; such a state must not be emitted.
    pub fn is_finite(&self) -> bool {
        let s = &self.area_stats;
        [self.pixel_count, s.count, s.sum, s.m2, s.sum_w2]
            .iter()
            .all(|v| v.is_finite())
    }

    /// Merge a disjoint contribution state into this state.
    pub fn merge(&mut self, other: &Self) {
        self.pixel_count += other.pixel_count;
        self.area_stats.merge(&other.area_stats);
    }

    /// Merge a unit-weight contiguous row span where every pixel has ground area `area_m2`.
    pub(crate) fn merge_unit_span(
        &mut self,
        unit: &ContinuousAccumulator,
        area_m2: f64,
    ) -> Result<()> {
        if unit.count == 0.0 {
            return Ok(());
        }
        if !area_m2.is_finite() || area_m2 <= 0.0 || unit.quantiles.is_some() {
            return Err(DggsStreamError::InvalidParameter(
                "invalid or overflowing area-weighted pixel contribution".into(),
            ));
        }
        let span = Self {
            pixel_count: unit.count,
            area_stats: ContinuousAccumulator::from_weighted_stats(
                unit.sum * area_m2,
                unit.count * area_m2,
                unit.count * area_m2 * area_m2,
                unit.min,
                unit.max,
                unit.m2 * area_m2,
            ),
        };
        if !span.is_finite() || !span.area_stats.min.is_finite() || !span.area_stats.max.is_finite()
        {
            return Err(DggsStreamError::InvalidParameter(
                "invalid or overflowing area-weighted pixel contribution".into(),
            ));
        }
        let mut merged = self.clone();
        merged.merge(&span);
        if !merged.is_finite() {
            return Err(DggsStreamError::InvalidParameter(
                "invalid or overflowing area-weighted pixel contribution".into(),
            ));
        }
        *self = merged;
        Ok(())
    }
}

impl AccumulatorMerge for AreaAccumulator {
    fn merge(&mut self, other: &Self) {
        AreaAccumulator::merge(self, other);
    }

    fn heap_bytes(&self) -> usize {
        AccumulatorMerge::heap_bytes(&self.area_stats)
    }
}

impl SpillAccumulator for AreaAccumulator {
    fn write_state(&self, out: &mut impl Write) -> io::Result<()> {
        out.write_all(&self.pixel_count.to_le_bytes())?;
        self.area_stats.write_state(out)
    }

    fn read_state(input: &mut impl Read, limit: usize) -> io::Result<Self> {
        let mut bytes = [0u8; 8];
        input.read_exact(&mut bytes)?;
        let pixel_count = f64::from_le_bytes(bytes);
        if !pixel_count.is_finite() || pixel_count < 0.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid pixel count",
            ));
        }
        let area_stats = ContinuousAccumulator::read_state(input, limit)?;
        if (pixel_count == 0.0) != (area_stats.count == 0.0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inconsistent area state",
            ));
        }
        let acc = Self {
            pixel_count,
            area_stats,
        };
        if acc.memory_bytes() > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "area state exceeds budget",
            ));
        }
        Ok(acc)
    }
}

impl PartialState for AreaAccumulator {
    const KIND: u8 = 3;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::multi_horizon::partial::{decode_partial_state, encode_partial_state};
    use crate::raster::ground_area::wgs84_rectangle_area;

    #[test]
    fn area_weights_keep_pixel_count_separate_and_survive_merge_and_partial_state() {
        let equator = wgs84_rectangle_area(0.0, 0.0, 1.0, 1.0).unwrap();
        let north = wgs84_rectangle_area(0.0, 59.0, 1.0, 60.0).unwrap();
        let mut a = AreaAccumulator::default();
        let mut b = AreaAccumulator::default();
        a.update_pixel(10.0, 1.0, equator).unwrap();
        b.update_pixel(30.0, 0.5, north).unwrap();
        let b: AreaAccumulator = decode_partial_state(&encode_partial_state(&b), 1 << 20).unwrap();
        a.merge(&b);
        let expected_area = equator + north * 0.5;
        let expected_mean = (equator * 10.0 + north * 0.5 * 30.0) / expected_area;
        assert_eq!(a.pixel_count(), 1.5);
        assert!((a.area_stats().count - expected_area).abs() < 1e-5);
        assert!((a.area_stats().mean() - expected_mean).abs() < 1e-12);
        assert!(a.area_stats().variance() > 0.0);
        let back: AreaAccumulator =
            decode_partial_state(&encode_partial_state(&a), 1 << 20).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn invalid_contributions_do_not_mutate_state() {
        let mut acc = AreaAccumulator::default();
        acc.update_pixel(2.0, 0.5, 4.0).unwrap();
        let before = acc.clone();
        for (value, fraction, area) in [
            (f64::NAN, 1.0, 1.0),
            (1.0, 0.0, 1.0),
            (1.0, 1.1, 1.0),
            (1.0, f64::NAN, 1.0),
            (1.0, 1.0, f64::INFINITY),
            (1.0, 1.0, f64::MAX),
        ] {
            assert!(acc.update_pixel(value, fraction, area).is_err());
            assert_eq!(acc, before);
        }
    }

    #[test]
    fn merge_unit_span_matches_pixel_updates_and_preserves_state_on_overflow() {
        let vals = [3.5, 7.0, 12.25, 19.0, 22.5, 1.25];
        let area_m2 = 4250.75;

        let mut expected = AreaAccumulator::default();
        expected.update_overlap(9.0, 0.5, 0.5 * area_m2).unwrap();
        for &v in &vals {
            expected.update_overlap(v, 1.0, area_m2).unwrap();
        }

        let mut unit = ContinuousAccumulator::default();
        for &v in &vals {
            unit.update(v);
        }
        let mut actual = AreaAccumulator::default();
        actual.update_overlap(9.0, 0.5, 0.5 * area_m2).unwrap();
        actual.merge_unit_span(&unit, area_m2).unwrap();

        assert_eq!(actual.pixel_count(), expected.pixel_count());
        assert!((actual.retained_area_m2() - expected.retained_area_m2()).abs() < 1e-9);
        assert!((actual.area_weighted_sum() - expected.area_weighted_sum()).abs() < 1e-9);
        assert!((actual.area_stats().variance() - expected.area_stats().variance()).abs() < 1e-11);

        // Overflow must error without mutating state.
        let before = actual.clone();
        assert!(actual.merge_unit_span(&unit, f64::MAX).is_err());
        assert_eq!(actual, before);
    }
}
