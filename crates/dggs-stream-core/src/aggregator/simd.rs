use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::nodata::{NativeNoData, PixelValidity};

/// Trait for types that support high-throughput SIMD / multi-lane scanline span accumulation.
pub trait SimdSpanAccumulate: NativeNoData {
    /// Vectorized accumulation of a contiguous horizontal span into an ContinuousAccumulator.
    fn accumulate_span(slice: &[Self], nodata: Option<Self>) -> ContinuousAccumulator;

    /// Scalar conversion of a single value to f64 (for sub-pixel sampling or fallback points).
    fn to_f64_val(self) -> f64;

    /// Fast test if value is valid (finite and not NoData).
    fn is_valid(self, nodata: Option<Self>) -> bool;
}

// =========================================================================
// Float32 Specialization (f32) - Dominant Continuous Format (DEM, Climate)
// =========================================================================

impl SimdSpanAccumulate for f32 {
    #[inline(always)]
    fn to_f64_val(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn is_valid(self, nodata: Option<Self>) -> bool {
        PixelValidity::new(nodata).is_valid(self)
    }

    fn accumulate_span(slice: &[Self], nodata: Option<Self>) -> ContinuousAccumulator {
        if slice.is_empty() {
            return ContinuousAccumulator::default();
        }

        match nodata {
            None => accumulate_span_f32_by(slice, |v| v.is_finite()),
            // Non-finite values are already excluded by the no-marker path.
            Some(nd) if !nd.is_finite() => accumulate_span_f32_by(slice, |v| v.is_finite()),
            Some(nd) => accumulate_span_f32_by(slice, move |v| {
                v.is_finite() && v != nd && (v - nd).abs() >= 1e-6
            }),
        }
    }
}

#[inline(always)]
fn accumulate_span_f32_by<F: Fn(f32) -> bool + Copy>(
    slice: &[f32],
    is_ok: F,
) -> ContinuousAccumulator {
    let mut sum0 = 0.0f64;
    let mut sum1 = 0.0f64;
    let mut sum2 = 0.0f64;
    let mut sum3 = 0.0f64;
    let mut sum4 = 0.0f64;
    let mut sum5 = 0.0f64;
    let mut sum6 = 0.0f64;
    let mut sum7 = 0.0f64;

    let mut min0 = f32::INFINITY;
    let mut min1 = f32::INFINITY;
    let mut min2 = f32::INFINITY;
    let mut min3 = f32::INFINITY;
    let mut min4 = f32::INFINITY;
    let mut min5 = f32::INFINITY;
    let mut min6 = f32::INFINITY;
    let mut min7 = f32::INFINITY;

    let mut max0 = f32::NEG_INFINITY;
    let mut max1 = f32::NEG_INFINITY;
    let mut max2 = f32::NEG_INFINITY;
    let mut max3 = f32::NEG_INFINITY;
    let mut max4 = f32::NEG_INFINITY;
    let mut max5 = f32::NEG_INFINITY;
    let mut max6 = f32::NEG_INFINITY;
    let mut max7 = f32::NEG_INFINITY;

    let mut count0 = 0usize;
    let mut count1 = 0usize;
    let mut count2 = 0usize;
    let mut count3 = 0usize;
    let mut count4 = 0usize;
    let mut count5 = 0usize;
    let mut count6 = 0usize;
    let mut count7 = 0usize;

    let (chunks, remainder) = slice.as_chunks::<8>();

    for chunk in chunks {
        let v0 = chunk[0];
        let v1 = chunk[1];
        let v2 = chunk[2];
        let v3 = chunk[3];
        let v4 = chunk[4];
        let v5 = chunk[5];
        let v6 = chunk[6];
        let v7 = chunk[7];

        let ok0 = is_ok(v0);
        let ok1 = is_ok(v1);
        let ok2 = is_ok(v2);
        let ok3 = is_ok(v3);
        let ok4 = is_ok(v4);
        let ok5 = is_ok(v5);
        let ok6 = is_ok(v6);
        let ok7 = is_ok(v7);

        if ok0 & ok1 & ok2 & ok3 & ok4 & ok5 & ok6 & ok7 {
            sum0 += v0 as f64;
            sum1 += v1 as f64;
            sum2 += v2 as f64;
            sum3 += v3 as f64;
            sum4 += v4 as f64;
            sum5 += v5 as f64;
            sum6 += v6 as f64;
            sum7 += v7 as f64;

            min0 = min0.min(v0);
            min1 = min1.min(v1);
            min2 = min2.min(v2);
            min3 = min3.min(v3);
            min4 = min4.min(v4);
            min5 = min5.min(v5);
            min6 = min6.min(v6);
            min7 = min7.min(v7);

            max0 = max0.max(v0);
            max1 = max1.max(v1);
            max2 = max2.max(v2);
            max3 = max3.max(v3);
            max4 = max4.max(v4);
            max5 = max5.max(v5);
            max6 = max6.max(v6);
            max7 = max7.max(v7);

            count0 += 1;
            count1 += 1;
            count2 += 1;
            count3 += 1;
            count4 += 1;
            count5 += 1;
            count6 += 1;
            count7 += 1;
        } else if ok0 | ok1 | ok2 | ok3 | ok4 | ok5 | ok6 | ok7 {
            sum0 += if ok0 { v0 as f64 } else { 0.0 };
            sum1 += if ok1 { v1 as f64 } else { 0.0 };
            sum2 += if ok2 { v2 as f64 } else { 0.0 };
            sum3 += if ok3 { v3 as f64 } else { 0.0 };
            sum4 += if ok4 { v4 as f64 } else { 0.0 };
            sum5 += if ok5 { v5 as f64 } else { 0.0 };
            sum6 += if ok6 { v6 as f64 } else { 0.0 };
            sum7 += if ok7 { v7 as f64 } else { 0.0 };

            min0 = if ok0 { min0.min(v0) } else { min0 };
            min1 = if ok1 { min1.min(v1) } else { min1 };
            min2 = if ok2 { min2.min(v2) } else { min2 };
            min3 = if ok3 { min3.min(v3) } else { min3 };
            min4 = if ok4 { min4.min(v4) } else { min4 };
            min5 = if ok5 { min5.min(v5) } else { min5 };
            min6 = if ok6 { min6.min(v6) } else { min6 };
            min7 = if ok7 { min7.min(v7) } else { min7 };

            max0 = if ok0 { max0.max(v0) } else { max0 };
            max1 = if ok1 { max1.max(v1) } else { max1 };
            max2 = if ok2 { max2.max(v2) } else { max2 };
            max3 = if ok3 { max3.max(v3) } else { max3 };
            max4 = if ok4 { max4.max(v4) } else { max4 };
            max5 = if ok5 { max5.max(v5) } else { max5 };
            max6 = if ok6 { max6.max(v6) } else { max6 };
            max7 = if ok7 { max7.max(v7) } else { max7 };

            count0 += usize::from(ok0);
            count1 += usize::from(ok1);
            count2 += usize::from(ok2);
            count3 += usize::from(ok3);
            count4 += usize::from(ok4);
            count5 += usize::from(ok5);
            count6 += usize::from(ok6);
            count7 += usize::from(ok7);
        }
    }

    for &v in remainder {
        if is_ok(v) {
            sum0 += v as f64;
            min0 = min0.min(v);
            max0 = max0.max(v);
            count0 += 1;
        }
    }

    let total_count =
        ((count0 + count1) + (count2 + count3) + (count4 + count5) + (count6 + count7)) as f64;
    if total_count == 0.0 {
        return ContinuousAccumulator::default();
    }

    let total_sum = ((sum0 + sum1) + (sum2 + sum3)) + ((sum4 + sum5) + (sum6 + sum7));
    let total_min =
        ((min0.min(min1)).min(min2.min(min3))).min((min4.min(min5)).min(min6.min(min7))) as f64;
    let total_max =
        ((max0.max(max1)).max(max2.max(max3))).max((max4.max(max5)).max(max6.max(max7))) as f64;

    // Fast-path: uniform span (e.g. flat water or uniform elevation) has zero variance
    if total_min == total_max {
        return ContinuousAccumulator::from_stats(
            total_sum,
            total_count,
            total_min,
            total_max,
            0.0,
        );
    }

    // Pass 2: Vectorized M2 variance computation
    let mean = total_sum / total_count;
    let mut m2_0 = 0.0f64;
    let mut m2_1 = 0.0f64;
    let mut m2_2 = 0.0f64;
    let mut m2_3 = 0.0f64;
    let mut m2_4 = 0.0f64;
    let mut m2_5 = 0.0f64;
    let mut m2_6 = 0.0f64;
    let mut m2_7 = 0.0f64;

    let (chunks2, remainder2) = slice.as_chunks::<8>();

    if (total_count as usize) == slice.len() {
        // All values are valid: branchless second pass
        for chunk in chunks2 {
            let d0 = (chunk[0] as f64) - mean;
            let d1 = (chunk[1] as f64) - mean;
            let d2 = (chunk[2] as f64) - mean;
            let d3 = (chunk[3] as f64) - mean;
            let d4 = (chunk[4] as f64) - mean;
            let d5 = (chunk[5] as f64) - mean;
            let d6 = (chunk[6] as f64) - mean;
            let d7 = (chunk[7] as f64) - mean;
            m2_0 += d0 * d0;
            m2_1 += d1 * d1;
            m2_2 += d2 * d2;
            m2_3 += d3 * d3;
            m2_4 += d4 * d4;
            m2_5 += d5 * d5;
            m2_6 += d6 * d6;
            m2_7 += d7 * d7;
        }
        for &v in remainder2 {
            let d = (v as f64) - mean;
            m2_0 += d * d;
        }
    } else {
        for chunk in chunks2 {
            let v0 = chunk[0];
            let v1 = chunk[1];
            let v2 = chunk[2];
            let v3 = chunk[3];
            let v4 = chunk[4];
            let v5 = chunk[5];
            let v6 = chunk[6];
            let v7 = chunk[7];

            let ok0 = is_ok(v0);
            let ok1 = is_ok(v1);
            let ok2 = is_ok(v2);
            let ok3 = is_ok(v3);
            let ok4 = is_ok(v4);
            let ok5 = is_ok(v5);
            let ok6 = is_ok(v6);
            let ok7 = is_ok(v7);

            if ok0 & ok1 & ok2 & ok3 & ok4 & ok5 & ok6 & ok7 {
                let d0 = (v0 as f64) - mean;
                let d1 = (v1 as f64) - mean;
                let d2 = (v2 as f64) - mean;
                let d3 = (v3 as f64) - mean;
                let d4 = (v4 as f64) - mean;
                let d5 = (v5 as f64) - mean;
                let d6 = (v6 as f64) - mean;
                let d7 = (v7 as f64) - mean;
                m2_0 += d0 * d0;
                m2_1 += d1 * d1;
                m2_2 += d2 * d2;
                m2_3 += d3 * d3;
                m2_4 += d4 * d4;
                m2_5 += d5 * d5;
                m2_6 += d6 * d6;
                m2_7 += d7 * d7;
            } else if ok0 | ok1 | ok2 | ok3 | ok4 | ok5 | ok6 | ok7 {
                let d0 = if ok0 { (v0 as f64) - mean } else { 0.0 };
                let d1 = if ok1 { (v1 as f64) - mean } else { 0.0 };
                let d2 = if ok2 { (v2 as f64) - mean } else { 0.0 };
                let d3 = if ok3 { (v3 as f64) - mean } else { 0.0 };
                let d4 = if ok4 { (v4 as f64) - mean } else { 0.0 };
                let d5 = if ok5 { (v5 as f64) - mean } else { 0.0 };
                let d6 = if ok6 { (v6 as f64) - mean } else { 0.0 };
                let d7 = if ok7 { (v7 as f64) - mean } else { 0.0 };
                m2_0 += d0 * d0;
                m2_1 += d1 * d1;
                m2_2 += d2 * d2;
                m2_3 += d3 * d3;
                m2_4 += d4 * d4;
                m2_5 += d5 * d5;
                m2_6 += d6 * d6;
                m2_7 += d7 * d7;
            }
        }
        for &v in remainder2 {
            if is_ok(v) {
                let d = (v as f64) - mean;
                m2_0 += d * d;
            }
        }
    }

    let total_m2 = ((m2_0 + m2_1) + (m2_2 + m2_3)) + ((m2_4 + m2_5) + (m2_6 + m2_7));

    ContinuousAccumulator::from_stats(total_sum, total_count, total_min, total_max, total_m2)
}

// =========================================================================
// Float64 Specialization (f64)
// =========================================================================

impl SimdSpanAccumulate for f64 {
    #[inline(always)]
    fn to_f64_val(self) -> f64 {
        self
    }

    #[inline(always)]
    fn is_valid(self, nodata: Option<Self>) -> bool {
        PixelValidity::new(nodata).is_valid(self)
    }

    fn accumulate_span(slice: &[Self], nodata: Option<Self>) -> ContinuousAccumulator {
        if slice.is_empty() {
            return ContinuousAccumulator::default();
        }
        match nodata {
            None => accumulate_span_f64_by(slice, |v| v.is_finite()),
            Some(nd) if !nd.is_finite() => accumulate_span_f64_by(slice, |v| v.is_finite()),
            Some(nd) => accumulate_span_f64_by(slice, move |v| {
                v.is_finite() && v != nd && (v - nd).abs() >= 1e-6
            }),
        }
    }
}

#[inline(always)]
fn accumulate_span_f64_by<F: Fn(f64) -> bool + Copy>(
    slice: &[f64],
    is_ok: F,
) -> ContinuousAccumulator {
    let mut sum0 = 0.0f64;
    let mut sum1 = 0.0f64;
    let mut sum2 = 0.0f64;
    let mut sum3 = 0.0f64;
    let mut min0 = f64::INFINITY;
    let mut min1 = f64::INFINITY;
    let mut min2 = f64::INFINITY;
    let mut min3 = f64::INFINITY;
    let mut max0 = f64::NEG_INFINITY;
    let mut max1 = f64::NEG_INFINITY;
    let mut max2 = f64::NEG_INFINITY;
    let mut max3 = f64::NEG_INFINITY;
    let mut count0 = 0usize;
    let mut count1 = 0usize;
    let mut count2 = 0usize;
    let mut count3 = 0usize;

    let (chunks, remainder) = slice.as_chunks::<4>();

    for chunk in chunks {
        let v0 = chunk[0];
        let v1 = chunk[1];
        let v2 = chunk[2];
        let v3 = chunk[3];
        let ok0 = is_ok(v0);
        let ok1 = is_ok(v1);
        let ok2 = is_ok(v2);
        let ok3 = is_ok(v3);

        if ok0 & ok1 & ok2 & ok3 {
            sum0 += v0;
            sum1 += v1;
            sum2 += v2;
            sum3 += v3;
            min0 = min0.min(v0);
            min1 = min1.min(v1);
            min2 = min2.min(v2);
            min3 = min3.min(v3);
            max0 = max0.max(v0);
            max1 = max1.max(v1);
            max2 = max2.max(v2);
            max3 = max3.max(v3);
            count0 += 1;
            count1 += 1;
            count2 += 1;
            count3 += 1;
        } else if ok0 | ok1 | ok2 | ok3 {
            sum0 += if ok0 { v0 } else { 0.0 };
            sum1 += if ok1 { v1 } else { 0.0 };
            sum2 += if ok2 { v2 } else { 0.0 };
            sum3 += if ok3 { v3 } else { 0.0 };
            min0 = if ok0 { min0.min(v0) } else { min0 };
            min1 = if ok1 { min1.min(v1) } else { min1 };
            min2 = if ok2 { min2.min(v2) } else { min2 };
            min3 = if ok3 { min3.min(v3) } else { min3 };
            max0 = if ok0 { max0.max(v0) } else { max0 };
            max1 = if ok1 { max1.max(v1) } else { max1 };
            max2 = if ok2 { max2.max(v2) } else { max2 };
            max3 = if ok3 { max3.max(v3) } else { max3 };
            count0 += usize::from(ok0);
            count1 += usize::from(ok1);
            count2 += usize::from(ok2);
            count3 += usize::from(ok3);
        }
    }
    for &v in remainder {
        if is_ok(v) {
            sum0 += v;
            min0 = min0.min(v);
            max0 = max0.max(v);
            count0 += 1;
        }
    }

    let total_count = ((count0 + count1) + (count2 + count3)) as f64;
    if total_count == 0.0 {
        return ContinuousAccumulator::default();
    }
    let total_sum = (sum0 + sum1) + (sum2 + sum3);
    let total_min = (min0.min(min1)).min(min2.min(min3));
    let total_max = (max0.max(max1)).max(max2.max(max3));

    if total_min == total_max {
        return ContinuousAccumulator::from_stats(
            total_sum,
            total_count,
            total_min,
            total_max,
            0.0,
        );
    }

    let mean = total_sum / total_count;
    let mut m2_0 = 0.0f64;
    let mut m2_1 = 0.0f64;
    let mut m2_2 = 0.0f64;
    let mut m2_3 = 0.0f64;

    let (chunks2, remainder2) = slice.as_chunks::<4>();

    if (total_count as usize) == slice.len() {
        for chunk in chunks2 {
            let d0 = chunk[0] - mean;
            let d1 = chunk[1] - mean;
            let d2 = chunk[2] - mean;
            let d3 = chunk[3] - mean;
            m2_0 += d0 * d0;
            m2_1 += d1 * d1;
            m2_2 += d2 * d2;
            m2_3 += d3 * d3;
        }
        for &v in remainder2 {
            let d = v - mean;
            m2_0 += d * d;
        }
    } else {
        for chunk in chunks2 {
            let v0 = chunk[0];
            let v1 = chunk[1];
            let v2 = chunk[2];
            let v3 = chunk[3];
            let ok0 = is_ok(v0);
            let ok1 = is_ok(v1);
            let ok2 = is_ok(v2);
            let ok3 = is_ok(v3);

            if ok0 & ok1 & ok2 & ok3 {
                let d0 = v0 - mean;
                let d1 = v1 - mean;
                let d2 = v2 - mean;
                let d3 = v3 - mean;
                m2_0 += d0 * d0;
                m2_1 += d1 * d1;
                m2_2 += d2 * d2;
                m2_3 += d3 * d3;
            } else if ok0 | ok1 | ok2 | ok3 {
                let d0 = if ok0 { v0 - mean } else { 0.0 };
                let d1 = if ok1 { v1 - mean } else { 0.0 };
                let d2 = if ok2 { v2 - mean } else { 0.0 };
                let d3 = if ok3 { v3 - mean } else { 0.0 };
                m2_0 += d0 * d0;
                m2_1 += d1 * d1;
                m2_2 += d2 * d2;
                m2_3 += d3 * d3;
            }
        }
        for &v in remainder2 {
            if is_ok(v) {
                let d = v - mean;
                m2_0 += d * d;
            }
        }
    }

    ContinuousAccumulator::from_stats(
        total_sum,
        total_count,
        total_min,
        total_max,
        (m2_0 + m2_1) + (m2_2 + m2_3),
    )
}

// =========================================================================
// Generic Integer Macro for High-Throughput Integer Accumulators
// =========================================================================

macro_rules! impl_simd_span_integer {
    ($type:ty) => {
        impl SimdSpanAccumulate for $type {
            #[inline(always)]
            fn to_f64_val(self) -> f64 {
                self as f64
            }

            #[inline(always)]
            fn is_valid(self, nodata: Option<Self>) -> bool {
                PixelValidity::new(nodata).is_valid(self)
            }

            fn accumulate_span(slice: &[Self], nodata: Option<Self>) -> ContinuousAccumulator {
                if slice.is_empty() {
                    return ContinuousAccumulator::default();
                }

                let mut sum0 = 0.0f64;
                let mut sum1 = 0.0f64;
                let mut sum2 = 0.0f64;
                let mut sum3 = 0.0f64;
                let mut min0 = <$type>::MAX;
                let mut min1 = <$type>::MAX;
                let mut min2 = <$type>::MAX;
                let mut min3 = <$type>::MAX;
                let mut max0 = <$type>::MIN;
                let mut max1 = <$type>::MIN;
                let mut max2 = <$type>::MIN;
                let mut max3 = <$type>::MIN;
                let mut count0 = 0usize;
                let mut count1 = 0usize;
                let mut count2 = 0usize;
                let mut count3 = 0usize;

                let (chunks, remainder) = slice.as_chunks::<4>();

                match nodata {
                    None => {
                        for chunk in chunks {
                            let v0 = chunk[0];
                            let v1 = chunk[1];
                            let v2 = chunk[2];
                            let v3 = chunk[3];
                            sum0 += v0 as f64;
                            min0 = min0.min(v0);
                            max0 = max0.max(v0);
                            count0 += 1;

                            sum1 += v1 as f64;
                            min1 = min1.min(v1);
                            max1 = max1.max(v1);
                            count1 += 1;

                            sum2 += v2 as f64;
                            min2 = min2.min(v2);
                            max2 = max2.max(v2);
                            count2 += 1;

                            sum3 += v3 as f64;
                            min3 = min3.min(v3);
                            max3 = max3.max(v3);
                            count3 += 1;
                        }
                        for &v in remainder {
                            sum0 += v as f64;
                            min0 = min0.min(v);
                            max0 = max0.max(v);
                            count0 += 1;
                        }
                    }
                    Some(nd) => {
                        for chunk in chunks {
                            let v0 = chunk[0];
                            let v1 = chunk[1];
                            let v2 = chunk[2];
                            let v3 = chunk[3];
                            let ok0 = v0 != nd;
                            let ok1 = v1 != nd;
                            let ok2 = v2 != nd;
                            let ok3 = v3 != nd;

                            if ok0 & ok1 & ok2 & ok3 {
                                sum0 += v0 as f64;
                                sum1 += v1 as f64;
                                sum2 += v2 as f64;
                                sum3 += v3 as f64;
                                min0 = min0.min(v0);
                                min1 = min1.min(v1);
                                min2 = min2.min(v2);
                                min3 = min3.min(v3);
                                max0 = max0.max(v0);
                                max1 = max1.max(v1);
                                max2 = max2.max(v2);
                                max3 = max3.max(v3);
                                count0 += 1;
                                count1 += 1;
                                count2 += 1;
                                count3 += 1;
                            } else if ok0 | ok1 | ok2 | ok3 {
                                sum0 += if ok0 { v0 as f64 } else { 0.0 };
                                sum1 += if ok1 { v1 as f64 } else { 0.0 };
                                sum2 += if ok2 { v2 as f64 } else { 0.0 };
                                sum3 += if ok3 { v3 as f64 } else { 0.0 };
                                min0 = if ok0 { min0.min(v0) } else { min0 };
                                min1 = if ok1 { min1.min(v1) } else { min1 };
                                min2 = if ok2 { min2.min(v2) } else { min2 };
                                min3 = if ok3 { min3.min(v3) } else { min3 };
                                max0 = if ok0 { max0.max(v0) } else { max0 };
                                max1 = if ok1 { max1.max(v1) } else { max1 };
                                max2 = if ok2 { max2.max(v2) } else { max2 };
                                max3 = if ok3 { max3.max(v3) } else { max3 };
                                count0 += usize::from(ok0);
                                count1 += usize::from(ok1);
                                count2 += usize::from(ok2);
                                count3 += usize::from(ok3);
                            }
                        }
                        for &v in remainder {
                            if v != nd {
                                sum0 += v as f64;
                                min0 = min0.min(v);
                                max0 = max0.max(v);
                                count0 += 1;
                            }
                        }
                    }
                }

                let total_count = ((count0 + count1) + (count2 + count3)) as f64;
                if total_count == 0.0 {
                    return ContinuousAccumulator::default();
                }

                let total_sum = (sum0 + sum1) + (sum2 + sum3);
                let total_min = (min0.min(min1)).min(min2.min(min3)) as f64;
                let total_max = (max0.max(max1)).max(max2.max(max3)) as f64;

                if total_min == total_max {
                    return ContinuousAccumulator::from_stats(
                        total_sum,
                        total_count,
                        total_min,
                        total_max,
                        0.0,
                    );
                }

                let mean = total_sum / total_count;
                let mut m2_0 = 0.0f64;
                let mut m2_1 = 0.0f64;
                let mut m2_2 = 0.0f64;
                let mut m2_3 = 0.0f64;

                let (chunks2, remainder2) = slice.as_chunks::<4>();

                if nodata.is_none() || (total_count as usize) == slice.len() {
                    for chunk in chunks2 {
                        let d0 = (chunk[0] as f64) - mean;
                        let d1 = (chunk[1] as f64) - mean;
                        let d2 = (chunk[2] as f64) - mean;
                        let d3 = (chunk[3] as f64) - mean;
                        m2_0 += d0 * d0;
                        m2_1 += d1 * d1;
                        m2_2 += d2 * d2;
                        m2_3 += d3 * d3;
                    }
                    for &v in remainder2 {
                        let d = (v as f64) - mean;
                        m2_0 += d * d;
                    }
                } else if let Some(nd) = nodata {
                    for chunk in chunks2 {
                        let v0 = chunk[0];
                        let v1 = chunk[1];
                        let v2 = chunk[2];
                        let v3 = chunk[3];
                        let ok0 = v0 != nd;
                        let ok1 = v1 != nd;
                        let ok2 = v2 != nd;
                        let ok3 = v3 != nd;

                        if ok0 & ok1 & ok2 & ok3 {
                            let d0 = (v0 as f64) - mean;
                            let d1 = (v1 as f64) - mean;
                            let d2 = (v2 as f64) - mean;
                            let d3 = (v3 as f64) - mean;
                            m2_0 += d0 * d0;
                            m2_1 += d1 * d1;
                            m2_2 += d2 * d2;
                            m2_3 += d3 * d3;
                        } else if ok0 | ok1 | ok2 | ok3 {
                            let d0 = if ok0 { (v0 as f64) - mean } else { 0.0 };
                            let d1 = if ok1 { (v1 as f64) - mean } else { 0.0 };
                            let d2 = if ok2 { (v2 as f64) - mean } else { 0.0 };
                            let d3 = if ok3 { (v3 as f64) - mean } else { 0.0 };
                            m2_0 += d0 * d0;
                            m2_1 += d1 * d1;
                            m2_2 += d2 * d2;
                            m2_3 += d3 * d3;
                        }
                    }
                    for &v in remainder2 {
                        if v != nd {
                            let d = (v as f64) - mean;
                            m2_0 += d * d;
                        }
                    }
                }

                ContinuousAccumulator::from_stats(
                    total_sum,
                    total_count,
                    total_min,
                    total_max,
                    (m2_0 + m2_1) + (m2_2 + m2_3),
                )
            }
        }
    };
}

impl_simd_span_integer!(u8);
impl_simd_span_integer!(u16);
impl_simd_span_integer!(u32);
impl_simd_span_integer!(u64);
impl_simd_span_integer!(i8);
impl_simd_span_integer!(i16);
impl_simd_span_integer!(i32);
impl_simd_span_integer!(i64);

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_matches_scalar<T: SimdSpanAccumulate + std::fmt::Debug>(
        slice: &[T],
        nodata: Option<T>,
    ) {
        let mut expected = ContinuousAccumulator::default();
        for &v in slice {
            if v.is_valid(nodata) {
                expected.update(v.to_f64_val());
            }
        }
        let actual = T::accumulate_span(slice, nodata);
        assert_eq!(actual.count, expected.count);
        assert!((actual.sum - expected.sum).abs() <= 1e-10 * expected.sum.abs().max(1.0));
        if expected.count > 0.0 {
            assert_eq!(actual.min, expected.min);
            assert_eq!(actual.max, expected.max);
            assert!((actual.m2 - expected.m2).abs() <= 1e-9 * expected.m2.abs().max(1.0));
        }
    }

    #[test]
    fn nonfinite_nodata_matches_scalar_validity() {
        // Exercise vector lanes, the scalar tail, and the variance pass.
        let values = [
            1.0f32,
            2.0,
            f32::NAN,
            4.0,
            f32::INFINITY,
            6.0,
            f32::NEG_INFINITY,
            8.0,
            9.0,
            f32::NAN,
            11.0,
        ];
        for marker in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -9999.0, 4.0] {
            for len in 0..=values.len() {
                assert_matches_scalar(&values[..len], Some(marker));
            }
        }
    }

    #[test]
    fn branchless_simd_spans_match_scalar_across_types_and_patterns() {
        let f32_all_valid: Vec<f32> = (0..33).map(|i| (i as f32) * 1.5 - 7.0).collect();
        let f32_mixed: Vec<f32> = (0..33)
            .map(|i| match i % 11 {
                0 => -9999.0,
                3 => -9999.0 + 0.5e-6,
                5 => f32::NAN,
                9 => f32::INFINITY,
                _ => (i as f32) * 0.25 + 1.0,
            })
            .collect();
        let f32_all_nd = [-9999.0f32; 33];
        for len in 0..=33 {
            assert_matches_scalar(&f32_all_valid[..len], None);
            assert_matches_scalar(&f32_all_valid[..len], Some(-9999.0));
            assert_matches_scalar(&f32_mixed[..len], Some(-9999.0));
            assert_matches_scalar(&f32_all_nd[..len], Some(-9999.0));
        }

        let f64_mixed: Vec<f64> = (0..33)
            .map(|i| match i % 7 {
                0 => -9999.0,
                2 => f64::NAN,
                4 => f64::NEG_INFINITY,
                _ => (i as f64) * 2.25 - 10.0,
            })
            .collect();
        let f64_valid: Vec<f64> = (0..33).map(|i| i as f64 * 0.75 + 3.0).collect();
        for len in 0..=33 {
            assert_matches_scalar(&f64_valid[..len], None);
            assert_matches_scalar(&f64_valid[..len], Some(-9999.0));
            assert_matches_scalar(&f64_mixed[..len], Some(-9999.0));
            assert_matches_scalar(&f64_mixed[..len], Some(f64::NAN));
        }

        let i16_vals: Vec<i16> = (0..33)
            .map(|i| {
                if i % 6 == 0 {
                    -32768
                } else {
                    (i as i16) * 17 - 100
                }
            })
            .collect();
        let u16_vals: Vec<u16> = (0..33)
            .map(|i| if i % 5 == 0 { 65535 } else { (i as u16) * 3 })
            .collect();
        let i32_vals: Vec<i32> = (0..33)
            .map(|i| if i % 4 == 0 { -9999 } else { i * 100 - 500 })
            .collect();
        for len in 0..=33 {
            assert_matches_scalar(&i16_vals[..len], Some(-32768));
            assert_matches_scalar(&i16_vals[..len], None);
            assert_matches_scalar(&u16_vals[..len], Some(65535));
            assert_matches_scalar(&i32_vals[..len], Some(-9999));
        }
    }
}
