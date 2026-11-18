use crate::aggregator::multi_horizon::spill::table_bytes;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Distinct categories held inline, without heap allocation.
///
/// Kept small so that horizon and worker maps, which charge every capacity
/// slot at the full accumulator size, hold several times more cells per byte.
/// Cells with more classes move to a compact overflow list.
pub const INLINE_CAPACITY: usize = 4;

/// Distinct categories held in the overflow list before it becomes a hash map.
/// The list keeps insertion order and is scanned linearly, which beats hashing
/// at land-cover cardinalities and stores 16 bytes per class.
pub const LIST_CAPACITY: usize = 64;

/// Upper bound on [`CategoricalAccumulator::heap_bytes`] per distinct class,
/// for an accumulator grown only by `update`, `update_weighted` and `merge`.
///
/// Each sample adds at most one class to one cell, so this also bounds the heap
/// bytes a sample can add, which worker window sizing relies on. The worst case
/// is the hash map tier just after it grows (about 69 bytes per class under
/// the `table_bytes` charge); the list tier stays under 34.
pub const OVERFLOW_BYTES_PER_CLASS: usize = 72;

/// Heap storage of a cell with more than [`INLINE_CAPACITY`] classes.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Overflow {
    /// Up to [`LIST_CAPACITY`] classes in insertion order.
    List(Vec<(i64, f64)>),
    /// More than [`LIST_CAPACITY`] classes.
    Map(HashMap<i64, f64, FxBuildHasher>),
}

/// Accumulator of categorical class frequencies for one cell.
///
/// Up to [`INLINE_CAPACITY`] classes are stored inline; more move, in insertion
/// order, to a boxed list and above [`LIST_CAPACITY`] to a boxed hash map.
/// Classes are visited in insertion order for cells of up to [`LIST_CAPACITY`]
/// classes, so sums over classes do not depend on the storage tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoricalAccumulator {
    inline_entries: [(i64, f64); INLINE_CAPACITY],
    inline_len: u8,
    overflow: Option<Box<Overflow>>,
    pub total_count: f64,
}

impl Default for CategoricalAccumulator {
    #[inline(always)]
    fn default() -> Self {
        Self {
            inline_entries: [(0, 0.0); INLINE_CAPACITY],
            inline_len: 0,
            overflow: None,
            total_count: 0.0,
        }
    }
}

/// Equal totals and equal per-class counts, whatever the storage tier or order.
impl PartialEq for CategoricalAccumulator {
    fn eq(&self, other: &Self) -> bool {
        if self.total_count != other.total_count || self.unique_classes() != other.unique_classes()
        {
            return false;
        }
        let mut equal = true;
        self.for_each_class(|cat, cnt| {
            equal &= other.get_class_count(cat) == cnt;
        });
        equal
    }
}

impl CategoricalAccumulator {
    /// Initialize a new empty categorical accumulator
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether every class is stored inline, with no heap allocation.
    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        self.overflow.is_none()
    }

    /// Bytes of heap storage owned by this accumulator, as the aggregation budget
    /// charges them (by capacity, not by the classes in use).
    pub fn heap_bytes(&self) -> usize {
        self.overflow.as_deref().map_or(0, |overflow| {
            size_of::<Overflow>()
                + match overflow {
                    Overflow::List(list) => list.capacity() * size_of::<(i64, f64)>(),
                    Overflow::Map(map) => table_bytes::<i64, f64>(map.capacity()),
                }
        })
    }

    /// Iterate through all category-count pairs
    pub fn for_each_class<F: FnMut(i64, f64)>(&self, mut f: F) {
        match self.overflow.as_deref() {
            None => {
                let len = self.inline_len as usize;
                for &(cat, cnt) in &self.inline_entries[..len] {
                    f(cat, cnt);
                }
            }
            Some(Overflow::List(list)) => {
                for &(cat, cnt) in list {
                    f(cat, cnt);
                }
            }
            Some(Overflow::Map(map)) => {
                for (&cat, &cnt) in map {
                    f(cat, cnt);
                }
            }
        }
    }

    /// Get count for a specific category
    pub fn get_class_count(&self, category: i64) -> f64 {
        let find = |entries: &[(i64, f64)]| {
            entries
                .iter()
                .find(|&&(cat, _)| cat == category)
                .map_or(0.0, |&(_, cnt)| cnt)
        };
        match self.overflow.as_deref() {
            None => find(&self.inline_entries[..self.inline_len as usize]),
            Some(Overflow::List(list)) => find(list),
            Some(Overflow::Map(map)) => map.get(&category).copied().unwrap_or(0.0),
        }
    }

    /// Update with a single unweighted category
    #[inline(always)]
    pub fn update(&mut self, category: i64) {
        self.update_weighted(category, 1.0);
    }

    /// Update with a weighted category (e.g. from sub-pixel super-sampling)
    #[inline(always)]
    pub fn update_weighted(&mut self, category: i64, weight: f64) {
        // NaN slips past `weight <= 0.0`, and an infinite weight can never be
        // outvoted: require a positive finite weight, as `H3Accumulator` does.
        if !(weight.is_finite() && weight > 0.0) {
            return;
        }
        self.total_count += weight;

        if self.overflow.is_none() {
            let len = self.inline_len as usize;
            if let Some(entry) = self.inline_entries[..len]
                .iter_mut()
                .find(|(cat, _)| *cat == category)
            {
                entry.1 += weight;
            } else if len < INLINE_CAPACITY {
                self.inline_entries[len] = (category, weight);
                self.inline_len += 1;
            } else {
                self.spill_inline(category, weight);
            }
            return;
        }
        self.update_overflow(category, weight);
    }

    /// Move the full inline array, plus one new class, to an overflow list.
    #[cold]
    fn spill_inline(&mut self, category: i64, weight: f64) {
        let mut list = Vec::with_capacity(2 * INLINE_CAPACITY);
        list.extend_from_slice(&self.inline_entries);
        list.push((category, weight));
        self.inline_entries = [(0, 0.0); INLINE_CAPACITY];
        self.inline_len = 0;
        self.overflow = Some(Box::new(Overflow::List(list)));
    }

    fn update_overflow(&mut self, category: i64, weight: f64) {
        let Some(overflow) = self.overflow.as_deref_mut() else {
            return;
        };
        let promoted = match overflow {
            Overflow::Map(map) => {
                *map.entry(category).or_insert(0.0) += weight;
                return;
            }
            Overflow::List(list) => {
                if let Some(entry) = list.iter_mut().find(|(cat, _)| *cat == category) {
                    entry.1 += weight;
                    return;
                }
                if list.len() < LIST_CAPACITY {
                    list.push((category, weight));
                    return;
                }
                // Sized to the classes held, so the table stays within
                // `OVERFLOW_BYTES_PER_CLASS`.
                let mut map = HashMap::with_capacity_and_hasher(list.len() + 1, FxBuildHasher);
                map.extend(list.iter().copied());
                map.insert(category, weight);
                map
            }
        };
        *overflow = Overflow::Map(promoted);
    }

    /// Merge another categorical accumulator
    pub fn merge(&mut self, other: &Self) {
        if other.total_count == 0.0 {
            return;
        }
        other.for_each_class(|cat, cnt| {
            self.update_weighted(cat, cnt);
        });
    }

    /// Return majority (mode) category, its count, and its fraction of total.
    ///
    /// Counts within a relative epsilon of each other are treated as tied, and ties resolve to
    /// the lowest category ID. Fractional sub-pixel weights summed in a different merge order
    /// (e.g. under different thread scheduling) can differ in the last few ulps, so an exact
    /// comparison would let class insertion order and rounding pick the winner.
    pub fn majority(&self) -> (i64, f64, f64) {
        if self.total_count == 0.0 {
            return (0, 0.0, 0.0);
        }
        let tie_eps = self.total_count.abs() * 1e-9;
        let mut max_cat = 0;
        let mut max_count = -1.0;
        let mut seen_any = false;
        self.for_each_class(|cat, cnt| {
            let is_tie = (cnt - max_count).abs() <= tie_eps;
            if !seen_any || (cnt > max_count && !is_tie) || (is_tie && cat < max_cat) {
                max_count = cnt;
                max_cat = cat;
                seen_any = true;
            }
        });
        let frac = if self.total_count > 0.0 && max_count > 0.0 {
            max_count / self.total_count
        } else {
            0.0
        };
        (max_cat, max_count, frac)
    }

    /// Return number of unique categories present (richness)
    #[inline(always)]
    pub fn unique_classes(&self) -> usize {
        match self.overflow.as_deref() {
            None => self.inline_len as usize,
            Some(Overflow::List(list)) => list.len(),
            Some(Overflow::Map(map)) => map.len(),
        }
    }

    /// Serialize histogram directly into a reusable string buffer without intermediate allocations
    pub fn histogram_json_into(&self, s: &mut String) {
        s.clear();
        let count = self.unique_classes();
        if count == 0 {
            s.push_str("{}");
            return;
        }

        let mut stack_entries = [(0i64, 0.0f64); 16];
        let use_stack = count <= 16;
        let mut heap_entries;

        let entries_slice: &mut [(i64, f64)] = if use_stack {
            let mut idx = 0;
            self.for_each_class(|cat, cnt| {
                if idx < 16 {
                    stack_entries[idx] = (cat, cnt);
                    idx += 1;
                }
            });
            &mut stack_entries[..count]
        } else {
            heap_entries = Vec::with_capacity(count);
            self.for_each_class(|cat, cnt| {
                heap_entries.push((cat, cnt));
            });
            heap_entries.as_mut_slice()
        };

        entries_slice.sort_unstable_by_key(|&(k, _)| k);

        let required_cap = 32 + count * 20;
        if s.capacity() < required_cap {
            s.reserve(required_cap - s.capacity());
        }

        s.push('{');
        for (i, &(k, cnt)) in entries_slice.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            let frac = if self.total_count > 0.0 {
                (cnt / self.total_count).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let frac_i = (frac * 10000.0 + 0.5) as u32;
            let int_part = frac_i / 10000;
            let dec_part = frac_i % 10000;
            use std::fmt::Write;
            let _ = write!(s, "\"{}\": {}.{:04}", k, int_part, dec_part);
        }
        s.push('}');
    }

    /// Serialize histogram to a JSON string representation
    pub fn histogram_json(&self) -> String {
        let count = self.unique_classes();
        let mut s = String::with_capacity(32 + count * 20);
        self.histogram_json_into(&mut s);
        s
    }

    /// Return Shannon entropy of the class distribution (measure of diversity / ambiguity)
    pub fn shannon_entropy(&self) -> f64 {
        if self.total_count <= 0.0 || self.unique_classes() <= 1 {
            return 0.0;
        }
        let mut entropy = 0.0f64;
        self.for_each_class(|_cat, cnt| {
            if cnt > 0.0 {
                let p = cnt / self.total_count;
                entropy -= p * p.ln();
            }
        });
        entropy
    }
}

use crate::aggregator::nodata::NativeNoData;

/// Trait for numeric raster pixel types that support high-throughput SIMD / branchless span uniformity detection.
pub trait CategoricalUniformity: NativeNoData {
    /// Return true if all values in the slice are identical to `slice[0]`, or if slice is empty.
    fn is_uniform(slice: &[Self]) -> bool;

    /// Convert native pixel value to an i64 category ID if in valid range
    fn to_category(self) -> Option<i64>;

    /// The pixel value as a number, for binning it into a histogram.
    fn to_bin_value(self) -> f64;
}

impl CategoricalUniformity for u8 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self as i64)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        if slice.len() <= 1 {
            return true;
        }
        let first = slice[0];
        let rest = &slice[1..];
        let (chunks, rem) = rest.as_chunks::<32>();
        for chunk in chunks {
            let mut diff = 0u8;
            for &v in chunk {
                diff |= v ^ first;
            }
            if diff != 0 {
                return false;
            }
        }
        for &v in rem {
            if v != first {
                return false;
            }
        }
        true
    }
}

impl CategoricalUniformity for i8 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self as i64)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        // SAFETY: `i8` and `u8` have identical size and alignment and every
        // bit pattern is valid for both, so the same memory reinterpreted as
        // `[u8; len]` is a valid shared borrow for the lifetime of `slice`.
        let u8_slice: &[u8] =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
        u8::is_uniform(u8_slice)
    }
}

impl CategoricalUniformity for u16 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self as i64)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        if slice.len() <= 1 {
            return true;
        }
        let first = slice[0];
        let rest = &slice[1..];
        let (chunks, rem) = rest.as_chunks::<16>();
        for chunk in chunks {
            let mut diff = 0u16;
            for &v in chunk {
                diff |= v ^ first;
            }
            if diff != 0 {
                return false;
            }
        }
        for &v in rem {
            if v != first {
                return false;
            }
        }
        true
    }
}

impl CategoricalUniformity for i16 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self as i64)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        // SAFETY: `i16` and `u16` have identical size and alignment and every
        // bit pattern is valid for both, so the same memory reinterpreted as
        // `[u16; len]` is a valid shared borrow for the lifetime of `slice`.
        let u16_slice: &[u16] =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u16, slice.len()) };
        u16::is_uniform(u16_slice)
    }
}

impl CategoricalUniformity for u32 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self as i64)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        if slice.len() <= 1 {
            return true;
        }
        let first = slice[0];
        let rest = &slice[1..];
        let (chunks, rem) = rest.as_chunks::<8>();
        for chunk in chunks {
            let mut diff = 0u32;
            for &v in chunk {
                diff |= v ^ first;
            }
            if diff != 0 {
                return false;
            }
        }
        for &v in rem {
            if v != first {
                return false;
            }
        }
        true
    }
}

impl CategoricalUniformity for i32 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self as i64)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        // SAFETY: `i32` and `u32` have identical size and alignment and every
        // bit pattern is valid for both, so the same memory reinterpreted as
        // `[u32; len]` is a valid shared borrow for the lifetime of `slice`.
        let u32_slice: &[u32] =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u32, slice.len()) };
        u32::is_uniform(u32_slice)
    }
}

impl CategoricalUniformity for u64 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        if self <= i64::MAX as u64 {
            Some(self as i64)
        } else {
            None
        }
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        if slice.len() <= 1 {
            return true;
        }
        let first = slice[0];
        let rest = &slice[1..];
        let (chunks, rem) = rest.as_chunks::<8>();
        for chunk in chunks {
            let mut diff = 0u64;
            for &v in chunk {
                diff |= v ^ first;
            }
            if diff != 0 {
                return false;
            }
        }
        for &v in rem {
            if v != first {
                return false;
            }
        }
        true
    }
}

impl CategoricalUniformity for i64 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        Some(self)
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        // SAFETY: `i64` and `u64` have identical size and alignment and every
        // bit pattern is valid for both, so the same memory reinterpreted as
        // `[u64; len]` is a valid shared borrow for the lifetime of `slice`.
        let u64_slice: &[u64] =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u64, slice.len()) };
        u64::is_uniform(u64_slice)
    }
}

impl CategoricalUniformity for f32 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self as f64
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        if self.is_finite() {
            Some(self.round() as i64)
        } else {
            None
        }
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        if slice.len() <= 1 {
            return true;
        }
        let first = slice[0];
        let first_bits = first.to_bits();
        let rest = &slice[1..];
        let (chunks, rem) = rest.as_chunks::<8>();
        for chunk in chunks {
            let mut diff = 0u32;
            for &v in chunk {
                diff |= v.to_bits() ^ first_bits;
            }
            if diff != 0 {
                for &v in chunk {
                    if v != first {
                        return false;
                    }
                }
            }
        }
        for &v in rem {
            if v != first {
                return false;
            }
        }
        true
    }
}

impl CategoricalUniformity for f64 {
    #[inline(always)]
    fn to_bin_value(self) -> f64 {
        self
    }

    #[inline(always)]
    fn to_category(self) -> Option<i64> {
        if self.is_finite() {
            Some(self.round() as i64)
        } else {
            None
        }
    }

    #[inline(always)]
    fn is_uniform(slice: &[Self]) -> bool {
        if slice.len() <= 1 {
            return true;
        }
        let first = slice[0];
        let first_bits = first.to_bits();
        let rest = &slice[1..];
        let (chunks, rem) = rest.as_chunks::<8>();
        for chunk in chunks {
            let mut diff = 0u64;
            for &v in chunk {
                diff |= v.to_bits() ^ first_bits;
            }
            if diff != 0 {
                for &v in chunk {
                    if v != first {
                        return false;
                    }
                }
            }
        }
        for &v in rem {
            if v != first {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_weighted_rejects_non_finite_and_non_positive_weights() {
        let bad = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0];

        for w in bad {
            let mut acc = CategoricalAccumulator::new();
            acc.update_weighted(7, w);
            assert_eq!(acc.total_count, 0.0, "weight {w} must be rejected");
            assert_eq!(acc.unique_classes(), 0, "weight {w} must not add a class");
        }

        // Inline, list and map storage: bad weights leave earlier samples intact,
        // whether they name an existing class or a new one.
        for classes in [3i64, INLINE_CAPACITY as i64 + 4, LIST_CAPACITY as i64 + 4] {
            let mut acc = CategoricalAccumulator::new();
            for c in 0..classes {
                acc.update_weighted(c, 2.0);
            }
            acc.update_weighted(0, 1.0);
            let expected = acc.clone();
            for w in bad {
                acc.update_weighted(0, w);
                acc.update_weighted(1_000, w);
            }
            assert_eq!(acc, expected);
            assert_eq!(acc.total_count, 2.0 * classes as f64 + 1.0);
            assert_eq!(acc.majority().0, 0);
            assert!(acc.shannon_entropy().is_finite());
        }

        // A merge does not carry a poisoned count across either.
        let mut poisoned = CategoricalAccumulator::new();
        poisoned.inline_entries[0] = (5, f64::NAN);
        poisoned.inline_entries[1] = (6, 3.0);
        poisoned.inline_len = 2;
        poisoned.total_count = f64::NAN;
        let mut acc = CategoricalAccumulator::new();
        acc.update_weighted(6, 1.0);
        acc.merge(&poisoned);
        assert_eq!(acc.total_count, 4.0);
        assert_eq!(acc.get_class_count(6), 4.0);
        assert_eq!(acc.get_class_count(5), 0.0);
    }

    #[test]
    fn test_categorical_inline_to_heap_spillover_boundary() {
        let mut acc = CategoricalAccumulator::new();
        assert_eq!(acc.unique_classes(), 0);
        assert!(acc.is_inline());

        // Fill exactly INLINE_CAPACITY classes: no heap.
        for c in 0..INLINE_CAPACITY as i64 {
            acc.update_weighted(c, 1.0);
            assert_eq!(acc.unique_classes(), (c + 1) as usize);
            assert!(acc.is_inline(), "<= INLINE_CAPACITY classes stay inline");
            assert_eq!(acc.heap_bytes(), 0);
        }
        assert_eq!(acc.total_count, INLINE_CAPACITY as f64);

        // Updating an existing class does not allocate.
        acc.update_weighted(1, 4.0);
        assert!(
            acc.is_inline(),
            "updating an existing class must not allocate"
        );
        assert_eq!(acc.get_class_count(1), 5.0);

        // One more class moves to the overflow list, keeping every count.
        acc.update_weighted(100, 10.0);
        assert!(!acc.is_inline());
        assert!(acc.heap_bytes() > 0);
        assert_eq!(acc.unique_classes(), INLINE_CAPACITY + 1);
        for c in 0..INLINE_CAPACITY as i64 {
            assert_eq!(acc.get_class_count(c), if c == 1 { 5.0 } else { 1.0 });
        }
        assert_eq!(acc.get_class_count(100), 10.0);

        // Past LIST_CAPACITY the list becomes a map, still keeping every count.
        for c in 200..200 + LIST_CAPACITY as i64 {
            acc.update(c);
        }
        assert_eq!(acc.unique_classes(), INLINE_CAPACITY + 1 + LIST_CAPACITY);
        assert!(matches!(acc.overflow.as_deref(), Some(Overflow::Map(_))));
        assert_eq!(acc.get_class_count(1), 5.0);
        assert_eq!(acc.get_class_count(100), 10.0);
        assert_eq!(acc.get_class_count(200 + LIST_CAPACITY as i64 - 1), 1.0);

        // Merging an inline accumulator into the map.
        let mut acc2 = CategoricalAccumulator::new();
        for c in 0..INLINE_CAPACITY as i64 {
            acc2.update_weighted(c, 2.0);
        }
        acc.merge(&acc2);
        assert_eq!(acc.get_class_count(1), 7.0);
        assert_eq!(acc.get_class_count(0), 3.0);
    }

    #[test]
    fn accumulator_is_compact() {
        assert!(
            size_of::<CategoricalAccumulator>() <= 88,
            "{} bytes",
            size_of::<CategoricalAccumulator>()
        );
    }

    #[test]
    fn classes_are_visited_in_insertion_order_up_to_list_capacity() {
        let mut acc = CategoricalAccumulator::new();
        let order: Vec<i64> = (0..LIST_CAPACITY as i64)
            .map(|i| (i * 37) % 101 - 50)
            .collect();
        for (i, &c) in order.iter().enumerate() {
            acc.update_weighted(c, 1.0 + i as f64);
            let mut seen = Vec::new();
            acc.for_each_class(|cat, _| seen.push(cat));
            assert_eq!(seen, order[..=i], "after {} classes", i + 1);
        }
    }

    #[test]
    fn heap_bytes_per_class_stay_within_the_worker_allowance() {
        // Every update adds a new class: the worst case for heap per sample.
        let mut acc = CategoricalAccumulator::new();
        let mut worst = 0.0f64;
        for k in 1..=5000usize {
            acc.update(k as i64 * 7919);
            worst = worst.max(acc.heap_bytes() as f64 / k as f64);
            assert!(
                acc.heap_bytes() <= OVERFLOW_BYTES_PER_CLASS * k,
                "{} heap bytes for {k} classes",
                acc.heap_bytes()
            );
        }
        // Merges grow storage the same way.
        for parts in [2usize, 3, 50] {
            let mut merged = CategoricalAccumulator::new();
            let mut k = 0;
            for p in 0..parts {
                let mut part = CategoricalAccumulator::new();
                for c in 0..(1000 / parts) as i64 {
                    part.update_weighted(c * parts as i64 + p as i64, 0.5);
                }
                merged.merge(&part);
                k += part.unique_classes();
                assert!(merged.heap_bytes() <= OVERFLOW_BYTES_PER_CLASS * k);
            }
        }
        assert!(worst > 0.0);
    }

    #[test]
    fn equality_ignores_storage_order() {
        let mut a = CategoricalAccumulator::new();
        let mut b = CategoricalAccumulator::new();
        for c in 0..10 {
            a.update(c);
            b.update(9 - c);
        }
        assert_eq!(a, b);
        b.update(3);
        assert_ne!(a, b);
    }

    #[test]
    fn test_categorical_shannon_entropy_theoretical_bounds() {
        // 1. Empty accumulator: entropy must be 0.0
        let empty = CategoricalAccumulator::new();
        assert_eq!(empty.shannon_entropy(), 0.0);

        // 2. Single class: entropy must be 0.0 regardless of sample count
        let mut single = CategoricalAccumulator::new();
        single.update_weighted(42, 50000.0);
        assert_eq!(single.shannon_entropy(), 0.0);

        // 3. K equiprobable classes: entropy must theoretically equal ln(K)
        for &k in &[2, 4, 8, 16, 32, 64] {
            let mut acc = CategoricalAccumulator::new();
            for i in 0..k {
                acc.update_weighted(i, 10.0); // uniform weight
            }
            let entropy = acc.shannon_entropy();
            let theoretical = (k as f64).ln();
            assert!(
                (entropy - theoretical).abs() < 1e-12,
                "Entropy for K={} must be ln({}): {} vs {}",
                k,
                k,
                entropy,
                theoretical
            );
        }

        // 4. Heavily skewed distribution: entropy must be strictly > 0 and < ln(2)
        let mut skewed = CategoricalAccumulator::new();
        skewed.update_weighted(1, 1_000_000.0);
        skewed.update_weighted(2, 1.0);
        let s_entropy = skewed.shannon_entropy();
        assert!(s_entropy > 0.0);
        assert!(s_entropy < 2.0f64.ln());
        assert!(
            s_entropy < 1e-4,
            "Skewed distribution must have near-zero entropy: got {}",
            s_entropy
        );
    }

    #[test]
    fn test_categorical_tied_majority_determinism() {
        let mut acc = CategoricalAccumulator::new();
        // Empty accumulator majority
        assert_eq!(acc.majority(), (0, 0.0, 0.0));

        // Two perfectly tied classes: 10 and 20 each with 50.0
        acc.update_weighted(10, 50.0);
        acc.update_weighted(20, 50.0);
        let (maj_cat, maj_cnt, maj_frac) = acc.majority();
        assert_eq!(maj_cnt, 50.0);
        assert!((maj_frac - 0.5).abs() < 1e-9);
        assert_eq!(maj_cat, 10, "Ties resolve to the lowest category ID");

        // Insertion order and last-ulp rounding differences must not change the winner
        let mut reordered = CategoricalAccumulator::new();
        reordered.update_weighted(20, 0.1 + 0.2);
        reordered.update_weighted(10, 0.3);
        assert_eq!(reordered.majority().0, 10);

        // Adding 0.001 to class 20 cleanly breaks tie
        acc.update_weighted(20, 0.001);
        let (maj_cat2, maj_cnt2, _) = acc.majority();
        assert_eq!(maj_cat2, 20);
        assert_eq!(maj_cnt2, 50.001);
    }

    #[test]
    fn test_categorical_histogram_json_formatting() {
        let mut acc = CategoricalAccumulator::new();
        acc.update_weighted(3, 25.0);
        acc.update_weighted(1, 75.0);

        let json = acc.histogram_json();
        // Keys must be ordered numerically: "1" before "3"
        assert_eq!(json, r#"{"1": 0.7500, "3": 0.2500}"#);

        // Test with > 16 classes (stack buffer of histogram_json exceeded)
        for i in 4..25 {
            acc.update_weighted(i, 1.0);
        }
        let json_heap = acc.histogram_json();
        assert!(json_heap.starts_with(r#"{"1":"#));
        assert!(json_heap.ends_with('}'));
    }
}
