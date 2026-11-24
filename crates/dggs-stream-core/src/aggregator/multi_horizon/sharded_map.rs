use crate::aggregator::cell_hash::{mix_cell_id, CellIdBuildHasher};
use rayon::slice::ParallelSliceMut;
use std::collections::{BinaryHeap, HashMap};

use super::eviction_space::CellKeys;
use super::spill::table_bytes;
use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::categorical::CategoricalAccumulator;
use crate::aggregator::horizon_streamer::compute_cell_south_lat;
use crate::aggregator::quantiles::QuantileSketch;

/// Number of concurrent shards for parallel active map merging
pub const NUM_SHARDS: usize = 32;

/// Mix the full H3 index before selecting a power-of-two shard.
/// Unused H3 child digits fill the low bits with ones, so masking the low
/// bits of a simple product sends every cell at resolutions 0..=13 to one shard.
///
/// Shard maps hash with the same finalizer ([`CellIdBuildHasher`]), and hashbrown
/// takes bucket positions from the low bits and tags from the top 7. The shard
/// therefore comes from bits 32.., so keys within one shard still spread over
/// every bucket and tag.
#[inline(always)]
pub fn get_shard(cell_u64: u64) -> usize {
    ((mix_cell_id(cell_u64) >> 32) as usize) & (NUM_SHARDS - 1)
}

/// Trait defining an accumulator that can be merged into another
pub trait AccumulatorMerge: Clone + Send + Sync + 'static {
    fn merge(&mut self, other: &Self);
    /// Owned dynamic state used by the aggregation budget. Inline-only custom
    /// accumulators can use the default; heap-owning implementations must override.
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl AccumulatorMerge for ContinuousAccumulator {
    fn heap_bytes(&self) -> usize {
        self.quantiles.as_ref().map_or(0, |q| {
            size_of::<QuantileSketch>()
                + table_bytes::<i32, f64>(q.pos_bins.capacity())
                + table_bytes::<i32, f64>(q.neg_bins.capacity())
        })
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        ContinuousAccumulator::merge(self, other);
    }
}

impl AccumulatorMerge for CategoricalAccumulator {
    fn heap_bytes(&self) -> usize {
        CategoricalAccumulator::heap_bytes(self)
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        CategoricalAccumulator::merge(self, other);
    }
}

/// A pending eviction: the cell's key (see [`super::eviction_space`]) and the
/// latitude-histogram bucket it was counted in.
#[derive(Debug, Clone, Copy)]
struct EvictionEntry {
    key: f64,
    cell_u64: u64,
    lat_bucket: u32,
}

impl PartialEq for EvictionEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for EvictionEntry {}

impl Ord for EvictionEntry {
    /// Max-heap on the key: the earliest-completing cell is popped first.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.total_cmp(&other.key)
    }
}

impl PartialOrd for EvictionEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Buckets per degree of [`LatHistogram`].
const LAT_BUCKETS_PER_DEG: f64 = 100.0;
const LAT_BUCKETS: usize = 180 * LAT_BUCKETS_PER_DEG as usize + 1;

/// Counts of held cells by an upper bound of their southern latitude, rounded
/// up to 0.01°, so the largest such bound among held cells is cheap to read.
#[derive(Debug, Clone)]
struct LatHistogram {
    counts: Vec<u32>,
    held: usize,
    /// No bucket above this index is non-empty.
    top: usize,
}

impl LatHistogram {
    fn new() -> Self {
        Self {
            counts: vec![0; LAT_BUCKETS],
            held: 0,
            top: 0,
        }
    }

    fn bucket(lat: f64) -> u32 {
        let b = ((lat + 90.0) * LAT_BUCKETS_PER_DEG).ceil();
        if b.is_nan() {
            (LAT_BUCKETS - 1) as u32
        } else {
            b.clamp(0.0, (LAT_BUCKETS - 1) as f64) as u32
        }
    }

    fn insert(&mut self, bucket: u32) {
        let b = bucket as usize;
        self.counts[b] += 1;
        self.held += 1;
        self.top = self.top.max(b);
    }

    fn remove(&mut self, bucket: u32) {
        let b = bucket as usize;
        self.counts[b] -= 1;
        self.held -= 1;
        while self.top > 0 && self.counts[self.top] == 0 {
            self.top -= 1;
        }
    }

    /// At least the southern latitude of every held cell; `-inf` when empty.
    fn max_south_lat(&self) -> f64 {
        if self.held == 0 {
            f64::NEG_INFINITY
        } else {
            // Bucket b covers latitudes up to b / 100 - 90; round outwards.
            (self.top as f64 / LAT_BUCKETS_PER_DEG - 90.0).next_up()
        }
    }

    fn clear(&mut self) {
        self.counts.iter_mut().for_each(|c| *c = 0);
        self.held = 0;
        self.top = 0;
    }
}

/// A 32-way hash-partitioned active map and eviction heap for a single H3 resolution.
///
/// The controller joins bounded worker waves, merges owned states with budget
/// checks, and pops completed cells incrementally. Merging and eviction both
/// require exclusive access, so the phases cannot overlap in safe Rust.
pub struct ShardedResolutionMap<A: AccumulatorMerge> {
    shards: Vec<HashMap<u64, A, CellIdBuildHasher>>,
    eviction: Vec<BinaryHeap<EvictionEntry>>,
    /// Southern-latitude bounds of held cells, when eviction keys are not
    /// latitudes (see [`Self::set_lat_tracking`]).
    lat_histogram: Option<LatHistogram>,
    dynamic_bytes: usize,
    /// Table and heap allocation, maintained incrementally so budget checks
    /// stay O(1) per merged cell.
    structure_bytes: usize,
    track_eviction: bool,
}

impl<A: AccumulatorMerge> Default for ShardedResolutionMap<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: AccumulatorMerge> ShardedResolutionMap<A> {
    /// Create a new ShardedResolutionMap with `NUM_SHARDS` shards
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        let mut eviction = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(HashMap::with_capacity_and_hasher(0, CellIdBuildHasher));
            eviction.push(BinaryHeap::new());
        }
        Self {
            shards,
            eviction,
            dynamic_bytes: 0,
            structure_bytes: 0,
            track_eviction: true,
            lat_histogram: None,
        }
    }

    #[inline]
    fn shard_bytes(&self, s: usize) -> usize {
        table_bytes::<u64, A>(self.shards[s].capacity())
            + self.eviction[s].capacity() * size_of::<EvictionEntry>()
    }

    fn recount_structure(&mut self) {
        self.structure_bytes = (0..NUM_SHARDS).map(|s| self.shard_bytes(s)).sum();
    }

    /// Return total active in-flight cell count across all 32 shards
    pub fn active_cell_count(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// The active accumulator for `cell`, if any
    pub fn get(&self, cell: u64) -> Option<&A> {
        self.shards[get_shard(cell)].get(&cell)
    }

    /// Whether `cell` is active in this map.
    pub fn contains(&self, cell: u64) -> bool {
        self.shards[get_shard(cell)].contains_key(&cell)
    }

    /// Whether newly inserted cells get an eviction key (false once spilled).
    pub fn tracks_eviction(&self) -> bool {
        self.track_eviction
    }

    /// Count held cells by southern latitude, so [`Self::held_south_lat_bound`]
    /// works when eviction keys are not latitudes. Set before inserting.
    pub fn set_lat_tracking(&mut self, enabled: bool) {
        self.lat_histogram = enabled.then(LatHistogram::new);
    }

    /// At least the southern latitude of every cell still awaiting eviction,
    /// `-inf` when none is; `None` without [`Self::set_lat_tracking`].
    pub fn held_south_lat_bound(&self) -> Option<f64> {
        self.lat_histogram.as_ref().map(LatHistogram::max_south_lat)
    }

    /// Per shard: (active cells, pending eviction entries)
    pub fn shard_occupancy(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.shards
            .iter()
            .zip(&self.eviction)
            .map(|(map, heap)| (map.len(), heap.len()))
    }
}

impl<A: super::spill::SpillAccumulator> ShardedResolutionMap<A> {
    pub fn set_eviction_enabled(&mut self, enabled: bool) {
        self.track_eviction = enabled;
        if !enabled {
            for heap in &mut self.eviction {
                *heap = BinaryHeap::new();
            }
            if let Some(h) = &mut self.lat_histogram {
                h.clear();
            }
            self.recount_structure();
        }
    }

    /// Owned merge avoids cloning a large quantile or categorical state.
    /// Returns the merged accumulator's memory footprint.
    /// New cells are keyed by latitude ([`compute_cell_south_lat`]).
    pub fn merge_owned(&mut self, key: u64, acc: A) -> usize {
        self.merge_owned_with(key, acc, || {
            let south = compute_cell_south_lat(key);
            CellKeys {
                key: south,
                south_lat_upper: south,
            }
        })
    }

    /// [`Self::merge_owned`] taking the cell's eviction keys from `keys`,
    /// which must return the stream's [`super::eviction_space::EvictionSpace::cell_keys`]
    /// of `key`. It is called only when the cell is new and eviction is
    /// tracked, so callers can compute the keys ahead of time, off the
    /// merging thread.
    pub fn merge_owned_with(&mut self, key: u64, acc: A, keys: impl FnOnce() -> CellKeys) -> usize {
        let s = get_shard(key);
        let before = self.shard_bytes(s);
        let merged_bytes = match self.shards[s].entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                self.dynamic_bytes -= existing.heap_bytes();
                existing.merge(&acc);
                self.dynamic_bytes += existing.heap_bytes();
                existing.memory_bytes()
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let bytes = acc.memory_bytes();
                self.dynamic_bytes += acc.heap_bytes();
                entry.insert(acc);
                if self.track_eviction {
                    let keys = keys();
                    let lat_bucket = LatHistogram::bucket(keys.south_lat_upper);
                    if let Some(h) = &mut self.lat_histogram {
                        h.insert(lat_bucket);
                    }
                    self.eviction[s].push(EvictionEntry {
                        key: keys.key,
                        cell_u64: key,
                        lat_bucket,
                    });
                }
                bytes
            }
        };
        self.structure_bytes = self.structure_bytes - before + self.shard_bytes(s);
        merged_bytes
    }

    /// Merge without computing an eviction key. Returns the merged
    /// accumulator's memory footprint and whether the cell is new to the map
    /// while eviction is tracked; such a cell must get its keys from
    /// [`Self::push_eviction`] before the next [`Self::pop_completed`].
    pub fn merge_owned_deferred(&mut self, key: u64, acc: A) -> (usize, bool) {
        let s = get_shard(key);
        let before = self.shard_bytes(s);
        let (merged_bytes, needs_keys) = match self.shards[s].entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                self.dynamic_bytes -= existing.heap_bytes();
                existing.merge(&acc);
                self.dynamic_bytes += existing.heap_bytes();
                (existing.memory_bytes(), false)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let bytes = acc.memory_bytes();
                self.dynamic_bytes += acc.heap_bytes();
                entry.insert(acc);
                (bytes, self.track_eviction)
            }
        };
        self.structure_bytes = self.structure_bytes - before + self.shard_bytes(s);
        (merged_bytes, needs_keys)
    }

    /// Queue `key` for eviction with `keys`, for a cell inserted by
    /// [`Self::merge_owned_deferred`]. Ignored when eviction is no longer
    /// tracked or the cell has left the map (a spill since its insertion).
    pub fn push_eviction(&mut self, key: u64, keys: CellKeys) {
        let s = get_shard(key);
        if !self.track_eviction || !self.shards[s].contains_key(&key) {
            return;
        }
        let before = self.shard_bytes(s);
        let lat_bucket = LatHistogram::bucket(keys.south_lat_upper);
        if let Some(h) = &mut self.lat_histogram {
            h.insert(lat_bucket);
        }
        self.eviction[s].push(EvictionEntry {
            key: keys.key,
            cell_u64: key,
            lat_bucket,
        });
        self.structure_bytes = self.structure_bytes - before + self.shard_bytes(s);
    }

    pub fn estimated_bytes(&self) -> usize {
        self.dynamic_bytes + self.structure_bytes
    }

    /// Remove at most one completed cell, keeping output backpressure effective.
    pub fn pop_completed(&mut self, horizon: f64) -> Option<(u64, A)> {
        for s in 0..NUM_SHARDS {
            while self.eviction[s].peek().is_some_and(|e| e.key > horizon) {
                // Removal can leave a tombstone that lowers reported capacity.
                let before = self.shard_bytes(s);
                let entry = self.eviction[s].pop().unwrap();
                if let Some(h) = &mut self.lat_histogram {
                    h.remove(entry.lat_bucket);
                }
                let key = entry.cell_u64;
                let removed = self.shards[s].remove(&key);
                if removed.is_some() && self.shards[s].is_empty() {
                    self.shards[s] = HashMap::with_hasher(CellIdBuildHasher);
                    self.eviction[s] = BinaryHeap::new();
                }
                self.structure_bytes = self.structure_bytes - before + self.shard_bytes(s);
                if let Some(acc) = removed {
                    self.dynamic_bytes -= acc.heap_bytes();
                    return Some((key, acc));
                }
            }
        }
        None
    }

    /// A run is bounded by the active-map budget. Release buckets/heaps rather
    /// than retaining their high-water capacity during external merging.
    pub fn take_sorted(&mut self) -> Vec<(u64, A)> {
        let shards = self.take_shards_and_clear();
        let total_len: usize = shards.iter().map(HashMap::len).sum();
        let mut records = Vec::with_capacity(total_len);
        for map in shards {
            records.extend(map);
        }
        // Only engage global Rayon sort when large and not already running
        // inside a Rayon thread (avoids work-stealing lock inversion).
        if records.len() >= 16_384 && rayon::current_thread_index().is_none() {
            records.par_sort_unstable_by_key(|(key, _)| *key);
        } else {
            records.sort_unstable_by_key(|(key, _)| *key);
        }
        records
    }

    /// Extract the 32 independent shard hash maps for parallel wave merging,
    /// replacing them temporarily with empty maps. Must be paired with
    /// [`Self::restore_shards`].
    pub(crate) fn take_shards(&mut self) -> Vec<HashMap<u64, A, CellIdBuildHasher>> {
        let mut out = Vec::with_capacity(NUM_SHARDS);
        for map in &mut self.shards {
            out.push(std::mem::replace(
                map,
                HashMap::with_hasher(CellIdBuildHasher),
            ));
        }
        out
    }

    /// Restore the 32 shard maps after parallel wave merging, applying the
    /// net accumulator heap delta and recounting table structure bytes.
    pub(crate) fn restore_shards(
        &mut self,
        shards: Vec<HashMap<u64, A, CellIdBuildHasher>>,
        dynamic_delta: isize,
    ) {
        assert_eq!(shards.len(), NUM_SHARDS);
        self.shards = shards;
        self.dynamic_bytes = self
            .dynamic_bytes
            .checked_add_signed(dynamic_delta)
            .expect("dynamic_bytes underflow");
        self.recount_structure();
    }

    /// Take all 32 shards and clear eviction heaps, latitude histogram, and byte counters.
    pub(crate) fn take_shards_and_clear(&mut self) -> Vec<HashMap<u64, A, CellIdBuildHasher>> {
        let shards = self.take_shards();
        for heap in &mut self.eviction {
            *heap = BinaryHeap::new();
        }
        if let Some(h) = &mut self.lat_histogram {
            h.clear();
        }
        self.dynamic_bytes = 0;
        self.structure_bytes = 0;
        shards
    }
}

/// Result of merging one shard's window-ordered buckets in parallel.
pub(crate) struct ShardMergeOutput {
    pub(crate) new_cells: Vec<u64>,
    /// `window_offsets[w]..window_offsets[w + 1]` is the slice of `new_cells`
    /// created during window `w` of the wave (`len == buckets.len() + 1`).
    pub(crate) window_offsets: Vec<usize>,
    pub(crate) dynamic_delta: isize,
}

/// Merge `buckets` (ordered earliest-window to latest-window) into one disjoint
/// `shard`, enforcing `accumulator_limit` and recording created cell keys by window.
/// Always returns the partial [`ShardMergeOutput`] alongside the merge `Result` so
/// `dynamic_bytes` accounting stays exact even if a limit check trips mid-shard.
pub(crate) fn merge_into_shard<A: super::spill::SpillAccumulator>(
    shard: &mut HashMap<u64, A, CellIdBuildHasher>,
    buckets: Vec<Vec<(u64, A)>>,
    track_eviction: bool,
    accumulator_limit: usize,
) -> (ShardMergeOutput, crate::error::Result<()>) {
    let mut new_cells = Vec::new();
    let mut window_offsets = Vec::with_capacity(buckets.len() + 1);
    window_offsets.push(0);
    let mut dynamic_delta: isize = 0;

    for window_bucket in buckets {
        for (key, acc) in window_bucket {
            if acc.memory_bytes() > accumulator_limit {
                return (
                    ShardMergeOutput {
                        new_cells,
                        window_offsets,
                        dynamic_delta,
                    },
                    Err(crate::error::DggsStreamError::InvalidParameter(
                        "single accumulator exceeds aggregation budget; increase aggregation_budget_bytes"
                            .into(),
                    )),
                );
            }
            let merged_bytes = match shard.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    let before = existing.heap_bytes() as isize;
                    existing.merge(&acc);
                    dynamic_delta += existing.heap_bytes() as isize - before;
                    existing.memory_bytes()
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let bytes = acc.memory_bytes();
                    dynamic_delta += acc.heap_bytes() as isize;
                    entry.insert(acc);
                    if track_eviction {
                        new_cells.push(key);
                    }
                    bytes
                }
            };
            if merged_bytes > accumulator_limit {
                return (
                    ShardMergeOutput {
                        new_cells,
                        window_offsets,
                        dynamic_delta,
                    },
                    Err(crate::error::DggsStreamError::InvalidParameter(
                        "merged accumulator exceeds aggregation budget; increase aggregation_budget_bytes"
                            .into(),
                    )),
                );
            }
        }
        window_offsets.push(new_cells.len());
    }

    (
        ShardMergeOutput {
            new_cells,
            window_offsets,
            dynamic_delta,
        },
        Ok(()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use h3o::{LatLng, Resolution};

    fn recounted<A: AccumulatorMerge>(map: &ShardedResolutionMap<A>) -> usize {
        let dynamic: usize = map
            .shards
            .iter()
            .flat_map(|s| s.values())
            .map(|a| a.heap_bytes())
            .sum();
        dynamic + (0..NUM_SHARDS).map(|s| map.shard_bytes(s)).sum::<usize>()
    }

    #[test]
    fn incremental_estimate_matches_full_recount() {
        let mut map = ShardedResolutionMap::<ContinuousAccumulator>::new();
        let res = Resolution::try_from(7).unwrap();
        let cells: Vec<u64> = (0..2000)
            .map(|i| {
                let ll = LatLng::new(60.0 - i as f64 * 0.05, -120.0 + (i % 40) as f64 * 0.1);
                u64::from(ll.unwrap().to_cell(res))
            })
            .collect();
        for (i, &cell) in cells.iter().enumerate() {
            let mut acc = if i % 3 == 0 {
                ContinuousAccumulator::with_quantiles()
            } else {
                ContinuousAccumulator::default()
            };
            acc.update(i as f64);
            map.merge_owned(cell, acc);
            assert_eq!(map.estimated_bytes(), recounted(&map));
        }
        // Pop until shards empty out and are replaced.
        while map.pop_completed(f64::NEG_INFINITY).is_some() {
            assert_eq!(map.estimated_bytes(), recounted(&map));
        }
        assert_eq!(map.estimated_bytes(), 0);
        for &cell in &cells[..500] {
            map.merge_owned(cell, ContinuousAccumulator::new(1.0));
        }
        map.set_eviction_enabled(false);
        assert_eq!(map.estimated_bytes(), recounted(&map));
        map.take_sorted();
        assert_eq!(map.estimated_bytes(), 0);
    }

    #[test]
    fn shard_wave_merge_matches_serial_and_tracks_bytes() {
        let res = Resolution::try_from(8).unwrap();
        let mut windows_cont = Vec::new();
        let mut windows_cat = Vec::new();
        for w in 0..6usize {
            let mut m_cont = HashMap::with_hasher(CellIdBuildHasher);
            let mut m_cat = HashMap::with_hasher(CellIdBuildHasher);
            for i in 0..400usize {
                let idx = (w * 150 + i) % 900;
                let ll = LatLng::new(
                    45.0 - (idx / 30) as f64 * 0.02,
                    -100.0 + (idx % 30) as f64 * 0.02,
                )
                .unwrap();
                let cell = u64::from(ll.to_cell(res));
                let mut acc = ContinuousAccumulator::with_quantiles();
                acc.update((w * 1000 + i) as f64 * 0.37 - 50.0);
                m_cont
                    .entry(cell)
                    .and_modify(|e: &mut ContinuousAccumulator| e.merge(&acc))
                    .or_insert(acc);
                let mut cacc = CategoricalAccumulator::new();
                cacc.update(((i + w) % 25) as i64);
                m_cat
                    .entry(cell)
                    .and_modify(|e: &mut CategoricalAccumulator| e.merge(&cacc))
                    .or_insert(cacc);
            }
            windows_cont.push(m_cont);
            windows_cat.push(m_cat);
        }

        // Continuous comparison
        let mut serial_cont = ShardedResolutionMap::<ContinuousAccumulator>::new();
        let mut serial_new_cont = Vec::new();
        for w in &windows_cont {
            for (&cell, acc) in w {
                let (_, new) = serial_cont.merge_owned_deferred(cell, acc.clone());
                if new {
                    serial_new_cont.push(cell);
                }
            }
        }
        let mut wave_cont = ShardedResolutionMap::<ContinuousAccumulator>::new();
        let mut per_shard_cont: Vec<Vec<Vec<(u64, ContinuousAccumulator)>>> =
            (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for w in &windows_cont {
            let mut buckets: [Vec<(u64, ContinuousAccumulator)>; NUM_SHARDS] =
                std::array::from_fn(|_| Vec::new());
            for (&cell, acc) in w {
                buckets[get_shard(cell)].push((cell, acc.clone()));
            }
            for s in 0..NUM_SHARDS {
                per_shard_cont[s].push(std::mem::take(&mut buckets[s]));
            }
        }
        let mut shards = wave_cont.take_shards();
        let mut outputs = Vec::with_capacity(NUM_SHARDS);
        let mut delta = 0isize;
        for (s, shard) in shards.iter_mut().enumerate() {
            let (out, res) = merge_into_shard(
                shard,
                std::mem::take(&mut per_shard_cont[s]),
                true,
                usize::MAX,
            );
            res.unwrap();
            delta += out.dynamic_delta;
            outputs.push(out);
        }
        wave_cont.restore_shards(shards, delta);
        assert_eq!(wave_cont.estimated_bytes(), recounted(&wave_cont));
        let wave_sorted = wave_cont.take_sorted();
        let serial_sorted = serial_cont.take_sorted();
        assert_eq!(wave_sorted.len(), serial_sorted.len());
        for ((wk, wv), (sk, sv)) in wave_sorted.iter().zip(&serial_sorted) {
            assert_eq!(wk, sk);
            assert_eq!(wv.sum.to_bits(), sv.sum.to_bits());
            assert_eq!(wv.count.to_bits(), sv.count.to_bits());
            assert_eq!(wv.m2.to_bits(), sv.m2.to_bits());
            assert_eq!(wv.min.to_bits(), sv.min.to_bits());
            assert_eq!(wv.max.to_bits(), sv.max.to_bits());
            assert_eq!(wv.quantiles, sv.quantiles);
        }
        let wave_new_set: std::collections::HashSet<u64> = outputs
            .iter()
            .flat_map(|o| o.new_cells.iter().copied())
            .collect();
        let serial_new_set: std::collections::HashSet<u64> =
            serial_new_cont.iter().copied().collect();
        assert_eq!(wave_new_set, serial_new_set);

        // Categorical comparison
        let mut serial_cat = ShardedResolutionMap::<CategoricalAccumulator>::new();
        for w in &windows_cat {
            for (&cell, acc) in w {
                serial_cat.merge_owned_deferred(cell, acc.clone());
            }
        }
        let mut wave_cat = ShardedResolutionMap::<CategoricalAccumulator>::new();
        let mut per_shard_cat: Vec<Vec<Vec<(u64, CategoricalAccumulator)>>> =
            (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for w in &windows_cat {
            let mut buckets: [Vec<(u64, CategoricalAccumulator)>; NUM_SHARDS] =
                std::array::from_fn(|_| Vec::new());
            for (&cell, acc) in w {
                buckets[get_shard(cell)].push((cell, acc.clone()));
            }
            for s in 0..NUM_SHARDS {
                per_shard_cat[s].push(std::mem::take(&mut buckets[s]));
            }
        }
        let mut shards_cat = wave_cat.take_shards();
        let mut delta_cat = 0isize;
        for (s, shard) in shards_cat.iter_mut().enumerate() {
            let (out, res) = merge_into_shard(
                shard,
                std::mem::take(&mut per_shard_cat[s]),
                false,
                usize::MAX,
            );
            res.unwrap();
            delta_cat += out.dynamic_delta;
        }
        wave_cat.restore_shards(shards_cat, delta_cat);
        assert_eq!(wave_cat.estimated_bytes(), recounted(&wave_cat));
        let wave_cat_sorted = wave_cat.take_sorted();
        let serial_cat_sorted = serial_cat.take_sorted();
        assert_eq!(wave_cat_sorted.len(), serial_cat_sorted.len());
        for ((wk, wv), (sk, sv)) in wave_cat_sorted.iter().zip(&serial_cat_sorted) {
            assert_eq!(wk, sk);
            assert_eq!(wv.total_count.to_bits(), sv.total_count.to_bits());
            assert_eq!(wv.unique_classes(), sv.unique_classes());
        }
    }
}
