//! Worker windows and the pipeline that merges their map sets in window order.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

use super::super::eviction_space::CellKeys;
use super::super::sharded_map::{
    get_shard, merge_into_shard, ShardMergeOutput, ShardedResolutionMap, NUM_SHARDS,
};
use super::super::spill::{SpillAccumulator, SpillRuns};
use crate::aggregator::cell_hash::CellIdBuildHasher;
use crate::error::{DggsStreamError, Result};
use crate::raster::mosaic::MosaicReader;
use crate::raster::prefetch::MosaicPrefetchItem;
use crate::raster::RasterChunk;

/// One worker window: (tile index, chunk index, window bounds, borrowed samples, row stride, overlaps another tile).
pub(super) type WindowJob<'a> = (
    usize,
    u32,
    RasterChunk,
    super::super::borrowed::BorrowedSamples<'a>,
    u32,
    bool,
);
/// One worker's per-resolution cell maps.
pub(super) type CellMaps<A> = Vec<HashMap<u64, A, CellIdBuildHasher>>;

/// A batch's windows, generated on demand without storage per window.
pub(super) struct WindowJobs<'a> {
    items: &'a [MosaicPrefetchItem],
    mosaic: &'a MosaicReader,
    worker_pixels: usize,
    item: usize,
    row: usize,
    col: usize,
    remaining: usize,
}

impl<'a> WindowJobs<'a> {
    pub(super) fn new(
        items: &'a [MosaicPrefetchItem],
        mosaic: &'a MosaicReader,
        worker_pixels: usize,
        decoded_bytes: &mut u64,
    ) -> Result<Self> {
        use super::super::borrowed::BorrowedSamples;
        let mut remaining = 0;
        for item in items {
            let (tile, index, chunk, data, _) = item
                .as_ref()
                .map_err(|e| DggsStreamError::StreamFailed(e.to_string()))?;
            if chunk.width == 0
                || chunk.height == 0
                || mosaic.tiles[*tile].reader.is_sparse_chunk(*index)
            {
                continue;
            }
            let width = chunk.width as usize;
            let height = chunk.height as usize;
            let spp = mosaic.tiles[*tile].reader.metadata.samples_per_pixel.max(1) as usize;
            let samples = BorrowedSamples::from(data);
            let len = width
                .checked_mul(height)
                .and_then(|pixels| pixels.checked_mul(spp))
                .ok_or_else(|| DggsStreamError::InvalidMetadata("sample window overflow".into()))?;
            samples.window(0, len)?;
            *decoded_bytes += samples.bytes() as u64;
            remaining += if width <= worker_pixels {
                height.div_ceil(worker_pixels / width)
            } else {
                width.div_ceil(worker_pixels) * height
            };
        }
        Ok(Self {
            items,
            mosaic,
            worker_pixels,
            item: 0,
            row: 0,
            col: 0,
            remaining,
        })
    }
}

impl<'a> Iterator for WindowJobs<'a> {
    type Item = WindowJob<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        use super::super::borrowed::BorrowedSamples;
        while let Some(item) = self.items.get(self.item) {
            let (tile, index, chunk, data, overlap) = item.as_ref().expect("validated batch");
            let width = chunk.width as usize;
            let height = chunk.height as usize;
            // Sparse fills must not be interpreted as observed data.
            if width == 0
                || self.row == height
                || self.mosaic.tiles[*tile].reader.is_sparse_chunk(*index)
            {
                self.item += 1;
                self.row = 0;
                self.col = 0;
                continue;
            }
            let spp = self.mosaic.tiles[*tile]
                .reader
                .metadata
                .samples_per_pixel
                .max(1) as usize;
            let (w, h) = if width <= self.worker_pixels {
                (width, (self.worker_pixels / width).min(height - self.row))
            } else {
                (self.worker_pixels.min(width - self.col), 1)
            };
            let start = (self.row * width + self.col) * spp;
            let len = ((h - 1) * width + w) * spp;
            let samples = BorrowedSamples::from(data)
                .window(start, len)
                .expect("validated sample window");
            let bounds = RasterChunk {
                col_offset: chunk.col_offset + self.col as u32,
                row_offset: chunk.row_offset + self.row as u32,
                width: w as u32,
                height: h as u32,
            };
            if w == width {
                self.row += h;
            } else {
                self.col += w;
                if self.col == width {
                    self.col = 0;
                    self.row += 1;
                }
            }
            self.remaining -= 1;
            return Some((*tile, *index, bounds, samples, width as u32, *overlap));
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for WindowJobs<'_> {}

/// Dedicated aggregation pool of `threads` workers, shared by all live
/// streams requesting the same size so concurrent queries do not each spawn
/// their own threads.
pub(crate) fn shared_aggregation_pool(threads: usize) -> Result<Arc<rayon::ThreadPool>> {
    static POOLS: OnceLock<Mutex<HashMap<usize, Weak<rayon::ThreadPool>>>> = OnceLock::new();
    let mut pools = POOLS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if let Some(pool) = pools.get(&threads).and_then(Weak::upgrade) {
        return Ok(pool);
    }
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("raster-h3-agg-{i}"))
            .build()
            .map_err(|e| DggsStreamError::InvalidParameter(format!("aggregation pool: {e}")))?,
    );
    pools.retain(|_, w| w.strong_count() > 0);
    pools.insert(threads, Arc::downgrade(&pool));
    Ok(pool)
}

/// Fixed bytes a worker map and its key list are charged for regardless of
/// entries: the `+ 1` bucket and alignment slack of `spill::table_bytes`.
pub(super) fn worker_map_fixed_bytes<A>() -> usize {
    2 * (size_of::<(u64, A)>() + 1) + 64
}

/// Worst-case worker bytes per sample for an accumulator that owns no heap
/// state, in the same terms the budget check measures them: a sample adds at
/// most one entry to a map whose reused capacity stays under twice its
/// largest entry count, charged at `spill::table_bytes`'s two
/// buckets per unit of capacity.
pub fn inline_worker_bytes_per_sample<A>() -> usize {
    4 * (size_of::<(u64, A)>() + 1)
}

/// New cells per eviction-key job sent to the workers.
const KEY_CHUNK: usize = 256;

/// Minimum entries across a wave before offloading per-shard merges to workers.
const SHARD_PARTITION_THRESHOLD: usize = 512;

/// Minimum active cells in a resolution before sorting spill shards in parallel.
const PARALLEL_SORT_THRESHOLD: usize = 4096;

/// Computed eviction keys of one key job: (job number, `(resolution index,
/// cell)` pairs, their keys).
pub(super) type KeyChunk = (usize, Vec<(usize, u64)>, Vec<CellKeys>);

/// Per-window entries partitioned across the 32 horizon shards for each resolution.
type ShardedWindow<A> = Vec<[Vec<(u64, A)>; NUM_SHARDS]>;

/// Metadata carried alongside a finished window map set.
type WindowMeta<A> = (Option<ShardedWindow<A>>, Vec<usize>, usize);

/// Input item for a shard-merge worker task.
type MergeShardJob<A> = (
    usize,
    HashMap<u64, A, CellIdBuildHasher>,
    Vec<Vec<(u64, A)>>,
);

/// Result returned by a shard-merge worker task.
type MergeShardResult<A> = (
    usize,
    HashMap<u64, A, CellIdBuildHasher>,
    ShardMergeOutput,
    Result<()>,
);

/// Work handed to a pipeline worker.
enum Work<A, J> {
    /// Aggregate window `index` into `maps`, an empty map set.
    Window {
        index: usize,
        job: J,
        maps: CellMaps<A>,
    },
    /// Compute eviction keys of `(resolution index, cell)` pairs.
    Keys {
        chunk: usize,
        cells: Vec<(usize, u64)>,
    },
    /// Merge window-ordered entry buckets into disjoint horizon shards.
    MergeStripe {
        shards: Vec<MergeShardJob<A>>,
        track_eviction: bool,
        accumulator_limit: usize,
    },
    /// Flatten and sort a subset of disjoint shards for a spill run.
    SortStripe {
        shards: Vec<HashMap<u64, A, CellIdBuildHasher>>,
    },
}

/// A pipeline worker's reply.
enum Done<A> {
    Window {
        index: usize,
        maps: CellMaps<A>,
        sharded: Option<ShardedWindow<A>>,
        res_bytes: Vec<usize>,
        worker_bytes: usize,
        profile: super::super::profile::WorkerProfile,
        ns: u64,
    },
    Keys {
        chunk: usize,
        cells: Vec<(usize, u64)>,
        keys: Vec<CellKeys>,
    },
    MergeStripe {
        results: Vec<MergeShardResult<A>>,
    },
    SortStripe {
        sorted: Vec<(u64, A)>,
    },
    Panicked(Box<dyn std::any::Any + Send>),
}

/// Two-priority shared work queue so urgent shard-merge / spill-sort tasks run
/// immediately ahead of queued raster windows and eviction-key chunks.
struct WorkQueueState<A, J> {
    urgent: VecDeque<Work<A, J>>,
    normal: VecDeque<Work<A, J>>,
    closed: bool,
}

struct WorkQueue<A, J> {
    state: Mutex<WorkQueueState<A, J>>,
    cv: Condvar,
}

impl<A, J> WorkQueue<A, J> {
    fn new() -> Self {
        Self {
            state: Mutex::new(WorkQueueState {
                urgent: VecDeque::new(),
                normal: VecDeque::new(),
                closed: false,
            }),
            cv: Condvar::new(),
        }
    }

    fn push_normal(&self, work: Work<A, J>) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.normal.push_back(work);
        self.cv.notify_one();
    }

    fn push_urgent(&self, work: Work<A, J>) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.urgent.push_back(work);
        self.cv.notify_one();
    }

    fn close(&self) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.closed = true;
        self.cv.notify_all();
    }

    fn pop(&self) -> Option<Work<A, J>> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(work) = guard.urgent.pop_front() {
                return Some(work);
            }
            if let Some(work) = guard.normal.pop_front() {
                return Some(work);
            }
            if guard.closed {
                return None;
            }
            guard = self.cv.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
    }
}

struct CloseOnDrop<A, J>(Arc<WorkQueue<A, J>>);

impl<A, J> Drop for CloseOnDrop<A, J> {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Zero-copy k-way merge iterator over pre-sorted disjoint shard stripes.
struct KWayStripeMerge<A> {
    iters: Vec<std::vec::IntoIter<(u64, A)>>,
    heads: Vec<Option<(u64, A)>>,
}

impl<A> KWayStripeMerge<A> {
    fn new(mut iters: Vec<std::vec::IntoIter<(u64, A)>>) -> Self {
        let mut heads = Vec::with_capacity(iters.len());
        for it in &mut iters {
            heads.push(it.next());
        }
        Self { iters, heads }
    }
}

impl<A> Iterator for KWayStripeMerge<A> {
    type Item = (u64, A);

    fn next(&mut self) -> Option<Self::Item> {
        let mut best_idx = None;
        let mut best_key = u64::MAX;
        for (idx, head) in self.heads.iter().enumerate() {
            if let Some((key, _)) = head {
                if best_idx.is_none() || *key < best_key {
                    best_key = *key;
                    best_idx = Some(idx);
                }
            }
        }
        let idx = best_idx?;
        let item = self.heads[idx].take();
        self.heads[idx] = self.iters[idx].next();
        item
    }
}

fn compute_window_stats<A: SpillAccumulator>(maps: &CellMaps<A>) -> (Vec<usize>, usize, usize) {
    let mut per_res = Vec::with_capacity(maps.len());
    let mut total_bytes = 0usize;
    let mut total_entries = 0usize;
    for m in maps {
        let b = super::super::spill::table_bytes::<u64, A>(m.capacity())
            + m.values().map(|a| a.heap_bytes()).sum::<usize>();
        per_res.push(b);
        total_bytes = total_bytes.saturating_add(b);
        total_entries += m.len();
    }
    (per_res, total_bytes, total_entries)
}

fn partition_maps_to_shards<A>(maps: &mut CellMaps<A>) -> ShardedWindow<A> {
    let mut out = Vec::with_capacity(maps.len());
    for map in maps.iter_mut() {
        let per_shard = (map.len() / NUM_SHARDS).saturating_add(2);
        let mut buckets: [Vec<(u64, A)>; NUM_SHARDS] =
            std::array::from_fn(|_| Vec::with_capacity(per_shard));
        for (key, acc) in map.drain() {
            buckets[get_shard(key)].push((key, acc));
        }
        out.push(buckets);
    }
    out
}

/// Spill one resolution's horizon map to a sorted run; it is then finalized
/// only by the external merge at EOF.
pub(super) fn spill_resolution<A: SpillAccumulator>(
    shards: &mut [ShardedResolutionMap<A>],
    spill_runs: &mut [SpillRuns<A>],
    metrics: &mut super::super::profile::StreamProfile,
    index: usize,
) -> Result<()> {
    let started = std::time::Instant::now();
    let records = shards[index].take_sorted();
    if !records.is_empty() {
        spill_runs[index].push_sorted(records)?;
        shards[index].set_eviction_enabled(false);
    }
    metrics.spill_ns += started.elapsed().as_nanos() as u64;
    Ok(())
}

/// The streamer state a batch's merges write to, borrowed apart from the
/// read-only state the workers use.
pub(super) struct MergeTarget<'a, A: SpillAccumulator> {
    pub(super) shards: &'a mut Vec<ShardedResolutionMap<A>>,
    pub(super) spill_runs: &'a mut Vec<SpillRuns<A>>,
    pub(super) metrics: &'a mut super::super::profile::StreamProfile,
    pub(super) peak_active_bytes: &'a mut usize,
    pub(super) failure_counter: Option<&'a Arc<std::sync::atomic::AtomicU64>>,
    pub(super) compactor_bytes: usize,
    pub(super) accumulator_limit: usize,
    pub(super) map_budget: usize,
    /// Worker storage allowance of one map set.
    pub(super) set_budget: usize,
    pub(super) map_sets: usize,
    /// Cells inserted into a horizon map that still need eviction keys.
    pub(super) new_cells: Vec<(usize, u64)>,
    /// Computed keys: (chunk number, cells, keys).
    pub(super) key_chunks: Vec<KeyChunk>,
}

impl<A: SpillAccumulator> MergeTarget<'_, A> {
    fn record(&mut self, profile: super::super::profile::WorkerProfile, ns: u64) {
        if let (Some(counter), n @ 1..) = (self.failure_counter, profile.transform_failures) {
            counter.fetch_add(n, Ordering::Relaxed);
        }
        self.metrics.worker.merge(profile);
        self.metrics.worker_ns += ns;
        self.metrics.jobs += 1;
    }

    fn total_shard_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.estimated_bytes()).sum()
    }

    #[inline]
    fn merge_entry(
        &mut self,
        res_idx: usize,
        key: u64,
        acc: A,
        other_res_bytes: usize,
    ) -> Result<bool> {
        if acc.memory_bytes() > self.accumulator_limit {
            return Err(DggsStreamError::InvalidParameter(
                "single accumulator exceeds aggregation budget; increase aggregation_budget_bytes"
                    .into(),
            ));
        }
        let (merged, new) = self.shards[res_idx].merge_owned_deferred(key, acc);
        if new {
            self.new_cells.push((res_idx, key));
        }
        if merged > self.accumulator_limit {
            return Err(DggsStreamError::InvalidParameter(
                "merged accumulator exceeds aggregation budget; increase aggregation_budget_bytes"
                    .into(),
            ));
        }
        let cur = self.shards[res_idx].estimated_bytes();
        *self.peak_active_bytes =
            (*self.peak_active_bytes).max(other_res_bytes + cur + self.compactor_bytes);
        Ok(cur >= self.map_budget)
    }

    /// Merge one job's map set into the horizon maps, leaving it empty.
    fn merge(&mut self, maps: &mut CellMaps<A>) -> Result<()> {
        let (_, bytes, _) = compute_window_stats(maps);
        // Every set in flight is bounded like this one.
        self.metrics.peak_worker_bytes = self
            .metrics
            .peak_worker_bytes
            .max(bytes.saturating_mul(self.map_sets));
        let t = std::time::Instant::now();
        let result = self.merge_maps(maps);
        self.metrics.merge_ns += t.elapsed().as_nanos() as u64;
        // Retained buckets are part of worker storage, not free capacity.
        if bytes > self.set_budget {
            for map in maps.iter_mut() {
                map.shrink_to_fit();
            }
        }
        result
    }

    fn merge_maps(&mut self, maps: &mut CellMaps<A>) -> Result<()> {
        let mut total_shards = self.total_shard_bytes();
        for (i, map) in maps.iter_mut().enumerate() {
            let other_res_bytes = total_shards - self.shards[i].estimated_bytes();
            for (key, acc) in map.drain() {
                if self.merge_entry(i, key, acc, other_res_bytes)? {
                    spill_resolution(self.shards, self.spill_runs, self.metrics, i)?;
                }
            }
            total_shards = other_res_bytes + self.shards[i].estimated_bytes();
        }
        Ok(())
    }
}

/// Run a batch on the calling thread: windows in order, then the new cells'
/// eviction keys.
pub(super) fn run_serial<A, J, W, C>(
    jobs: impl Iterator<Item = J>,
    free: &mut Vec<CellMaps<A>>,
    run_window: &W,
    compute_keys: &C,
    target: &mut MergeTarget<'_, A>,
) -> Result<()>
where
    A: SpillAccumulator,
    W: Fn(J, &mut CellMaps<A>) -> (super::super::profile::WorkerProfile, u64),
    C: Fn(&[(usize, u64)]) -> Vec<CellKeys>,
{
    let mut maps = free.pop().expect("at least one worker map set");
    let mut result = Ok(());
    for job in jobs {
        let (profile, ns) = run_window(job, &mut maps);
        target.record(profile, ns);
        result = target.merge(&mut maps);
        if result.is_err() {
            maps.iter_mut().for_each(HashMap::clear);
            break;
        }
    }
    free.push(maps);
    result?;
    let cells = {
        let cells = target.new_cells.to_vec();
        target.new_cells.clear();
        cells
    };
    let keys = compute_keys(&cells);
    target.key_chunks.push((0, cells, keys));
    Ok(())
}

struct ReadyWindow<A> {
    maps: CellMaps<A>,
    sharded: Option<ShardedWindow<A>>,
    res_bytes: Vec<usize>,
    worker_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn record_non_stripe_done<A: SpillAccumulator>(
    reply: Done<A>,
    slots: usize,
    finished: &mut [Option<CellMaps<A>>],
    finished_meta: &mut [Option<WindowMeta<A>>],
    target: &mut MergeTarget<'_, A>,
    keys_out: &mut usize,
    panic: &mut Option<Box<dyn std::any::Any + Send>>,
) -> bool {
    match reply {
        Done::Panicked(payload) => {
            *panic = Some(payload);
            true
        }
        Done::Keys { chunk, cells, keys } => {
            *keys_out -= 1;
            target.key_chunks.push((chunk, cells, keys));
            false
        }
        Done::Window {
            index,
            maps,
            sharded,
            res_bytes,
            worker_bytes,
            profile,
            ns,
        } => {
            target.record(profile, ns);
            let slot = index % slots;
            finished[slot] = Some(maps);
            finished_meta[slot] = Some((sharded, res_bytes, worker_bytes));
            false
        }
        Done::MergeStripe { .. } | Done::SortStripe { .. } => {
            unreachable!("unexpected stripe reply")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spill_resolution_adaptive<A: SpillAccumulator, J>(
    res_idx: usize,
    workers: usize,
    work_queue: &WorkQueue<A, J>,
    done_rx: &std::sync::mpsc::Receiver<Done<A>>,
    slots: usize,
    finished: &mut [Option<CellMaps<A>>],
    finished_meta: &mut [Option<WindowMeta<A>>],
    target: &mut MergeTarget<'_, A>,
    keys_out: &mut usize,
    panic: &mut Option<Box<dyn std::any::Any + Send>>,
) -> Result<()> {
    if workers <= 1 || target.shards[res_idx].active_cell_count() < PARALLEL_SORT_THRESHOLD {
        return spill_resolution(target.shards, target.spill_runs, target.metrics, res_idx);
    }
    let started = std::time::Instant::now();
    let mut shards = target.shards[res_idx].take_shards_and_clear();
    let num_stripes = workers.min(NUM_SHARDS);
    let mut buckets: Vec<Vec<HashMap<u64, A, CellIdBuildHasher>>> =
        (0..num_stripes).map(|_| Vec::new()).collect();
    for (s, shard) in shards.drain(..).enumerate() {
        if !shard.is_empty() {
            buckets[s % num_stripes].push(shard);
        }
    }
    let active_stripes: Vec<_> = buckets.into_iter().filter(|b| !b.is_empty()).collect();
    if active_stripes.is_empty() {
        target.metrics.spill_ns += started.elapsed().as_nanos() as u64;
        return Ok(());
    }
    let mut pending = active_stripes.len();
    let mut sorted_stripes = Vec::with_capacity(pending);
    for stripe in active_stripes {
        work_queue.push_urgent(Work::SortStripe { shards: stripe });
    }
    while pending > 0 {
        let Ok(reply) = done_rx.recv() else { break };
        match reply {
            Done::SortStripe { sorted } => {
                pending -= 1;
                if !sorted.is_empty() {
                    sorted_stripes.push(sorted.into_iter());
                }
            }
            other => {
                if record_non_stripe_done(
                    other,
                    slots,
                    finished,
                    finished_meta,
                    target,
                    keys_out,
                    panic,
                ) {
                    return Ok(());
                }
            }
        }
    }
    if !sorted_stripes.is_empty() {
        target.spill_runs[res_idx].push_sorted(KWayStripeMerge::new(sorted_stripes))?;
        target.shards[res_idx].set_eviction_enabled(false);
    }
    target.metrics.spill_ns += started.elapsed().as_nanos() as u64;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn merge_res_parallel_wave<A: SpillAccumulator, J>(
    res_idx: usize,
    ready_wave: &mut [ReadyWindow<A>],
    other_res_bytes: usize,
    workers: usize,
    work_queue: &WorkQueue<A, J>,
    done_rx: &std::sync::mpsc::Receiver<Done<A>>,
    slots: usize,
    finished: &mut [Option<CellMaps<A>>],
    finished_meta: &mut [Option<WindowMeta<A>>],
    target: &mut MergeTarget<'_, A>,
    keys_out: &mut usize,
    panic: &mut Option<Box<dyn std::any::Any + Send>>,
) -> Result<()> {
    let k = ready_wave.len();
    let mut shard_buckets: Vec<Vec<Vec<(u64, A)>>> =
        (0..NUM_SHARDS).map(|_| Vec::with_capacity(k)).collect();
    for win in ready_wave.iter_mut() {
        let sharded = win.sharded.as_mut().expect("sharded wave window");
        for s in 0..NUM_SHARDS {
            shard_buckets[s].push(std::mem::take(&mut sharded[res_idx][s]));
        }
    }

    let mut shards = target.shards[res_idx].take_shards();
    let track_eviction = target.shards[res_idx].tracks_eviction();
    let accumulator_limit = target.accumulator_limit;

    let mut restored: Vec<Option<HashMap<u64, A, CellIdBuildHasher>>> =
        (0..NUM_SHARDS).map(|_| None).collect();
    let mut outputs: Vec<Option<ShardMergeOutput>> = (0..NUM_SHARDS).map(|_| None).collect();
    let mut errors: Vec<Option<DggsStreamError>> = (0..NUM_SHARDS).map(|_| None).collect();

    let mut active = Vec::new();
    for (s, (shard, buckets)) in shards.drain(..).zip(shard_buckets).enumerate() {
        if buckets.iter().all(Vec::is_empty) {
            restored[s] = Some(shard);
        } else {
            active.push((s, shard, buckets));
        }
    }

    let num_stripes = workers.min(active.len()).max(1);
    let mut stripes: Vec<Vec<MergeShardJob<A>>> = (0..num_stripes).map(|_| Vec::new()).collect();
    for (idx, item) in active.into_iter().enumerate() {
        stripes[idx % num_stripes].push(item);
    }
    let non_empty_stripes: Vec<_> = stripes.into_iter().filter(|s| !s.is_empty()).collect();
    let mut pending = non_empty_stripes.len();
    for stripe in non_empty_stripes {
        work_queue.push_urgent(Work::MergeStripe {
            shards: stripe,
            track_eviction,
            accumulator_limit,
        });
    }

    while pending > 0 {
        let Ok(reply) = done_rx.recv() else { break };
        match reply {
            Done::MergeStripe { results } => {
                pending -= 1;
                for (s, shard, out, res) in results {
                    restored[s] = Some(shard);
                    outputs[s] = Some(out);
                    if let Err(e) = res {
                        errors[s] = Some(e);
                    }
                }
            }
            other => {
                if record_non_stripe_done(
                    other,
                    slots,
                    finished,
                    finished_meta,
                    target,
                    keys_out,
                    panic,
                ) {
                    return Ok(());
                }
            }
        }
    }

    let final_shards: Vec<_> = restored
        .into_iter()
        .map(|s| s.expect("all 32 shards returned"))
        .collect();
    let total_delta: isize = outputs.iter().flatten().map(|o| o.dynamic_delta).sum();
    target.shards[res_idx].restore_shards(final_shards, total_delta);

    if let Some(err) = errors.into_iter().flatten().next() {
        return Err(err);
    }

    if track_eviction {
        for w in 0..k {
            for out in outputs.iter().flatten() {
                let range = out.window_offsets[w]..out.window_offsets[w + 1];
                for &cell in &out.new_cells[range] {
                    target.new_cells.push((res_idx, cell));
                }
            }
        }
    }

    let cur = target.shards[res_idx].estimated_bytes();
    *target.peak_active_bytes =
        (*target.peak_active_bytes).max(other_res_bytes + cur + target.compactor_bytes);
    if cur >= target.map_budget {
        spill_resolution_adaptive(
            res_idx,
            workers,
            work_queue,
            done_rx,
            slots,
            finished,
            finished_meta,
            target,
            keys_out,
            panic,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn merge_res_sharded_serial<A: SpillAccumulator, J>(
    res_idx: usize,
    ready_wave: &mut [ReadyWindow<A>],
    other_res_bytes: usize,
    workers: usize,
    work_queue: &WorkQueue<A, J>,
    done_rx: &std::sync::mpsc::Receiver<Done<A>>,
    slots: usize,
    finished: &mut [Option<CellMaps<A>>],
    finished_meta: &mut [Option<WindowMeta<A>>],
    target: &mut MergeTarget<'_, A>,
    keys_out: &mut usize,
    panic: &mut Option<Box<dyn std::any::Any + Send>>,
) -> Result<()> {
    for win in ready_wave.iter_mut() {
        let sharded = win.sharded.as_mut().expect("sharded wave window");
        for shard_vec in &mut sharded[res_idx] {
            for (key, acc) in shard_vec.drain(..) {
                if target.merge_entry(res_idx, key, acc, other_res_bytes)? {
                    spill_resolution_adaptive(
                        res_idx,
                        workers,
                        work_queue,
                        done_rx,
                        slots,
                        finished,
                        finished_meta,
                        target,
                        keys_out,
                        panic,
                    )?;
                    if panic.is_some() {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Run a batch on `workers` tasks of a Rayon scope while the calling thread
/// merges finished windows in window order (see `process_batch`).
/// `finished` holds sets that finished out of order; it is reused across batches.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_pipelined<'s, A, J, W, C>(
    scope: &rayon::Scope<'s>,
    workers: usize,
    mut jobs: impl ExactSizeIterator<Item = J>,
    free: &mut Vec<CellMaps<A>>,
    run_window: &'s W,
    compute_keys: &'s C,
    target: &mut MergeTarget<'_, A>,
    finished: &mut Vec<Option<CellMaps<A>>>,
) -> Result<()>
where
    A: SpillAccumulator + Send + 's,
    J: Send + 's,
    W: Fn(J, &mut CellMaps<A>) -> (super::super::profile::WorkerProfile, u64) + Sync,
    C: Fn(&[(usize, u64)]) -> Vec<CellKeys> + Sync,
{
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::mpsc;

    let work_queue = Arc::new(WorkQueue::<A, J>::new());
    let _close = CloseOnDrop(Arc::clone(&work_queue));
    let (done_tx, done_rx) = mpsc::channel::<Done<A>>();
    let allow_partition = workers > 1;
    let set_budget = target.set_budget;

    for _ in 0..workers.max(1) {
        let (queue, tx) = (Arc::clone(&work_queue), done_tx.clone());
        scope.spawn(move |_| loop {
            let Some(work) = queue.pop() else { break };
            let reply = catch_unwind(AssertUnwindSafe(|| match work {
                Work::Window {
                    index,
                    job,
                    mut maps,
                } => {
                    let (profile, ns) = run_window(job, &mut maps);
                    let (res_bytes, worker_bytes, total_entries) = compute_window_stats(&maps);
                    let sharded = if allow_partition
                        && total_entries >= SHARD_PARTITION_THRESHOLD
                        && total_entries.saturating_mul(size_of::<(u64, A)>()) <= set_budget
                    {
                        let partitioned = partition_maps_to_shards(&mut maps);
                        if worker_bytes > set_budget {
                            for map in &mut maps {
                                map.shrink_to_fit();
                            }
                        }
                        Some(partitioned)
                    } else {
                        None
                    };
                    Done::Window {
                        index,
                        maps,
                        sharded,
                        res_bytes,
                        worker_bytes,
                        profile,
                        ns,
                    }
                }
                Work::Keys { chunk, cells } => {
                    let keys = compute_keys(&cells);
                    Done::Keys { chunk, cells, keys }
                }
                Work::MergeStripe {
                    shards,
                    track_eviction,
                    accumulator_limit,
                } => {
                    let mut results = Vec::with_capacity(shards.len());
                    for (s, mut shard, buckets) in shards {
                        let (out, res) = merge_into_shard(
                            &mut shard,
                            buckets,
                            track_eviction,
                            accumulator_limit,
                        );
                        results.push((s, shard, out, res));
                    }
                    Done::MergeStripe { results }
                }
                Work::SortStripe { shards } => {
                    let total_len: usize = shards.iter().map(HashMap::len).sum();
                    let mut sorted = Vec::with_capacity(total_len);
                    for map in shards {
                        sorted.extend(map);
                    }
                    sorted.sort_unstable_by_key(|(key, _)| *key);
                    Done::SortStripe { sorted }
                }
            }))
            .unwrap_or_else(Done::Panicked);
            if tx.send(reply).is_err() {
                break;
            }
        });
    }
    drop(done_tx);

    finished.clear();
    let slots = free.len();
    finished.resize_with(slots, || None);
    let mut finished_meta: Vec<Option<WindowMeta<A>>> = vec![None; slots];
    let mut ready_wave: Vec<ReadyWindow<A>> = Vec::with_capacity(slots);
    let count = jobs.len();
    let (mut next, mut expected, mut end) = (0, 0, count);
    let (mut key_chunk, mut keys_out) = (0, 0);
    let mut result = Ok(());
    let mut panic = None;
    let num_res = target.shards.len();

    while next < end {
        let Some(maps) = free.pop() else { break };
        work_queue.push_normal(Work::Window {
            index: next,
            job: jobs.next().expect("window count"),
            maps,
        });
        next += 1;
    }

    while expected < end || keys_out > 0 {
        let already_ready = expected < end && finished[expected % slots].is_some();
        if !already_ready {
            let Ok(first_reply) = done_rx.recv() else {
                break;
            };
            if record_non_stripe_done(
                first_reply,
                slots,
                finished,
                &mut finished_meta,
                target,
                &mut keys_out,
                &mut panic,
            ) {
                break;
            }
        }
        while let Ok(reply) = done_rx.try_recv() {
            if record_non_stripe_done(
                reply,
                slots,
                finished,
                &mut finished_meta,
                target,
                &mut keys_out,
                &mut panic,
            ) {
                break;
            }
        }
        if panic.is_some() {
            break;
        }

        ready_wave.clear();
        while expected < end {
            let slot = expected % slots;
            let Some(maps) = finished[slot].take() else {
                break;
            };
            let (sharded, res_bytes, worker_bytes) = finished_meta[slot]
                .take()
                .expect("matching window metadata");
            ready_wave.push(ReadyWindow {
                maps,
                sharded,
                res_bytes,
                worker_bytes,
            });
            expected += 1;
        }

        if !ready_wave.is_empty() {
            if result.is_ok() {
                for w in &ready_wave {
                    target.metrics.peak_worker_bytes = target
                        .metrics
                        .peak_worker_bytes
                        .max(w.worker_bytes.saturating_mul(target.map_sets));
                }
                let t = std::time::Instant::now();
                let all_sharded = workers > 1 && ready_wave.iter().all(|w| w.sharded.is_some());
                let mut wave_res = Ok(());
                if all_sharded {
                    let mut total_shards = target.total_shard_bytes();
                    for i in 0..num_res {
                        let other_res_bytes = total_shards - target.shards[i].estimated_bytes();
                        let wave_entries: usize = ready_wave
                            .iter()
                            .map(|w| {
                                w.sharded.as_ref().unwrap()[i]
                                    .iter()
                                    .map(Vec::len)
                                    .sum::<usize>()
                            })
                            .sum();
                        let wave_res_bytes: usize = ready_wave.iter().map(|w| w.res_bytes[i]).sum();
                        let est = target.shards[i].estimated_bytes();
                        let res_outcome = if wave_entries >= SHARD_PARTITION_THRESHOLD
                            && est.saturating_add(wave_res_bytes.saturating_mul(2))
                                < target.map_budget
                        {
                            merge_res_parallel_wave(
                                i,
                                &mut ready_wave,
                                other_res_bytes,
                                workers,
                                &work_queue,
                                &done_rx,
                                slots,
                                finished,
                                &mut finished_meta,
                                target,
                                &mut keys_out,
                                &mut panic,
                            )
                        } else {
                            merge_res_sharded_serial(
                                i,
                                &mut ready_wave,
                                other_res_bytes,
                                workers,
                                &work_queue,
                                &done_rx,
                                slots,
                                finished,
                                &mut finished_meta,
                                target,
                                &mut keys_out,
                                &mut panic,
                            )
                        };
                        if panic.is_some() {
                            break;
                        }
                        if let Err(e) = res_outcome {
                            wave_res = Err(e);
                            break;
                        }
                        total_shards = other_res_bytes + target.shards[i].estimated_bytes();
                    }
                } else {
                    for w in &mut ready_wave {
                        if let Some(mut sharded) = w.sharded.take() {
                            let mut total_shards = target.total_shard_bytes();
                            for (i, shard_vecs) in sharded.iter_mut().enumerate() {
                                let other_res_bytes =
                                    total_shards - target.shards[i].estimated_bytes();
                                for shard_vec in shard_vecs.iter_mut() {
                                    for (key, acc) in shard_vec.drain(..) {
                                        match target.merge_entry(i, key, acc, other_res_bytes) {
                                            Ok(true) => {
                                                if let Err(e) = spill_resolution_adaptive(
                                                    i,
                                                    workers,
                                                    &work_queue,
                                                    &done_rx,
                                                    slots,
                                                    finished,
                                                    &mut finished_meta,
                                                    target,
                                                    &mut keys_out,
                                                    &mut panic,
                                                ) {
                                                    wave_res = Err(e);
                                                    break;
                                                }
                                                if panic.is_some() {
                                                    break;
                                                }
                                            }
                                            Ok(false) => {}
                                            Err(e) => {
                                                wave_res = Err(e);
                                                break;
                                            }
                                        }
                                    }
                                    if wave_res.is_err() || panic.is_some() {
                                        break;
                                    }
                                }
                                if wave_res.is_err() || panic.is_some() {
                                    break;
                                }
                                total_shards = other_res_bytes + target.shards[i].estimated_bytes();
                            }
                        } else {
                            let mut total_shards = target.total_shard_bytes();
                            for (i, map) in w.maps.iter_mut().enumerate() {
                                let other_res_bytes =
                                    total_shards - target.shards[i].estimated_bytes();
                                for (key, acc) in map.drain() {
                                    match target.merge_entry(i, key, acc, other_res_bytes) {
                                        Ok(true) => {
                                            if let Err(e) = spill_resolution_adaptive(
                                                i,
                                                workers,
                                                &work_queue,
                                                &done_rx,
                                                slots,
                                                finished,
                                                &mut finished_meta,
                                                target,
                                                &mut keys_out,
                                                &mut panic,
                                            ) {
                                                wave_res = Err(e);
                                                break;
                                            }
                                            if panic.is_some() {
                                                break;
                                            }
                                        }
                                        Ok(false) => {}
                                        Err(e) => {
                                            wave_res = Err(e);
                                            break;
                                        }
                                    }
                                }
                                if wave_res.is_err() || panic.is_some() {
                                    break;
                                }
                                total_shards = other_res_bytes + target.shards[i].estimated_bytes();
                            }
                            if w.worker_bytes > target.set_budget {
                                for map in w.maps.iter_mut() {
                                    map.shrink_to_fit();
                                }
                            }
                        }
                        if wave_res.is_err() || panic.is_some() {
                            break;
                        }
                    }
                }
                target.metrics.merge_ns += t.elapsed().as_nanos() as u64;
                if panic.is_some() {
                    for mut w in ready_wave.drain(..) {
                        w.maps.iter_mut().for_each(HashMap::clear);
                        free.push(w.maps);
                    }
                    break;
                }
                if wave_res.is_err() {
                    result = wave_res;
                }
            }

            let err = result.is_err();
            if err {
                end = next;
            }
            for mut w in ready_wave.drain(..) {
                if err {
                    w.maps.iter_mut().for_each(HashMap::clear);
                }
                free.push(w.maps);
                if next < end {
                    let maps = free.pop().expect("a set was just returned");
                    work_queue.push_normal(Work::Window {
                        index: next,
                        job: jobs.next().expect("window count"),
                        maps,
                    });
                    next += 1;
                }
            }

            let last = expected == count;
            if result.is_ok()
                && (target.new_cells.len() >= KEY_CHUNK || (last && !target.new_cells.is_empty()))
            {
                let cells = std::mem::take(&mut target.new_cells);
                let chunk_size = if cells.len() <= KEY_CHUNK * 2 {
                    cells.len()
                } else {
                    KEY_CHUNK.max(cells.len().div_ceil(workers.max(1)))
                };
                let mut rest = cells;
                while !rest.is_empty() {
                    let take = chunk_size.min(rest.len());
                    let tail = rest.split_off(take);
                    work_queue.push_normal(Work::Keys {
                        chunk: key_chunk,
                        cells: rest,
                    });
                    key_chunk += 1;
                    keys_out += 1;
                    rest = tail;
                }
            }
        }
    }

    work_queue.close();
    if let Some(payload) = panic {
        std::panic::resume_unwind(payload);
    }
    result
}
