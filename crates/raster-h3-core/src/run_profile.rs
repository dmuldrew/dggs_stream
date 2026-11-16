//! Opt-in runtime telemetry of a scan: where the time went, by pipeline phase.
//!
//! A [`RunProfile`] is a set of atomic counters shared by everything that works on one
//! scan (`MultiResolutionConfig::profile`). Stages add to it at chunk, window and batch
//! boundaries only, never per pixel, and each stage checks for it once per chunk, so a
//! scan without one pays a branch per chunk and nothing else. It is available in release
//! builds; the `stream-profile` cargo feature only adds exact per-sample timers that
//! refine the kernel breakdown.
//!
//! Wall time is summed over the threads working in a phase, so with parallel workers a
//! phase can exceed the scan's elapsed time; CPU time (where the platform reports it per
//! thread) shows how much of that was spent running rather than waiting.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;

/// A pipeline phase time is attributed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum Phase {
    /// Waiting for remote byte ranges of chunks.
    Fetch,
    /// TIFF chunk decompression and predictor decoding.
    Decode,
    /// The scan kernel: CRS transform, grid indexing and accumulation of a window.
    Kernel,
    /// Estimated share of the kernel spent transforming pixel positions to WGS84.
    KernelCrs,
    /// Estimated share of the kernel spent indexing positions to cells.
    KernelIndex,
    /// Merging window maps into the horizon maps, eviction and compaction.
    Horizon,
    /// Writing and reading spill runs.
    Spill,
    /// The calling thread waiting for decoded chunks.
    Stall,
    /// Building output: Arrow batches, Parquet row groups, PMTiles tiles.
    Serialize,
}

const PHASES: usize = 9;

/// A count kept alongside the phase times.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum Counter {
    /// Chunks decoded.
    Chunks,
    /// Bytes of decoded chunks.
    DecodedBytes,
    /// Kernel windows run.
    Windows,
    /// Pixels handed to the kernel.
    PixelsRead,
    /// Samples aggregated into cells of the first resolution (valid, located pixels).
    SamplesIndexed,
    /// Samples dropped because their position could not be transformed.
    TransformFailures,
    /// Cells output, over all resolutions.
    Cells,
    /// Rows (or features) serialized.
    RowsSerialized,
    /// Spill runs written.
    SpillRuns,
    /// Sibling groups a compaction byte limit forced out early.
    ForcedCompactionFlushes,
    /// Highest bytes of horizon maps held at once.
    PeakActiveBytes,
    /// Highest bytes of decoded chunks held at once.
    PeakDecodedBytes,
    /// Highest bytes of pending compaction groups.
    PeakCompactionBytes,
}

const COUNTERS: usize = 13;

/// Shared, thread-safe telemetry of one scan. Create it with [`RunProfile::new`], put it
/// in `MultiResolutionConfig::profile`, run the scan, then read [`RunProfile::report`].
pub struct RunProfile {
    started: Instant,
    wall_ns: [AtomicU64; PHASES],
    cpu_ns: [AtomicU64; PHASES],
    counters: [AtomicU64; COUNTERS],
    /// Whether [`Phase::KernelCrs`] and [`Phase::KernelIndex`] hold exact per-sample
    /// timings (the `stream-profile` feature) rather than sampled estimates.
    exact_kernel_split: AtomicU64,
}

impl fmt::Debug for RunProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunProfile").finish_non_exhaustive()
    }
}

impl RunProfile {
    /// A new profile; its elapsed time runs from here.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            wall_ns: Default::default(),
            cpu_ns: Default::default(),
            counters: Default::default(),
            exact_kernel_split: AtomicU64::new(0),
        })
    }

    /// Add wall (and, if known, CPU) time to a phase.
    #[inline]
    pub fn add(&self, phase: Phase, wall_ns: u64, cpu_ns: Option<u64>) {
        self.wall_ns[phase as usize].fetch_add(wall_ns, Ordering::Relaxed);
        if let Some(cpu) = cpu_ns {
            self.cpu_ns[phase as usize].fetch_add(cpu, Ordering::Relaxed);
        }
    }

    /// Add to a counter.
    #[inline]
    pub fn count(&self, counter: Counter, n: u64) {
        self.counters[counter as usize].fetch_add(n, Ordering::Relaxed);
    }

    /// Raise a peak counter to `value` if it is higher.
    #[inline]
    pub fn peak(&self, counter: Counter, value: u64) {
        self.counters[counter as usize].fetch_max(value, Ordering::Relaxed);
    }

    /// Record that the kernel split comes from exact per-sample timers.
    pub fn mark_exact_kernel_split(&self) {
        self.exact_kernel_split.store(1, Ordering::Relaxed);
    }

    /// Time `work` as `phase` on the calling thread.
    #[inline]
    pub fn time<T>(&self, phase: Phase, work: impl FnOnce() -> T) -> T {
        let timer = PhaseTimer::start();
        let out = work();
        timer.stop(self, phase);
        out
    }

    fn wall(&self, phase: Phase) -> u64 {
        self.wall_ns[phase as usize].load(Ordering::Relaxed)
    }

    fn cpu(&self, phase: Phase) -> u64 {
        self.cpu_ns[phase as usize].load(Ordering::Relaxed)
    }

    fn counter(&self, counter: Counter) -> u64 {
        self.counters[counter as usize].load(Ordering::Relaxed)
    }

    /// A snapshot of the profile so far.
    pub fn report(&self) -> ProfileReport {
        let phase = |name: &'static str, phase: Phase, children: Vec<PhaseReport>| PhaseReport {
            name,
            wall_ms: self.wall(phase) as f64 / 1e6,
            cpu_ms: (self.cpu(phase) > 0).then(|| self.cpu(phase) as f64 / 1e6),
            estimated: false,
            children,
        };
        let kernel = self.wall(Phase::Kernel);
        let exact = self.exact_kernel_split.load(Ordering::Relaxed) != 0;
        // The split is an estimate (or covers only the instrumented calls): keep it
        // within the kernel's own time.
        let crs = self.wall(Phase::KernelCrs).min(kernel);
        let index = self.wall(Phase::KernelIndex).min(kernel - crs);
        let part = |name: &'static str, ns: u64| PhaseReport {
            name,
            wall_ms: ns as f64 / 1e6,
            cpu_ms: None,
            estimated: !exact,
            children: Vec::new(),
        };
        let kernel_children = if crs + index > 0 {
            vec![
                part("crs_transform", crs),
                part("grid_index", index),
                part("accumulate", kernel - crs - index),
            ]
        } else {
            Vec::new()
        };
        let pixels = self.counter(Counter::PixelsRead);
        let samples = self.counter(Counter::SamplesIndexed);
        let elapsed_ms = self.started.elapsed().as_secs_f64() * 1e3;
        let stall_ms = self.wall(Phase::Stall) as f64 / 1e6;
        ProfileReport {
            elapsed_ms,
            phases: vec![
                phase("fetch", Phase::Fetch, Vec::new()),
                phase("tiff_decode", Phase::Decode, Vec::new()),
                phase("kernel", Phase::Kernel, kernel_children),
                phase(
                    "horizon",
                    Phase::Horizon,
                    vec![phase("spill", Phase::Spill, Vec::new())],
                ),
                phase("stall", Phase::Stall, Vec::new()),
                phase("serialize", Phase::Serialize, Vec::new()),
            ],
            stall_pct: percent(stall_ms, elapsed_ms),
            chunks: self.counter(Counter::Chunks),
            decoded_bytes: self.counter(Counter::DecodedBytes),
            windows: self.counter(Counter::Windows),
            pixels_read: pixels,
            samples_indexed: samples,
            indexed_pct: percent(samples as f64, pixels as f64),
            transform_failures: self.counter(Counter::TransformFailures),
            cells: self.counter(Counter::Cells),
            rows_serialized: self.counter(Counter::RowsSerialized),
            spill_runs: self.counter(Counter::SpillRuns),
            forced_compaction_flushes: self.counter(Counter::ForcedCompactionFlushes),
            peak_active_bytes: self.counter(Counter::PeakActiveBytes),
            peak_decoded_bytes: self.counter(Counter::PeakDecodedBytes),
            peak_compaction_bytes: self.counter(Counter::PeakCompactionBytes),
        }
    }
}

fn percent(part: f64, whole: f64) -> f64 {
    if whole > 0.0 {
        100.0 * part / whole
    } else {
        0.0
    }
}

/// Wall and thread-CPU clocks started together; [`PhaseTimer::stop`] adds the elapsed
/// time to a phase.
pub struct PhaseTimer {
    wall: Instant,
    cpu: Option<u64>,
}

impl PhaseTimer {
    /// Start timing on the calling thread.
    #[inline]
    pub fn start() -> Self {
        Self {
            wall: Instant::now(),
            cpu: thread_cpu_ns(),
        }
    }

    /// `(wall, cpu)` nanoseconds since [`Self::start`]; CPU time only if this is still
    /// the thread that started the timer and the platform reports it.
    #[inline]
    pub fn elapsed(&self) -> (u64, Option<u64>) {
        let wall = self.wall.elapsed().as_nanos() as u64;
        let cpu = match (self.cpu, thread_cpu_ns()) {
            (Some(start), Some(now)) => Some(now.saturating_sub(start)),
            _ => None,
        };
        (wall, cpu)
    }

    /// Add the elapsed time to `phase` of `profile`.
    #[inline]
    pub fn stop(self, profile: &RunProfile, phase: Phase) {
        let (wall, cpu) = self.elapsed();
        profile.add(phase, wall, cpu);
    }
}

/// Times a consumer loop as [`Phase::Serialize`], leaving out the time it spends
/// waiting for its input. The time is added when the clock is dropped, so every exit
/// path is covered.
pub struct SerializeClock {
    profile: Option<Arc<RunProfile>>,
    started: Instant,
    waited_ns: u64,
}

impl SerializeClock {
    /// Start the clock; with no profile it does nothing.
    pub fn start(profile: Option<Arc<RunProfile>>) -> Self {
        Self {
            profile,
            started: Instant::now(),
            waited_ns: 0,
        }
    }

    /// Run `wait` (a blocking receive), not counting its duration.
    #[inline]
    pub fn wait<T>(&mut self, wait: impl FnOnce() -> T) -> T {
        if self.profile.is_none() {
            return wait();
        }
        let start = Instant::now();
        let out = wait();
        self.waited_ns += start.elapsed().as_nanos() as u64;
        out
    }

    /// Count `rows` rows or features written.
    #[inline]
    pub fn rows(&self, rows: usize) {
        if let Some(profile) = &self.profile {
            profile.count(Counter::RowsSerialized, rows as u64);
        }
    }
}

impl Drop for SerializeClock {
    fn drop(&mut self) {
        if let Some(profile) = &self.profile {
            let total = self.started.elapsed().as_nanos() as u64;
            profile.add(Phase::Serialize, total.saturating_sub(self.waited_ns), None);
        }
    }
}

/// CPU time the calling thread has used, in nanoseconds, where the platform has a
/// per-thread CPU clock.
#[cfg(unix)]
pub fn thread_cpu_ns() -> Option<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `clock_gettime` only writes the `timespec` it is given.
    let ok = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0;
    ok.then(|| ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
}

/// CPU time the calling thread has used; not available on this platform.
#[cfg(not(unix))]
pub fn thread_cpu_ns() -> Option<u64> {
    None
}

/// One phase of a [`ProfileReport`], with the phases nested in it.
#[derive(Debug, Clone, Serialize)]
pub struct PhaseReport {
    pub name: &'static str,
    /// Wall time summed over the threads that worked in the phase.
    pub wall_ms: f64,
    /// CPU time of those threads, where the platform reports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_ms: Option<f64>,
    /// Whether the time is a sampled estimate rather than a measurement.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub estimated: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<PhaseReport>,
}

/// A snapshot of a [`RunProfile`]: serialize it with serde, or print it with `{}`.
#[derive(Debug, Clone, Serialize)]
pub struct ProfileReport {
    /// Time since the profile was created.
    pub elapsed_ms: f64,
    pub phases: Vec<PhaseReport>,
    /// Share of the elapsed time the calling thread waited for decoded chunks.
    pub stall_pct: f64,
    pub chunks: u64,
    pub decoded_bytes: u64,
    pub windows: u64,
    pub pixels_read: u64,
    pub samples_indexed: u64,
    /// `samples_indexed` as a share of `pixels_read` (above 100 with sub-pixel sampling
    /// weights or overlay areas).
    pub indexed_pct: f64,
    pub transform_failures: u64,
    pub cells: u64,
    pub rows_serialized: u64,
    pub spill_runs: u64,
    pub forced_compaction_flushes: u64,
    pub peak_active_bytes: u64,
    pub peak_decoded_bytes: u64,
    pub peak_compaction_bytes: u64,
}

impl ProfileReport {
    /// The report as a JSON object.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }

    /// The phase called `name`, at any depth.
    pub fn phase(&self, name: &str) -> Option<&PhaseReport> {
        fn find<'a>(phases: &'a [PhaseReport], name: &str) -> Option<&'a PhaseReport> {
            phases.iter().find_map(|p| {
                if p.name == name {
                    Some(p)
                } else {
                    find(&p.children, name)
                }
            })
        }
        find(&self.phases, name)
    }
}

impl fmt::Display for ProfileReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn line(
            f: &mut fmt::Formatter<'_>,
            phase: &PhaseReport,
            depth: usize,
            elapsed_ms: f64,
        ) -> fmt::Result {
            let name = format!("{}{}", "  ".repeat(depth), phase.name);
            let cpu = phase
                .cpu_ms
                .map_or_else(|| "         -".to_string(), |c| format!("{c:>10.1}"));
            writeln!(
                f,
                "  {name:<20} {:>10.1} {cpu} {:>6.1}%{}",
                phase.wall_ms,
                percent(phase.wall_ms, elapsed_ms),
                if phase.estimated { "  (estimated)" } else { "" }
            )?;
            phase
                .children
                .iter()
                .try_for_each(|child| line(f, child, depth + 1, elapsed_ms))
        }
        writeln!(f, "raster_h3 profile: {:.1} ms elapsed", self.elapsed_ms)?;
        writeln!(
            f,
            "  {:<20} {:>10} {:>10} {:>7}",
            "phase", "wall ms", "cpu ms", "of run"
        )?;
        for phase in &self.phases {
            line(f, phase, 0, self.elapsed_ms)?;
        }
        writeln!(
            f,
            "  (wall time is summed over worker threads, so phases can exceed the run)"
        )?;
        writeln!(
            f,
            "  chunks {} ({:.1} MiB decoded), windows {}, stall {:.1}%",
            self.chunks,
            self.decoded_bytes as f64 / 1048576.0,
            self.windows,
            self.stall_pct
        )?;
        writeln!(
            f,
            "  pixels read {}, indexed {} ({:.1}%), transform failures {}",
            self.pixels_read, self.samples_indexed, self.indexed_pct, self.transform_failures
        )?;
        writeln!(
            f,
            "  cells {}, rows serialized {}, spill runs {}, forced compaction flushes {}",
            self.cells, self.rows_serialized, self.spill_runs, self.forced_compaction_flushes
        )?;
        write!(
            f,
            "  peak bytes: horizon maps {}, decoded chunks {}, compaction {}",
            self.peak_active_bytes, self.peak_decoded_bytes, self.peak_compaction_bytes
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phases_and_counters_accumulate_across_threads() {
        let profile = RunProfile::new();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..100 {
                        profile.add(Phase::Decode, 1_000_000, Some(500_000));
                        profile.count(Counter::Chunks, 1);
                        profile.count(Counter::PixelsRead, 10);
                    }
                    profile.peak(Counter::PeakActiveBytes, 7);
                });
            }
        });
        profile.peak(Counter::PeakActiveBytes, 3);
        profile.add(Phase::Kernel, 10_000_000, None);
        profile.add(Phase::KernelCrs, 4_000_000, None);
        profile.add(Phase::KernelIndex, 9_000_000, None);
        profile.count(Counter::SamplesIndexed, 1000);
        let report = profile.report();
        let decode = report.phase("tiff_decode").unwrap();
        assert_eq!(decode.wall_ms, 400.0);
        assert_eq!(decode.cpu_ms, Some(200.0));
        assert_eq!(
            (report.chunks, report.pixels_read, report.peak_active_bytes),
            (400, 4000, 7)
        );
        assert_eq!(report.indexed_pct, 25.0);
        // The estimated split never exceeds the kernel's time.
        let kernel = report.phase("kernel").unwrap();
        assert_eq!(kernel.wall_ms, 10.0);
        let parts: f64 = kernel.children.iter().map(|c| c.wall_ms).sum();
        assert!((parts - 10.0).abs() < 1e-9);
        assert_eq!(report.phase("crs_transform").unwrap().wall_ms, 4.0);
        assert_eq!(report.phase("grid_index").unwrap().wall_ms, 6.0);
        assert!(report.phase("grid_index").unwrap().estimated);
        assert!(report.phase("spill").is_some() && report.phase("nope").is_none());
        profile.mark_exact_kernel_split();
        assert!(!profile.report().phase("grid_index").unwrap().estimated);
    }

    #[test]
    fn report_prints_and_serializes() {
        let profile = RunProfile::new();
        let value = profile.time(Phase::Serialize, || (0..10_000u64).sum::<u64>());
        assert_eq!(value, 49_995_000);
        profile.count(Counter::Cells, 12);
        let report = profile.report();
        assert!(report.phase("serialize").unwrap().wall_ms >= 0.0);
        let text = report.to_string();
        for needle in [
            "raster_h3 profile",
            "tiff_decode",
            "kernel",
            "horizon",
            "serialize",
            "cells 12",
        ] {
            assert!(text.contains(needle), "{needle} missing from:\n{text}");
        }
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert_eq!(json["cells"], 12);
        assert_eq!(json["phases"][1]["name"], "tiff_decode");
        assert!(json["elapsed_ms"].as_f64().unwrap() >= 0.0);
    }

    #[test]
    fn timers_measure_this_thread() {
        let timer = PhaseTimer::start();
        let mut x = 0u64;
        for i in 0..200_000u64 {
            x = x.wrapping_mul(31).wrapping_add(i);
        }
        std::hint::black_box(x);
        let (wall, cpu) = timer.elapsed();
        assert!(wall > 0);
        if cfg!(unix) {
            assert!(cpu.is_some());
        }
    }
}
