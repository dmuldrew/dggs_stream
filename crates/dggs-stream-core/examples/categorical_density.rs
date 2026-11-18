//! Categorical streaming density benchmark.
//!
//! Generates a synthetic land-cover-like categorical GeoTIFF (patchy classes with
//! pixel-scale speckle and nodata "ocean"), scans it with the categorical streamer and
//! reports windows, spill runs, peak active/worker bytes, CPU time, peak RSS and the
//! distribution of distinct classes per cell. It can also dump every output cell in a
//! canonical, bit-exact text form so two builds can be compared.
//!
//! ```text
//! cargo run --release -p dggs-stream-core --example categorical_density -- \
//!     gen <out.tif> [width height [speckle [local]]]
//! cargo run --release -p dggs-stream-core --example categorical_density -- \
//!     run <raster.tif> <grid> <h3_res> [dump.txt] [budget_mb]
//! ```
//!
//! `run` takes any categorical GeoTIFF (such as `data/LF2024_FBFM40_HI.tif`); `h3_res`
//! is an H3 level, and other grids use the level closest to it in cell area. Two
//! synthetic regimes are useful: `gen noisy.tif 8192 6144 0.12` (12% uniform speckle,
//! ~40 classes per res-8 cell) and `gen smooth.tif 8192 6144 0.03 local` (3% speckle
//! between similar classes, ~12 classes per res-8 cell). Compare two builds by diffing
//! their dumps.

#[path = "../tests/it/helpers.rs"]
mod helpers;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig,
};
use dggs_stream_core::grid::GridKind;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

fn hash(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

fn unit(seed: u64, x: i64, y: i64) -> f64 {
    let h = hash(
        seed ^ (x as u64).wrapping_mul(0x9e3779b97f4a7c15)
            ^ (y as u64).wrapping_mul(0xbf58476d1ce4e5b9),
    );
    (h >> 11) as f64 / (1u64 << 53) as f64
}

/// Smooth value noise with lattice spacing `scale` pixels.
fn value_noise(seed: u64, x: u32, y: u32, scale: f64) -> f64 {
    let fx = x as f64 / scale;
    let fy = y as f64 / scale;
    let (x0, y0) = (fx.floor() as i64, fy.floor() as i64);
    let (tx, ty) = (fx - x0 as f64, fy - y0 as f64);
    let s = |t: f64| t * t * (3.0 - 2.0 * t);
    let (sx, sy) = (s(tx), s(ty));
    let a = unit(seed, x0, y0);
    let b = unit(seed, x0 + 1, y0);
    let c = unit(seed, x0, y0 + 1);
    let d = unit(seed, x0 + 1, y0 + 1);
    (a + (b - a) * sx) * (1.0 - sy) + (c + (d - c) * sx) * sy
}

/// Class 1..=40, or 0 (nodata) for "ocean".
/// `speckle` is the fraction of pixels replaced by a random class; `local` draws that
/// class near the patch class (as misclassification between similar types does)
/// instead of uniformly from all 40.
fn class_at(x: u32, y: u32, speckle: f64, local: bool) -> u8 {
    let land = value_noise(1, x, y, 1500.0) * 0.7 + value_noise(2, x, y, 300.0) * 0.3;
    if land < 0.38 {
        return 0;
    }
    // Patch structure at a few scales, quantized into 40 classes with a skewed mapping.
    let f = value_noise(3, x, y, 120.0) * 0.55
        + value_noise(4, x, y, 30.0) * 0.3
        + value_noise(5, x, y, 8.0) * 0.15;
    let mut class = ((f * f) * 40.0) as u8 % 40 + 1;
    // Pixel-scale speckle, as in classified land-cover rasters.
    let r = unit(6, x as i64, y as i64);
    if r < speckle {
        let u = unit(7, x as i64, y as i64);
        class = if local {
            ((class as i64 - 1 + (u * 7.0) as i64 - 3).rem_euclid(40) + 1) as u8
        } else {
            (u * 40.0) as u8 % 40 + 1
        };
    }
    class
}

/// User CPU, system CPU (seconds) and peak RSS (bytes) of this process.
#[cfg(unix)]
fn rusage() -> (f64, f64, u64) {
    // SAFETY: `getrusage` fills the zeroed struct it is given and has no other effect.
    let u = unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        u
    };
    let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 * 1e-6;
    // `ru_maxrss` is in kilobytes on Linux but already in bytes on macOS.
    let scale = if cfg!(target_os = "macos") { 1 } else { 1024 };
    (t(u.ru_utime), t(u.ru_stime), u.ru_maxrss as u64 * scale)
}

/// `getrusage` is Unix-only: CPU times and peak RSS are not measured here and print as 0.
#[cfg(not(unix))]
fn rusage() -> (f64, f64, u64) {
    (0.0, 0.0, 0)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("gen") => {
            let w: u32 = args.get(2).map_or(8192, |s| s.parse().unwrap());
            let h: u32 = args.get(3).map_or(6144, |s| s.parse().unwrap());
            let speckle: f64 = args.get(4).map_or(0.12, |s| s.parse().unwrap());
            let local = args.get(5).is_some_and(|s| s == "local");
            helpers::write_tiled(
                Path::new(&args[1]),
                (w, h),
                (512, 512),
                helpers::Crs::Geographic {
                    lon: -158.0,
                    lat: 22.0,
                    pixel_deg: 0.00027,
                },
                // Longer than 4 bytes: the fixture writer always stores ASCII
                // values out of line, which TIFF only allows above 4 bytes.
                "0.0000",
                |x, y| class_at(x, y, speckle, local),
            );
        }
        Some("run") => run(&args[1..]),
        _ => eprintln!("usage: gen <out.tif> [w h [speckle [local]]] | run <tif> <grid> <res> [dump] [budget_mb]"),
    }
}

fn run(args: &[String]) {
    let path = &args[0];
    let grid = GridKind::parse(&args[1]).expect("grid");
    // The resolution is an H3 level; other grids use the level closest in cell area.
    let h3_res: u8 = args[2].parse().unwrap();
    let res = if grid == GridKind::H3 {
        h3_res
    } else {
        let area = h3o::Resolution::try_from(h3_res).unwrap().area_m2();
        dggs_stream_core::resolution::area_matched_resolution(grid, area)
    };
    let dump = args.get(3).filter(|s| s.as_str() != "-");
    let mut config = MultiResolutionConfig::new(vec![res]).with_grid(grid);
    if let Some(mb) = args.get(4) {
        config.aggregation_budget_bytes = mb.parse::<usize>().unwrap() << 20;
    }
    let reader = GeoTiffStreamReader::open(path).expect("open raster");
    let start = Instant::now();
    let mut streamer = MultiCategoricalHorizonStreamer::new(reader, &config).unwrap();
    let mut class_hist = vec![0u64; 42];
    let mut cells = 0u64;
    let mut lines: Vec<String> = Vec::new();
    while streamer
        .drain_completed_into(4096, |_, rec| {
            let acc = &rec.accumulator;
            cells += 1;
            class_hist[acc.unique_classes().min(41)] += 1;
            if dump.is_some() {
                let mut classes = Vec::new();
                acc.for_each_class(|k, v| classes.push((k, v.to_bits())));
                classes.sort_unstable();
                let (maj, cnt, frac) = acc.majority();
                lines.push(format!(
                    "{:016x} {} {} {:x} {:x} {} {:x} {:x} {:?} {}",
                    rec.h3_index,
                    rec.resolution,
                    maj,
                    cnt.to_bits(),
                    frac.to_bits(),
                    acc.unique_classes(),
                    acc.shannon_entropy().to_bits(),
                    acc.total_count.to_bits(),
                    classes,
                    acc.histogram_json(),
                ));
            }
        })
        .unwrap()
        > 0
    {}
    let wall = start.elapsed().as_secs_f64();
    let (user, sys, rss) = rusage();
    let m = streamer.metrics();
    if let Some(dump) = dump {
        lines.sort_unstable();
        let mut f = std::io::BufWriter::new(std::fs::File::create(dump).unwrap());
        for l in &lines {
            writeln!(f, "{l}").unwrap();
        }
    }
    // Distinct-class percentiles over cells.
    let pct = |p: f64| {
        let target = (cells as f64 * p).ceil() as u64;
        let mut acc = 0;
        for (k, &n) in class_hist.iter().enumerate() {
            acc += n;
            if acc >= target {
                return k;
            }
        }
        41
    };
    let le = |k: usize| class_hist[..=k].iter().sum::<u64>() as f64 / cells.max(1) as f64 * 100.0;
    println!(
        "grid={} res={} cells={} wall={:.2}s user={:.2}s sys={:.2}s rss={:.0}MiB windows={} waves={} spill_runs={} peak_active={:.1}MiB peak_worker={:.1}MiB",
        grid.as_str(),
        res,
        cells,
        wall,
        user,
        sys,
        rss as f64 / 1048576.0,
        m.jobs,
        m.waves,
        streamer.spill_run_count(),
        streamer.peak_active_bytes() as f64 / 1048576.0,
        m.peak_worker_bytes as f64 / 1048576.0,
    );
    println!(
        "  classes/cell p50={} p90={} p99={} p99.9={} max={}  <=2:{:.1}% <=4:{:.1}% <=8:{:.1}% <=16:{:.1}%",
        pct(0.5),
        pct(0.9),
        pct(0.99),
        pct(0.999),
        class_hist.iter().rposition(|&n| n > 0).unwrap_or(0),
        le(2),
        le(4),
        le(8),
        le(16),
    );
}
