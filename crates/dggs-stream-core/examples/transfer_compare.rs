//! Time one scan in a chosen transfer mode or sampling preset, in a fresh process.
//!
//! Usage: transfer_compare TIFF GRID RESOLUTION KIND MODE [CELLS_CSV]
//!
//! KIND is `continuous` or `categorical`. MODE is `center` or another sampling preset of
//! the default kernel (`rgss`, `5point`, `9point`, `16point`, ...; sub-pixel samples),
//! `overlay`, `mass` (mass-preserving overlay), `nearest`, `bilinear`, `bicubic` or
//! `lanczos` (interpolation; categorical scans accept only `nearest`), or `auto`. An
//! interpolation or `auto` can take a sampling preset after `+`, e.g. `bicubic+16point`
//! (sub-cell samples) or `auto+rgss`.
//!
//! Prints `MODE_RUN CELLS MILLISECONDS PEAK_RSS_KIB RATIO`: the mode actually run (`auto`
//! reports its choice), the cells emitted, the scan time, the process's peak resident
//! memory from `/proc/self/status` (`-` where that file doesn't exist), and the mean cell
//! area over the ground area of the raster's centre pixel, the ratio `auto` decides by.
//! With CELLS_CSV it also writes one line per cell for accuracy comparisons:
//! `cell_id,count,mean` (continuous) or `cell_id,total,class:weight;class:weight...`
//! (categorical). Time separate runs without it.
//! `scripts/benchmark_transfer_modes.py` drives it.
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::aggregator::SamplingPattern;
use dggs_stream_core::grid::GridKind;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule, TileDescriptor};
use dggs_stream_core::resolution::pixel_ground_area_m2;
use dggs_stream_core::transfer::{Interpolation, TransferMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("missing raster path")?;
    let grid = GridKind::parse(&args.next().ok_or("missing grid")?).ok_or("unknown grid")?;
    let resolution: u8 = args.next().ok_or("missing resolution")?.parse()?;
    let categorical = match args.next().as_deref() {
        Some("continuous") => false,
        Some("categorical") => true,
        _ => return Err("expected continuous or categorical".into()),
    };
    let mode = args.next().ok_or("missing mode")?;
    let mut output = args
        .next()
        .map(File::create)
        .transpose()?
        .map(BufWriter::new);
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }

    let tile = TileDescriptor::new(0, GeoTiffStreamReader::open(&path)?, None)?;
    let pixel_m2 = pixel_ground_area_m2(&tile)?;
    let ratio = grid.area_m2(resolution) / pixel_m2;

    let mut config = MultiResolutionConfig::single(resolution);
    config.grid = grid;
    // `<interpolation or auto>+<sampling preset>`: the preset applies to the whole mode.
    let (mode, preset) = match mode.split_once('+') {
        Some((base, preset)) => {
            config.sampling = SamplingPattern::parse(preset)?;
            (base.to_string(), format!("+{preset}"))
        }
        None => (mode, String::new()),
    };
    let label = match mode.as_str() {
        "overlay" => {
            config.transfer = TransferMode::OVERLAY;
            "overlay".to_string()
        }
        "mass" => {
            config.transfer = TransferMode::Overlay {
                weighted: true,
                mass_preserve: true,
            };
            "mass".to_string()
        }
        "auto" => {
            // Resolved here only to report the choice, by the same rule the streamers use
            // (sampling included); the scan itself runs `Auto`.
            let mosaic = MosaicReader::open(
                std::slice::from_ref(&PathBuf::from(&path)),
                None,
                None,
                OverlapRule::default(),
            )?;
            config.transfer = TransferMode::Auto;
            let chosen = TransferMode::Auto.resolve_for_mosaic(&mosaic, &config, categorical);
            let name = match chosen {
                TransferMode::Interpolate(method) => format!("{method:?}").to_lowercase(),
                other => other.as_str().to_string(),
            };
            format!("auto:{name}{preset}")
        }
        name => match Interpolation::parse(name) {
            Some(method) => {
                config.transfer = TransferMode::Interpolate(method);
                format!("{name}{preset}")
            }
            None if preset.is_empty() => {
                config.sampling = SamplingPattern::parse(name)?;
                name.to_string()
            }
            None => {
                return Err(format!(
                    "'{name}{preset}': only an interpolation or auto takes +preset"
                )
                .into())
            }
        },
    };

    let start = Instant::now();
    let mut cells = 0usize;
    if categorical {
        let reader = GeoTiffStreamReader::open(&path)?;
        let mut stream = MultiCategoricalHorizonStreamer::new(reader, &config)?;
        while !stream.is_finished() {
            let records = stream.fetch_next_batch(4096)?;
            cells += records.len();
            if let Some(writer) = &mut output {
                for record in records {
                    let acc = &record.accumulator;
                    write!(writer, "{},{},", record.cell_id, acc.total_count)?;
                    let mut first = true;
                    let mut result = Ok(());
                    acc.for_each_class(|class, weight| {
                        if result.is_ok() {
                            let sep = if first { "" } else { ";" };
                            result = write!(writer, "{sep}{class}:{weight}");
                            first = false;
                        }
                    });
                    result?;
                    writeln!(writer)?;
                }
            }
        }
    } else {
        let reader = GeoTiffStreamReader::open(&path)?;
        let mut stream = MultiScanHorizonStreamer::new(reader, &config)?;
        while !stream.is_finished() {
            let records = stream.fetch_next_batch(4096)?;
            cells += records.len();
            if let Some(writer) = &mut output {
                for record in records {
                    let acc = &record.accumulator;
                    writeln!(writer, "{},{},{}", record.cell_id, acc.count, acc.mean())?;
                }
            }
        }
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1e3;
    if let Some(mut writer) = output {
        writer.flush()?;
    }
    let peak_rss_kib = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| {
                    value
                        .trim()
                        .trim_end_matches("kB")
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
        })
        .map_or_else(|| "-".to_string(), |kib| kib.to_string());
    println!("{label} {cells} {elapsed_ms:.3} {peak_rss_kib} {ratio:.6}");
    Ok(())
}
