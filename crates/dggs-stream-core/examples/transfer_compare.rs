//! Time one continuous scan in a chosen transfer mode or sampling preset, in a fresh process.
//!
//! Usage: transfer_compare TIFF GRID RESOLUTION MODE [CELLS_CSV]
//!
//! MODE is `center` or another sampling preset of the default kernel (`rgss`, `5point`,
//! `9point`, `16point`, ...), `overlay`, `mass` (mass-preserving overlay), `bilinear`,
//! `nearest`, `bicubic` or `lanczos` (interpolation), or `auto`.
//!
//! Prints `MODE_RUN CELLS MILLISECONDS PEAK_RSS_KIB`, where MODE_RUN is the mode actually
//! run (`auto` reports its choice) and PEAK_RSS_KIB is the process's peak resident memory
//! from `/proc/self/status` (`-` where that file doesn't exist). With CELLS_CSV, also
//! writes `cell_id,count,mean` for every emitted cell, for accuracy comparisons; time
//! separate runs without it.
//! `scripts/benchmark_transfer_modes.py` drives it.
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::aggregator::SamplingPattern;
use dggs_stream_core::grid::GridKind;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::raster::mosaic::TileDescriptor;
use dggs_stream_core::resolution::pixel_ground_area_m2;
use dggs_stream_core::transfer::{resolve_auto_transfer, Interpolation, TransferMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("missing raster path")?;
    let grid = GridKind::parse(&args.next().ok_or("missing grid")?).ok_or("unknown grid")?;
    let resolution: u8 = args.next().ok_or("missing resolution")?.parse()?;
    let mode = args.next().ok_or("missing mode")?;
    let mut output = args
        .next()
        .map(File::create)
        .transpose()?
        .map(BufWriter::new);
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }

    let mut config = MultiResolutionConfig::single(resolution);
    config.grid = grid;
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
            // Resolve here only to report the choice; the scan itself runs `Auto`.
            let tile = TileDescriptor::new(0, GeoTiffStreamReader::open(&path)?, None)?;
            let chosen = resolve_auto_transfer(
                grid,
                &[resolution],
                pixel_ground_area_m2(&tile)?,
                false,
                true,
            );
            config.transfer = TransferMode::Auto;
            match chosen {
                TransferMode::Interpolate(_) => "auto:bilinear".to_string(),
                other => format!("auto:{}", other.as_str()),
            }
        }
        name => match Interpolation::parse(name) {
            Some(method) => {
                config.transfer = TransferMode::Interpolate(method);
                name.to_string()
            }
            None => {
                config.sampling = SamplingPattern::parse(name)?;
                name.to_string()
            }
        },
    };

    let start = Instant::now();
    let mut stream = MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(&path)?, &config)?;
    let mut cells = 0usize;
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
    println!("{label} {cells} {elapsed_ms:.3} {peak_rss_kib}");
    Ok(())
}
