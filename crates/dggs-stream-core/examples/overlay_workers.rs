//! Measure a single-band H3 overlay scan in a fresh process.
//! Usage: overlay_workers TIFF RESOLUTION WORKERS [ordinary|mass] [STATE_FILE]
//! Use a separate run with STATE_FILE to compare every emitted cell and accumulator.
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

use dggs_stream_core::aggregator::multi_horizon::{
    encode_partial_state, MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::transfer::TransferMode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("missing raster path")?;
    let resolution = args.next().ok_or("missing resolution")?.parse()?;
    let workers = args.next().ok_or("missing workers")?.parse()?;
    let mass_preserve = match args.next().as_deref() {
        None | Some("ordinary") => false,
        Some("mass") => true,
        _ => return Err("expected ordinary or mass".into()),
    };
    let mut output = args
        .next()
        .map(File::create_new)
        .transpose()?
        .map(BufWriter::new);
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let mut config = MultiResolutionConfig::single(resolution);
    config.aggregation_workers = Some(workers);
    config.transfer = TransferMode::Overlay {
        weighted: true,
        mass_preserve,
    };
    let start = Instant::now();
    let mut stream = MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(path)?, &config)?;
    let mut count = 0;
    while !stream.is_finished() {
        let records = stream.fetch_next_batch(2048)?;
        count += records.len();
        if let Some(writer) = &mut output {
            for record in records {
                let state = encode_partial_state(&record.accumulator);
                writer.write_all(&[record.resolution])?;
                writer.write_all(&record.cell_id.to_le_bytes())?;
                writer.write_all(&(state.len() as u64).to_le_bytes())?;
                writer.write_all(&state)?;
            }
        }
    }
    if let Some(mut writer) = output {
        writer.flush()?;
    }
    println!("{count} {:.3}", start.elapsed().as_secs_f64() * 1e3);
    Ok(())
}
