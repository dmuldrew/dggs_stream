use super::*;
use dggs_stream_core::aggregator::multi_horizon::multiband::{Bands, MultiBandStreamer};
use dggs_stream_core::aggregator::multi_horizon::MultiResolutionConfig;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::run_profile::RunProfile;

#[test]
fn twelve_band_range_scan_decodes_only_selected_planes_and_alpha() {
    if !MockHttpServer::is_networking_supported() {
        return;
    }
    let mut reference_sums = None;
    for (layout, chunks, corrupt_unused) in [
        ("interleaved", 4, false),
        ("planar", 12, false),
        ("planar", 12, true),
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/multiband-12-{layout}.tif"));
        let mut bytes = std::fs::read(&path).unwrap();
        if corrupt_unused {
            // Any accidental decode of an unselected plane must fail even if its
            // bytes are already in the HTTP header cache.
            let reader = GeoTiffStreamReader::open(&path).unwrap();
            let info = reader.chunk_info.unwrap();
            // Relocate the TIFF's tiles after the probe block. All three selected
            // planes must now be served from HTTP, one contiguous range per plane.
            let offsets = info
                .chunk_offsets
                .iter()
                .flat_map(|&v| (v as u32).to_le_bytes())
                .collect::<Vec<_>>();
            let table = bytes
                .windows(offsets.len())
                .position(|w| w == offsets)
                .unwrap();
            let original = bytes.clone();
            for (index, (&offset, &len)) in info
                .chunk_offsets
                .iter()
                .zip(info.chunk_bytes.iter())
                .enumerate()
            {
                let band = index / 4 + 1;
                let start = bytes.len();
                bytes[table + index * 4..table + index * 4 + 4]
                    .copy_from_slice(&(start as u32).to_le_bytes());
                bytes.extend_from_slice(&original[offset as usize..(offset + len) as usize]);
                if ![3, 1, 12].contains(&band) {
                    bytes[start..].fill(0xff);
                }
            }
        }
        let server = MockHttpServer::start(bytes);
        let start = std::time::Instant::now();
        let reader = GeoTiffStreamReader::open(format!("{}/fixture.tif", server.url_base)).unwrap();
        // The probe holds the first block. Every subsequent body byte must
        // belong to a selected tile or alpha tile outside that block.
        let probe = dggs_stream_core::raster::http_range::DEFAULT_BLOCK_SIZE as u64;
        let info = reader.chunk_info.as_ref().unwrap();
        let planar_transfer = probe
            + info
                .chunk_offsets
                .iter()
                .zip(info.chunk_bytes.iter())
                .enumerate()
                .filter(|(i, (offset, length))| {
                    [3, 1, 12].contains(&(i / 4 + 1)) && **offset + **length > probe
                })
                .map(|(_, (_, length))| *length)
                .sum::<u64>();
        let profile = RunProfile::new();
        let cfg = MultiResolutionConfig {
            profile: Some(profile.clone()),
            ..MultiResolutionConfig::single(12)
        };
        let mut scan = MultiBandStreamer::new(reader, &cfg, Bands::Selected(&[3, 1])).unwrap();
        let mut sums = [0.0; 2];
        let mut counts = [0.0; 2];
        loop {
            let rows = scan.fetch_next_batch(64).unwrap();
            if rows.is_empty() {
                break;
            }
            for row in rows {
                for (i, stat) in row.statistics.into_iter().enumerate() {
                    sums[i] += stat.sum;
                    counts[i] += stat.count;
                }
            }
        }
        drop(scan);
        let elapsed = start.elapsed();
        assert_eq!(counts, [8652.0, 8755.0]);
        assert_eq!(profile.report().chunks, chunks);
        assert_eq!(sums, *reference_sums.get_or_insert(sums));
        if layout == "planar" {
            let requests = server.request_count.load(Ordering::SeqCst);
            assert_eq!(requests, if corrupt_unused { 4 } else { 2 });
            assert_eq!(
                server.bytes_served.load(Ordering::SeqCst) as u64,
                planar_transfer
            );
        }
        if corrupt_unused {
            continue;
        }
        eprintln!(
            "{layout}: {chunks} decoded chunks, {} transferred bytes, {} requests, {elapsed:.2?}",
            server.bytes_served.load(Ordering::SeqCst),
            server.request_count.load(Ordering::SeqCst)
        );
    }
}
