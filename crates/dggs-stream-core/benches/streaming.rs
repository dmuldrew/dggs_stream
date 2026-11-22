//! End-to-end streaming baselines; fixture generation is outside the measured loop.

#[path = "../tests/it/helpers.rs"]
mod helpers;
#[path = "streaming_support/mod.rs"]
mod support;

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dggs_stream_core::aggregator::accumulator::ContinuousAccumulator;
use dggs_stream_core::aggregator::categorical::CategoricalAccumulator;
use dggs_stream_core::aggregator::cell_hash::CellIdBuildHasher;
use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig, MultiScanHorizonStreamer, QuantilePlan,
    QuantileTarget,
};
use dggs_stream_core::aggregator::quantiles::QuantileSketch;
use dggs_stream_core::aggregator::sampling::SamplingPattern;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;

fn streaming(c: &mut Criterion) {
    let (_file, path) = helpers::TestGeoTiffBuilder::new(256, 256)
        .create_f32_tempfile(|col, row| ((col + row) % 8) as f32);
    let mut group = c.benchmark_group("streaming");
    group.throughput(Throughput::Elements(256 * 256));
    for (name, config) in [
        ("continuous", MultiResolutionConfig::single(8)),
        (
            "multiresolution",
            MultiResolutionConfig::new(vec![6, 7, 8, 9]),
        ),
        ("supersampling", {
            let mut config = MultiResolutionConfig::single(8);
            config.sampling = SamplingPattern::rgss();
            config
        }),
    ] {
        group.bench_with_input(BenchmarkId::new(name, "256x256"), &config, |b, config| {
            b.iter(|| {
                let reader = GeoTiffStreamReader::open(&path).unwrap();
                let mut stream = MultiScanHorizonStreamer::new(reader, config).unwrap();
                loop {
                    let batch = stream.fetch_next_batch(32).unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    black_box(batch);
                }
            });
        });
    }
    group.bench_function("categorical/256x256", |b| {
        b.iter(|| {
            let reader = GeoTiffStreamReader::open(&path).unwrap();
            let mut stream =
                MultiCategoricalHorizonStreamer::new(reader, &MultiResolutionConfig::single(8))
                    .unwrap();
            loop {
                let batch = stream.fetch_next_batch(32).unwrap();
                if batch.is_empty() {
                    break;
                }
                black_box(batch);
            }
        });
    });
    group.finish();
}

fn single_quantile(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_quantile");
    group.throughput(Throughput::Elements(1));
    for (name, values) in [
        ("positive_16", (1..=16).map(f64::from).collect::<Vec<_>>()),
        ("mixed_1024", (-512..512).map(f64::from).collect()),
    ] {
        let mut acc = ContinuousAccumulator::with_quantiles();
        for value in values {
            acc.update(value);
        }
        let sketch = acc.quantiles.as_deref().unwrap();
        for (label, q) in [("p50", 0.5), ("p90", 0.9)] {
            let targets = [QuantileTarget::Percentile(q, label.into())];
            let plan = QuantilePlan::new(&targets);
            let mut out = Vec::with_capacity(1);
            let input = format!("{name}/{label}");
            group.bench_function(BenchmarkId::new("scalar", &input), |b| {
                b.iter(|| black_box(acc.quantile(black_box(q))));
            });
            group.bench_function(BenchmarkId::new("batch", &input), |b| {
                b.iter(|| black_box(sketch.quantiles_batch(black_box(&[q]), acc.min, acc.max)));
            });
            group.bench_function(BenchmarkId::new("plan", &input), |b| {
                b.iter(|| {
                    plan.evaluate_into(black_box(&acc), &mut out);
                    black_box(&out);
                });
            });
        }
    }
    group.finish();
}

fn hash_sensitive(c: &mut Criterion) {
    let categories: Vec<_> = (0..65536).map(|i| i64::from(i % 64)).collect();
    let values: Vec<_> = (0..65536).map(|i| f64::from(i % 4096 + 1)).collect();
    let mut group = c.benchmark_group("hash_sensitive");
    group.throughput(Throughput::Elements(65536));
    for classes in [8, 64] {
        let inputs: Vec<_> = categories
            .iter()
            .map(|category| category % classes)
            .collect();
        group.bench_with_input(
            BenchmarkId::new("categorical", classes),
            &classes,
            |b, _| {
                b.iter(|| {
                    let mut accumulator = CategoricalAccumulator::new();
                    for &category in &inputs {
                        accumulator.update(black_box(category));
                    }
                    black_box(accumulator);
                });
            },
        );
    }
    group.bench_function("quantile_sketch", |b| {
        b.iter(|| {
            let mut sketch = QuantileSketch::new();
            for &value in &values {
                sketch.insert(black_box(value));
            }
            black_box(sketch);
        });
    });
    group.finish();
}

fn h3_map(c: &mut Criterion) {
    let cells: Vec<_> = h3o::CellIndex::try_from(0x8a2a1072b59ffffu64)
        .unwrap()
        .parent(h3o::Resolution::Three)
        .unwrap()
        .children(h3o::Resolution::Ten)
        .map(u64::from)
        .collect();
    let mut group = c.benchmark_group("h3_map");
    group.throughput(Throughput::Elements((cells.len() * 2) as u64));
    group.bench_function("res10_insert_remove", |b| {
        b.iter(|| {
            let mut map: HashMap<u64, u64, CellIdBuildHasher> = HashMap::default();
            for &cell in &cells {
                map.insert(cell, cell);
            }
            for cell in &cells {
                black_box(map.remove(cell));
            }
            black_box(map);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    streaming,
    single_quantile,
    hash_sensitive,
    h3_map,
    pruning,
    grids,
    transfer,
    multiband,
    codecs,
    remote,
    sinks
);
criterion_main!(benches);

/// Local uncompressed synthetic tiles. Includes open/planning/decode/aggregation;
/// fixtures are created outside timing. One worker makes comparisons repeatable.
fn pruning(c: &mut Criterion) {
    use dggs_stream_core::crs::transformer::CrsTransformer;
    use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule};
    use std::sync::Arc;
    let mut group = c.benchmark_group("pruning");
    for (epsg, x, y) in [
        (3413, 1_000_000.0, -2_000_000.0),
        (3031, 1_000_000.0, 2_000_000.0),
        (2154, 700_000.0, 6_600_000.0),
        (3035, 4_000_000.0, 3_000_000.0),
    ] {
        let fixtures: Vec<_> = (0..16)
            .map(|i| {
                helpers::TestGeoTiffBuilder::new(128, 128)
                    .epsg(epsg)
                    .origin(
                        x + (i % 4) as f64 * 250_000.0,
                        y + (i / 4) as f64 * 250_000.0,
                    )
                    .pixel_size(100.0)
                    .create_constant_tempfile(1.0f32)
            })
            .collect();
        let paths: Vec<_> = fixtures.iter().map(|(_, p)| p.clone()).collect();
        let tf = CrsTransformer::from_epsg_code(epsg as u32).unwrap();
        let (lon, lat) = tf.transform_point(x + 6400.0, y - 6400.0).unwrap();
        for (name, rule, bbox) in [
            (
                "small_bbox",
                OverlapRule::First,
                Some([lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01]),
            ),
            ("disjoint_first", OverlapRule::First, None),
            ("disjoint_average", OverlapRule::Average, None),
        ] {
            let mut config = MultiResolutionConfig::single(7);
            config.aggregation_workers = Some(1);
            config.decode_workers = 1;
            config.bbox = bbox;
            let plan = MosaicReader::open(&paths, bbox, None, rule).unwrap();
            eprintln!(
                "pruning/{name}/{epsg}: tiles={} chunks={} overlaps={}",
                plan.tiles.len(),
                plan.chunk_refs.len(),
                plan.chunk_refs.iter().filter(|c| c.has_overlap).count()
            );
            group.bench_function(BenchmarkId::new(name, epsg), |b| {
                b.iter(|| {
                    let mosaic = Arc::new(MosaicReader::open(&paths, bbox, None, rule).unwrap());
                    let mut stream = MultiScanHorizonStreamer::new_mosaic(mosaic, &config).unwrap();
                    loop {
                        let batch = stream.fetch_next_batch(4096).unwrap();
                        if batch.is_empty() {
                            break;
                        }
                        black_box(batch);
                    }
                })
            });
        }
    }
    group.finish();
}

fn drain_continuous(
    reader: GeoTiffStreamReader,
    config: &MultiResolutionConfig,
) -> (usize, f64, f64) {
    let mut stream = MultiScanHorizonStreamer::new(reader, config).unwrap();
    let mut cells = 0usize;
    let mut count = 0.0f64;
    let mut sum = 0.0f64;
    loop {
        let batch = stream.fetch_next_batch(1024).unwrap();
        if batch.is_empty() {
            break;
        }
        cells += batch.len();
        for rec in &batch {
            count += rec.accumulator.count;
            sum += rec.accumulator.sum;
        }
        black_box(batch);
    }
    (cells, count, sum)
}

fn drain_multiband(
    reader: GeoTiffStreamReader,
    config: &MultiResolutionConfig,
    bands: dggs_stream_core::aggregator::multi_horizon::multiband::Bands<'_>,
) -> (usize, Vec<f64>, Vec<f64>) {
    use dggs_stream_core::aggregator::multi_horizon::multiband::MultiBandStreamer;
    let mut stream = MultiBandStreamer::new(reader, config, bands).unwrap();
    let mut cells = 0usize;
    let mut counts = Vec::new();
    let mut sums = Vec::new();
    loop {
        let batch = stream.fetch_next_batch(1024).unwrap();
        if batch.is_empty() {
            break;
        }
        cells += batch.len();
        for rec in &batch {
            if counts.is_empty() {
                counts.resize(rec.statistics.len(), 0.0);
                sums.resize(rec.statistics.len(), 0.0);
            }
            for (i, stat) in rec.statistics.iter().enumerate() {
                counts[i] += stat.count;
                sums[i] += stat.sum;
            }
        }
        black_box(batch);
    }
    (cells, counts, sums)
}

fn grids(c: &mut Criterion) {
    use dggs_stream_core::resolution::area_matched_resolution;
    use dggs_stream_core::GridKind;

    let (_file, path) = helpers::TestGeoTiffBuilder::new(256, 256)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|col, row| ((col * 7 + row * 11) % 4096) as f32);

    let target_area = GridKind::H3.area_m2(8);
    let mut group = c.benchmark_group("grids");
    group.throughput(Throughput::Elements(256 * 256));

    for grid in GridKind::ALL {
        let res = area_matched_resolution(grid, target_area);
        let mut config = MultiResolutionConfig::single(res).with_grid(grid);
        config.aggregation_workers = Some(1);
        config.decode_workers = 1;

        let (cells, count, _) =
            drain_continuous(GeoTiffStreamReader::open(&path).unwrap(), &config);
        assert!(cells > 0, "grids/{} produced no cells", grid.as_str());
        assert!(
            (count - 256.0 * 256.0).abs() < 1e-6,
            "grids/{} pixel count mismatch: {count}",
            grid.as_str()
        );

        group.bench_with_input(
            BenchmarkId::new("continuous", grid.as_str()),
            &config,
            |b, config| {
                b.iter(|| {
                    let reader = GeoTiffStreamReader::open(&path).unwrap();
                    black_box(drain_continuous(reader, config));
                });
            },
        );
    }
    group.finish();
}

fn transfer(c: &mut Criterion) {
    use dggs_stream_core::transfer::{Interpolation, TransferMode};

    let (_file, path) = helpers::TestGeoTiffBuilder::new(128, 128)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|col, row| ((col * 7 + row * 11) % 4096) as f32);

    let mut group = c.benchmark_group("transfer");
    group.throughput(Throughput::Elements(128 * 128));

    for (name, res, mode) in [
        ("assign_centers", 8u8, TransferMode::AssignCenters),
        (
            "overlay",
            8u8,
            TransferMode::Overlay {
                weighted: true,
                mass_preserve: false,
            },
        ),
        (
            "overlay_mass_preserve",
            8u8,
            TransferMode::Overlay {
                weighted: true,
                mass_preserve: true,
            },
        ),
        (
            "interpolate_bilinear",
            10u8,
            TransferMode::Interpolate(Interpolation::Bilinear),
        ),
    ] {
        let mut config = MultiResolutionConfig::single(res);
        config.aggregation_workers = Some(1);
        config.decode_workers = 1;
        config.transfer = mode;

        let (cells, count, _) =
            drain_continuous(GeoTiffStreamReader::open(&path).unwrap(), &config);
        assert!(cells > 0, "transfer/{name} produced no cells");
        assert!(count > 0.0, "transfer/{name} produced zero total count");

        group.bench_with_input(BenchmarkId::new(name, "128x128"), &config, |b, config| {
            b.iter(|| {
                let reader = GeoTiffStreamReader::open(&path).unwrap();
                black_box(drain_continuous(reader, config));
            });
        });
    }
    group.finish();
}

fn multiband(c: &mut Criterion) {
    use dggs_stream_core::aggregator::multi_horizon::multiband::Bands;
    use dggs_stream_core::transfer::TransferMode;

    let (_f_int, path_int) =
        support::write_tiled_multiband_u16_tempfile(256, 256, 64, 64, 8, false);
    let (_f_pln, path_pln) = support::write_tiled_multiband_u16_tempfile(256, 256, 64, 64, 8, true);
    let (_f_ovr, path_ovr) = support::write_tiled_multiband_u16_tempfile(128, 128, 64, 64, 8, true);

    let mut group = c.benchmark_group("multiband");
    let mut ref_all_sums = None;
    let mut ref_sel_sums = None;

    for (name, path, pixels, selected, transfer_mode) in [
        (
            "interleaved_all",
            &path_int,
            256u64 * 256,
            None,
            TransferMode::AssignCenters,
        ),
        (
            "interleaved_selected",
            &path_int,
            256u64 * 256,
            Some(&[7usize, 2][..]),
            TransferMode::AssignCenters,
        ),
        (
            "planar_all",
            &path_pln,
            256u64 * 256,
            None,
            TransferMode::AssignCenters,
        ),
        (
            "planar_selected",
            &path_pln,
            256u64 * 256,
            Some(&[7usize, 2][..]),
            TransferMode::AssignCenters,
        ),
        (
            "planar_selected_overlay",
            &path_ovr,
            128u64 * 128,
            Some(&[7usize, 2][..]),
            TransferMode::OVERLAY,
        ),
    ] {
        let mut config = MultiResolutionConfig::single(8);
        config.aggregation_workers = Some(1);
        config.decode_workers = 1;
        config.transfer = transfer_mode;

        let bands = match selected {
            None => Bands::All,
            Some(sel) => Bands::Selected(sel),
        };
        let (cells, counts, sums) =
            drain_multiband(GeoTiffStreamReader::open(path).unwrap(), &config, bands);
        assert!(cells > 0, "multiband/{name} produced no cells");
        assert_eq!(counts.len(), selected.map_or(8, |s| s.len()));
        if transfer_mode.is_assign_centers() {
            for &cnt in &counts {
                assert!(
                    (cnt - pixels as f64).abs() < 1e-6,
                    "multiband/{name} count mismatch: {cnt}"
                );
            }
            if selected.is_none() {
                assert_eq!(sums, *ref_all_sums.get_or_insert_with(|| sums.clone()));
            } else {
                assert_eq!(sums, *ref_sel_sums.get_or_insert_with(|| sums.clone()));
            }
        }

        group.throughput(Throughput::Elements(pixels));
        group.bench_function(name, |b| {
            b.iter(|| {
                let reader = GeoTiffStreamReader::open(path).unwrap();
                let bands = match selected {
                    None => Bands::All,
                    Some(sel) => Bands::Selected(sel),
                };
                black_box(drain_multiband(reader, &config, bands));
            });
        });
    }
    group.finish();
}

fn codecs(c: &mut Criterion) {
    use support::TiledCodec;

    let mut group = c.benchmark_group("codecs");
    group.throughput(Throughput::Elements(256 * 256));

    let mut config = MultiResolutionConfig::single(8);
    config.aggregation_workers = Some(1);
    config.decode_workers = 1;

    let mut ref_f32_sum = None;
    for (name, codec, is_f32) in [
        ("tiled_uncompressed_f32", TiledCodec::UncompressedF32, true),
        (
            "tiled_deflate_f32",
            TiledCodec::DeflateF32 {
                fp_predictor: false,
            },
            true,
        ),
        (
            "tiled_deflate_fp3_f32",
            TiledCodec::DeflateF32 { fp_predictor: true },
            true,
        ),
        ("tiled_lzw_horiz_u16", TiledCodec::LzwU16Horiz, false),
    ] {
        let (_file, path) = support::write_tiled_codec_tempfile(256, 256, 64, 64, codec);
        let (cells, count, sum) =
            drain_continuous(GeoTiffStreamReader::open(&path).unwrap(), &config);
        assert!(cells > 0, "codecs/{name} produced no cells");
        assert!(
            (count - 256.0 * 256.0).abs() < 1e-6,
            "codecs/{name} pixel count mismatch: {count}"
        );
        if is_f32 {
            assert_eq!(sum, *ref_f32_sum.get_or_insert(sum));
        }

        group.bench_function(name, |b| {
            b.iter(|| {
                let reader = GeoTiffStreamReader::open(&path).unwrap();
                black_box(drain_continuous(reader, &config));
            });
        });
    }
    group.finish();
}

fn remote(c: &mut Criterion) {
    use dggs_stream_core::aggregator::multi_horizon::multiband::Bands;
    use dggs_stream_core::raster::http_range::DEFAULT_BLOCK_SIZE;
    use dggs_stream_core::raster::remote_prefetch::{
        coalesce_chunk_ranges, ChunkLocation, DEFAULT_MAX_COALESCE_BYTES, DEFAULT_MAX_COALESCE_GAP,
    };
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use support::TiledCodec;

    let mut group = c.benchmark_group("remote");

    let mut locations = Vec::with_capacity(1024);
    for run in 0..32u64 {
        let base = run * (32 * 4096 + 65536);
        for chunk in 0..32u64 {
            locations.push(ChunkLocation {
                tile_idx: 0,
                chunk_idx: (run * 32 + chunk) as u32,
                offset: base + chunk * 4096,
                length: 4096,
            });
        }
    }
    assert_eq!(
        coalesce_chunk_ranges(
            &locations,
            DEFAULT_MAX_COALESCE_GAP,
            DEFAULT_MAX_COALESCE_BYTES
        )
        .len(),
        32
    );
    group.throughput(Throughput::Elements(locations.len() as u64));
    group.bench_function("coalesce_chunk_ranges", |b| {
        b.iter(|| {
            black_box(coalesce_chunk_ranges(
                black_box(&locations),
                DEFAULT_MAX_COALESCE_GAP,
                DEFAULT_MAX_COALESCE_BYTES,
            ))
        });
    });

    let cog_bytes = Arc::new(support::write_tiled_codec_bytes(
        512,
        256,
        64,
        64,
        TiledCodec::DeflateF32 {
            fp_predictor: false,
        },
        DEFAULT_BLOCK_SIZE,
    ));
    let requests = Arc::new(AtomicUsize::new(0));
    let factory = support::memory_source_factory(Arc::clone(&cog_bytes), Arc::clone(&requests));
    let remote_path = Path::new("https://bench.invalid/tiled.tif");
    let mut cfg = MultiResolutionConfig::single(8);
    cfg.aggregation_workers = Some(1);
    cfg.decode_workers = 1;
    cfg.fetch_workers = 2;

    requests.store(0, Ordering::SeqCst);
    let reader =
        GeoTiffStreamReader::open_with_remote_source_factory(remote_path, Some(&factory)).unwrap();
    let (cells, count, _) = drain_continuous(reader, &cfg);
    let reqs = requests.load(Ordering::SeqCst);
    assert!(cells > 0);
    assert!((count - 512.0 * 256.0).abs() < 1e-6);
    assert!(
        (2..=4).contains(&reqs),
        "expected coalesced remote scan to take 2..=4 requests, got {reqs}"
    );

    group.throughput(Throughput::Elements(512 * 256));
    group.bench_function("transport_tiled_coalesced", |b| {
        b.iter(|| {
            let reader =
                GeoTiffStreamReader::open_with_remote_source_factory(remote_path, Some(&factory))
                    .unwrap();
            black_box(drain_continuous(reader, &cfg));
        });
    });

    let planar_bytes =
        support::write_tiled_multiband_u16_bytes(256, 256, 64, 64, 8, true, DEFAULT_BLOCK_SIZE);
    if let Some(server) = support::BenchHttpServer::try_start(planar_bytes) {
        server.requests.store(0, Ordering::SeqCst);
        let reader = GeoTiffStreamReader::open(&server.url).unwrap();
        let (cells, counts, _) = drain_multiband(reader, &cfg, Bands::Selected(&[7, 2]));
        assert!(cells > 0);
        assert_eq!(counts, vec![256.0 * 256.0, 256.0 * 256.0]);
        let http_reqs = server.requests.load(Ordering::SeqCst);
        assert_eq!(
            http_reqs, 3,
            "expected 1 probe + 2 coalesced plane ranges, got {http_reqs}"
        );

        group.throughput(Throughput::Elements(256 * 256));
        group.bench_function("http_planar_selected", |b| {
            b.iter(|| {
                let reader = GeoTiffStreamReader::open(&server.url).unwrap();
                black_box(drain_multiband(reader, &cfg, Bands::Selected(&[7, 2])));
            });
        });
    } else {
        eprintln!("remote/http_planar_selected: skipped (loopback networking unavailable)");
    }

    group.finish();
}

fn sinks(c: &mut Criterion) {
    use dggs_stream_core::columnar::ipc::write_ipc_stream;
    use dggs_stream_core::columnar::multiband::MultiBandBatchReader;
    use dggs_stream_core::columnar::ContinuousBatchReader;
    use dggs_stream_core::parquet::{write_multiband_parquet, ParquetExportConfig, ParquetWriter};

    let (_single_file, single_path) = helpers::TestGeoTiffBuilder::new(256, 256)
        .origin(-122.45, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|col, row| ((col * 7 + row * 11) % 4096) as f32);
    let (_mb_file, mb_path) =
        support::write_tiled_multiband_u16_tempfile(256, 256, 64, 64, 8, false);
    let out_dir = tempfile::tempdir().unwrap();
    let single_pq = out_dir.path().join("single.parquet");
    let mb_pq = out_dir.path().join("multiband.parquet");

    let mut cfg = MultiResolutionConfig::single(9);
    cfg.aggregation_workers = Some(1);
    cfg.decode_workers = 1;
    let pq_cfg = ParquetExportConfig {
        geoparquet: true,
        row_group_size: 1024,
        ..Default::default()
    };

    let mut group = c.benchmark_group("sinks");
    group.throughput(Throughput::Elements(256 * 256));

    let mut ipc_buf = Vec::with_capacity(1 << 20);
    {
        let mut reader =
            ContinuousBatchReader::open(std::slice::from_ref(&single_path), &cfg, None, 256, None)
                .unwrap();
        let schema = reader.schema();
        let rows = write_ipc_stream(&mut ipc_buf, &schema, &mut reader).unwrap();
        assert!(rows > 1000, "expected > 1000 rows, got {rows}");
        assert!(!ipc_buf.is_empty());
    }
    group.bench_function("ipc_single_band", |b| {
        b.iter(|| {
            ipc_buf.clear();
            let mut reader = ContinuousBatchReader::open(
                std::slice::from_ref(&single_path),
                &cfg,
                None,
                256,
                None,
            )
            .unwrap();
            let schema = reader.schema();
            let rows = write_ipc_stream(&mut ipc_buf, &schema, &mut reader).unwrap();
            black_box((rows, ipc_buf.len()));
        });
    });

    {
        ipc_buf.clear();
        let reader = GeoTiffStreamReader::open(&mb_path).unwrap();
        let mut batch_reader =
            MultiBandBatchReader::open(reader, &cfg, &[7, 2], None, 256, None).unwrap();
        let schema = batch_reader.schema();
        let rows = write_ipc_stream(&mut ipc_buf, &schema, &mut batch_reader).unwrap();
        assert!(rows > 1000, "expected > 1000 rows, got {rows}");
        assert!(!ipc_buf.is_empty());
    }
    group.bench_function("ipc_multiband", |b| {
        b.iter(|| {
            ipc_buf.clear();
            let reader = GeoTiffStreamReader::open(&mb_path).unwrap();
            let mut batch_reader =
                MultiBandBatchReader::open(reader, &cfg, &[7, 2], None, 256, None).unwrap();
            let schema = batch_reader.schema();
            let rows = write_ipc_stream(&mut ipc_buf, &schema, &mut batch_reader).unwrap();
            black_box((rows, ipc_buf.len()));
        });
    });

    {
        let reader = GeoTiffStreamReader::open(&single_path).unwrap();
        let streamer = MultiScanHorizonStreamer::new(reader, &cfg).unwrap();
        let rows = ParquetWriter::write_continuous_streamer_to_parquet(
            streamer,
            &single_pq,
            pq_cfg.clone(),
        )
        .unwrap();
        assert!(rows > 1000, "expected > 1000 rows, got {rows}");
    }
    group.bench_function("geoparquet_single_band", |b| {
        b.iter(|| {
            let _ = std::fs::remove_file(&single_pq);
            let reader = GeoTiffStreamReader::open(&single_path).unwrap();
            let streamer = MultiScanHorizonStreamer::new(reader, &cfg).unwrap();
            let rows = ParquetWriter::write_continuous_streamer_to_parquet(
                streamer,
                &single_pq,
                pq_cfg.clone(),
            )
            .unwrap();
            black_box(rows);
        });
    });

    {
        let reader = GeoTiffStreamReader::open(&mb_path).unwrap();
        let mut batch_reader =
            MultiBandBatchReader::open(reader, &cfg, &[7, 2], None, 256, None).unwrap();
        let rows = write_multiband_parquet(&mut batch_reader, &mb_pq, true, 1024).unwrap();
        assert!(rows > 1000, "expected > 1000 rows, got {rows}");
    }
    group.bench_function("geoparquet_multiband", |b| {
        b.iter(|| {
            let _ = std::fs::remove_file(&mb_pq);
            let reader = GeoTiffStreamReader::open(&mb_path).unwrap();
            let mut batch_reader =
                MultiBandBatchReader::open(reader, &cfg, &[7, 2], None, 256, None).unwrap();
            let rows = write_multiband_parquet(&mut batch_reader, &mb_pq, true, 1024).unwrap();
            black_box(rows);
        });
    });

    group.finish();
}
