use super::super::eviction_space::CellKeys;
use super::super::sharded_map::ShardedResolutionMap;
use super::super::spill::SpillRuns;
use super::pipeline::{
    run_pipelined, run_serial, CellMaps, MergeTarget, PipelineScratch, WindowJobs,
};
use crate::aggregator::accumulator::ContinuousAccumulator;
use crate::aggregator::categorical::CategoricalAccumulator;
use crate::aggregator::cell_hash::CellIdBuildHasher;
use crate::error::{DggsStreamError, Result};
use crate::raster::geotiff::GeoTiffStreamReader;
use crate::raster::mosaic::MosaicReader;
use crate::raster::RasterChunk;
use h3o::{LatLng, Resolution};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tiff::decoder::DecodingResult;

fn test_mosaic() -> (tempfile::NamedTempFile, MosaicReader) {
    use tiff::encoder::{colortype::Gray8, TiffEncoder};
    use tiff::tags::Tag;

    let file = tempfile::NamedTempFile::new().unwrap();
    {
        let mut encoder = TiffEncoder::new(file.reopen().unwrap()).unwrap();
        let mut image = encoder.new_image::<Gray8>(1, 1).unwrap();
        image
            .encoder()
            .write_tag(Tag::ModelPixelScaleTag, &[0.01, 0.01, 0.0][..])
            .unwrap();
        image
            .encoder()
            .write_tag(
                Tag::ModelTiepointTag,
                &[0.0, 0.0, 0.0, -100.0, 40.0, 0.0][..],
            )
            .unwrap();
        image
            .encoder()
            .write_tag(
                Tag::GeoKeyDirectoryTag,
                &[1u16, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326][..],
            )
            .unwrap();
        image.write_data(&[1]).unwrap();
    }
    let reader = GeoTiffStreamReader::open(file.path()).unwrap();
    (
        file,
        MosaicReader::from_single_reader(reader, None, None).unwrap(),
    )
}

#[test]
fn large_tile_windows_are_lazy_and_cover_each_pixel_once() {
    let (_file, mosaic) = test_mosaic();
    let items = vec![Ok((
        0,
        0,
        RasterChunk {
            col_offset: 7,
            row_offset: 11,
            width: 4096,
            height: 4096,
        },
        DecodingResult::U8(vec![1; 4096 * 4096]),
        true,
    ))];
    let mut decoded_bytes = 0;
    let mut jobs = WindowJobs::new(&items, &mosaic, 33, &mut decoded_bytes).unwrap();
    let count = 4096usize.div_ceil(33) * 4096;
    assert_eq!(jobs.len(), count);
    assert_eq!(decoded_bytes, 4096 * 4096);
    let mut pixels = 0;
    let mut windows = 0;
    while let Some((tile, _, chunk, samples, stride, overlap)) = jobs.next() {
        assert_eq!(tile, 0);
        assert_eq!(chunk.col_offset as usize - 7, pixels % 4096);
        assert_eq!(chunk.row_offset as usize - 11, pixels / 4096);
        assert_eq!(chunk.height, 1);
        assert!(chunk.width <= 33);
        assert_eq!(samples.bytes(), chunk.width as usize);
        assert_eq!(stride, 4096);
        assert!(overlap);
        pixels += chunk.width as usize;
        windows += 1;
        assert_eq!(jobs.len(), count - windows);
    }
    assert_eq!(windows, count);
    assert_eq!(pixels, 4096 * 4096);
    assert!(jobs.next().is_none());
}

#[test]
fn windows_combine_rows_and_preserve_interleaved_sample_offsets() {
    let (_file, mut mosaic) = test_mosaic();
    mosaic.tiles[0].reader.metadata.samples_per_pixel = 3;
    let items = vec![
        Ok((
            0,
            0,
            RasterChunk {
                col_offset: 0,
                row_offset: 0,
                width: 0,
                height: 3,
            },
            DecodingResult::U8(Vec::new()),
            false,
        )),
        Ok((
            0,
            0,
            RasterChunk {
                col_offset: 4,
                row_offset: 6,
                width: 3,
                height: 5,
            },
            DecodingResult::U8((0..45).collect()),
            false,
        )),
        Ok((
            0,
            0,
            RasterChunk {
                col_offset: 8,
                row_offset: 9,
                width: 3,
                height: 1,
            },
            DecodingResult::U8((45..54).collect()),
            false,
        )),
    ];
    let mut decoded_bytes = 0;
    let jobs = WindowJobs::new(&items, &mosaic, 7, &mut decoded_bytes).unwrap();
    assert_eq!(jobs.len(), 4);
    let windows: Vec<_> = jobs
        .map(|(_, _, chunk, samples, stride, _)| {
            let super::super::borrowed::BorrowedSamples::U8(samples) = samples else {
                panic!("wrong sample type")
            };
            assert_eq!(stride, 3);
            (chunk.row_offset, chunk.height, samples.to_vec())
        })
        .collect();
    assert_eq!(
        windows,
        vec![
            (6, 2, (0..18).collect()),
            (8, 2, (18..36).collect()),
            (10, 1, (36..45).collect()),
            (9, 1, (45..54).collect()),
        ]
    );
    assert_eq!(decoded_bytes, 54);
}

#[test]
fn malformed_chunks_fail_before_dispatch() {
    let (_file, mosaic) = test_mosaic();
    let chunk = RasterChunk {
        col_offset: 0,
        row_offset: 0,
        width: 2,
        height: 2,
    };
    let mut items = vec![
        Ok((0, 0, chunk, DecodingResult::U8(vec![0; 4]), false)),
        Ok((0, 0, chunk, DecodingResult::U8(vec![0; 3]), false)),
    ];
    let mut decoded_bytes = 0;
    assert!(matches!(
        WindowJobs::new(&items, &mosaic, 1, &mut decoded_bytes),
        Err(DggsStreamError::InvalidMetadata(_))
    ));
    items[1] = Err(DggsStreamError::StreamFailed("decode failed".into()));
    assert!(matches!(
        WindowJobs::new(&items, &mosaic, 1, &mut decoded_bytes),
        Err(DggsStreamError::StreamFailed(_))
    ));
}

#[test]
fn scheduling_storage_is_bounded_for_half_a_million_windows() {
    struct Job<'a>(&'a AtomicUsize);
    impl Drop for Job<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let live = AtomicUsize::new(0);
    let peak_live = AtomicUsize::new(0);
    let jobs = (0..512_000).map(|_| {
        peak_live.fetch_max(live.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Relaxed);
        Job(&live)
    });
    let run_window = |_: Job<'_>, _: &mut CellMaps<ContinuousAccumulator>| {
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |_: &[(usize, u64)]| Vec::new();
    let mut shards = vec![ShardedResolutionMap::new()];
    let mut spill_runs = vec![SpillRuns::new(usize::MAX, None)];
    let mut metrics = super::super::profile::StreamProfile::default();
    let mut peak = 0;
    let mut free = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut finished = PipelineScratch::default();
    let mut target = MergeTarget {
        shards: &mut shards,
        spill_runs: &mut spill_runs,
        metrics: &mut metrics,
        peak_active_bytes: &mut peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: usize::MAX,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    pool.in_place_scope(|scope| {
        run_pipelined(
            scope,
            4,
            jobs,
            &mut free,
            &run_window,
            &compute_keys,
            &mut target,
            &mut finished,
        )
    })
    .unwrap();
    assert_eq!(metrics.jobs, 512_000);
    assert_eq!(live.load(Ordering::Relaxed), 0);
    assert!(peak_live.load(Ordering::Relaxed) <= 8);
    assert_eq!(finished.len(), 8);
    assert_eq!(finished.capacity(), 8);
    assert!(finished.iter().all(Option::is_none));
    assert_eq!(free.len(), 8);
}

fn run(jobs: usize, panic_at: Option<usize>) -> std::thread::Result<Result<Vec<usize>>> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let mut shards: Vec<ShardedResolutionMap<ContinuousAccumulator>> =
        vec![ShardedResolutionMap::new()];
    let mut spill_runs = vec![SpillRuns::new(usize::MAX, None)];
    let mut metrics = super::super::profile::StreamProfile::default();
    let mut peak = 0;
    let mut free: Vec<CellMaps<ContinuousAccumulator>> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let order = Mutex::new(Vec::new());
    // Window i adds one cell whose index records i, with a delay that
    // makes later windows finish first.
    let run_window = |index: usize, maps: &mut CellMaps<ContinuousAccumulator>| {
        if Some(index) == panic_at {
            panic!("window {index} failed");
        }
        std::thread::sleep(std::time::Duration::from_micros(
            ((jobs - index) % 5) as u64 * 200,
        ));
        let cell = u64::from(
            LatLng::new(10.0 + index as f64 * 0.01, 20.0)
                .unwrap()
                .to_cell(Resolution::Nine),
        );
        maps[0].insert(cell, ContinuousAccumulator::new(index as f64));
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |cells: &[(usize, u64)]| -> Vec<CellKeys> {
        cells
            .iter()
            .map(|&(_, c)| {
                order.lock().unwrap().push(c);
                CellKeys {
                    key: 0.0,
                    south_lat_upper: 0.0,
                }
            })
            .collect()
    };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut target = MergeTarget {
            shards: &mut shards,
            spill_runs: &mut spill_runs,
            metrics: &mut metrics,
            peak_active_bytes: &mut peak,
            failure_counter: None,
            compactor_bytes: 0,
            accumulator_limit: usize::MAX,
            map_budget: usize::MAX,
            set_budget: usize::MAX,
            map_sets: 8,
            new_cells: Vec::new(),
            key_chunks: Vec::new(),
        };
        let result = pool.in_place_scope(|scope| {
            run_pipelined(
                scope,
                4,
                0..jobs,
                &mut free,
                &run_window,
                &compute_keys,
                &mut target,
                &mut PipelineScratch::default(),
            )
        });
        result.map(|()| {
            let mut chunks = std::mem::take(&mut target.key_chunks);
            chunks.sort_unstable_by_key(|c| c.0);
            // Merge order: the window each new cell came from.
            chunks
                .into_iter()
                .flat_map(|(_, cells, _)| cells)
                .map(|(_, cell)| target.shards[0].get(cell).map(|a| a.sum as usize).unwrap())
                .collect()
        })
    }))
}

#[test]
fn merges_in_window_order_when_later_windows_finish_first() {
    let order = run(600, None).unwrap().unwrap();
    assert_eq!(order, (0..600).collect::<Vec<_>>());
}

#[test]
fn a_worker_panic_reaches_the_caller() {
    let outcome = run(200, Some(37));
    assert!(outcome.is_err(), "the panic was swallowed");
}

#[test]
fn parallel_shard_wave_merge_matches_serial_bit_for_bit() {
    let res = Resolution::try_from(9).unwrap();
    let jobs = 12usize;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    let run_window = |index: usize, maps: &mut CellMaps<ContinuousAccumulator>| {
        std::thread::sleep(std::time::Duration::from_micros(
            ((jobs - index) % 4) as u64 * 250,
        ));
        for i in 0..1500usize {
            let k = index * 400 + i;
            let ll = LatLng::new(
                42.0 - (k / 80) as f64 * 0.004,
                -100.0 + (k % 80) as f64 * 0.004,
            )
            .unwrap();
            let cell = u64::from(ll.to_cell(res));
            let mut acc = ContinuousAccumulator::with_quantiles();
            acc.update(((index * 1500 + i) % 1000) as f64 * 0.13 - 25.0);
            maps[0]
                .entry(cell)
                .and_modify(|e| e.merge(&acc))
                .or_insert(acc);
        }
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |cells: &[(usize, u64)]| -> Vec<CellKeys> {
        cells
            .iter()
            .map(|_| CellKeys {
                key: 0.0,
                south_lat_upper: 0.0,
            })
            .collect()
    };

    // Serial reference.
    let mut s_shards = vec![ShardedResolutionMap::new()];
    let mut s_spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut s_metrics = super::super::profile::StreamProfile::default();
    let mut s_peak = 0;
    let mut s_free = vec![vec![HashMap::with_hasher(CellIdBuildHasher)]];
    let mut s_target = MergeTarget {
        shards: &mut s_shards,
        spill_runs: &mut s_spills,
        metrics: &mut s_metrics,
        peak_active_bytes: &mut s_peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: usize::MAX,
        map_sets: 1,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    run_serial(
        0..jobs,
        &mut s_free,
        &run_window,
        &compute_keys,
        &mut s_target,
    )
    .unwrap();

    // Parallel 32-shard wave run.
    let mut p_shards = vec![ShardedResolutionMap::new()];
    let mut p_spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut p_metrics = super::super::profile::StreamProfile::default();
    let mut p_peak = 0;
    let mut p_free: Vec<_> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut p_target = MergeTarget {
        shards: &mut p_shards,
        spill_runs: &mut p_spills,
        metrics: &mut p_metrics,
        peak_active_bytes: &mut p_peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: usize::MAX,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    pool.in_place_scope(|scope| {
        run_pipelined(
            scope,
            4,
            0..jobs,
            &mut p_free,
            &run_window,
            &compute_keys,
            &mut p_target,
            &mut PipelineScratch::default(),
        )
    })
    .unwrap();

    let mut key_chunks = std::mem::take(&mut p_target.key_chunks);
    drop(p_target);
    drop(s_target);
    let s_sorted = s_shards[0].take_sorted();
    let p_sorted = p_shards[0].take_sorted();
    assert_eq!(s_sorted.len(), p_sorted.len());
    for ((sk, sv), (pk, pv)) in s_sorted.iter().zip(&p_sorted) {
        assert_eq!(sk, pk);
        assert_eq!(sv.sum.to_bits(), pv.sum.to_bits());
        assert_eq!(sv.count.to_bits(), pv.count.to_bits());
        assert_eq!(sv.sum_w2.to_bits(), pv.sum_w2.to_bits());
        assert_eq!(sv.min.to_bits(), pv.min.to_bits());
        assert_eq!(sv.max.to_bits(), pv.max.to_bits());
        assert_eq!(sv.m2.to_bits(), pv.m2.to_bits());
        assert_eq!(sv.quantiles, pv.quantiles);
    }

    // Verify exact global window-creation order of new cells.
    let mut cell_first_window: HashMap<u64, usize> = HashMap::new();
    let mut map_buf = vec![HashMap::with_hasher(CellIdBuildHasher)];
    for w in 0..jobs {
        map_buf[0].clear();
        run_window(w, &mut map_buf);
        for &c in map_buf[0].keys() {
            cell_first_window.entry(c).or_insert(w);
        }
    }
    key_chunks.sort_unstable_by_key(|c| c.0);
    let created_windows: Vec<usize> = key_chunks
        .iter()
        .flat_map(|(_, cells, _)| cells.iter().map(|&(_, c)| cell_first_window[&c]))
        .collect();
    assert_eq!(created_windows.len(), p_sorted.len());
    assert!(created_windows.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn parallel_shard_wave_merge_categorical_matches_serial() {
    let res = Resolution::try_from(9).unwrap();
    let jobs = 10usize;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    let run_window = |index: usize, maps: &mut CellMaps<CategoricalAccumulator>| {
        for i in 0..1200usize {
            let k = index * 300 + i;
            let ll = LatLng::new(
                40.0 - (k / 70) as f64 * 0.004,
                -105.0 + (k % 70) as f64 * 0.004,
            )
            .unwrap();
            let cell = u64::from(ll.to_cell(res));
            let mut acc = CategoricalAccumulator::new();
            acc.update(((i + index * 3) % 19) as i64);
            maps[0]
                .entry(cell)
                .and_modify(|e| e.merge(&acc))
                .or_insert(acc);
        }
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |cells: &[(usize, u64)]| -> Vec<CellKeys> {
        cells
            .iter()
            .map(|_| CellKeys {
                key: 0.0,
                south_lat_upper: 0.0,
            })
            .collect()
    };

    let mut s_shards = vec![ShardedResolutionMap::new()];
    let mut s_spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut s_metrics = super::super::profile::StreamProfile::default();
    let mut s_peak = 0;
    let mut s_free = vec![vec![HashMap::with_hasher(CellIdBuildHasher)]];
    let mut s_target = MergeTarget {
        shards: &mut s_shards,
        spill_runs: &mut s_spills,
        metrics: &mut s_metrics,
        peak_active_bytes: &mut s_peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: usize::MAX,
        map_sets: 1,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    run_serial(
        0..jobs,
        &mut s_free,
        &run_window,
        &compute_keys,
        &mut s_target,
    )
    .unwrap();

    let mut p_shards = vec![ShardedResolutionMap::new()];
    let mut p_spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut p_metrics = super::super::profile::StreamProfile::default();
    let mut p_peak = 0;
    let mut p_free: Vec<_> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut p_target = MergeTarget {
        shards: &mut p_shards,
        spill_runs: &mut p_spills,
        metrics: &mut p_metrics,
        peak_active_bytes: &mut p_peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: usize::MAX,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    pool.in_place_scope(|scope| {
        run_pipelined(
            scope,
            4,
            0..jobs,
            &mut p_free,
            &run_window,
            &compute_keys,
            &mut p_target,
            &mut PipelineScratch::default(),
        )
    })
    .unwrap();

    drop(p_target);
    drop(s_target);
    let s_sorted = s_shards[0].take_sorted();
    let p_sorted = p_shards[0].take_sorted();
    assert_eq!(s_sorted.len(), p_sorted.len());
    for ((sk, sv), (pk, pv)) in s_sorted.iter().zip(&p_sorted) {
        assert_eq!(sk, pk);
        assert_eq!(sv.total_count.to_bits(), pv.total_count.to_bits());
        assert_eq!(sv.unique_classes(), pv.unique_classes());
        sv.for_each_class(|c, cnt| assert_eq!(cnt.to_bits(), pv.get_class_count(c).to_bits()));
    }
}

#[test]
fn parallel_spill_sort_produces_identical_merged_output() {
    let res = Resolution::try_from(10).unwrap();
    let jobs = 12usize;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    let run_window = |index: usize, maps: &mut CellMaps<ContinuousAccumulator>| {
        for i in 0..2000usize {
            let k = index * 1000 + i;
            let ll = LatLng::new(
                39.5 - (k / 120) as f64 * 0.001,
                -99.5 + (k % 120) as f64 * 0.001,
            )
            .unwrap();
            let cell = u64::from(ll.to_cell(res));
            let acc = ContinuousAccumulator::new((k % 31) as f64 + 1.0);
            maps[0]
                .entry(cell)
                .and_modify(|e| e.merge(&acc))
                .or_insert(acc);
        }
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |cells: &[(usize, u64)]| -> Vec<CellKeys> {
        cells
            .iter()
            .map(|_| CellKeys {
                key: 0.0,
                south_lat_upper: 0.0,
            })
            .collect()
    };

    // Unspilled serial reference.
    let mut s_shards = vec![ShardedResolutionMap::new()];
    let mut s_spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut s_metrics = super::super::profile::StreamProfile::default();
    let mut s_peak = 0;
    let mut s_free = vec![vec![HashMap::with_hasher(CellIdBuildHasher)]];
    let mut s_target = MergeTarget {
        shards: &mut s_shards,
        spill_runs: &mut s_spills,
        metrics: &mut s_metrics,
        peak_active_bytes: &mut s_peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: usize::MAX,
        map_sets: 1,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    run_serial(
        0..jobs,
        &mut s_free,
        &run_window,
        &compute_keys,
        &mut s_target,
    )
    .unwrap();
    let expected = s_shards[0].take_sorted();

    // Parallel with small map_budget (~850 KiB) so >4096 cells trigger `spill_resolution_adaptive` parallel sort.
    let dir = tempfile::tempdir().unwrap();
    let mut p_shards = vec![ShardedResolutionMap::new()];
    let mut p_spills = vec![SpillRuns::new(1024 * 1024, Some(dir.path()))];
    let mut p_metrics = super::super::profile::StreamProfile::default();
    let mut p_peak = 0;
    let mut p_free: Vec<_> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut p_target = MergeTarget {
        shards: &mut p_shards,
        spill_runs: &mut p_spills,
        metrics: &mut p_metrics,
        peak_active_bytes: &mut p_peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: 1024 * 1024,
        map_budget: 850 * 1024,
        set_budget: 16 * 1024 * 1024,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    pool.in_place_scope(|scope| {
        run_pipelined(
            scope,
            4,
            0..jobs,
            &mut p_free,
            &run_window,
            &compute_keys,
            &mut p_target,
            &mut PipelineScratch::default(),
        )
    })
    .unwrap();
    assert!(p_spills[0].has_spilled());
    let rem = p_shards[0].take_sorted();
    if !rem.is_empty() {
        p_spills[0].push_sorted(rem).unwrap();
    }
    let mut reader = p_spills[0].finish().unwrap().unwrap();
    let mut actual = Vec::with_capacity(expected.len());
    while let Some(rec) = reader.next_record().unwrap() {
        actual.push(rec);
    }
    assert_eq!(actual.len(), expected.len());
    for ((ek, ev), (ak, av)) in expected.iter().zip(&actual) {
        assert_eq!(ek, ak);
        assert_eq!(ev.sum.to_bits(), av.sum.to_bits());
        assert_eq!(ev.count.to_bits(), av.count.to_bits());
    }
}

#[test]
fn parallel_wave_merge_enforces_accumulator_limit() {
    let res = Resolution::try_from(9).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    // Case 1: Single accumulator exceeds limit inside parallel shard merge.
    let run_window = |index: usize, maps: &mut CellMaps<CategoricalAccumulator>| {
        for i in 0..1500usize {
            let ll = LatLng::new(
                35.0 + (i / 40) as f64 * 0.005,
                -90.0 + index as f64 * 0.5 + (i % 40) as f64 * 0.005,
            )
            .unwrap();
            let cell = u64::from(ll.to_cell(res));
            let mut acc = CategoricalAccumulator::new();
            if i == 0 && index == 1 {
                for c in 0..300 {
                    acc.update(c);
                }
            } else {
                acc.update(i as i64);
            }
            maps[0].insert(cell, acc);
        }
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |_: &[(usize, u64)]| Vec::new();
    let mut shards = vec![ShardedResolutionMap::new()];
    let mut spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut metrics = super::super::profile::StreamProfile::default();
    let mut peak = 0;
    let mut free: Vec<_> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut target = MergeTarget {
        shards: &mut shards,
        spill_runs: &mut spills,
        metrics: &mut metrics,
        peak_active_bytes: &mut peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: 2048,
        map_budget: usize::MAX,
        set_budget: 1024 * 1024,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    let err = pool
        .in_place_scope(|scope| {
            run_pipelined(
                scope,
                4,
                0..4usize,
                &mut free,
                &run_window,
                &compute_keys,
                &mut target,
                &mut PipelineScratch::default(),
            )
        })
        .unwrap_err();
    assert!(matches!(err, DggsStreamError::InvalidParameter(_)), "{err}");

    // Case 2: Merged accumulator exceeds limit across two windows hitting same cell.
    let run_merge_overflow = |index: usize, maps: &mut CellMaps<CategoricalAccumulator>| {
        for i in 0..1500usize {
            let ll = LatLng::new(
                35.0 + (i / 40) as f64 * 0.005,
                -90.0 + (i % 40) as f64 * 0.005,
            )
            .unwrap();
            let cell = u64::from(ll.to_cell(res));
            let mut acc = CategoricalAccumulator::new();
            if i == 0 {
                let offset = index as i64 * 100;
                for c in offset..offset + 45 {
                    acc.update(c);
                }
            } else {
                acc.update(i as i64);
            }
            maps[0].insert(cell, acc);
        }
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let mut shards = vec![ShardedResolutionMap::new()];
    let mut spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut metrics = super::super::profile::StreamProfile::default();
    let mut peak = 0;
    let mut free: Vec<_> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut target = MergeTarget {
        shards: &mut shards,
        spill_runs: &mut spills,
        metrics: &mut metrics,
        peak_active_bytes: &mut peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: 2048,
        map_budget: usize::MAX,
        set_budget: 1024 * 1024,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };
    let err = pool
        .in_place_scope(|scope| {
            run_pipelined(
                scope,
                4,
                0..2usize,
                &mut free,
                &run_merge_overflow,
                &compute_keys,
                &mut target,
                &mut PipelineScratch::default(),
            )
        })
        .unwrap_err();
    assert!(matches!(err, DggsStreamError::InvalidParameter(_)), "{err}");
}

#[test]
fn parallel_wave_merge_drains_windows_stashed_during_stripe_wait() {
    let res = Resolution::try_from(9).unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    // Window 0 finishes immediately with >512 cells to enter `MergeStripe`;
    // windows 1..6 finish right alongside `MergeStripe` and get stashed into
    // `finished` while `done_rx` drains to empty (`jobs <= slots = 8`).
    let run_window = |index: usize, maps: &mut CellMaps<ContinuousAccumulator>| {
        if index > 0 {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        for i in 0..800usize {
            let k = index * 1000 + i;
            let ll = LatLng::new(
                36.0 + (k / 50) as f64 * 0.005,
                -98.0 + (k % 50) as f64 * 0.005,
            )
            .unwrap();
            let cell = u64::from(ll.to_cell(res));
            maps[0].insert(cell, ContinuousAccumulator::new(index as f64 + 1.0));
        }
        (super::super::profile::WorkerProfile::default(), 0)
    };
    let compute_keys = |cells: &[(usize, u64)]| -> Vec<CellKeys> {
        cells
            .iter()
            .map(|_| CellKeys {
                key: 0.0,
                south_lat_upper: 0.0,
            })
            .collect()
    };

    let mut shards = vec![ShardedResolutionMap::new()];
    let mut spills = vec![SpillRuns::new(usize::MAX, None)];
    let mut metrics = super::super::profile::StreamProfile::default();
    let mut peak = 0;
    let mut free: Vec<_> = (0..8)
        .map(|_| vec![HashMap::with_hasher(CellIdBuildHasher)])
        .collect();
    let mut target = MergeTarget {
        shards: &mut shards,
        spill_runs: &mut spills,
        metrics: &mut metrics,
        peak_active_bytes: &mut peak,
        failure_counter: None,
        compactor_bytes: 0,
        accumulator_limit: usize::MAX,
        map_budget: usize::MAX,
        set_budget: 1024 * 1024,
        map_sets: 8,
        new_cells: Vec::new(),
        key_chunks: Vec::new(),
    };

    pool.in_place_scope(|scope| {
        run_pipelined(
            scope,
            4,
            0..6usize,
            &mut free,
            &run_window,
            &compute_keys,
            &mut target,
            &mut PipelineScratch::default(),
        )
    })
    .unwrap();

    drop(target);
    assert_eq!(metrics.jobs, 6);
    let cells = shards[0].take_sorted();
    assert!(!cells.is_empty());
    let total_weight: f64 = cells.iter().map(|(_, a)| a.count).sum();
    assert_eq!(total_weight, 6.0 * 800.0);
}
