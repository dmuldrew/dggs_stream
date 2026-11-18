//! `nodata_policy`: `omit` drops cells with no valid sample; `emit` keeps every cell a
//! pixel falls in, with zero count where all its samples are NoData.

use std::collections::BTreeMap;
use std::path::Path;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::aggregator::{NodataPolicy, OutputPolicy};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::GridKind;

use crate::helpers::TestGeoTiffBuilder;

const W: u32 = 48;
const H: u32 = 40;
const NODATA: f32 = -9999.0;

/// NoData in the left third and in a diagonal band; values elsewhere.
fn pixel(c: u32, r: u32) -> f32 {
    if c < W / 3 || (c + r).is_multiple_of(11) {
        NODATA
    } else {
        ((c * 3 + r) % 13 + 1) as f32
    }
}

fn res_of(grid: GridKind) -> u8 {
    match grid {
        GridKind::H3 => 9,
        GridKind::A5 | GridKind::Healpix => 14,
        GridKind::S2 => 15,
    }
}

/// `cell -> (valid count, valid sum, all pixels)` by pixel centre.
fn expected(grid: GridKind) -> BTreeMap<u64, (f64, f64, f64)> {
    let mut cells = BTreeMap::new();
    for r in 0..H {
        for c in 0..W {
            let lon = -122.45 + 0.0005 * (f64::from(c) + 0.5);
            let lat = 37.80 - 0.0005 * (f64::from(r) + 0.5);
            let e = cells
                .entry(grid.cell_at(lat, lon, res_of(grid)).unwrap())
                .or_insert((0.0, 0.0, 0.0));
            let v = pixel(c, r);
            if v != NODATA {
                e.0 += 1.0;
                e.1 += f64::from(v);
            }
            e.2 += 1.0;
        }
    }
    cells
}

fn config(grid: GridKind, policy: NodataPolicy) -> MultiResolutionConfig {
    let mut config = MultiResolutionConfig::single(res_of(grid)).with_grid(grid);
    config.custom_nodata = Some(f64::from(NODATA));
    config.nodata_policy = policy;
    config
}

fn continuous(path: &Path, config: &MultiResolutionConfig) -> BTreeMap<u64, (f64, f64)> {
    let reader = GeoTiffStreamReader::open(path).unwrap();
    let mut streamer = MultiScanHorizonStreamer::new(reader, config).unwrap();
    let mut cells = BTreeMap::new();
    while streamer
        .drain_completed_into(1024, |_, rec| {
            let acc = &rec.accumulator;
            assert!(cells.insert(rec.cell_id, (acc.count, acc.sum)).is_none());
        })
        .unwrap()
        > 0
    {}
    cells
}

#[test]
fn emit_keeps_all_nodata_cells_on_every_grid() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .pixel_size(0.0005)
        .create_f32_tempfile(pixel);
    for grid in GridKind::ALL {
        let want = expected(grid);
        let empty = want.values().filter(|v| v.0 == 0.0).count();
        assert!(
            empty > 2 && empty < want.len(),
            "{grid}: {empty} empty cells"
        );

        // `omit` (the default): only cells with a valid sample.
        let omit = continuous(&path, &config(grid, NodataPolicy::Omit));
        let valid: BTreeMap<u64, (f64, f64)> = want
            .iter()
            .filter(|(_, v)| v.0 > 0.0)
            .map(|(k, v)| (*k, (v.0, v.1)))
            .collect();
        assert_eq!(omit, valid, "{grid} omit");
        assert_eq!(
            continuous(
                &path,
                &MultiResolutionConfig {
                    custom_nodata: Some(f64::from(NODATA)),
                    ..MultiResolutionConfig::single(res_of(grid)).with_grid(grid)
                }
            ),
            omit,
            "{grid}: omit is the default"
        );

        // `emit`: every cell a pixel centre falls in; statistics from valid samples only.
        let emit = continuous(&path, &config(grid, NodataPolicy::Emit));
        let all: BTreeMap<u64, (f64, f64)> = want.iter().map(|(k, v)| (*k, (v.0, v.1))).collect();
        assert_eq!(emit, all, "{grid} emit");
    }
}

#[test]
fn emit_keeps_all_nodata_cells_in_categorical_scans() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .pixel_size(0.0005)
        .gdal_nodata("0")
        .create_tempfile(|c, r| {
            if pixel(c, r) == NODATA {
                0u8
            } else {
                ((c / 5 + r / 5) % 4 + 1) as u8
            }
        });
    for grid in GridKind::ALL {
        let want = expected(grid);
        for policy in [NodataPolicy::Omit, NodataPolicy::Emit] {
            let mut config = MultiResolutionConfig::single(res_of(grid)).with_grid(grid);
            config.nodata_policy = policy;
            let reader = GeoTiffStreamReader::open(&path).unwrap();
            let mut streamer = MultiCategoricalHorizonStreamer::new(reader, &config).unwrap();
            let mut totals = BTreeMap::new();
            while streamer
                .drain_completed_into(1024, |_, rec| {
                    totals.insert(rec.cell_id, rec.accumulator.total_count);
                })
                .unwrap()
                > 0
            {}
            let expected: BTreeMap<u64, f64> = want
                .iter()
                .filter(|(_, v)| policy == NodataPolicy::Emit || v.0 > 0.0)
                .map(|(k, v)| (*k, v.0))
                .collect();
            assert_eq!(totals, expected, "{grid} {policy:?}");
        }
    }
}

#[test]
fn emit_refuses_options_that_need_samples() {
    let mut config = config(GridKind::H3, NodataPolicy::Emit);
    assert!(config.validate().is_ok());
    assert_eq!(
        config.output_policy(),
        OutputPolicy {
            nodata: NodataPolicy::Emit,
            nodata_value: None,
            decimals: None
        }
    );
    config.min_count = Some(1.0);
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("min_count"));
    config.min_count = None;
    config.compact_h3_children = true;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("compaction"));
    config.compact_h3_children = false;
    config.emit_nodata_value = Some(f64::NAN);
    assert!(config.validate().is_err());
    config.emit_nodata_value = Some(-1.0);
    config.decimals = Some(16);
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("decimals"));
    config.decimals = Some(15);
    assert!(config.validate().is_ok());

    // The class standing for NoData pixels is reserved: no remap may produce it.
    use dggs_stream_core::aggregator::remap::CategoryRemapper;
    for spec in [
        "1: -9223372036854775808",
        "1-3: -9223372036854775808",
        "1: 2, *: -9223372036854775808",
    ] {
        config.remapper = Some(CategoryRemapper::parse(spec).unwrap().into_arc());
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("reserves class"), "{spec}: {err}");
        config.nodata_policy = NodataPolicy::Omit;
        assert!(config.validate().is_ok(), "{spec}");
        config.nodata_policy = NodataPolicy::Emit;
    }
    config.remapper = Some(CategoryRemapper::parse("1: 2").unwrap().into_arc());
    assert!(config.validate().is_ok());
}

mod batches {
    use super::*;
    use dggs_stream_core::columnar::{
        CategoricalBatchReader, CategoricalFormat, ColumnArray, ContinuousBatchReader, RecordBatch,
    };

    fn f64s(batch: &RecordBatch, name: &str) -> (Vec<f64>, Option<Vec<bool>>) {
        match batch.column_by_name(name).unwrap() {
            ColumnArray::Float64 { values, validity } => (values.clone(), validity.clone()),
            other => panic!("{name}: {:?}", other.data_type()),
        }
    }

    fn i64s(batch: &RecordBatch, name: &str) -> (Vec<i64>, Option<Vec<bool>>) {
        match batch.column_by_name(name).unwrap() {
            ColumnArray::Int64 { values, validity } => (values.clone(), validity.clone()),
            other => panic!("{name}: {:?}", other.data_type()),
        }
    }

    fn nullable(batch: &RecordBatch, name: &str) -> bool {
        let i = batch.schema.index_of(name).unwrap();
        batch.schema.fields[i].nullable
    }

    #[test]
    fn continuous_batches_apply_the_nodata_value_and_decimals() {
        let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
            .pixel_size(0.0005)
            .create_f32_tempfile(pixel);
        let paths = [path];
        for grid in GridKind::ALL {
            let want = expected(grid);
            let index = grid.index_column_name();
            let read = |config: &MultiResolutionConfig| -> Vec<RecordBatch> {
                ContinuousBatchReader::open(&paths, config, None, 64, None)
                    .unwrap()
                    .map(|b| b.unwrap())
                    .collect()
            };
            let cells = |b: &RecordBatch| match b.column_by_name(index).unwrap() {
                ColumnArray::UInt64 { values, .. } => values.clone(),
                _ => unreachable!(),
            };

            // emit, no value: NULL mean/min/max where the cell has no valid sample.
            let null_cfg = config(grid, NodataPolicy::Emit);
            let mut rows = 0;
            for b in read(&null_cfg) {
                assert!(nullable(&b, "mean") && nullable(&b, "min") && nullable(&b, "max"));
                assert!(!nullable(&b, "count") && !nullable(&b, "sum"));
                let (means, mean_valid) = f64s(&b, "mean");
                let (counts, _) = f64s(&b, "count");
                let (sums, _) = f64s(&b, "sum");
                let (_, min_valid) = f64s(&b, "min");
                for (i, cell) in cells(&b).into_iter().enumerate() {
                    let (count, sum, _) = want[&cell];
                    assert_eq!((counts[i], sums[i]), (count, sum));
                    let valid = mean_valid.as_ref().is_none_or(|v| v[i]);
                    assert_eq!(valid, count > 0.0, "{grid}");
                    assert_eq!(min_valid.as_ref().is_none_or(|v| v[i]), count > 0.0);
                    if valid {
                        assert_eq!(means[i], sum / count);
                    }
                    rows += 1;
                }
            }
            assert_eq!(rows, want.len(), "{grid}");

            // emit with a value and decimals: the value fills empty cells (unrounded),
            // statistics of the others are rounded, counts are not.
            let mut filled = config(grid, NodataPolicy::Emit);
            filled.emit_nodata_value = Some(-1.25);
            filled.decimals = Some(1);
            for b in read(&filled) {
                assert!(!nullable(&b, "mean"));
                let (means, mean_valid) = f64s(&b, "mean");
                assert!(mean_valid.is_none());
                let (mins, _) = f64s(&b, "min");
                let (stddevs, sd_valid) = f64s(&b, "stddev");
                for (i, cell) in cells(&b).into_iter().enumerate() {
                    let (count, sum, _) = want[&cell];
                    if count == 0.0 {
                        assert_eq!((means[i], mins[i]), (-1.25, -1.25));
                        assert!(!sd_valid.as_ref().unwrap()[i]);
                    } else {
                        assert_eq!(means[i], ((sum / count) * 10.0).round() / 10.0, "{grid}");
                        if sd_valid.as_ref().is_none_or(|v| v[i]) {
                            assert_eq!(stddevs[i], (stddevs[i] * 10.0).round() / 10.0);
                        }
                    }
                }
            }

            // omit with decimals: same cells as without, rounded.
            let mut rounded = config(grid, NodataPolicy::Omit);
            rounded.decimals = Some(0);
            let plain = read(&config(grid, NodataPolicy::Omit));
            let round = read(&rounded);
            let plain_means: BTreeMap<u64, f64> = plain
                .iter()
                .flat_map(|b| cells(b).into_iter().zip(f64s(b, "mean").0))
                .collect();
            let round_means: BTreeMap<u64, f64> = round
                .iter()
                .flat_map(|b| cells(b).into_iter().zip(f64s(b, "mean").0))
                .collect();
            assert_eq!(plain_means.len(), round_means.len());
            assert!(plain_means.values().any(|m| m.fract() != 0.0));
            for (cell, mean) in &plain_means {
                assert_eq!(round_means[cell], mean.round(), "{grid}");
            }
        }
    }

    #[test]
    fn categorical_batches_keep_empty_cells_in_wide_and_long_form() {
        let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
            .pixel_size(0.0005)
            .gdal_nodata("0")
            .create_tempfile(|c, r| {
                if pixel(c, r) == NODATA {
                    0u8
                } else {
                    ((c / 5 + r / 5) % 4 + 1) as u8
                }
            });
        let paths = [path];
        for grid in GridKind::ALL {
            let want = expected(grid);
            let empty = want.values().filter(|v| v.0 == 0.0).count();
            for value in [None, Some(255.0)] {
                let mut cfg = MultiResolutionConfig::single(res_of(grid)).with_grid(grid);
                cfg.nodata_policy = NodataPolicy::Emit;
                cfg.emit_nodata_value = value;
                cfg.decimals = Some(2);

                let (mut rows, mut empties) = (0, 0);
                for b in CategoricalBatchReader::open(
                    &paths,
                    &cfg,
                    CategoricalFormat::Wide,
                    None,
                    64,
                    None,
                )
                .unwrap()
                .map(|b| b.unwrap())
                {
                    assert_eq!(nullable(&b, "majority_class"), value.is_none());
                    let (classes, valid) = i64s(&b, "majority_class");
                    let (totals, _) = f64s(&b, "total_count");
                    let (fractions, _) = f64s(&b, "majority_fraction");
                    for i in 0..b.num_rows {
                        rows += 1;
                        let has_class = valid.as_ref().is_none_or(|v| v[i]);
                        if totals[i] == 0.0 {
                            empties += 1;
                            assert_eq!(has_class, value.is_some());
                            if has_class {
                                assert_eq!(classes[i], 255);
                            }
                            assert_eq!(fractions[i], 0.0);
                        } else {
                            assert!(has_class && (1..=4).contains(&classes[i]));
                            assert_eq!(fractions[i], (fractions[i] * 100.0).round() / 100.0);
                        }
                    }
                }
                assert_eq!((rows, empties), (want.len(), empty), "{grid} wide");

                let mut long_empties = 0;
                let mut long_cells = std::collections::BTreeSet::new();
                for b in CategoricalBatchReader::open(
                    &paths,
                    &cfg,
                    CategoricalFormat::Long,
                    None,
                    7,
                    None,
                )
                .unwrap()
                .map(|b| b.unwrap())
                {
                    let (categories, valid) = i64s(&b, "category");
                    let (totals, _) = f64s(&b, "total_count");
                    let (counts, _) = f64s(&b, "count");
                    let ColumnArray::UInt64 { values: ids, .. } =
                        b.column_by_name(grid.index_column_name()).unwrap()
                    else {
                        unreachable!()
                    };
                    for i in 0..b.num_rows {
                        long_cells.insert(ids[i]);
                        if totals[i] == 0.0 {
                            long_empties += 1;
                            assert_eq!(counts[i], 0.0);
                            let has_class = valid.as_ref().is_none_or(|v| v[i]);
                            assert_eq!(has_class, value.is_some());
                            if has_class {
                                assert_eq!(categories[i], 255);
                            }
                        }
                    }
                }
                assert_eq!(
                    (long_cells.len(), long_empties),
                    (want.len(), empty),
                    "{grid} long"
                );
            }
        }
    }
}
