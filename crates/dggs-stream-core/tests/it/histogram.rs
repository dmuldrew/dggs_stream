//! `HistogramSpec`: value and binned per-cell histograms against a brute-force count of
//! pixel centres, on every grid, through the streamer and the columnar reader.

use std::collections::BTreeMap;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig,
};
use dggs_stream_core::columnar::ipc::{write_ipc_stream, IpcStreamReader};
use dggs_stream_core::columnar::{
    ArrowDataType, CategoricalBatchReader, CategoricalFormat, ColumnArray,
};
use dggs_stream_core::histogram::{
    HistogramBins, HistogramNormalize, HistogramSpec, HistogramWeight,
};
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::resolution::area_matched_resolution;
use dggs_stream_core::transfer::{Interpolation, TransferMode};
use dggs_stream_core::GridKind;

use crate::helpers::TestGeoTiffBuilder;

const ORIGIN: (f64, f64) = (-122.45, 37.80);
const PIXEL: f64 = 0.001;
const W: u32 = 60;
const H: u32 = 48;

/// A continuous field with a fractional part, so binning differs from rounding.
fn field(c: u32, r: u32) -> f32 {
    ((c * 7 + r * 13) % 40) as f32 * 0.25 + 0.1
}

fn coarse(grid: GridKind) -> u8 {
    let pixel_m2 = 111_320.0 * PIXEL * 111_320.0 * PIXEL * ORIGIN.1.to_radians().cos();
    area_matched_resolution(grid, pixel_m2) - 2
}

fn config(grid: GridKind, spec: HistogramSpec) -> MultiResolutionConfig {
    let mut config = MultiResolutionConfig::single(coarse(grid)).with_grid(grid);
    config.set_histogram(spec).unwrap();
    config
}

/// `cell -> weights per bin`, from the streamer's records.
fn scan(path: &std::path::Path, config: &MultiResolutionConfig) -> BTreeMap<u64, Vec<f64>> {
    let spec = config.histogram.clone().unwrap();
    let reader = GeoTiffStreamReader::open(path).unwrap();
    let mut streamer = MultiCategoricalHorizonStreamer::new(reader, config).unwrap();
    assert_eq!(streamer.histogram(), Some(&spec));
    let mut cells = BTreeMap::new();
    while streamer
        .drain_completed_into(500, |_, rec| {
            let weights = spec
                .entries(&rec.accumulator)
                .iter()
                .map(|e| e.weight)
                .collect();
            assert!(cells.insert(rec.cell_id, weights).is_none());
        })
        .unwrap()
        > 0
    {}
    cells
}

#[test]
fn binned_histograms_count_pixel_centres_per_bin() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(field);
    // Values run 0.1..=9.85; the last bin stops at 8, so larger values are not counted.
    let bins = HistogramBins::uniform(4, 0.0, 8.0).unwrap();
    for grid in GridKind::ALL {
        let res = coarse(grid);
        let mut expected: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
        for r in 0..H {
            for c in 0..W {
                let lon = ORIGIN.0 + PIXEL * (f64::from(c) + 0.5);
                let lat = ORIGIN.1 - PIXEL * (f64::from(r) + 0.5);
                if let Some(bin) = bins.bin_of(f64::from(field(c, r))) {
                    expected
                        .entry(grid.cell_at(lat, lon, res).unwrap())
                        .or_insert_with(|| vec![0.0; 4])[bin as usize] += 1.0;
                }
            }
        }
        let spec = HistogramSpec {
            bins: bins.clone(),
            ..Default::default()
        };
        let counts = scan(&path, &config(grid, spec.clone()));
        assert_eq!(counts, expected, "{grid}");

        // Fractions sum to 1 and densities integrate to 1 (bins are 2 wide).
        for normalize in [HistogramNormalize::Fraction, HistogramNormalize::Density] {
            let scaled = scan(
                &path,
                &config(
                    grid,
                    HistogramSpec {
                        normalize,
                        ..spec.clone()
                    },
                ),
            );
            for (cell, weights) in &scaled {
                let total: f64 = expected[cell].iter().sum();
                let width = if normalize == HistogramNormalize::Density {
                    2.0
                } else {
                    1.0
                };
                for (got, count) in weights.iter().zip(&expected[cell]) {
                    assert!((got - count / total / width).abs() < 1e-12, "{grid}");
                }
                assert!((weights.iter().sum::<f64>() * width - 1.0).abs() < 1e-12);
            }
        }
    }
}

#[test]
fn area_weights_conserve_the_raster_area() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(field);
    let bins = HistogramBins::uniform(5, 0.0, 10.0).unwrap();
    let mut per_bin = [0.0f64; 5];
    for r in 0..H {
        for c in 0..W {
            per_bin[bins.bin_of(f64::from(field(c, r))).unwrap() as usize] += 1.0;
        }
    }
    for grid in GridKind::ALL {
        let spec = HistogramSpec {
            bins: bins.clone(),
            weight: HistogramWeight::Area,
            normalize: HistogramNormalize::None,
        };
        let config = config(grid, spec);
        assert!(matches!(
            config.transfer,
            TransferMode::Overlay { weighted: true, .. }
        ));
        let cells = scan(&path, &config);
        // Every pixel's area lands in its bin, split between the cells it overlaps.
        for (bin, pixels) in per_bin.iter().enumerate() {
            let area: f64 = cells.values().map(|w| w[bin]).sum();
            assert!(
                (area - pixels).abs() < pixels * 1e-6,
                "{grid} bin {bin}: {area} vs {pixels}"
            );
        }
        assert!(
            cells.values().any(|w| w.iter().any(|v| v.fract() != 0.0)),
            "{grid}"
        );
    }
}

#[test]
fn the_reader_builds_structured_columns() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_tempfile(|c, r| ((c / 7 + r / 5) % 4) as u8 + 1);
    for grid in GridKind::ALL {
        for (spec, data_type) in [
            (
                HistogramSpec {
                    normalize: HistogramNormalize::Fraction,
                    ..Default::default()
                },
                ArrowDataType::ValueHistogram,
            ),
            (
                HistogramSpec {
                    bins: HistogramBins::parse("0.5,2.5,4.5").unwrap(),
                    ..Default::default()
                },
                ArrowDataType::BinHistogram,
            ),
        ] {
            let mut config = config(grid, spec.clone());
            config.decimals = Some(3);
            let reader = CategoricalBatchReader::open(
                std::slice::from_ref(&path),
                &config,
                CategoricalFormat::Wide,
                None,
                64,
                None,
            )
            .unwrap();
            let schema = reader.schema();
            let index = schema.index_of("histogram").unwrap();
            assert_eq!(schema.fields[index].data_type, data_type);
            let batches: Vec<_> = reader.map(Result::unwrap).collect();
            let mut rows = 0;
            for batch in &batches {
                let ColumnArray::Histogram(column) = &batch.columns[index] else {
                    panic!("not a histogram column");
                };
                assert_eq!(column.len(), batch.num_rows);
                let ColumnArray::Float64 { values: totals, .. } =
                    batch.column_by_name("total_count").unwrap()
                else {
                    panic!()
                };
                for (row, total) in totals.iter().enumerate() {
                    let entries = column.row(row);
                    let sum: f64 = entries.iter().map(|e| e.weight).sum();
                    if spec.is_binned() {
                        assert_eq!(entries.len(), 2);
                        assert_eq!((entries[0].lower, entries[1].upper), (0.5, 4.5));
                        assert_eq!(sum, *total, "{grid}");
                    } else {
                        assert!(entries.windows(2).all(|w| w[0].class < w[1].class));
                        assert!(entries.iter().all(|e| (1..=4).contains(&e.class)));
                        assert!((sum - 1.0).abs() < 0.003, "{grid}: {sum}");
                    }
                    // A sliced batch keeps each row's entries.
                    let ColumnArray::Histogram(one) = batch.columns[index].slice(row, 1) else {
                        panic!()
                    };
                    assert_eq!(one.row(0), entries);
                }
                rows += batch.num_rows;
            }
            assert!(rows > 3, "{grid}");

            // The histogram column survives the IPC stream.
            let mut bytes = Vec::new();
            write_ipc_stream(&mut bytes, &schema, batches.iter().cloned().map(Ok)).unwrap();
            let read: Vec<_> = IpcStreamReader::new(bytes.as_slice())
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(read, batches, "{grid}");
        }
    }
}

#[test]
fn inconsistent_histograms_are_refused() {
    let mut config = MultiResolutionConfig::single(8);
    let density = HistogramSpec {
        normalize: HistogramNormalize::Density,
        ..Default::default()
    };
    assert!(config.set_histogram(density).is_err());
    let area = HistogramSpec {
        weight: HistogramWeight::Area,
        ..Default::default()
    };
    config.transfer = TransferMode::Interpolate(Interpolation::Nearest);
    assert!(config.set_histogram(area.clone()).is_err());
    // Set behind `set_histogram`'s back, the mismatch is caught when the scan starts.
    config.transfer = TransferMode::AssignCenters;
    config.histogram = Some(area);
    assert!(config.validate().is_err());
}
