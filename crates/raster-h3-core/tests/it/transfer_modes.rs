//! The `sample` and `overlay` transfer modes against analytic rasters, on every grid.

use std::collections::BTreeMap;
use std::path::Path;

use raster_h3_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig, MultiScanHorizonStreamer,
};
use raster_h3_core::aggregator::H3Accumulator;
use raster_h3_core::raster::geotiff::GeoTiffStreamReader;
use raster_h3_core::resolution::area_matched_resolution;
use raster_h3_core::transfer::{SampleMethod, TransferMode};
use raster_h3_core::GridKind;

use crate::helpers::TestGeoTiffBuilder;

const ORIGIN: (f64, f64) = (-122.45, 37.80);
const PIXEL: f64 = 0.001;
const W: u32 = 60;
const H: u32 = 48;

/// A field linear in longitude and latitude.
fn plane(lon: f64, lat: f64) -> f64 {
    1000.0 + 4000.0 * (lon - ORIGIN.0) - 2500.0 * (lat - ORIGIN.1)
}

fn pixel_centre(c: u32, r: u32) -> (f64, f64) {
    (
        ORIGIN.0 + PIXEL * (f64::from(c) + 0.5),
        ORIGIN.1 - PIXEL * (f64::from(r) + 0.5),
    )
}

fn scan(path: &Path, config: &MultiResolutionConfig) -> BTreeMap<u64, H3Accumulator> {
    let reader = GeoTiffStreamReader::open(path).unwrap();
    let mut streamer = MultiScanHorizonStreamer::new(reader, config).unwrap();
    assert_eq!(streamer.transfer_mode(), config.transfer);
    let mut cells = BTreeMap::new();
    while streamer
        .drain_completed_into(777, |_, rec| {
            assert!(
                cells.insert(rec.h3_index, rec.accumulator).is_none(),
                "cell twice"
            );
        })
        .unwrap()
        > 0
    {}
    assert!(streamer.is_finished());
    assert_eq!(streamer.current_lat_horizon(), f64::NEG_INFINITY);
    cells
}

fn config(grid: GridKind, res: u8, transfer: TransferMode) -> MultiResolutionConfig {
    let mut config = MultiResolutionConfig::single(res).with_grid(grid);
    config.transfer = transfer;
    config
}

/// The resolution whose cells match the test pixels, plus `offset`.
fn matched(grid: GridKind, offset: i32) -> u8 {
    let pixel_m2 = 111_320.0 * PIXEL * 111_320.0 * PIXEL * ORIGIN.1.to_radians().cos();
    (i32::from(area_matched_resolution(grid, pixel_m2)) + offset) as u8
}

#[test]
fn sampling_fills_cells_finer_than_pixels_with_interpolated_values() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| {
            let (lon, lat) = pixel_centre(c, r);
            plane(lon, lat) as f32
        });
    for grid in GridKind::ALL {
        // Two steps finer than the pixels: several cells per pixel.
        let res = matched(grid, 2);
        let centres = scan(&path, &config(grid, res, TransferMode::AssignCenters));
        for method in [
            SampleMethod::Nearest,
            SampleMethod::Bilinear,
            SampleMethod::Bicubic,
            SampleMethod::Lanczos,
        ] {
            let sampled = scan(&path, &config(grid, res, TransferMode::Sample(method)));
            // Centre assignment leaves most fine cells empty; sampling fills them.
            assert!(
                sampled.len() > 3 * centres.len(),
                "{grid} {method:?}: {} sampled vs {} assigned",
                sampled.len(),
                centres.len()
            );
            let mut worst = 0.0f64;
            for (&cell, acc) in &sampled {
                assert_eq!(acc.count, 1.0);
                let (lat, lng) = grid.center_lat_lng(cell).unwrap();
                // Every sampled cell's centre is on the raster.
                assert!(lng >= ORIGIN.0 && lng <= ORIGIN.0 + PIXEL * f64::from(W));
                assert!(lat <= ORIGIN.1 && lat >= ORIGIN.1 - PIXEL * f64::from(H));
                // Away from the border, where kernels are not clamped.
                let col = (lng - ORIGIN.0) / PIXEL;
                let row = (ORIGIN.1 - lat) / PIXEL;
                if col > 3.5 && col < f64::from(W) - 3.5 && row > 3.5 && row < f64::from(H) - 3.5 {
                    worst = worst.max((acc.mean() - plane(lng, lat)).abs());
                }
            }
            // f32 pixels hold the plane to about 1e-4; one pixel spans 4 units.
            let tolerance = match method {
                SampleMethod::Nearest => 4.0,
                SampleMethod::Bilinear | SampleMethod::Bicubic => 2e-3,
                SampleMethod::Lanczos => 0.2,
            };
            assert!(worst < tolerance, "{grid} {method:?}: off by {worst}");
            if method == SampleMethod::Nearest {
                assert!(worst > 0.1, "nearest should show the pixel steps");
            }
        }
        // Every cell with a pixel centre is also sampled.
        let sampled = scan(
            &path,
            &config(grid, res, TransferMode::Sample(SampleMethod::Bilinear)),
        );
        assert!(
            centres.keys().all(|cell| sampled.contains_key(cell)),
            "{grid}"
        );
    }
}

#[test]
fn sampling_a_projected_raster_follows_its_crs() {
    // 30 m UTM zone 10N pixels; the field is linear in easting and northing.
    let field = |x: f64, y: f64| 0.01 * (x - 500_000.0) + 0.02 * (4_200_000.0 - y);
    let (_tmp, path) = TestGeoTiffBuilder::new(64, 64)
        .epsg(32610)
        .origin(500_000.0, 4_200_000.0)
        .pixel_size(30.0)
        .create_f32_tempfile(|c, r| {
            field(
                500_000.0 + 30.0 * (f64::from(c) + 0.5),
                4_200_000.0 - 30.0 * (f64::from(r) + 0.5),
            ) as f32
        });
    let transformer =
        raster_h3_core::crs::transformer::CrsTransformer::from_epsg_code(32610).unwrap();
    for grid in GridKind::ALL {
        let res = area_matched_resolution(grid, 900.0) + 1;
        let cells = scan(
            &path,
            &config(grid, res, TransferMode::Sample(SampleMethod::Bilinear)),
        );
        assert!(cells.len() > 64 * 64, "{grid}: {}", cells.len());
        let mut checked = 0;
        for (&cell, acc) in &cells {
            let (lat, lng) = grid.center_lat_lng(cell).unwrap();
            let (x, y) = transformer.wgs84_to_source(lng, lat).unwrap();
            let (col, row) = ((x - 500_000.0) / 30.0, (4_200_000.0 - y) / 30.0);
            assert!(
                (0.0..64.0).contains(&col) && (0.0..64.0).contains(&row),
                "{grid}"
            );
            if col > 2.0 && col < 62.0 && row > 2.0 && row < 62.0 {
                assert!((acc.mean() - field(x, y)).abs() < 2e-3, "{grid}");
                checked += 1;
            }
        }
        assert!(checked > 1000);
    }
}

#[test]
fn overlay_is_area_weighted_and_conserves_mass() {
    // Values vary, so weights matter; integers keep the total exact.
    let value = |c: u32, r: u32| ((c * 7 + r * 13) % 23 + 1) as f32;
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(value);
    let total: f64 = (0..H)
        .flat_map(|r| (0..W).map(move |c| f64::from(value(c, r))))
        .sum();
    let pixels = f64::from(W * H);
    for grid in GridKind::ALL {
        // Cells a few pixels across.
        for offset in [-2, 0] {
            let res = matched(grid, offset);
            let overlay = scan(&path, &config(grid, res, TransferMode::OVERLAY));
            let (mut area, mut mass) = (0.0, 0.0);
            for acc in overlay.values() {
                assert!(acc.count > 0.0);
                assert!(acc.min >= 1.0 && acc.max <= 23.0);
                assert!(acc.mean() >= acc.min - 1e-9 && acc.mean() <= acc.max + 1e-9);
                area += acc.count;
                mass += acc.sum;
            }
            // Cells tile the plane: their overlaps cover every pixel once, to the
            // precision neighbouring cells agree on their shared edges.
            assert!(
                (area / pixels - 1.0).abs() < 1e-6,
                "{grid} res {res}: area {area}"
            );
            assert!(
                (mass / total - 1.0).abs() < 1e-6,
                "{grid} res {res}: mass {mass}"
            );

            // mass_preserve normalises each pixel's weights: conservation is exact.
            let preserve = TransferMode::Overlay {
                weighted: true,
                mass_preserve: true,
            };
            let preserved = scan(&path, &config(grid, res, preserve));
            assert_eq!(preserved.len(), overlay.len(), "{grid}");
            let mass: f64 = preserved.values().map(|a| a.sum).sum();
            let area: f64 = preserved.values().map(|a| a.count).sum();
            assert!(
                (mass - total).abs() < 1e-7 * total,
                "{grid} res {res}: {mass} vs {total}"
            );
            assert!((area - pixels).abs() < 1e-7 * pixels, "{grid} res {res}");

            // More cells than centre assignment reaches: every cell touching the raster.
            let centres = scan(&path, &config(grid, res, TransferMode::AssignCenters));
            assert!(overlay.len() >= centres.len(), "{grid}");
            assert!(centres.keys().all(|c| overlay.contains_key(c)), "{grid}");
            // A cell well inside the raster averages what centre assignment averages,
            // roughly: both see the same pixels, weighted differently at the rim.
            let unweighted = TransferMode::Overlay {
                weighted: false,
                mass_preserve: false,
            };
            let touched = scan(&path, &config(grid, res, unweighted));
            assert_eq!(touched.len(), overlay.len());
            for (cell, acc) in &touched {
                // Touch weights are whole pixels and at least the centre-assigned ones.
                assert_eq!(acc.count.fract(), 0.0);
                assert!(acc.count >= centres.get(cell).map_or(0.0, |c| c.count));
                assert!(acc.count + 1e-9 >= overlay[cell].count);
            }
        }
    }
}

#[test]
fn overlay_of_a_constant_is_that_constant_and_skips_nodata() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, _| if c < W / 2 { -9999.0 } else { 7.5 });
    for grid in GridKind::ALL {
        let res = matched(grid, -1);
        let mut cfg = config(grid, res, TransferMode::OVERLAY);
        cfg.custom_nodata = Some(-9999.0);
        let cells = scan(&path, &cfg);
        assert!(!cells.is_empty());
        let area: f64 = cells.values().map(|a| a.count).sum();
        // Only the valid half carries weight.
        assert!(
            (area / f64::from(W * H / 2) - 1.0).abs() < 1e-6,
            "{grid}: {area}"
        );
        for acc in cells.values() {
            assert!((acc.mean() - 7.5).abs() < 1e-9 && acc.min == 7.5 && acc.max == 7.5);
        }
        // `emit` adds the cells that only NoData pixels touch.
        cfg.nodata_policy = raster_h3_core::aggregator::NodataPolicy::Emit;
        let emitted = scan(&path, &cfg);
        let empty = emitted.values().filter(|a| a.count == 0.0).count();
        assert!(empty > 0 && emitted.len() == cells.len() + empty, "{grid}");

        // Sampling skips NoData too, and `emit` keeps those cells empty.
        let mut sample = config(
            grid,
            matched(grid, 1),
            TransferMode::Sample(SampleMethod::Nearest),
        );
        sample.custom_nodata = Some(-9999.0);
        let valid = scan(&path, &sample);
        assert!(valid.values().all(|a| a.mean() == 7.5));
        sample.nodata_policy = raster_h3_core::aggregator::NodataPolicy::Emit;
        let all = scan(&path, &sample);
        assert!(
            all.len() > valid.len() && all.values().any(|a| a.count == 0.0),
            "{grid}"
        );
    }
}

#[test]
fn categorical_transfers_keep_class_codes() {
    let class = |c: u32, r: u32| ((c / 6 + r / 6) % 4 + 1) as u8;
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_tempfile(class);
    let drain = |config: &MultiResolutionConfig| {
        let reader = GeoTiffStreamReader::open(&path).unwrap();
        let mut streamer = MultiCategoricalHorizonStreamer::new(reader, config)?;
        let mut cells = BTreeMap::new();
        while streamer.drain_completed_into(500, |_, rec| {
            cells.insert(rec.h3_index, rec.accumulator);
        })? > 0
        {}
        Ok::<_, raster_h3_core::error::RasterH3Error>(cells)
    };
    for grid in GridKind::ALL {
        // Nearest sampling: each fine cell takes the class of the pixel under its centre.
        let sample = config(
            grid,
            matched(grid, 2),
            TransferMode::Sample(SampleMethod::Nearest),
        );
        let cells = drain(&sample).unwrap();
        assert!(cells.len() > 3 * (W * H) as usize, "{grid}");
        for (&cell, acc) in &cells {
            let (lat, lng) = grid.center_lat_lng(cell).unwrap();
            let c = ((lng - ORIGIN.0) / PIXEL).floor() as u32;
            let r = ((ORIGIN.1 - lat) / PIXEL).floor() as u32;
            assert_eq!(acc.majority().0, i64::from(class(c, r)), "{grid}");
            assert_eq!(acc.total_count, 1.0);
        }
        // Classes cannot be interpolated.
        let bilinear = config(
            grid,
            matched(grid, 2),
            TransferMode::Sample(SampleMethod::Bilinear),
        );
        let err = drain(&bilinear).unwrap_err().to_string();
        assert!(err.contains("nearest"), "{err}");

        // Overlay: class areas add up to the raster.
        let overlay = drain(&config(grid, matched(grid, -2), TransferMode::OVERLAY)).unwrap();
        let area: f64 = overlay.values().map(|a| a.total_count).sum();
        assert!(
            (area / f64::from(W * H) - 1.0).abs() < 1e-6,
            "{grid}: {area}"
        );
        assert!(overlay.values().all(|a| (1..=4).contains(&a.majority().0)));
    }
}

#[test]
fn transfer_modes_refuse_options_of_the_centre_kernel() {
    let (_tmp, path) = TestGeoTiffBuilder::new(8, 8)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|_, _| 1.0);
    let open = |config: &MultiResolutionConfig| {
        MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(&path).unwrap(), config).map(|_| ())
    };
    let base = config(GridKind::H3, 9, TransferMode::OVERLAY);
    assert!(open(&base).is_ok());
    let mut compact = base.clone();
    compact.compact_h3_children = true;
    assert!(open(&compact)
        .unwrap_err()
        .to_string()
        .contains("compaction"));
    let mut sampling = base.clone();
    sampling.sampling = raster_h3_core::aggregator::SamplingPattern::parse("5point").unwrap();
    assert!(open(&sampling)
        .unwrap_err()
        .to_string()
        .contains("sampling"));
    let mut spectral = base.clone();
    spectral.spectral_formula =
        raster_h3_core::aggregator::multi_horizon::SpectralFormula::parse("ndvi", 4, 3, 2, 1, 6);
    assert!(open(&spectral)
        .unwrap_err()
        .to_string()
        .contains("spectral"));
    // Filters apply to transferred cells too.
    let mut filtered = config(GridKind::H3, 9, TransferMode::Sample(SampleMethod::Nearest));
    filtered.min_mean = Some(2.0);
    assert!(scan(&path, &filtered).is_empty());
    filtered.min_mean = Some(0.5);
    assert!(!scan(&path, &filtered).is_empty());
}
