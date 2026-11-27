//! The `interpolate` and `overlay` transfer modes against analytic rasters, on every grid.

use std::collections::BTreeMap;
use std::path::Path;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiCategoricalHorizonStreamer, MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::aggregator::ContinuousAccumulator;
use dggs_stream_core::raster::geotiff::GeoTiffStreamReader;
use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule};
use dggs_stream_core::resolution::area_matched_resolution;
use dggs_stream_core::transfer::{Interpolation, TransferMode};
use dggs_stream_core::GridKind;

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

fn scan(path: &Path, config: &MultiResolutionConfig) -> BTreeMap<u64, ContinuousAccumulator> {
    let reader = GeoTiffStreamReader::open(path).unwrap();
    let streamer = MultiScanHorizonStreamer::new(reader, config).unwrap();
    if config.transfer.is_auto() {
        assert!(!streamer.transfer_mode().is_auto());
    } else {
        assert_eq!(streamer.transfer_mode(), config.transfer);
    }
    collect_cells(streamer)
}

fn collect_cells(mut streamer: MultiScanHorizonStreamer) -> BTreeMap<u64, ContinuousAccumulator> {
    let mut cells = BTreeMap::new();
    while streamer
        .drain_completed_into(777, |_, rec| {
            assert!(
                cells.insert(rec.cell_id, rec.accumulator).is_none(),
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
fn parallel_overlay_matches_serial_on_all_grids() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| ((3 * c + r) % 23) as f32);
    for grid in GridKind::ALL {
        for mass_preserve in [false, true] {
            let mut cfg = config(
                grid,
                matched(grid, 0),
                TransferMode::Overlay {
                    weighted: true,
                    mass_preserve,
                },
            );
            cfg.custom_nodata = Some(0.0);
            if !mass_preserve {
                cfg.valid_coverage_threshold = Some(0.8);
            }
            let expected = scan(&path, &cfg);
            assert!(expected.len() > 128);
            cfg.aggregation_workers = Some(4);
            assert_eq!(
                scan(&path, &cfg),
                expected,
                "{grid} mass_preserve={mass_preserve}"
            );
        }
    }
}

#[test]
fn oversized_parallel_overlay_cells_retry_without_losing_or_repeating_pixels() {
    // Narrow columns and tall rows: the mean cell area fits the worker gate,
    // yet one cell can intersect over 1,024 columns within a single row.
    let (_tmp, path) = TestGeoTiffBuilder::new(2048, 8)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size_xy(0.000002, 0.25)
        .create_f32_tempfile(|c, r| (1 + (c + 3 * r) % 31) as f32);
    let mut cfg = config(
        GridKind::H3,
        8,
        TransferMode::Overlay {
            weighted: false,
            mass_preserve: false,
        },
    );
    let touched = scan(&path, &cfg);
    assert!(touched.len() > 128);
    assert!(touched.values().any(|a| a.count > 1024.0));
    cfg.aggregation_workers = Some(4);
    assert_eq!(scan(&path, &cfg), touched);

    for mass_preserve in [false, true] {
        cfg.transfer = TransferMode::Overlay {
            weighted: true,
            mass_preserve,
        };
        cfg.aggregation_workers = Some(1);
        let serial = scan(&path, &cfg);
        cfg.aggregation_workers = Some(4);
        assert_eq!(scan(&path, &cfg), serial);
    }
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
            Interpolation::Nearest,
            Interpolation::Bilinear,
            Interpolation::Bicubic,
            Interpolation::Lanczos,
        ] {
            let sampled = scan(&path, &config(grid, res, TransferMode::Interpolate(method)));
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
                Interpolation::Nearest => 4.0,
                Interpolation::Bilinear | Interpolation::Bicubic => 2e-3,
                Interpolation::Lanczos => 0.2,
            };
            assert!(worst < tolerance, "{grid} {method:?}: off by {worst}");
            if method == Interpolation::Nearest {
                assert!(worst > 0.1, "nearest should show the pixel steps");
            }
        }
        // Every cell with a pixel centre is also sampled.
        let sampled = scan(
            &path,
            &config(
                grid,
                res,
                TransferMode::Interpolate(Interpolation::Bilinear),
            ),
        );
        assert!(
            centres.keys().all(|cell| sampled.contains_key(cell)),
            "{grid}"
        );
    }
}

#[test]
fn full_width_interpolation_matches_every_h3_centre_in_the_latitude_band() {
    let grid = GridKind::H3;
    let config = config(grid, 2, TransferMode::Interpolate(Interpolation::Nearest));
    for west in [-180.0, 0.0] {
        let (_tmp, path) = TestGeoTiffBuilder::new(360, 8)
            .origin(west, 4.0)
            .pixel_size(1.0)
            .create_f32_tempfile(|col, _| (west + f64::from(col)).rem_euclid(360.0) as f32);
        let actual: BTreeMap<_, _> = scan(&path, &config)
            .into_iter()
            .map(|(cell, acc)| (cell, acc.mean()))
            .collect();

        // Enumerate H3 independently of the transfer kernel's candidate bounding box.
        let expected: BTreeMap<_, _> = h3o::CellIndex::base_cells()
            .flat_map(|base| base.children(h3o::Resolution::Two))
            .filter_map(|cell| {
                let id = u64::from(cell);
                let (lat, lon) = grid.center_lat_lng(id).unwrap();
                ((0.0..8.0).contains(&(4.0 - lat))).then_some((id, lon.floor().rem_euclid(360.0)))
            })
            .collect();
        assert_eq!(actual.len(), expected.len(), "raster starts at {west}");
        assert_eq!(actual, expected, "raster starts at {west}");
    }
}

#[test]
fn full_width_overlay_matches_a_regional_scan_inside_the_longitude_gap() {
    let grid = GridKind::H3;
    let config = config(grid, 4, TransferMode::OVERLAY);
    let make_tile = |west, width, col_offset| {
        TestGeoTiffBuilder::new(width, 2)
            .origin(west, 0.25)
            .pixel_size(0.25)
            .create_f32_tempfile(|c, r| ((c + col_offset) % 17 + r) as f32)
    };
    for west in [-180.0, 0.0] {
        let (_global, path) = make_tile(west, 1440, 0);
        let actual = scan(&path, &config);
        // Same pixels around the omitted strip, with ample margins on both sides.
        let (_region, path) = make_tile(west + 330.0, 64, 1320);
        let mut checked = 0;
        for (id, expected) in scan(&path, &config) {
            let (_, lon) = grid.center_lat_lng(id).unwrap();
            if (336.0..339.0).contains(&(lon - west).rem_euclid(360.0)) {
                let cell = actual
                    .get(&id)
                    .expect("global overlay missed regional cell");
                assert!((cell.count - expected.count).abs() < 1e-8);
                assert!((cell.sum - expected.sum).abs() < 1e-7);
                checked += 1;
            }
        }
        assert!(checked > 0);
    }
}

#[test]
fn overlay_counts_both_sides_of_each_global_longitude_seam() {
    let grid = GridKind::H3;
    let tile = |west: i32, width, epsg| {
        let unit = if epsg == 3857 {
            6_378_137.0 * 1_f64.to_radians()
        } else {
            1.0
        };
        TestGeoTiffBuilder::new(width, 4)
            .epsg(epsg)
            .origin(f64::from(west) * unit, 0.5 * unit)
            .pixel_size(0.25 * unit)
            .create_f32_tempfile(|c, _| {
                if (west * 4 + c as i32).rem_euclid(720) < 360 {
                    3.0
                } else {
                    11.0
                }
            })
    };
    for (west, epsg) in [(-180, 4326), (0, 4326), (-180, 3857), (0, 3857)] {
        let (_global, full) = tile(west, 1440, epsg);
        let (_region, region) = tile(west - 4, 32, epsg);
        for (weighted, mass_preserve) in [(true, false), (true, true), (false, false)] {
            let config = config(
                grid,
                4,
                TransferMode::Overlay {
                    weighted,
                    mass_preserve,
                },
            );
            let global = scan(&full, &config);
            let mut crossing = 0;
            for (id, reference) in scan(&region, &config) {
                // Only cells straddling the seam receive both 3 and 11.
                if reference.min == reference.max {
                    continue;
                }
                crossing += 1;
                let cell = &global[&id];
                assert_eq!((cell.min, cell.max), (3.0, 11.0), "seam {west}, cell {id}");
                assert!((cell.count - reference.count).abs() < 1e-8);
                assert!((cell.sum - reference.sum).abs() < 1e-7);
            }
            assert!(crossing > 0);
        }
    }
}

#[test]
fn seam_pieces_in_one_source_pixel_are_one_observation() {
    let (_tmp, path) = TestGeoTiffBuilder::new(1, 1)
        .origin(-180.0, 0.125)
        .pixel_size_xy(360.0, 0.25)
        .create_f32_tempfile(|_, _| 7.0);
    for weighted in [true, false] {
        let mode = TransferMode::Overlay {
            weighted,
            mass_preserve: false,
        };
        let cells = scan(&path, &config(GridKind::H3, 4, mode));
        assert!(!cells.is_empty());
        for acc in cells.values() {
            if !weighted {
                assert_eq!(acc.count, 1.0);
            }
            assert_eq!(acc.sum_w2, acc.count * acc.count);
        }
    }
}

#[test]
fn projected_mosaic_keeps_cells_across_uncertified_tile_bounds() {
    let tile = |width, start_col| {
        TestGeoTiffBuilder::new(width, 16)
            .epsg(2154)
            .origin(700_000.0 + 100.0 * f64::from(start_col), 6_600_000.0)
            .pixel_size(100.0)
            .create_f32_tempfile(|col, _| if col + start_col < 16 { 3.0 } else { 11.0 })
    };
    let (_full, full_path) = tile(32, 0);
    let (_left, left_path) = tile(16, 0);
    let (_right, right_path) = tile(16, 16);
    let config = config(GridKind::H3, 9, TransferMode::OVERLAY);
    let expected = scan(&full_path, &config);
    assert!(expected.values().any(|a| a.min == 3.0 && a.max == 11.0));

    let mut mosaic =
        MosaicReader::open(&[left_path, right_path], None, None, OverlapRule::First).unwrap();
    let right = &mut mosaic.tiles[1];
    assert!(!right.crs_transformer.bounds_certified());
    // Inject an underestimated western extent. This exercises the safety
    // contract independently of the projection sampler's current probe spacing.
    right.bounds_wgs84[0] = right.bounds_wgs84[2] - 0.00001;
    let actual =
        collect_cells(MultiScanHorizonStreamer::new_mosaic(mosaic.into(), &config).unwrap());
    assert!(expected.keys().eq(actual.keys()));
    for (id, a) in actual {
        let reference = &expected[&id];
        assert_eq!((a.min, a.max), (reference.min, reference.max));
        assert!((a.count - reference.count).abs() < 1e-8, "count for {id}");
        assert!((a.sum - reference.sum).abs() < 1e-7, "sum for {id}");
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
        dggs_stream_core::crs::transformer::CrsTransformer::from_epsg_code(32610).unwrap();
    for grid in GridKind::ALL {
        let res = area_matched_resolution(grid, 900.0) + 1;
        let cells = scan(
            &path,
            &config(
                grid,
                res,
                TransferMode::Interpolate(Interpolation::Bilinear),
            ),
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

/// Area of `polygon` inside the unit pixel `(c, r)`, clipped against its four
/// edges independently of the library's row/column slicing.
fn reference_pixel_area(polygon: &[(f64, f64)], c: i64, r: i64) -> f64 {
    let (x0, x1, y0, y1) = (c as f64, (c + 1) as f64, r as f64, (r + 1) as f64);
    let edges: [&dyn Fn((f64, f64)) -> f64; 4] =
        [&|p| p.0 - x0, &|p| x1 - p.0, &|p| p.1 - y0, &|p| y1 - p.1];
    let mut ring = polygon.to_vec();
    for inside in edges {
        let mut out = Vec::with_capacity(ring.len() + 2);
        for (i, &p) in ring.iter().enumerate() {
            let q = ring[(i + 1) % ring.len()];
            let (dp, dq) = (inside(p), inside(q));
            if dp >= 0.0 {
                out.push(p);
            }
            if (dp >= 0.0) != (dq >= 0.0) {
                let t = dp / (dp - dq);
                out.push((p.0 + t * (q.0 - p.0), p.1 + t * (q.1 - p.1)));
            }
        }
        ring = out;
        if ring.is_empty() {
            return 0.0;
        }
    }
    dggs_stream_core::transfer::polygon_area(&ring).abs()
}

#[test]
fn overlay_pixel_weights_of_cells_spanning_many_pixels() {
    use dggs_stream_core::transfer::{pixel_overlaps, polygon_area};
    // Hexagons and trapezoids wide enough for whole interior pixels, some rotated
    // or clipped by the raster edge, plus a concave notch with two spans per row.
    let hexagon = |cx: f64, cy: f64, rad: f64, turn: f64| -> Vec<(f64, f64)> {
        (0..6)
            .map(|k| {
                let a = turn + std::f64::consts::FRAC_PI_3 * f64::from(k);
                (cx + rad * a.cos(), cy + rad * a.sin())
            })
            .collect()
    };
    let shapes = [
        hexagon(12.3, 9.7, 8.4, 0.0),
        hexagon(12.3, 9.7, 8.4, 0.37),
        hexagon(2.0, 20.5, 9.1, 0.11),
        vec![(-3.5, 0.5), (30.25, 0.5), (26.0, 17.0), (1.75, 17.0)],
        vec![
            (1.0, 1.0),
            (22.0, 1.0),
            (22.0, 15.0),
            (15.0, 15.0),
            (15.0, 5.5),
            (8.0, 5.5),
            (8.0, 15.0),
            (1.0, 15.0),
        ],
    ];
    let (width, rows) = (24, (0, 26));
    let mut whole = 0;
    for shape in &shapes {
        let mut seen = std::collections::BTreeSet::new();
        let mut total = 0.0;
        pixel_overlaps(shape, rows, width, |c, r, a| {
            assert!(seen.insert((c, r)), "pixel ({c}, {r}) visited twice");
            assert!(a > 0.0 && a <= 1.0, "({c}, {r}): {a}");
            let expected = reference_pixel_area(shape, c, r);
            assert!(
                (a - expected).abs() < 1e-12,
                "({c}, {r}): {a} vs {expected}"
            );
            whole += usize::from(a == 1.0);
            total += a;
        });
        // Every pixel the polygon overlaps by more than a sliver is visited.
        let mut inside = 0.0;
        for r in rows.0..rows.1 {
            for c in 0..width {
                let expected = reference_pixel_area(shape, c, r);
                inside += expected;
                if expected > 1e-12 {
                    assert!(seen.contains(&(c, r)), "missed ({c}, {r}): {expected}");
                }
            }
        }
        assert!((total - inside).abs() < 1e-9, "{total} vs {inside}");
        assert!(total <= polygon_area(shape).abs() + 1e-9);
    }
    assert!(whole > 500, "interior pixels: {whole}");

    // End to end, cells many pixels across still tile the raster exactly.
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
        let res = matched(grid, -3);
        for mass_preserve in [false, true] {
            let mode = TransferMode::Overlay {
                weighted: true,
                mass_preserve,
            };
            let cells = scan(&path, &config(grid, res, mode));
            let area: f64 = cells.values().map(|a| a.count).sum();
            let mass: f64 = cells.values().map(|a| a.sum).sum();
            assert!(
                (area / pixels - 1.0).abs() < 1e-6,
                "{grid} res {res}: {area}"
            );
            assert!(
                (mass / total - 1.0).abs() < 1e-6,
                "{grid} res {res}: {mass}"
            );
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
        cfg.nodata_policy = dggs_stream_core::aggregator::NodataPolicy::Emit;
        let emitted = scan(&path, &cfg);
        let empty = emitted.values().filter(|a| a.count == 0.0).count();
        assert!(empty > 0 && emitted.len() == cells.len() + empty, "{grid}");

        // Sampling skips NoData too, and `emit` keeps those cells empty.
        let mut sample = config(
            grid,
            matched(grid, 1),
            TransferMode::Interpolate(Interpolation::Nearest),
        );
        sample.custom_nodata = Some(-9999.0);
        let valid = scan(&path, &sample);
        assert!(valid.values().all(|a| a.mean() == 7.5));
        sample.nodata_policy = dggs_stream_core::aggregator::NodataPolicy::Emit;
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
            cells.insert(rec.cell_id, rec.accumulator);
        })? > 0
        {}
        Ok::<_, dggs_stream_core::error::DggsStreamError>(cells)
    };
    for grid in GridKind::ALL {
        // Nearest sampling: each fine cell takes the class of the pixel under its centre.
        let sample = config(
            grid,
            matched(grid, 2),
            TransferMode::Interpolate(Interpolation::Nearest),
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
            TransferMode::Interpolate(Interpolation::Bilinear),
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
    sampling.sampling = dggs_stream_core::aggregator::SamplingPattern::parse("5point").unwrap();
    assert!(open(&sampling)
        .unwrap_err()
        .to_string()
        .contains("sampling"));
    let mut interp_sampling = config(
        GridKind::H3,
        9,
        TransferMode::Interpolate(Interpolation::Bilinear),
    );
    interp_sampling.sampling = sampling.sampling.clone();
    assert!(open(&interp_sampling).is_ok());
    let mut spectral = base.clone();
    spectral.spectral_formula =
        dggs_stream_core::aggregator::multi_horizon::SpectralFormula::parse("ndvi", 4, 3, 2, 1, 6);
    assert!(open(&spectral)
        .unwrap_err()
        .to_string()
        .contains("spectral"));
    // Filters apply to transferred cells too.
    let mut filtered = config(
        GridKind::H3,
        9,
        TransferMode::Interpolate(Interpolation::Nearest),
    );
    filtered.min_mean = Some(2.0);
    assert!(scan(&path, &filtered).is_empty());
    filtered.min_mean = Some(0.5);
    assert!(!scan(&path, &filtered).is_empty());
}

/// Cells larger than the raster, or than the bbox, still take every pixel: a bbox
/// selects pixels by centre (as the centre kernel does), not cells by centre.
#[test]
fn overlay_keeps_cells_larger_than_the_raster_or_the_bbox() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| (c + 2 * r) as f32);
    let total: f64 = (0..H)
        .flat_map(|r| (0..W).map(move |c| f64::from(c + 2 * r)))
        .sum();
    // Columns 10..30 and rows 8..20, with edges between pixel centres.
    let bbox = [
        ORIGIN.0 + PIXEL * 10.0,
        ORIGIN.1 - PIXEL * 20.0,
        ORIGIN.0 + PIXEL * 30.0,
        ORIGIN.1 - PIXEL * 8.0,
    ];
    let boxed: f64 = (8..20)
        .flat_map(|r| (10..30).map(move |c| f64::from(c + 2 * r)))
        .sum();
    let overlay = TransferMode::Overlay {
        weighted: true,
        mass_preserve: false,
    };
    let totals = |cells: &BTreeMap<u64, ContinuousAccumulator>| {
        cells
            .values()
            .fold((0.0, 0.0), |(n, s), a| (n + a.count, s + a.sum))
    };
    for grid in GridKind::ALL {
        // Resolution 0 (one cell holds the raster), a coarse one, and one near the pixels.
        for res in [0, matched(grid, -6), matched(grid, -2)] {
            let whole = scan(&path, &config(grid, res, overlay));
            let (count, sum) = totals(&whole);
            assert!(
                (count / f64::from(W * H) - 1.0).abs() < 1e-6,
                "{grid} res {res}: {count}"
            );
            assert!((sum / total - 1.0).abs() < 1e-6, "{grid} res {res}: {sum}");

            let mut config = config(grid, res, overlay);
            config.bbox = Some(bbox);
            let cut = scan(&path, &config);
            let (count, sum) = totals(&cut);
            assert!(
                (count / 240.0 - 1.0).abs() < 1e-6,
                "{grid} res {res} bbox: {count}"
            );
            assert!(
                (sum / boxed - 1.0).abs() < 1e-6,
                "{grid} res {res} bbox: {sum}"
            );
            // The centre kernel sees the same pixels.
            config.transfer = TransferMode::AssignCenters;
            assert_eq!(
                totals(&scan(&path, &config)),
                (240.0, boxed),
                "{grid} res {res}"
            );
        }
    }
}

#[test]
fn transfer_streamers_advance_explicitly() {
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| {
            let (lon, lat) = pixel_centre(c, r);
            plane(lon, lat) as f32
        });
    let res = matched(GridKind::H3, 0);
    for mode in [
        TransferMode::Interpolate(Interpolation::Nearest),
        TransferMode::OVERLAY,
    ] {
        let config = config(GridKind::H3, res, mode);
        let want = scan(&path, &config);
        let reader = GeoTiffStreamReader::open(&path).unwrap();
        let mut streamer = MultiScanHorizonStreamer::new(reader, &config).unwrap();
        let mut got = BTreeMap::new();
        loop {
            streamer.advance_until_completed(1).unwrap();
            let drained = streamer
                .drain_completed_into(1, |_, rec| {
                    got.insert(rec.cell_id, rec.accumulator.count);
                })
                .unwrap();
            if drained == 0 {
                break;
            }
        }
        assert_eq!(got.len(), want.len(), "{mode:?}");

        let reader = GeoTiffStreamReader::open(&path).unwrap();
        let mut categorical = MultiCategoricalHorizonStreamer::new(reader, &config).unwrap();
        categorical.advance_until_completed(1).unwrap();
        assert!(categorical.drain_completed_into(1, |_, _| {}).unwrap() > 0);
    }
}

#[test]
fn overlay_emit_lists_only_cells_with_selected_pixels() {
    // No NoData anywhere: under `emit` every listed cell must hold a selected pixel.
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| {
            let (lon, lat) = pixel_centre(c, r);
            plane(lon, lat) as f32
        });
    let bbox = [
        ORIGIN.0 + 0.0153,
        ORIGIN.1 - 0.0317,
        ORIGIN.0 + 0.0411,
        ORIGIN.1 - 0.0089,
    ];
    for grid in GridKind::ALL {
        let mut config = config(grid, matched(grid, -1), TransferMode::OVERLAY);
        config.bbox = Some(bbox);
        let omit = scan(&path, &config);
        config.nodata_policy = dggs_stream_core::aggregator::NodataPolicy::Emit;
        let emit = scan(&path, &config);
        assert!(!emit.is_empty(), "{grid}");
        assert!(emit.values().all(|a| a.count > 0.0), "{grid}");
        assert_eq!(
            emit.keys().collect::<Vec<_>>(),
            omit.keys().collect::<Vec<_>>(),
            "{grid}"
        );
    }
}

/// An RGBA GeoTIFF on the test grid: red 200 under zero alpha in the left third,
/// red 100 under full alpha elsewhere.
fn rgba_with_transparent_border() -> tempfile::NamedTempFile {
    use std::io::BufWriter;
    use tiff::encoder::colortype::RGBA8;
    use tiff::encoder::TiffEncoder;
    use tiff::tags::Tag;

    let file = tempfile::NamedTempFile::new().unwrap();
    let mut data = Vec::with_capacity((W * H * 4) as usize);
    for _ in 0..H {
        for c in 0..W {
            let transparent = c < W / 3;
            data.extend_from_slice(&[if transparent { 200 } else { 100 }, 0, 0]);
            data.push(if transparent { 0 } else { 255 });
        }
    }
    let mut encoder =
        TiffEncoder::new(BufWriter::new(std::fs::File::create(file.path()).unwrap())).unwrap();
    let mut image = encoder.new_image::<RGBA8>(W, H).unwrap();
    let tie = [0.0f64, 0.0, 0.0, ORIGIN.0, ORIGIN.1, 0.0];
    image
        .encoder()
        .write_tag(Tag::Unknown(33922), &tie[..])
        .unwrap();
    image
        .encoder()
        .write_tag(Tag::Unknown(33550), &[PIXEL, PIXEL, 0.0][..])
        .unwrap();
    let geokeys: [u16; 12] = [1, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326];
    image
        .encoder()
        .write_tag(Tag::Unknown(34735), &geokeys[..])
        .unwrap();
    // The fourth sample is unassociated alpha.
    image
        .encoder()
        .write_tag(Tag::ExtraSamples, &[2u16][..])
        .unwrap();
    image.write_data(&data).unwrap();
    file
}

#[test]
fn transfer_modes_skip_transparent_pixels() {
    let file = rgba_with_transparent_border();
    for grid in GridKind::ALL {
        for (mode, offset) in [
            (TransferMode::AssignCenters, 0),
            (TransferMode::Interpolate(Interpolation::Nearest), 1),
            (TransferMode::Interpolate(Interpolation::Bilinear), 1),
            (TransferMode::OVERLAY, -1),
        ] {
            let cells = scan(file.path(), &config(grid, matched(grid, offset), mode));
            assert!(!cells.is_empty(), "{grid} {mode:?}");
            for acc in cells.values() {
                let close = |v: f64| (v - 100.0).abs() < 1e-9;
                assert!(close(acc.min) && close(acc.max), "{grid} {mode:?}: {acc:?}");
            }
        }
    }
}

#[test]
fn internal_mask_matches_nodata_without_discarding_valid_black_pixels() {
    // GDAL-generated tiled GeoTIFF. The right and bottom tiles are partial.
    // Values alternate in pairs of columns (0, 0, 30, 30). The validity
    // pattern cuts across those pairs, and the left three columns are masked.
    let masked = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/internal-mask.tif");
    let reader = GeoTiffStreamReader::open(&masked).unwrap();
    assert!(reader.has_internal_mask());
    let gt = reader.metadata.geotransform;
    let valid = |x: u32, y: u32| x >= 3 && !(x + y).is_multiple_of(3);
    let (_tmp, reference) = TestGeoTiffBuilder::new(19, 17)
        .origin(gt.c0, gt.f0)
        .pixel_size_xy(gt.a, -gt.e)
        .gdal_nodata("255")
        .create_tempfile(|x, y| {
            if !valid(x, y) {
                255u8
            } else if x % 4 < 2 {
                0
            } else {
                30
            }
        });
    let grid = GridKind::H3;
    for mode in [
        TransferMode::AssignCenters,
        TransferMode::OVERLAY,
        TransferMode::Interpolate(Interpolation::Nearest),
        TransferMode::Interpolate(Interpolation::Bilinear),
    ] {
        for policy in [
            dggs_stream_core::aggregator::NodataPolicy::Omit,
            dggs_stream_core::aggregator::NodataPolicy::Emit,
        ] {
            let mut opts = config(grid, matched(grid, 0), mode);
            opts.nodata_policy = policy;
            let expected = scan(&reference, &opts);
            let actual = scan(&masked, &opts);
            assert!(expected.values().any(|a| a.min == 0.0 && a.count > 0.0));
            assert!(actual.keys().eq(expected.keys()), "{mode:?} {policy:?}");
            for (id, a) in actual {
                let e = &expected[&id];
                assert!(
                    (a.count - e.count).abs() < 1e-8 && (a.sum - e.sum).abs() < 1e-7,
                    "{mode:?} {policy:?} {id}"
                );
            }
        }
    }
    let mut classes = MultiCategoricalHorizonStreamer::new(
        reader,
        &config(grid, matched(grid, 0), TransferMode::AssignCenters),
    )
    .unwrap();
    let mut black = 0.0;
    while classes
        .drain_completed_into(50, |_, rec| black += rec.accumulator.get_class_count(0))
        .unwrap()
        > 0
    {}
    let expected_black = (0..17)
        .flat_map(|y| (0..19).map(move |x| (x, y)))
        .filter(|&(x, y)| valid(x, y) && x % 4 < 2)
        .count();
    assert_eq!(black, expected_black as f64);
}

#[test]
fn striped_subifd_mask_combines_with_alpha() {
    use tiff::{
        encoder::{colortype::RGBA8, TiffEncoder},
        tags::Tag,
    };
    let file = tempfile::NamedTempFile::new().unwrap();
    {
        let mut enc = TiffEncoder::new(std::fs::File::create(file.path()).unwrap()).unwrap();
        let mut mask = enc.extra_directory().unwrap();
        // Three nine-pixel rows in two strips, least significant bit first.
        let a = mask.write_data(&[1u8, 0, 2, 0][..]).unwrap() as u32;
        let b = mask.write_data(&[0u8, 1][..]).unwrap() as u32;
        mask.write_tag(Tag::NewSubfileType, 4u32).unwrap();
        mask.write_tag(Tag::ImageWidth, 9u32).unwrap();
        mask.write_tag(Tag::ImageLength, 3u32).unwrap();
        mask.write_tag(Tag::RowsPerStrip, 2u32).unwrap();
        mask.write_tag(Tag::FillOrder, 2u16).unwrap();
        mask.write_tag(Tag::PhotometricInterpretation, 4u16)
            .unwrap();
        mask.write_tag(Tag::StripOffsets, &[a, b][..]).unwrap();
        mask.write_tag(Tag::StripByteCounts, &[4u32, 2][..])
            .unwrap();
        let ptr = mask.finish_with_offsets().unwrap();
        let mut image = enc.new_image::<RGBA8>(9, 3).unwrap();
        image.rows_per_strip(1).unwrap();
        let tags = image.encoder();
        tags.write_tag(Tag::SubIfd, ptr.offset).unwrap();
        tags.write_tag(Tag::ExtraSamples, &[2u16][..]).unwrap();
        tags.write_tag(Tag::ModelPixelScaleTag, &[PIXEL, PIXEL, 0.0][..])
            .unwrap();
        tags.write_tag(
            Tag::ModelTiepointTag,
            &[0.0, 0.0, 0.0, ORIGIN.0, ORIGIN.1, 0.0][..],
        )
        .unwrap();
        tags.write_tag(
            Tag::GeoKeyDirectoryTag,
            &[1u16, 1, 0, 1, 2048, 0, 1, 4326][..],
        )
        .unwrap();
        let samples: Vec<_> = (0..27u8)
            .flat_map(|i| [i, 0, 0, if i == 10 { 0 } else { 255 }])
            .collect();
        image.write_data(&samples).unwrap();
    }
    // Only pixels 0, 10 and 26 have a set mask bit. Alpha removes pixel 10.
    let cells = scan(
        file.path(),
        &config(GridKind::H3, 9, TransferMode::AssignCenters),
    );
    assert_eq!(cells.values().map(|a| a.count).sum::<f64>(), 2.0);
    assert_eq!(cells.values().map(|a| a.sum).sum::<f64>(), 26.0);
}

#[test]
fn valid_coverage_uses_selected_area_even_for_unweighted_statistics() {
    // With all samples valid, the weighted mean of this binary indicator is
    // an independent reference for the valid fraction after zeros become NoData.
    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|x, y| f32::from((x + y) % 3 != 0));
    let mut cfg = config(
        GridKind::H3,
        matched(GridKind::H3, 0),
        TransferMode::OVERLAY,
    );
    cfg.bbox = Some([ORIGIN.0, ORIGIN.1 - 0.02, ORIGIN.0 + 0.02, ORIGIN.1]);
    let reference = scan(&path, &cfg);
    cfg.custom_nodata = Some(0.0);
    cfg.nodata_policy = dggs_stream_core::aggregator::NodataPolicy::Emit;
    for weighted in [false, true] {
        cfg.transfer = TransferMode::Overlay {
            weighted,
            mass_preserve: false,
        };
        for t in [0.0, 0.6, 1.0] {
            cfg.valid_coverage_threshold = Some(t);
            let got = scan(&path, &cfg);
            let expected: Vec<_> = reference
                .iter()
                .filter(|(_, a)| a.sum >= t * a.count)
                .map(|(&id, _)| id)
                .collect();
            assert!(got.keys().copied().eq(expected.iter().copied()));
            if t > 0.0 {
                assert!(!got.is_empty() && got.len() < reference.len());
            }
        }
    }
    cfg.transfer = TransferMode::Overlay {
        weighted: true,
        mass_preserve: true,
    };
    let exempt = scan(&path, &cfg);
    cfg.valid_coverage_threshold = None;
    assert_eq!(exempt, scan(&path, &cfg));
    for t in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        cfg.valid_coverage_threshold = Some(t);
        assert!(cfg.validate().is_err());
    }
    cfg.valid_coverage_threshold = Some(0.5);
    cfg.transfer = TransferMode::AssignCenters;
    assert!(cfg.validate().is_err());
}

#[test]
fn ground_area_overlay_conserves_masked_density_on_all_grids() {
    use dggs_stream_core::raster::ground_area::wgs84_rectangle_area;
    use dggs_stream_core::transfer::scan_ground_area_overlay;
    for (west, north) in [(179.995, 0.02), (14.0, 75.0)] {
        let step = 0.005;
        let (_tmp, path) = TestGeoTiffBuilder::new(4, 3)
            .origin(west, north)
            .pixel_size(step)
            .create_f32_tempfile(|c, r| if c == r { -99.0 } else { (r + 1) as f32 });
        let mut expected_area = 0.0;
        let mut expected_sum = 0.0;
        for r in 0..3 {
            let top = north - f64::from(r) * step;
            let area = 3.0 * wgs84_rectangle_area(0.0, top - step, step, top).unwrap();
            expected_area += area;
            expected_sum += area * f64::from(r + 1);
        }
        for grid in GridKind::ALL {
            let mut cfg = config(grid, matched(grid, -1), TransferMode::OVERLAY);
            cfg.custom_nodata = Some(-99.0);
            let open = || GeoTiffStreamReader::open(&path).unwrap();
            let mut count = 0.0;
            let mut m2 = 0.0;
            let mut sum = 0.0;
            let mut partial = false;
            let mut ids = std::collections::HashSet::new();
            for row in scan_ground_area_overlay(open(), &cfg).unwrap() {
                let (_, id, a) = row.unwrap();
                assert!(ids.insert(id));
                partial |= a.pixel_count() > 0.0 && a.pixel_count() < 1.0;
                count += a.pixel_count();
                m2 += a.retained_area_m2();
                sum += a.area_weighted_sum();
            }
            assert!(partial, "{grid}");
            assert!((count - 9.0).abs() < 1e-7, "{grid}: {count}");
            assert!(
                (m2 / expected_area - 1.0).abs() < 1e-8,
                "{grid}: {m2} vs {expected_area}"
            );
            assert!(
                (sum / expected_sum - 1.0).abs() < 1e-8,
                "{grid}: {sum} vs {expected_sum}"
            );
        }
    }
}

#[test]
fn ground_area_overlay_coverage_uses_mask_and_completed_cell_area() {
    use dggs_stream_core::transfer::scan_ground_area_overlay;
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/internal-mask.tif");
    let mut cfg = config(GridKind::H3, 10, TransferMode::OVERLAY);
    let run = |cfg: &MultiResolutionConfig| {
        scan_ground_area_overlay(GeoTiffStreamReader::open(&path).unwrap(), cfg)
            .unwrap()
            .map(|row| {
                let (_, id, a) = row.unwrap();
                (id, a)
            })
            .collect::<BTreeMap<_, _>>()
    };
    let before = run(&cfg);
    cfg.valid_coverage_threshold = Some(1.0);
    let after = run(&cfg);
    assert!(!after.is_empty() && after.len() < before.len());
    for (id, a) in after {
        assert_eq!(a, before[&id]);
    }
    cfg.transfer = TransferMode::AssignCenters;
    assert!(scan_ground_area_overlay(GeoTiffStreamReader::open(&path).unwrap(), &cfg).is_err());
}

#[test]
fn ground_area_small_wrapped_bbox_selects_whole_pixels_on_a_wide_raster() {
    let (_tmp, path) = TestGeoTiffBuilder::new(36000, 2)
        .origin(-180.0, 0.01)
        .pixel_size(0.01)
        .create_f32_tempfile(|_, r| if r == 0 { 3.0 } else { 8.0 });
    let mut cfg = config(GridKind::S2, 18, TransferMode::OVERLAY);
    cfg.bbox = Some([179.994, 0.004, -179.994, 0.006]);
    let rows = dggs_stream_core::transfer::scan_ground_area_overlay(
        GeoTiffStreamReader::open(path).unwrap(),
        &cfg,
    )
    .unwrap();
    let mut pixels = 0.0;
    for row in rows {
        let (_, _, a) = row.unwrap();
        pixels += a.pixel_count();
        assert!((a.area_stats().mean() - 3.0).abs() < 1e-12);
    }
    assert!((pixels - 2.0).abs() < 1e-8, "{pixels}");
}

#[test]
fn auto_transfer_resolves_across_scales_and_constraints() {
    let (_tmp, cont_path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| {
            let (lon, lat) = pixel_centre(c, r);
            plane(lon, lat) as f32
        });
    let (_cat_tmp, cat_path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_tempfile(|c, r| ((c / 4 + r / 4) % 5 + 1) as u8);

    let mosaic = MosaicReader::open(
        std::slice::from_ref(&cont_path),
        None,
        None,
        OverlapRule::default(),
    )
    .unwrap();
    let pixel_m2 = dggs_stream_core::resolution::mosaic_pixel_ground_area_m2(&mosaic).unwrap();

    for grid in GridKind::ALL {
        let fine_res = (0..=grid.max_resolution())
            .find(|&r| grid.area_m2(r) < pixel_m2)
            .unwrap();
        let near_res = (0..=grid.max_resolution())
            .rev()
            .find(|&r| grid.area_m2(r) >= pixel_m2 && grid.area_m2(r) < 16.0 * pixel_m2)
            .unwrap();
        let overlay_res = (0..=grid.max_resolution())
            .rev()
            .find(|&r| grid.area_m2(r) >= 16.0 * pixel_m2)
            .unwrap();

        // Fine cells (< 1:1): continuous -> Interpolate(Bilinear), categorical -> Interpolate(Nearest).
        let auto_cont_fine = scan(&cont_path, &config(grid, fine_res, TransferMode::Auto));
        let exp_cont_fine = scan(
            &cont_path,
            &config(
                grid,
                fine_res,
                TransferMode::Interpolate(Interpolation::Bilinear),
            ),
        );
        assert_eq!(
            auto_cont_fine, exp_cont_fine,
            "{grid} fine_res={fine_res} continuous"
        );

        let cat_reader = GeoTiffStreamReader::open(&cat_path).unwrap();
        let auto_cat = MultiCategoricalHorizonStreamer::new(
            cat_reader,
            &config(grid, fine_res, TransferMode::Auto),
        )
        .unwrap();
        assert_eq!(
            auto_cat.transfer_mode(),
            TransferMode::Interpolate(Interpolation::Nearest)
        );

        // Near 1:1 -> AssignCenters.
        let s_near = MultiScanHorizonStreamer::new(
            GeoTiffStreamReader::open(&cont_path).unwrap(),
            &config(grid, near_res, TransferMode::Auto),
        )
        .unwrap();
        assert_eq!(
            s_near.transfer_mode(),
            TransferMode::AssignCenters,
            "{grid} near_res={near_res}"
        );

        // Coarse (>= 16:1) -> OVERLAY.
        let s_overlay = MultiScanHorizonStreamer::new(
            GeoTiffStreamReader::open(&cont_path).unwrap(),
            &config(grid, overlay_res, TransferMode::Auto),
        )
        .unwrap();
        assert_eq!(
            s_overlay.transfer_mode(),
            TransferMode::OVERLAY,
            "{grid} overlay_res={overlay_res}"
        );

        // valid_coverage_threshold forces OVERLAY even near 1:1.
        let mut cov_cfg = config(grid, near_res, TransferMode::Auto);
        cov_cfg.valid_coverage_threshold = Some(0.5);
        let s_cov =
            MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(&cont_path).unwrap(), &cov_cfg)
                .unwrap();
        assert_eq!(s_cov.transfer_mode(), TransferMode::OVERLAY);

        // Compaction forces AssignCenters even at coarse resolution.
        if grid == GridKind::H3 {
            let mut cmp_cfg = config(grid, overlay_res, TransferMode::Auto);
            cmp_cfg.compact_h3_children = true;
            let s_cmp = MultiScanHorizonStreamer::new(
                GeoTiffStreamReader::open(&cont_path).unwrap(),
                &cmp_cfg,
            )
            .unwrap();
            assert_eq!(s_cmp.transfer_mode(), TransferMode::AssignCenters);
        }
    }
}

/// Fixed, dense native-grid references, independent of the overlay subdivision.
fn reference_ring(
    grid: GridKind,
    cell: u64,
    corners: &[(f64, f64)],
    steps: u32,
) -> Vec<(f64, f64)> {
    match grid {
        GridKind::A5 => a5::cell_to_boundary(
            cell,
            Some(a5::CellToBoundaryOptions {
                closed_ring: false,
                segments: Some(steps as i32),
            }),
        )
        .unwrap()
        .iter()
        .map(|v| (v.longitude(), v.latitude()))
        .collect(),
        GridKind::Healpix => {
            let (depth, id) = dggs_stream_core::grid::healpix_decode(cell).unwrap();
            healpix::get(depth)
                .path_along_cell_edge(id, healpix::dir::Cardinal::S, false, steps)
                .iter()
                .map(|v| (v.lon.to_degrees(), v.lat.to_degrees()))
                .collect()
        }
        GridKind::S2 | GridKind::H3 => {
            use s2::{latlng::LatLng, point::Point};
            let points: Vec<_> = corners
                .iter()
                .map(|&(lon, lat)| Point::from(LatLng::from_degrees(lat, lon)))
                .collect();
            let mut out = Vec::new();
            for (i, a) in points.iter().enumerate() {
                let b = &points[(i + 1) % points.len()];
                for j in 0..steps {
                    let p = LatLng::from(s2::edgeutil::interpolate(
                        f64::from(j) / f64::from(steps),
                        a,
                        b,
                    ));
                    out.push((p.lng.deg(), p.lat.deg()));
                }
            }
            out
        }
    }
}

#[test]
fn adaptive_overlay_per_cell_area_matches_dense_edges() {
    const STEPS: u32 = 1024;
    const SIZE: u32 = 96;
    const METRES_PER_RADIAN: f64 = 6_378_137.0;
    for grid in GridKind::ALL {
        let res = (0..=grid.max_resolution())
            .find(|&r| grid.spec().max_cell_diameter_km(r) <= 500.0)
            .unwrap();
        for (lon, lat, epsg) in [
            (-37.0, 76.0, 4326),
            (180.0, 54.125, 4326),
            (-37.0, 76.0, 3857),
        ] {
            let id = grid.cell_at(lat, lon, res).unwrap();
            let mut vertices = [(0., 0.); 10];
            let n = grid.boundary_lng_lat(id, &mut vertices);
            let corners = &vertices[..n];
            let dense = reference_ring(grid, id, corners, STEPS);
            let wrap = |x: f64| (x + 180.0).rem_euclid(360.0) - 180.0;
            // Place a small raster astride a real edge. At the seam, S2 and
            // HEALPix can have an entire edge on the meridian: sample near
            // its endpoint so the footprint also contains a curved edge.
            let center = if lon == 180.0 {
                let nearest = dense
                    .iter()
                    .min_by(|a, b| wrap(a.0 - lon).abs().total_cmp(&wrap(b.0 - lon).abs()))
                    .unwrap();
                (lon, nearest.1)
            } else {
                dense[STEPS as usize / 2]
            };
            let project = |lon: f64, lat: f64| {
                let x = center.0 + wrap(lon - center.0);
                if epsg == 3857 {
                    (
                        METRES_PER_RADIAN * x.to_radians(),
                        METRES_PER_RADIAN * lat.to_radians().tan().asinh(),
                    )
                } else {
                    (x, lat)
                }
            };
            let pixel = if epsg == 3857 { 25.0 } else { 0.00025 };
            let span = f64::from(SIZE) * pixel;
            let (x, y) = project(center.0, center.1);
            let west = x - span * 0.5;
            let north = y + span * 0.5;
            let fraction = |ring: &[(f64, f64)]| {
                let unit: Vec<_> = ring
                    .iter()
                    .map(|&(lon, lat)| {
                        let (x, y) = project(lon, lat);
                        ((x - west) / span, (north - y) / span)
                    })
                    .collect();
                let mut area = 0.0;
                dggs_stream_core::transfer::pixel_overlaps(&unit, (0, 1), 1, |_, _, v| area += v);
                area
            };
            let expected = fraction(&dense);
            let baseline = (fraction(corners) - expected).abs();
            let (_dir, path) = TestGeoTiffBuilder::new(SIZE, SIZE)
                .origin(west, north)
                .epsg(epsg)
                .pixel_size(pixel)
                .create_f32_tempfile(|_, _| 1.0);
            let result = scan(&path, &config(grid, res, TransferMode::OVERLAY));
            let actual = result.get(&id).map_or(0., |a| a.count) / f64::from(SIZE).powi(2);
            let error = (actual - expected).abs();
            eprintln!("{grid} {lat} EPSG:{epsg} res={res} expected={expected:.8} corner_error={baseline:.8} adaptive_error={error:.8}");
            if epsg == 4326 {
                // Independent physical-area oracle: clip the dense ring to narrow
                // latitude strips and weight each fraction by its WGS84 rectangle area.
                use dggs_stream_core::raster::ground_area::wgs84_rectangle_area;
                use dggs_stream_core::transfer::{pixel_overlaps, scan_ground_area_overlay};
                const STRIPS: i64 = 128;
                let strip_height = span / STRIPS as f64;
                let strips: Vec<_> = dense
                    .iter()
                    .map(|&(lon, lat)| {
                        (
                            0.5 + wrap(lon - center.0) / span,
                            (north - lat) / strip_height,
                        )
                    })
                    .collect();
                let mut expected_m2 = 0.0;
                pixel_overlaps(&strips, (0, STRIPS), 1, |_, row, frac| {
                    let top = north - row as f64 * strip_height;
                    expected_m2 +=
                        frac * wgs84_rectangle_area(0.0, top - strip_height, span, top).unwrap();
                });
                let records = scan_ground_area_overlay(
                    GeoTiffStreamReader::open(&path).unwrap(),
                    &config(grid, res, TransferMode::OVERLAY),
                )
                .unwrap();
                let areas: BTreeMap<_, _> = records
                    .map(|row| {
                        let (_, cell, stats) = row.unwrap();
                        (cell, stats.retained_area_m2())
                    })
                    .collect();
                let raster_area = wgs84_rectangle_area(0.0, north - span, span, north).unwrap();
                assert!((areas[&id] - expected_m2).abs() / raster_area < 0.0005);
            }
            assert!(expected > 0.0 && expected < 1.0);
            assert!(error < 0.0005, "{grid} {lat}: {error}");
            assert!(
                error < baseline * 0.1,
                "{grid} {lat}: {error} vs {baseline}"
            );
        }
    }
}

/// Mean of `n` unit-weight observations of `value`, accumulated one pixel at a time.
fn sequential_mean(value: f64, n: f64) -> f64 {
    let mut sum = 0.0;
    for _ in 0..n as u64 {
        sum += value;
    }
    sum / n
}

#[test]
fn mean_filter_bounds_are_inclusive_for_fully_covered_runs() {
    // 0.99 is inexact in binary, so summing a run in several lanes can round its
    // mean one ulp away from per-pixel accumulation. A cell whose mean is exactly
    // the bound must still pass `min_mean` and `max_mean`.
    const VALUE: f64 = 0.99;
    let (_tmp, path) = TestGeoTiffBuilder::new(160, 120)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_tempfile::<f64, _>(|_, _| VALUE);
    let mut cfg = config(
        GridKind::H3,
        matched(GridKind::H3, -2),
        TransferMode::Overlay {
            weighted: false,
            mass_preserve: false,
        },
    );
    let unfiltered = scan(&path, &cfg);
    let expected: Vec<u64> = unfiltered
        .iter()
        .filter(|(_, acc)| sequential_mean(VALUE, acc.count) == VALUE)
        .map(|(&id, _)| id)
        .collect();
    assert!(
        expected.len() > 4 && expected.len() < unfiltered.len(),
        "{} of {} cells sit exactly on the bound",
        expected.len(),
        unfiltered.len()
    );
    for (min_mean, max_mean) in [
        (Some(VALUE), None),
        (None, Some(VALUE)),
        (Some(VALUE), Some(VALUE)),
    ] {
        cfg.min_mean = min_mean;
        cfg.max_mean = max_mean;
        let kept: Vec<u64> = scan(&path, &cfg).keys().copied().collect();
        if min_mean.is_some() && max_mean.is_some() {
            assert_eq!(kept, expected);
        } else {
            // One-sided bounds keep every cell whose mean is on the right side.
            assert!(
                expected.iter().all(|id| kept.contains(id)),
                "{min_mean:?} {max_mean:?}"
            );
        }
    }
}

/// Total weighted pixel count and total retained ground area of every cell a scan
/// emits for the raster at `path`, with the raster's own footprint area.
fn overlay_totals(
    path: &Path,
    grid: GridKind,
    res: u8,
    footprint: [f64; 4],
) -> (f64, f64, f64, usize) {
    use dggs_stream_core::raster::ground_area::wgs84_rectangle_area;
    use dggs_stream_core::transfer::scan_ground_area_overlay;
    let cells = scan(path, &config(grid, res, TransferMode::OVERLAY));
    let pixels: f64 = cells.values().map(|a| a.count).sum();
    let retained: f64 = scan_ground_area_overlay(
        GeoTiffStreamReader::open(path).unwrap(),
        &config(grid, res, TransferMode::OVERLAY),
    )
    .unwrap()
    .map(|row| row.unwrap().2.retained_area_m2())
    .sum();
    let [west, south, east, north] = footprint;
    let area = wgs84_rectangle_area(west, south, east, north).unwrap();
    (pixels, retained, area, cells.len())
}

#[test]
fn polar_cells_follow_the_curve_they_share_with_a_non_polar_neighbour() {
    // At HEALPix depth 6 the polar cell of the 0-90°E quadrant shares an edge with a
    // non-polar cell whose native boundary bows away from the chord between its end
    // vertices (at 60°E the chord is at 88.78°N, the edge at 88.90°N). A raster in
    // the strip between them was counted by both cells while the polar cell kept
    // the chord.
    const SIZE: u32 = 40;
    let (west, north, dx, dy) = (59.5, 88.88, 0.025, 0.002);
    let (_tmp, path) = TestGeoTiffBuilder::new(SIZE, SIZE)
        .origin(west, north)
        .pixel_size_xy(dx, dy)
        .create_f32_tempfile(|_, _| 1.0);
    let footprint = [
        west,
        north - f64::from(SIZE) * dy,
        west + f64::from(SIZE) * dx,
        north,
    ];
    let (pixels, retained, area, cells) = overlay_totals(&path, GridKind::Healpix, 6, footprint);
    assert!(cells >= 1);
    // Every pixel is shared out exactly once across the cells.
    assert!((pixels - f64::from(SIZE * SIZE)).abs() < 1e-6, "{pixels}");
    assert!((retained / area - 1.0).abs() < 1e-6, "{retained} vs {area}");
}

#[test]
fn rasters_along_a_pole_are_shared_out_exactly_once_on_grids_with_a_pole_vertex() {
    // A band of rows just south of the north pole crosses the polar cells and
    // their non-polar neighbours. Pole sides stay straight; every other side is
    // traced identically from both cells.
    let (width, height, west, north, dx, dy) = (180u32, 100u32, 0.0, 89.5, 0.5, 0.005);
    let (_tmp, path) = TestGeoTiffBuilder::new(width, height)
        .origin(west, north)
        .pixel_size_xy(dx, dy)
        .create_f32_tempfile(|c, r| 1.0 + ((c + r) % 7) as f32);
    let footprint = [
        west,
        north - f64::from(height) * dy,
        west + f64::from(width) * dx,
        north,
    ];
    for (grid, res) in [(GridKind::Healpix, 6), (GridKind::S2, 8)] {
        let (pixels, retained, area, cells) = overlay_totals(&path, grid, res, footprint);
        assert!(cells > 2, "{grid}");
        assert!(
            (pixels - f64::from(width * height)).abs() < 1e-5,
            "{grid}: {pixels}"
        );
        assert!(
            (retained / area - 1.0).abs() < 1e-6,
            "{grid}: {retained} vs {area}"
        );
    }
}

#[test]
fn overlay_slice_accumulation_matches_per_pixel_accumulation_on_all_grids() {
    // An always-true mean filter makes the scan accumulate pixel by pixel, which
    // is the reference for the span path used when no mean filter is set.
    let (_tmp, path) = TestGeoTiffBuilder::new(160, 120)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_tempfile::<f64, _>(|c, r| {
            1.0 + f64::from((3 * c + r) % 23) * 0.37 + f64::from(c) * 0.001
        });
    for grid in GridKind::ALL {
        for (weighted, mass_preserve) in [(false, false), (true, false), (true, true)] {
            let mut cfg = config(
                grid,
                matched(grid, -2),
                TransferMode::Overlay {
                    weighted,
                    mass_preserve,
                },
            );
            let spans = scan(&path, &cfg);
            cfg.min_mean = Some(-1.0e300);
            let per_pixel = scan(&path, &cfg);
            let label = format!("{grid} weighted={weighted} mass_preserve={mass_preserve}");
            assert!(spans.len() > 4, "{label}");
            assert_eq!(
                spans.keys().collect::<Vec<_>>(),
                per_pixel.keys().collect::<Vec<_>>(),
                "{label}"
            );
            for (id, a) in &spans {
                let b = &per_pixel[id];
                assert_eq!((a.min, a.max), (b.min, b.max), "{label} cell {id}");
                assert!(
                    (a.count - b.count).abs() <= 1e-9 * b.count.max(1.0),
                    "{label} {id}"
                );
                assert!(
                    (a.sum - b.sum).abs() <= 1e-9 * b.sum.abs().max(1.0),
                    "{label} {id}"
                );
                assert!(
                    (a.m2 - b.m2).abs() <= 1e-8 * b.m2.abs().max(1.0),
                    "{label} {id}"
                );
            }
        }
    }
}

fn collect_multi_scan(
    path: &Path,
    cfg: &MultiResolutionConfig,
) -> BTreeMap<(u8, u64), ContinuousAccumulator> {
    use std::sync::Arc;
    let mosaic =
        MosaicReader::open(&[path.to_path_buf()], None, None, OverlapRule::Cutline).unwrap();
    let mut streamer = MultiScanHorizonStreamer::new_mosaic(Arc::new(mosaic), cfg).unwrap();
    let mut out = BTreeMap::new();
    while streamer
        .drain_completed_into(1024, |_, row| {
            assert!(
                out.insert((row.resolution, row.cell_id), row.accumulator)
                    .is_none(),
                "duplicate cell ({}, {:x})",
                row.resolution,
                row.cell_id
            );
        })
        .unwrap()
        > 0
    {}
    out
}

fn assert_continuous_close(a: &ContinuousAccumulator, b: &ContinuousAccumulator, ctx: &str) {
    let close = |x: f64, y: f64, label: &str| {
        assert!(
            (x - y).abs() <= 1e-10 * y.abs().max(1.0),
            "{ctx} {label}: left={x} right={y}"
        );
    };
    close(a.count, b.count, "count");
    close(a.sum, b.sum, "sum");
    close(a.min, b.min, "min");
    close(a.max, b.max, "max");
    close(a.m2, b.m2, "m2");
    close(a.sum_w2, b.sum_w2, "sum_w2");
}

#[test]
fn multi_resolution_overlay_and_interpolate_decode_single_pass() {
    use dggs_stream_core::run_profile::RunProfile;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tiled_128.tif");
    crate::helpers::write_tiled(
        &path,
        (128, 128),
        (32, 32),
        crate::helpers::Crs::Geographic {
            lon: ORIGIN.0,
            lat: ORIGIN.1,
            pixel_deg: PIXEL,
        },
        "-9999",
        |x, y| -> f32 {
            if (x * 7 + y * 5).is_multiple_of(97) {
                -9999.0
            } else {
                plane(
                    ORIGIN.0 + (f64::from(x) + 0.5) * PIXEL,
                    ORIGIN.1 - (f64::from(y) + 0.5) * PIXEL,
                ) as f32
            }
        },
    );

    let resolutions = vec![7u8, 8, 9];
    for mode in [
        TransferMode::OVERLAY,
        TransferMode::Interpolate(Interpolation::Bilinear),
    ] {
        let mut multi_cfg = MultiResolutionConfig::new(resolutions.clone());
        multi_cfg.grid = GridKind::H3;
        multi_cfg.transfer = mode;
        let multi_profile = RunProfile::new();
        multi_cfg.profile = Some(Arc::clone(&multi_profile));
        let multi_cells = collect_multi_scan(&path, &multi_cfg);
        assert!(!multi_cells.is_empty());

        let mut single_expected = BTreeMap::new();
        let mut ref_single_report = None;
        for &res in &resolutions {
            let mut single_cfg = config(GridKind::H3, res, mode);
            let single_profile = RunProfile::new();
            single_cfg.profile = Some(Arc::clone(&single_profile));
            let res_cells = collect_multi_scan(&path, &single_cfg);
            assert!(!res_cells.is_empty());
            single_expected.extend(res_cells);
            ref_single_report.get_or_insert_with(|| single_profile.report());
        }

        assert_eq!(multi_cells, single_expected, "mode={mode:?}");

        let m_rep = multi_profile.report();
        let s_rep = ref_single_report.unwrap();
        assert!(m_rep.chunks > 0);
        assert!(
            m_rep.chunks <= s_rep.chunks,
            "mode={mode:?}: multi chunks {} exceeded single chunks {}",
            m_rep.chunks,
            s_rep.chunks
        );
        assert_eq!(
            m_rep.windows, s_rep.windows,
            "mode={mode:?}: expected single-pass window count"
        );
    }
}

#[test]
fn wrapped_bbox_decodes_each_chunk_of_a_tile_row_once() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use dggs_stream_core::run_profile::RunProfile;
    use std::sync::Arc;

    // 64 x 16 pixels of 5.625 degrees cover -180..180 and 45..-45, in 16 x 1 tiles of
    // 4 x 16 pixels.
    let dir = tempfile::tempdir().unwrap();
    let tiled_path = dir.path().join("globe_tiled.tif");
    let strip_path = dir.path().join("globe_strip.tif");
    let crs = Crs::Geographic {
        lon: -180.0,
        lat: 45.0,
        pixel_deg: 5.625,
    };
    let pixel = |x: u32, y: u32| -> f32 { 100.0 + ((x * 7 + y * 3) % 61) as f32 };
    write_geotiff(
        &tiled_path,
        (64, 16),
        Layout::Tiles(4, 16),
        crs,
        "-9999",
        pixel,
    );
    write_geotiff(
        &strip_path,
        (64, 16),
        Layout::Strips(16),
        crs,
        "-9999",
        pixel,
    );

    let profile = RunProfile::new();
    // H3 resolution 5 cells are far smaller than the pixels, so interpolation plans
    // one-row bands: every band of the one chunk row would otherwise reload both edges.
    let mut cfg = config(
        GridKind::H3,
        5,
        TransferMode::Interpolate(Interpolation::Nearest),
    );
    cfg.bbox = Some([170.0, -45.0, -170.0, 45.0]);
    let mut oracle_cfg = cfg.clone();
    cfg.profile = Some(Arc::clone(&profile));
    oracle_cfg.profile = None;

    let tiled_cells = collect_multi_scan(&tiled_path, &cfg);
    let oracle_cells = collect_multi_scan(&strip_path, &oracle_cfg);
    assert!(tiled_cells.len() > 500, "{} cells", tiled_cells.len());
    assert_eq!(
        tiled_cells.keys().collect::<Vec<_>>(),
        oracle_cells.keys().collect::<Vec<_>>()
    );
    for (k, a) in &tiled_cells {
        assert_continuous_close(a, &oracle_cells[k], &format!("key={k:?}"));
    }
    let chunks = profile.report().chunks;
    assert!(
        chunks <= 16,
        "{chunks} chunk decodes for a raster of 16 chunks"
    );
}

#[test]
fn bbox_ending_at_180_keeps_the_seam_chunk_for_interpolation() {
    use crate::helpers::{write_geotiff, Crs, Layout};

    // A tiled globe whose first chunk column starts at -180. The S2 face centre at
    // longitude 180, latitude 0 lies in a bbox ending at 180 and reads that chunk.
    let dir = tempfile::tempdir().unwrap();
    let tiled_path = dir.path().join("seam_tiled.tif");
    let strip_path = dir.path().join("seam_strip.tif");
    let crs = Crs::Geographic {
        lon: -180.0,
        lat: 45.0,
        pixel_deg: 5.625,
    };
    let pixel = |x: u32, y: u32| -> f32 { 100.0 + ((x * 7 + y * 3) % 61) as f32 };
    write_geotiff(
        &tiled_path,
        (64, 16),
        Layout::Tiles(4, 16),
        crs,
        "-9999",
        pixel,
    );
    write_geotiff(
        &strip_path,
        (64, 16),
        Layout::Strips(16),
        crs,
        "-9999",
        pixel,
    );

    let mut cfg = config(
        GridKind::S2,
        0,
        TransferMode::Interpolate(Interpolation::Nearest),
    );
    cfg.bbox = Some([179.0, -1.0, 180.0, 1.0]);
    let tiled_cells = collect_multi_scan(&tiled_path, &cfg);
    let strip_cells = collect_multi_scan(&strip_path, &cfg);
    assert!(
        !strip_cells.is_empty(),
        "the full-width scan finds the seam cell"
    );
    assert_eq!(
        tiled_cells.keys().collect::<Vec<_>>(),
        strip_cells.keys().collect::<Vec<_>>()
    );
    for (k, a) in &tiled_cells {
        assert_continuous_close(a, &strip_cells[k], &format!("key={k:?}"));
    }
}

#[test]
fn bbox_prunes_transfer_chunks_and_matches_full_scan() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use dggs_stream_core::run_profile::RunProfile;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tiled_path = dir.path().join("sf_tiled.tif");
    let strip_path = dir.path().join("sf_strip.tif");
    let crs = Crs::Geographic {
        lon: -122.45,
        lat: 37.80,
        pixel_deg: 0.0005,
    };
    let sf_pixel = |x: u32, y: u32| -> f32 {
        if (x * 11 + y * 13).is_multiple_of(89) {
            -9999.0
        } else {
            100.0 + 25.0 * (f32::from(x as u16) / 19.0).sin() + f32::from(y as u16) * 0.25
        }
    };
    write_geotiff(
        &tiled_path,
        (256, 256),
        Layout::Tiles(32, 32),
        crs,
        "-9999",
        sf_pixel,
    );
    write_geotiff(
        &strip_path,
        (256, 256),
        Layout::Strips(256),
        crs,
        "-9999",
        sf_pixel,
    );

    let interior_bbox = [-122.398, 37.740, -122.390, 37.748];
    let modes = [
        TransferMode::OVERLAY,
        TransferMode::Overlay {
            weighted: true,
            mass_preserve: true,
        },
        TransferMode::Interpolate(Interpolation::Nearest),
        TransferMode::Interpolate(Interpolation::Bilinear),
        TransferMode::Interpolate(Interpolation::Bicubic),
        TransferMode::Interpolate(Interpolation::Lanczos),
    ];

    for mode in modes {
        let profile = RunProfile::new();
        let mut cfg = config(GridKind::H3, 10, mode);
        cfg.bbox = Some(interior_bbox);
        let mut oracle_cfg = cfg.clone();
        cfg.profile = Some(Arc::clone(&profile));
        oracle_cfg.profile = None;

        let tiled_cells = collect_multi_scan(&tiled_path, &cfg);
        let oracle_cells = collect_multi_scan(&strip_path, &oracle_cfg);
        assert!(!tiled_cells.is_empty(), "mode={mode:?} produced no cells");
        assert_eq!(
            tiled_cells.keys().collect::<Vec<_>>(),
            oracle_cells.keys().collect::<Vec<_>>(),
            "mode={mode:?} key mismatch"
        );
        for (k, a) in &tiled_cells {
            assert_continuous_close(a, &oracle_cells[k], &format!("mode={mode:?} key={k:?}"));
        }
        let report = profile.report();
        assert!(
            report.chunks > 0 && report.chunks < 16,
            "mode={mode:?}: expected < 16 of 64 chunks, got {}",
            report.chunks
        );
    }

    // Disjoint bbox must decode 0 chunks and emit 0 cells.
    for mode in [
        TransferMode::OVERLAY,
        TransferMode::Interpolate(Interpolation::Bilinear),
    ] {
        let profile = RunProfile::new();
        let mut cfg = config(GridKind::H3, 10, mode);
        cfg.bbox = Some([10.0, 10.0, 11.0, 11.0]);
        cfg.profile = Some(Arc::clone(&profile));
        let cells = collect_multi_scan(&tiled_path, &cfg);
        assert!(cells.is_empty(), "mode={mode:?} expected empty cells");
        assert_eq!(
            profile.report().chunks,
            0,
            "mode={mode:?} expected 0 chunks"
        );
    }

    // Antimeridian-crossing bbox (`west > east`) on a global 360°-wide raster.
    let global_tiled = dir.path().join("global_tiled.tif");
    let global_strip = dir.path().join("global_strip.tif");
    let global_crs = Crs::Geographic {
        lon: -180.0,
        lat: 90.0,
        pixel_deg: 360.0 / 256.0,
    };
    let global_pixel = |x: u32, y: u32| -> f32 {
        if (x * 17 + y * 5).is_multiple_of(73) {
            -9999.0
        } else {
            ((x * 3 + y * 7) % 100 + 1) as f32
        }
    };
    write_geotiff(
        &global_tiled,
        (256, 128),
        Layout::Tiles(32, 32),
        global_crs,
        "-9999",
        global_pixel,
    );
    write_geotiff(
        &global_strip,
        (256, 128),
        Layout::Strips(128),
        global_crs,
        "-9999",
        global_pixel,
    );
    let antimeridian_bbox = [170.0, 2.0, -170.0, 35.0];
    for mode in [
        TransferMode::OVERLAY,
        TransferMode::Interpolate(Interpolation::Bilinear),
    ] {
        let profile = RunProfile::new();
        let mut cfg = config(GridKind::H3, 3, mode);
        cfg.bbox = Some(antimeridian_bbox);
        let mut oracle_cfg = cfg.clone();
        cfg.profile = Some(Arc::clone(&profile));
        oracle_cfg.profile = None;

        let tiled_cells = collect_multi_scan(&global_tiled, &cfg);
        let oracle_cells = collect_multi_scan(&global_strip, &oracle_cfg);
        assert!(!tiled_cells.is_empty(), "antimeridian mode={mode:?}");
        assert_eq!(
            tiled_cells.keys().collect::<Vec<_>>(),
            oracle_cells.keys().collect::<Vec<_>>(),
            "antimeridian key mismatch for mode={mode:?}"
        );
        for (k, a) in &tiled_cells {
            assert_continuous_close(a, &oracle_cells[k], &format!("antimeridian {mode:?} {k:?}"));
        }
        let report = profile.report();
        assert!(
            report.chunks > 0 && report.chunks < 16,
            "antimeridian mode={mode:?}: expected < 16 of 32 chunks, got {}",
            report.chunks
        );
    }
}

#[test]
fn ground_area_multi_resolution_and_bbox_prune_match_single_resolution() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use dggs_stream_core::run_profile::RunProfile;
    use dggs_stream_core::transfer::scan_ground_area_overlay;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tiled_path = dir.path().join("ga_tiled.tif");
    let strip_path = dir.path().join("ga_strip.tif");
    let crs = Crs::Geographic {
        lon: -122.45,
        lat: 37.80,
        pixel_deg: 0.0005,
    };
    let px = |x: u32, y: u32| -> f32 {
        if (x * 9 + y * 7).is_multiple_of(83) {
            -9999.0
        } else {
            ((x * 5 + y * 3) % 41 + 1) as f32
        }
    };
    write_geotiff(
        &tiled_path,
        (256, 256),
        Layout::Tiles(32, 32),
        crs,
        "-9999",
        px,
    );
    write_geotiff(
        &strip_path,
        (256, 256),
        Layout::Strips(256),
        crs,
        "-9999",
        px,
    );

    let bbox = [-122.398, 37.724, -122.374, 37.748];
    let run_ga = |path: &Path, resolutions: Vec<u8>| {
        let reader = GeoTiffStreamReader::open(path).unwrap();
        let mut cfg = MultiResolutionConfig::new(resolutions);
        cfg.grid = GridKind::H3;
        cfg.transfer = TransferMode::OVERLAY;
        cfg.bbox = Some(bbox);
        let profile = RunProfile::new();
        cfg.profile = Some(Arc::clone(&profile));
        let mut out = BTreeMap::new();
        for row in scan_ground_area_overlay(reader, &cfg).unwrap() {
            let (res, id, acc) = row.unwrap();
            assert!(out.insert((res, id), acc).is_none());
        }
        (out, profile.report())
    };

    let (multi_tiled, multi_report) = run_ga(&tiled_path, vec![8, 9]);
    let (single_8, report_8) = run_ga(&tiled_path, vec![8]);
    let (single_9, _) = run_ga(&tiled_path, vec![9]);
    let (multi_oracle, _) = run_ga(&strip_path, vec![8, 9]);

    let mut single_combined = single_8;
    single_combined.extend(single_9);

    assert!(!multi_tiled.is_empty());
    assert_eq!(
        multi_tiled.keys().collect::<Vec<_>>(),
        single_combined.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        multi_tiled.keys().collect::<Vec<_>>(),
        multi_oracle.keys().collect::<Vec<_>>()
    );

    for other in [&single_combined, &multi_oracle] {
        for (k, a) in &multi_tiled {
            let b = &other[k];
            assert!(
                (a.pixel_count() - b.pixel_count()).abs() <= 1e-10 * b.pixel_count().max(1.0),
                "{k:?} pixel_count"
            );
            assert!(
                (a.retained_area_m2() - b.retained_area_m2()).abs()
                    <= 1e-10 * b.retained_area_m2().max(1.0),
                "{k:?} area"
            );
            assert!(
                (a.area_weighted_sum() - b.area_weighted_sum()).abs()
                    <= 1e-10 * b.area_weighted_sum().abs().max(1.0),
                "{k:?} sum"
            );
            assert!(
                (a.area_stats().mean() - b.area_stats().mean()).abs()
                    <= 1e-10 * b.area_stats().mean().abs().max(1.0),
                "{k:?} mean"
            );
            assert!(
                (a.area_stats().variance() - b.area_stats().variance()).abs()
                    <= 1e-10 * b.area_stats().variance().abs().max(1.0),
                "{k:?} variance"
            );
        }
    }

    assert!(
        multi_report.chunks > 0 && multi_report.chunks < 16,
        "expected < 16 of 64 chunks, got {}",
        multi_report.chunks
    );
    assert_eq!(
        multi_report.chunks, report_8.chunks,
        "multi-resolution ground_area should decode chunks once"
    );
}

#[test]
fn nested_overlay_rollup_matches_single_res_continuous_quantiles_and_filters() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use dggs_stream_core::aggregator::{NodataPolicy, QuantileTarget};
    use dggs_stream_core::run_profile::RunProfile;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested_cont.tif");
    let crs = Crs::Geographic {
        lon: -122.45,
        lat: 37.80,
        pixel_deg: 0.002,
    };
    write_geotiff(
        &path,
        (64, 64),
        Layout::Tiles(16, 16),
        crs,
        "-9999",
        |x, y| {
            if (x * 11 + y * 7) % 13 == 0 {
                -9999.0
            } else {
                10.0 + ((x * 5 + y * 3) % 47) as f32 * 0.5
            }
        },
    );

    for (grid, resolutions) in [
        (GridKind::S2, vec![12u8, 13, 14]),
        (GridKind::Healpix, vec![11u8, 12, 13]),
    ] {
        for (nodata_policy, min_count, valid_cov, bbox) in [
            (NodataPolicy::Omit, None, None, None),
            (
                NodataPolicy::Emit,
                None,
                Some(0.5f64),
                Some([-122.43, 37.70, -122.34, 37.78]),
            ),
            (
                NodataPolicy::Omit,
                Some(2.0f64),
                Some(0.5f64),
                Some([-122.43, 37.70, -122.34, 37.78]),
            ),
        ] {
            let mut multi_cfg = MultiResolutionConfig::new(resolutions.clone()).with_grid(grid);
            multi_cfg.transfer = TransferMode::OVERLAY;
            multi_cfg.quantiles = QuantileTarget::parse_list("p50").unwrap();
            multi_cfg.nodata_policy = nodata_policy;
            multi_cfg.min_count = min_count;
            multi_cfg.valid_coverage_threshold = valid_cov;
            multi_cfg.bbox = bbox;
            let multi_profile = RunProfile::new();
            multi_cfg.profile = Some(Arc::clone(&multi_profile));
            let multi_cells = collect_multi_scan(&path, &multi_cfg);
            assert!(!multi_cells.is_empty(), "{grid:?} produced no cells");

            let mut single_combined = BTreeMap::new();
            let mut finest_single_report = None;
            for &res in &resolutions {
                let mut single_cfg = multi_cfg.clone();
                single_cfg.resolutions = vec![res];
                let single_profile = RunProfile::new();
                single_cfg.profile = Some(Arc::clone(&single_profile));
                let res_cells = collect_multi_scan(&path, &single_cfg);
                assert!(
                    !res_cells.is_empty(),
                    "{grid:?} res {res} produced no cells"
                );
                single_combined.extend(res_cells);
                if res == *resolutions.iter().max().unwrap() {
                    finest_single_report = Some(single_profile.report());
                }
            }

            assert_eq!(
                multi_cells.keys().collect::<Vec<_>>(),
                single_combined.keys().collect::<Vec<_>>(),
                "{grid:?} key mismatch"
            );
            for (k, a) in &multi_cells {
                let b = &single_combined[k];
                let tol = if k.0 == *resolutions.iter().max().unwrap() {
                    1e-10
                } else {
                    // Coarser S2/HEALPix polygons use 4-corner chords while the
                    // nested finest children follow the subdivided curved edge.
                    2e-3
                };
                for (x, y, label) in [
                    (a.count, b.count, "count"),
                    (a.sum, b.sum, "sum"),
                    (a.min, b.min, "min"),
                    (a.max, b.max, "max"),
                    (a.m2, b.m2, "m2"),
                ] {
                    assert!(
                        (x - y).abs() <= tol * y.abs().max(1.0),
                        "{grid:?} {k:?} {label}: left={x} right={y}"
                    );
                }
                if k.0 == *resolutions.iter().max().unwrap() {
                    assert!(
                        (a.sum_w2 - b.sum_w2).abs() <= 1e-10 * b.sum_w2.abs().max(1.0),
                        "{grid:?} {k:?} sum_w2: left={} right={}",
                        a.sum_w2,
                        b.sum_w2
                    );
                }
                let (qa, qb) = (a.quantile(0.5), b.quantile(0.5));
                if qa.is_finite() && qb.is_finite() {
                    assert!(
                        (qa - qb).abs() <= 1.0,
                        "{grid:?} {k:?} median: left={qa} right={qb}"
                    );
                } else {
                    assert_eq!(qa.is_nan(), qb.is_nan(), "{grid:?} {k:?} median NaN");
                }
            }

            let m_rep = multi_profile.report();
            let f_rep = finest_single_report.unwrap();
            assert!(m_rep.chunks > 0);
            assert!(
                m_rep.chunks <= f_rep.chunks,
                "{grid:?}: multi chunks {} exceeded finest single chunks {}",
                m_rep.chunks,
                f_rep.chunks
            );
            assert_eq!(m_rep.windows, f_rep.windows, "{grid:?} windows");
        }
    }
}

#[test]
fn nested_overlay_rollup_matches_single_res_categorical_and_multi_tile() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tile_north = dir.path().join("cat_north.tif");
    let tile_south = dir.path().join("cat_south.tif");

    // Two vertically adjacent tiles so parents straddle the tile seam.
    write_geotiff(
        &tile_north,
        (48, 32),
        Layout::Tiles(16, 16),
        Crs::Geographic {
            lon: -122.45,
            lat: 37.80,
            pixel_deg: 0.002,
        },
        "255",
        |x, y| {
            if (x + y * 3) % 11 == 0 {
                255
            } else {
                ((x * 3 + y) % 5 + 1) as u8
            }
        },
    );
    write_geotiff(
        &tile_south,
        (48, 32),
        Layout::Tiles(16, 16),
        Crs::Geographic {
            lon: -122.45,
            lat: 37.80 - 32.0 * 0.002,
            pixel_deg: 0.002,
        },
        "255",
        |x, y| {
            if (x * 5 + y) % 13 == 0 {
                255
            } else {
                ((x + y * 2) % 5 + 1) as u8
            }
        },
    );

    let paths = [tile_north, tile_south];
    let collect_cat = |cfg: &MultiResolutionConfig| -> BTreeMap<(u8, u64), _> {
        let mosaic = Arc::new(MosaicReader::open(&paths, None, None, OverlapRule::First).unwrap());
        let mut streamer = MultiCategoricalHorizonStreamer::new_mosaic(mosaic, cfg).unwrap();
        let mut out = BTreeMap::new();
        while streamer
            .drain_completed_into(256, |_i, rec| {
                assert!(
                    out.insert((rec.resolution, rec.cell_id), rec.accumulator)
                        .is_none(),
                    "duplicate cell ({}, {})",
                    rec.resolution,
                    rec.cell_id
                );
            })
            .unwrap()
            > 0
        {}
        out
    };

    for (grid, resolutions) in [
        (GridKind::S2, vec![12u8, 13, 14]),
        (GridKind::Healpix, vec![11u8, 12, 13]),
    ] {
        let mut multi_cfg = MultiResolutionConfig::new(resolutions.clone()).with_grid(grid);
        multi_cfg.transfer = TransferMode::OVERLAY;
        multi_cfg.valid_coverage_threshold = Some(0.4);
        let multi_cells = collect_cat(&multi_cfg);
        assert!(!multi_cells.is_empty(), "{grid:?} produced no cells");

        let mut single_combined = BTreeMap::new();
        for &res in &resolutions {
            let mut single_cfg = multi_cfg.clone();
            single_cfg.resolutions = vec![res];
            single_combined.extend(collect_cat(&single_cfg));
        }

        assert_eq!(
            multi_cells.keys().collect::<Vec<_>>(),
            single_combined.keys().collect::<Vec<_>>(),
            "{grid:?} categorical key mismatch"
        );
        for (k, a) in &multi_cells {
            let b = &single_combined[k];
            let tol = if k.0 == *resolutions.iter().max().unwrap() {
                1e-10
            } else {
                2e-3
            };
            assert!(
                (a.total_count - b.total_count).abs() <= tol * b.total_count.max(1.0),
                "{grid:?} {k:?} total_count: left={} right={}",
                a.total_count,
                b.total_count
            );
            assert_eq!(a.majority().0, b.majority().0, "{grid:?} {k:?} mode");
            assert_eq!(
                a.unique_classes(),
                b.unique_classes(),
                "{grid:?} {k:?} classes"
            );
            a.for_each_class(|cls, w_a| {
                let w_b = b.get_class_count(cls);
                assert!(
                    (w_a - w_b).abs() <= tol * w_b.max(1.0),
                    "{grid:?} {k:?} class {cls} weight: left={w_a} right={w_b}"
                );
            });
        }
    }
}

#[test]
fn centre_scans_keep_minus_180_centres_in_a_bbox_ending_at_180() {
    // One-degree pixels whose first centre is longitude -180, which the point filter
    // places in a box reaching 180. Batched S2 and HEALPix rows must keep it too.
    let (_tmp, path) = TestGeoTiffBuilder::new(10, 4)
        .origin(-180.5, 10.0)
        .pixel_size(1.0)
        .create_tempfile::<f32, _>(|c, r| 1.0 + c as f32 + 10.0 * r as f32);
    for grid in [GridKind::S2, GridKind::Healpix, GridKind::H3] {
        let mut cfg = config(grid, 3, TransferMode::AssignCenters);
        cfg.bbox = Some([179.0, -90.0, 180.0, 90.0]);
        let cells = scan(&path, &cfg);
        let count: f64 = cells.values().map(|a| a.count).sum();
        let sum: f64 = cells.values().map(|a| a.sum).sum();
        // Column 0 of each of the 4 rows: values 1, 11, 21, 31.
        assert_eq!(count, 4.0, "{grid}");
        assert_eq!(sum, 64.0, "{grid}");
    }
}

#[test]
fn nested_rollup_emits_each_parent_once_when_children_wait_for_a_later_tile() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use std::sync::Arc;

    // Tile A touches 180 degrees just south of the equator: its fine children straddling
    // the antimeridian have bounds that overlap every longitude, so they wait for tile B
    // to end. Tile B lies farther west in the same level-7 parent (the equatorial
    // HEALPix cell centred on 180 degrees) and reaches south past it. Level-15 cells are
    // smaller than B's pixels, so B is planned in two row blocks and the parent's end row
    // on B passes in the first, while those children still wait.
    const GRID: GridKind = GridKind::Healpix;
    const COARSE: u8 = 7;
    const FINE: u8 = 15;
    let dir = tempfile::tempdir().unwrap();
    let seam = dir.path().join("seam.tif");
    let tall = dir.path().join("tall.tif");
    let class = |x: u32, y: u32| ((x * 3 + y) % 5 + 1) as u8;
    write_geotiff(
        &seam,
        (10, 20),
        Layout::Tiles(16, 16),
        Crs::Geographic {
            lon: 179.95,
            lat: 0.0,
            pixel_deg: 0.005,
        },
        "255",
        class,
    );
    write_geotiff(
        &tall,
        (20, 256),
        Layout::Tiles(16, 16),
        Crs::Geographic {
            lon: 179.5,
            lat: 0.5,
            pixel_deg: 0.01,
        },
        "255",
        class,
    );
    let paths = [seam, tall];
    let mut cfg = MultiResolutionConfig::new(vec![COARSE, FINE]).with_grid(GRID);
    cfg.transfer = TransferMode::OVERLAY;
    let mosaic = Arc::new(MosaicReader::open(&paths, None, None, OverlapRule::First).unwrap());
    let mut streamer = MultiCategoricalHorizonStreamer::new_mosaic(mosaic, &cfg).unwrap();
    let mut cells = BTreeMap::new();
    while streamer
        .drain_completed_into(256, |_i, rec| {
            assert!(
                cells
                    .insert((rec.resolution, rec.cell_id), rec.accumulator.total_count)
                    .is_none(),
                "duplicate cell ({}, {:x})",
                rec.resolution,
                rec.cell_id
            );
        })
        .unwrap()
        > 0
    {}
    // Each parent's weight is the sum of its children's.
    let mut children = BTreeMap::<u64, f64>::new();
    for (&(res, id), &total) in &cells {
        if res == FINE {
            *children
                .entry(GRID.parent(id, COARSE).unwrap())
                .or_default() += total;
        }
    }
    assert!(!children.is_empty());
    for (parent, total) in children {
        let got = cells[&(COARSE, parent)];
        assert!(
            (got - total).abs() <= 1e-9 * total.max(1.0),
            "{parent:x}: {got} vs {total}"
        );
    }
}

#[test]
fn interpolate_subcell_sampling_reproduces_planes_and_captures_cell_spread() {
    use crate::helpers::{write_geotiff, Crs, Layout};
    use dggs_stream_core::aggregator::{QuantileTarget, SamplingPattern};

    let dir = tempfile::tempdir().unwrap();
    let tiled_path = dir.path().join("plane_tiled.tif");
    let strip_path = dir.path().join("plane_strip.tif");
    let crs = Crs::Geographic {
        lon: ORIGIN.0,
        lat: ORIGIN.1,
        pixel_deg: PIXEL,
    };
    let linear_val = |x: u32, y: u32| -> f32 {
        let (lon, lat) = pixel_centre(x, y);
        plane(lon, lat) as f32
    };
    write_geotiff(
        &tiled_path,
        (W, H),
        Layout::Tiles(16, 16),
        crs,
        "-9999",
        linear_val,
    );
    write_geotiff(
        &strip_path,
        (W, H),
        Layout::Strips(H),
        crs,
        "-9999",
        linear_val,
    );

    for grid in GridKind::ALL {
        let res = matched(grid, 1);
        for bbox in [
            None,
            Some([
                ORIGIN.0 + 0.01,
                ORIGIN.1 - 0.038,
                ORIGIN.0 + 0.05,
                ORIGIN.1 - 0.01,
            ]),
        ] {
            let mut cfg = config(
                grid,
                res,
                TransferMode::Interpolate(Interpolation::Bilinear),
            );
            cfg.sampling = SamplingPattern::rgss();
            cfg.bbox = bbox;

            let tiled = scan(&tiled_path, &cfg);
            let strip = scan(&strip_path, &cfg);
            assert!(!tiled.is_empty(), "{grid}");
            assert_eq!(tiled.len(), strip.len(), "{grid} tiled vs strip count");
            for (&cell, acc_t) in &tiled {
                let acc_s = &strip[&cell];
                assert!((acc_t.count - acc_s.count).abs() < 1e-12, "{grid} {cell}");
                assert!(
                    (acc_t.mean() - acc_s.mean()).abs() < 1e-10,
                    "{grid} {cell} mean mismatch"
                );
                let (lat, lng) = grid.center_lat_lng(cell).unwrap();
                let col = (lng - ORIGIN.0) / PIXEL;
                let row = (ORIGIN.1 - lat) / PIXEL;
                if col > 4.0 && col < f64::from(W) - 4.0 && row > 4.0 && row < f64::from(H) - 4.0 {
                    assert!((acc_t.count - 1.0).abs() < 1e-12, "{grid} {cell} count");
                    // A5 pentagons are asymmetric, so polygon centroid differs slightly from
                    // spherical cell_to_lonlat (~0.01 px); H3/S2/HEALPix match closely.
                    let plane_tol = if grid == GridKind::A5 { 0.2 } else { 1e-2 };
                    assert!(
                        (acc_t.mean() - plane(lng, lat)).abs() < plane_tol,
                        "{grid} {cell} plane mean={} exact={}",
                        acc_t.mean(),
                        plane(lng, lat)
                    );
                }
            }
        }
    }

    // Smooth non-linear field: multi-point cell sampling captures intra-cell spread (min, max,
    // variance, quantiles) and reduces cell-mean error relative to single-point center sampling.
    let (_tmp, wave_path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|c, r| {
            let x = f64::from(c) * 0.45;
            let y = f64::from(r) * 0.45;
            (100.0 + 30.0 * x.sin() * y.cos() + 5.0 * (x - y)) as f32
        });

    for grid in GridKind::ALL {
        let res = matched(grid, 0);
        let overlay = scan(&wave_path, &config(grid, res, TransferMode::OVERLAY));

        let mut bilinear_rgss_cfg = config(
            grid,
            res,
            TransferMode::Interpolate(Interpolation::Bilinear),
        );
        bilinear_rgss_cfg.sampling = SamplingPattern::rgss();
        bilinear_rgss_cfg.quantiles = QuantileTarget::parse_list("p50").unwrap();
        let rgss_cells = scan(&wave_path, &bilinear_rgss_cfg);

        // With Interpolation::Nearest, Overlay is the exact K->inf limit over each cell polygon.
        let mut nearest_center_cfg =
            config(grid, res, TransferMode::Interpolate(Interpolation::Nearest));
        nearest_center_cfg.sampling = SamplingPattern::center();
        let nearest_center_cells = scan(&wave_path, &nearest_center_cfg);

        let mut nearest_16_cfg = nearest_center_cfg.clone();
        nearest_16_cfg.sampling = SamplingPattern::sixteen_point();
        let nearest_16_cells = scan(&wave_path, &nearest_16_cfg);

        let mut spread_ok = 0usize;
        let (mut err_center, mut err_q16, mut interior_n) = (0.0, 0.0, 0usize);
        for (&cell, acc_o) in &overlay {
            let (lat, lng) = grid.center_lat_lng(cell).unwrap();
            let col = (lng - ORIGIN.0) / PIXEL;
            let row = (ORIGIN.1 - lat) / PIXEL;
            if !(col > 4.0 && col < f64::from(W) - 4.0 && row > 4.0 && row < f64::from(H) - 4.0) {
                continue;
            }
            let acc_c = &nearest_center_cells[&cell];
            let acc_r = &rgss_cells[&cell];
            let acc_16 = &nearest_16_cells[&cell];
            if acc_r.max - acc_r.min > 1e-3 {
                assert!(acc_r.min < acc_r.mean() && acc_r.mean() < acc_r.max);
                assert!(acc_r.variance() > 0.0, "{grid} {cell}");
                let q50 = acc_r.quantile(0.5);
                assert!(q50.is_finite() && q50 >= acc_r.min - 1e-9 && q50 <= acc_r.max + 1e-9);
                spread_ok += 1;
            }
            err_center += (acc_c.mean() - acc_o.mean()).abs();
            err_q16 += (acc_16.mean() - acc_o.mean()).abs();
            interior_n += 1;
        }
        assert!(spread_ok > 20, "{grid}: spread_ok={spread_ok}");
        assert!(interior_n > 20, "{grid}: interior_n={interior_n}");
        assert!(
            err_q16 < err_center,
            "{grid}: expected Nearest+16point MAE ({}) < Nearest+center MAE ({})",
            err_q16 / interior_n as f64,
            err_center / interior_n as f64
        );
    }
}

#[test]
fn interpolate_subcell_sampling_categorical_projected_and_auto() {
    use dggs_stream_core::aggregator::SamplingPattern;
    use std::sync::Arc;

    // 1. Categorical Interpolate(Nearest) + 16point captures multi-class boundary fractions.
    let (_tmp_cat, cat_path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_tempfile::<u8, _>(|c, r| if c + r < W { 1 } else { 2 });

    for grid in GridKind::ALL {
        let mut cfg = config(
            grid,
            matched(grid, 0),
            TransferMode::Interpolate(Interpolation::Nearest),
        );
        cfg.sampling = SamplingPattern::sixteen_point();
        let reader = GeoTiffStreamReader::open(&cat_path).unwrap();
        let mut streamer = MultiCategoricalHorizonStreamer::new(reader, &cfg).unwrap();
        let mut out = BTreeMap::new();
        while streamer
            .drain_completed_into(256, |_, rec| {
                out.insert(rec.cell_id, rec.accumulator);
            })
            .unwrap()
            > 0
        {}
        assert!(!out.is_empty(), "{grid}");
        let boundary_cells = out.values().filter(|a| a.unique_classes() > 1).count();
        assert!(
            boundary_cells > 5,
            "{grid}: expected boundary cells with >1 class, got {boundary_cells}"
        );
        let full_cells = out
            .values()
            .filter(|a| (a.total_count - 1.0).abs() < 1e-10)
            .count();
        assert!(
            full_cells > out.len() / 2,
            "{grid}: full_cells={full_cells}"
        );
    }

    // 2. Projected UTM GeoTIFF succeeds with zero transform failures under RGSS sampling.
    let field = |x: f64, y: f64| 0.01 * (x - 500_000.0) + 0.02 * (4_200_000.0 - y);
    let (_tmp_utm, utm_path) = TestGeoTiffBuilder::new(64, 64)
        .epsg(32610)
        .origin(500_000.0, 4_200_000.0)
        .pixel_size(30.0)
        .create_f32_tempfile(|c, r| {
            field(
                500_000.0 + 30.0 * (f64::from(c) + 0.5),
                4_200_000.0 - 30.0 * (f64::from(r) + 0.5),
            ) as f32
        });
    for grid in GridKind::ALL {
        let res = area_matched_resolution(grid, 900.0) + 1;
        let mut cfg = config(
            grid,
            res,
            TransferMode::Interpolate(Interpolation::Bilinear),
        );
        cfg.sampling = SamplingPattern::rgss();
        let reader = GeoTiffStreamReader::open(&utm_path).unwrap();
        let mut streamer = MultiScanHorizonStreamer::new(reader, &cfg).unwrap();
        let mut count = 0usize;
        while streamer
            .drain_completed_into(256, |_, _| count += 1)
            .unwrap()
            > 0
        {}
        assert!(count > 64 * 64, "{grid}: count={count}");
        assert_eq!(streamer.transform_failures(), 0, "{grid}");
    }

    // 3. TransferMode::Auto with non-default sampling resolves to Interpolate below 1:1
    // and falls back to AssignCenters above 16:1 (never Overlay).
    let mosaic = Arc::new(MosaicReader::open(&[cat_path], None, None, OverlapRule::First).unwrap());
    for grid in GridKind::ALL {
        let fine = matched(grid, 2);
        let mut fine_cfg = config(grid, fine, TransferMode::Auto);
        fine_cfg.sampling = SamplingPattern::rgss();
        assert_eq!(
            TransferMode::Auto.resolve_for_mosaic(&mosaic, &fine_cfg, false),
            TransferMode::Interpolate(Interpolation::Bilinear),
            "{grid} fine Auto+sampling"
        );

        let coarse = matched(grid, -3);
        let mut coarse_cfg = config(grid, coarse, TransferMode::Auto);
        coarse_cfg.sampling = SamplingPattern::rgss();
        assert_eq!(
            TransferMode::Auto.resolve_for_mosaic(&mosaic, &coarse_cfg, false),
            TransferMode::AssignCenters,
            "{grid} coarse Auto+sampling"
        );
    }
}

#[test]
fn interpolate_subcell_sampling_keeps_fully_valid_cells_at_min_count_one() {
    use dggs_stream_core::aggregator::SamplingPattern;

    let (_tmp, path) = TestGeoTiffBuilder::new(W, H)
        .origin(ORIGIN.0, ORIGIN.1)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|_, _| 42.0);
    for grid in GridKind::ALL {
        let res = matched(grid, 0);
        for name in [
            "rgss", "5point", "gaussian", "hex", "8rooks", "9point", "16point",
        ] {
            let mut cfg = config(grid, res, TransferMode::Interpolate(Interpolation::Nearest));
            cfg.sampling = SamplingPattern::parse(name).unwrap();
            let all = scan(&path, &cfg);
            cfg.min_count = Some(1.0);
            let kept = scan(&path, &cfg);
            let mut interior = 0;
            for (&cell, acc) in &all {
                let (lat, lng) = grid.center_lat_lng(cell).unwrap();
                let col = (lng - ORIGIN.0) / PIXEL;
                let row = (ORIGIN.1 - lat) / PIXEL;
                if col > 4.0 && col < f64::from(W) - 4.0 && row > 4.0 && row < f64::from(H) - 4.0 {
                    interior += 1;
                    assert_eq!(acc.count, 1.0, "{grid} {name} {cell}");
                    assert!(
                        kept.contains_key(&cell),
                        "{grid} {name} {cell} dropped by min_count=1"
                    );
                }
            }
            assert!(interior > 0, "{grid} {name}");
        }
    }
}

#[test]
fn interpolate_subcell_sampling_refuses_stencils_beyond_the_window_margin() {
    use dggs_stream_core::aggregator::SamplingPattern;

    // 65,536 columns cap a block's row margin at 16 rows, while resolution-6 hexagons
    // (about 60 pixels across here) need more, so their stencils would lose samples.
    let (_tmp, path) = TestGeoTiffBuilder::new(65_536, 64)
        .origin(-170.0, 37.8)
        .pixel_size(PIXEL)
        .create_f32_tempfile(|_, _| 42.0);
    let mut cfg = config(
        GridKind::H3,
        6,
        TransferMode::Interpolate(Interpolation::Nearest),
    );
    cfg.sampling = SamplingPattern::sixteen_point();
    let err = MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(&path).unwrap(), &cfg)
        .err()
        .expect("a stencil beyond the window margin must be refused")
        .to_string();
    assert!(err.contains("window margin"), "{err}");

    cfg.sampling = SamplingPattern::center();
    assert!(MultiScanHorizonStreamer::new(GeoTiffStreamReader::open(&path).unwrap(), &cfg).is_ok());
}
