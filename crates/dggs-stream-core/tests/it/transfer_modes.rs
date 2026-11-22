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
    assert_eq!(streamer.transfer_mode(), config.transfer);
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
