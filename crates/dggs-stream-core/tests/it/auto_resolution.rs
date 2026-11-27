//! `auto` resolution: the ground pixel area measured through the raster's geotransform
//! and CRS, and the resolution of each grid chosen from it.

use std::path::PathBuf;

use dggs_stream_core::aggregator::multi_horizon::{
    MultiResolutionConfig, MultiScanHorizonStreamer,
};
use dggs_stream_core::raster::ground_area::wgs84_rectangle_area;
use dggs_stream_core::raster::mosaic::{MosaicReader, OverlapRule};
use dggs_stream_core::resolution::{
    area_matched_resolution, auto_resolution, directional_resolution, mosaic_pixel_ground_area_m2,
    ResolutionSpec,
};
use dggs_stream_core::transfer::{resolve_auto_transfer, Interpolation, TransferMode};
use dggs_stream_core::GridKind;

use crate::helpers::TestGeoTiffBuilder;

fn open(paths: &[PathBuf]) -> MosaicReader {
    MosaicReader::open(paths, None, None, OverlapRule::default()).unwrap()
}

#[test]
fn geographic_pixel_area_is_the_ellipsoidal_footprint() {
    for (lon, lat, pixel) in [
        (-122.45, 37.80, 0.001),
        (10.0, 1.0, 0.01),
        (20.0, 75.0, 0.0005),
        (-70.0, -55.0, 0.25),
    ] {
        let (_tmp, path) = TestGeoTiffBuilder::new(40, 30)
            .origin(lon, lat)
            .pixel_size(pixel)
            .create_f32_tempfile(|_, _| 1.0);
        let mosaic = open(&[path]);
        // The centre pixel: column 20, row 15.
        let (west, north) = (lon + 20.0 * pixel, lat - 15.0 * pixel);
        let exact = wgs84_rectangle_area(west, north - pixel, west + pixel, north).unwrap();
        let measured = mosaic_pixel_ground_area_m2(&mosaic).unwrap();
        assert!(
            (measured / exact - 1.0).abs() < 1e-3,
            "({lon}, {lat}) {pixel}: {measured} vs {exact}"
        );
    }
}

#[test]
fn projected_pixel_area_is_measured_on_the_ground() {
    // 30 m UTM zone 10N pixels near the central meridian cover very nearly 900 m^2.
    let (_tmp, utm) = TestGeoTiffBuilder::new(64, 64)
        .epsg(32610)
        .origin(500_000.0, 4_200_000.0)
        .pixel_size(30.0)
        .create_f32_tempfile(|_, _| 1.0);
    let area = mosaic_pixel_ground_area_m2(&open(&[utm])).unwrap();
    assert!((area / 900.0 - 1.0).abs() < 2e-3, "UTM: {area}");

    // Web Mercator stretches with latitude: a 100 m pixel at 60 N covers a quarter of
    // its nominal area on the ground (scale factor 1 / cos(60 deg) = 2 each way).
    let lat = 60.0f64;
    let y = 6_378_137.0
        * (std::f64::consts::FRAC_PI_4 + lat.to_radians() / 2.0)
            .tan()
            .ln();
    let (_tmp, mercator) = TestGeoTiffBuilder::new(20, 20)
        .epsg(3857)
        .origin(0.0, y + 1000.0)
        .pixel_size(100.0)
        .create_f32_tempfile(|_, _| 1.0);
    let area = mosaic_pixel_ground_area_m2(&open(&[mercator])).unwrap();
    assert!(
        (area / 2500.0 - 1.0).abs() < 0.02,
        "Web Mercator at 60N: {area}"
    );
}

#[test]
fn auto_picks_the_area_matched_resolution_on_every_grid() {
    let (_tmp, path) = TestGeoTiffBuilder::new(64, 48)
        .pixel_size(0.001)
        .create_f32_tempfile(|c, r| (c + r) as f32);
    let paths = [path];
    let mosaic = open(&paths);
    let area = mosaic_pixel_ground_area_m2(&mosaic).unwrap();
    for grid in GridKind::ALL {
        let matched = area_matched_resolution(grid, area);
        // The match is within one aperture step of the pixel area, and no neighbour is
        // closer.
        let ratio = (grid.area_m2(matched) / area).ln().abs();
        let aperture = f64::from(grid.spec().aperture);
        assert!(ratio <= aperture.ln() / 2.0 + 1e-9, "{grid}: res {matched}");
        for neighbour in [
            matched.saturating_sub(1),
            (matched + 1).min(grid.max_resolution()),
        ] {
            assert!(ratio <= (grid.area_m2(neighbour) / area).ln().abs() + 1e-12);
        }
        assert_eq!(auto_resolution(grid, &mosaic, 0).unwrap(), matched);
        assert_eq!(
            ResolutionSpec::AUTO.resolve(grid, &mosaic).unwrap(),
            vec![matched]
        );
        for offset in [-3, -1, 1, 2, 64, -64] {
            let spec = ResolutionSpec::parse(grid, &format!("auto{offset:+}")).unwrap();
            let expected = directional_resolution(grid, area, offset).unwrap();
            assert_eq!(auto_resolution(grid, &mosaic, offset).unwrap(), expected);
            assert_eq!(spec.resolve(grid, &mosaic).unwrap(), vec![expected]);
            assert_eq!(
                spec.resolve_paths(grid, &paths, None).unwrap(),
                vec![expected]
            );
        }
        // `auto+1` is the coarsest resolution finer than the pixel and `auto-1` the finest
        // at least a pixel: adjacent, with `auto` one of the two, and each on its side.
        let (fine, coarse) = (
            auto_resolution(grid, &mosaic, 1).unwrap(),
            auto_resolution(grid, &mosaic, -1).unwrap(),
        );
        assert_eq!(fine, coarse + 1, "{grid}");
        assert!(matched == fine || matched == coarse, "{grid}");
        assert!(grid.area_m2(fine) < area, "{grid}");
        assert!(grid.area_m2(coarse) >= area, "{grid}");
        assert_eq!(auto_resolution(grid, &mosaic, 2).unwrap(), fine + 1);
        assert_eq!(auto_resolution(grid, &mosaic, -2).unwrap(), coarse - 1);
        // Measured the same way, `transfer = "auto"` interpolates `auto+N` cells and
        // overlays `auto-N` cells.
        for n in 1..=3 {
            let fine = auto_resolution(grid, &mosaic, n).unwrap();
            let coarse = auto_resolution(grid, &mosaic, -n).unwrap();
            assert_eq!(
                resolve_auto_transfer(grid, &[fine], area, false, true),
                TransferMode::Interpolate(Interpolation::Bilinear),
                "{grid} auto+{n}"
            );
            assert_eq!(
                resolve_auto_transfer(grid, &[coarse], area, false, true),
                TransferMode::OVERLAY,
                "{grid} auto-{n}"
            );
        }
        let fixed = ResolutionSpec::parse(grid, "2,5").unwrap();
        assert_eq!(fixed.resolve_paths(grid, &paths, None).unwrap(), vec![2, 5]);

        // A scan at the auto resolution has about one pixel per cell.
        let config = MultiResolutionConfig::single(matched).with_grid(grid);
        let reader =
            dggs_stream_core::raster::geotiff::GeoTiffStreamReader::open(&paths[0]).unwrap();
        let mut streamer = MultiScanHorizonStreamer::new(reader, &config).unwrap();
        let (mut cells, mut pixels) = (0usize, 0.0);
        while streamer
            .drain_completed_into(4096, |_, rec| {
                cells += 1;
                pixels += rec.accumulator.count;
            })
            .unwrap()
            > 0
        {}
        assert_eq!(pixels, 64.0 * 48.0);
        let per_cell = pixels / cells as f64;
        assert!(
            (1.0 / aperture.sqrt() * 0.7..=aperture.sqrt() * 1.4).contains(&per_cell),
            "{grid} res {matched}: {per_cell} pixels per cell"
        );
    }
}

#[test]
fn a_mosaic_takes_its_finest_tile() {
    let (_a, coarse) = TestGeoTiffBuilder::new(16, 16)
        .origin(-122.45, 37.80)
        .pixel_size(0.004)
        .create_f32_tempfile(|_, _| 1.0);
    let (_b, fine) = TestGeoTiffBuilder::new(64, 64)
        .origin(-122.386, 37.80)
        .pixel_size(0.001)
        .create_f32_tempfile(|_, _| 2.0);
    let fine_only = mosaic_pixel_ground_area_m2(&open(std::slice::from_ref(&fine))).unwrap();
    let coarse_only = mosaic_pixel_ground_area_m2(&open(std::slice::from_ref(&coarse))).unwrap();
    assert!(coarse_only > 15.0 * fine_only);
    for paths in [vec![coarse.clone(), fine.clone()], vec![fine, coarse]] {
        let both = mosaic_pixel_ground_area_m2(&open(&paths)).unwrap();
        assert_eq!(both, fine_only);
    }
}

#[test]
fn auto_needs_a_raster_it_can_locate() {
    let (_tmp, path) = TestGeoTiffBuilder::new(8, 8)
        .with_geokeys(false)
        .pixel_size(0.001)
        .create_f32_tempfile(|_, _| 1.0);
    // With the CRS given, `auto` works as for a tagged raster.
    let with_crs = ResolutionSpec::AUTO
        .resolve_paths(GridKind::H3, std::slice::from_ref(&path), Some("EPSG:4326"))
        .unwrap();
    assert_eq!(with_crs.len(), 1);
    let missing = ResolutionSpec::AUTO.resolve_paths(
        GridKind::H3,
        &[PathBuf::from("/nonexistent/raster.tif")],
        None,
    );
    assert!(missing.is_err());
}
