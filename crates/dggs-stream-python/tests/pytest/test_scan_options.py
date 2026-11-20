"""Scan options carried by the `_v2` entry points (`band`, `sampling`, `overlap_rule`,
`min_majority_fraction`, `spectral_formula`), the `overlap_rule` of `plan_partitions`, and
`parquet_to_pmtiles`. Expected values are computed independently from the pixel values and
the `h3` package's cell of each pixel centre.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("pyarrow")

from conftest import ORIGIN, PIXEL, RES, frame, pixel_cells, sort, write_geotiff  # noqa: E402

W, H = 32, 24


def two_band_value(c: int, r: int):
    # Integers keep float32 sums exact; band 2 > band 1 keeps NDVI well defined.
    return float(c % 5 + 1), float(r % 7 + 10)


@pytest.fixture(scope="module")
def two_band_tif(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("options") / "two_band.tif"
    write_geotiff(path, W, H, two_band_value, bands=2)
    return path


def per_cell(value) -> dict:
    """Per-cell `(count, sum)` of `value(c, r)` over the two-band raster's pixel centres."""
    acc = defaultdict(lambda: [0.0, 0.0])
    for (c, r), cell in pixel_cells(W, H, RES).items():
        acc[cell][0] += 1.0
        acc[cell][1] += value(c, r)
    return dict(acc)


def assert_matches(df, expected) -> None:
    got = {row["h3_index"]: (row["count"], row["sum"]) for row in df.iter_rows(named=True)}
    assert got.keys() == expected.keys()
    for cell, (count, total) in expected.items():
        assert got[cell][0] == count
        assert got[cell][1] == pytest.approx(total)


# -----------------------------------------------------------------------------
# band
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("band", [1, 2])
def test_band_selects_that_band(dggs_stream, two_band_tif, band):
    df = dggs_stream.read_raster(two_band_tif, RES, band=band)
    assert_matches(df, per_cell(lambda c, r: two_band_value(c, r)[band - 1]))


def test_default_band_is_one(dggs_stream, two_band_tif):
    default = sort(dggs_stream.read_raster(two_band_tif, RES))
    assert default.equals(sort(dggs_stream.read_raster(two_band_tif, RES, band=1)))


def test_band_beyond_raster_is_an_error(dggs_stream, two_band_tif):
    with pytest.raises(RuntimeError, match="band 3 does not exist: the raster has 2 band"):
        dggs_stream.read_raster(two_band_tif, RES, band=3)


@pytest.mark.parametrize("band", [0, -1])
def test_band_below_one_is_an_error(dggs_stream, two_band_tif, band):
    with pytest.raises(RuntimeError, match="band must be at least 1"):
        dggs_stream.RasterArrowStream(two_band_tif, RES, band=band).to_dict()


def test_band_must_be_an_int(dggs_stream, two_band_tif):
    with pytest.raises(TypeError, match="band must be an int"):
        dggs_stream.RasterArrowStream(two_band_tif, RES, band="2")


# -----------------------------------------------------------------------------
# spectral_formula
# -----------------------------------------------------------------------------


def test_ndvi_from_chosen_bands(dggs_stream, two_band_tif):
    df = dggs_stream.read_raster(
        two_band_tif, RES, spectral_formula="NDVI", nir_band=2, red_band=1
    )

    def ndvi(c, r):
        red, nir = two_band_value(c, r)
        return (nir - red) / (nir + red)

    assert_matches(df, per_cell(ndvi))


def test_spectral_formula_default_bands_must_exist(dggs_stream, two_band_tif):
    # NDVI defaults to nir_band=4, red_band=3, as in DuckDB.
    with pytest.raises(RuntimeError, match="spectral formula band 4 does not exist"):
        dggs_stream.read_raster(two_band_tif, RES, spectral_formula="ndvi")


def test_unknown_spectral_formula_is_an_error(dggs_stream, two_band_tif):
    with pytest.raises(RuntimeError, match="Invalid spectral_formula 'savi'.*ndvi, ndwi"):
        dggs_stream.RasterArrowStream(two_band_tif, RES, spectral_formula="savi").to_dict()


def test_formula_bands_without_formula_are_an_error(dggs_stream, two_band_tif):
    with pytest.raises(RuntimeError, match="need a spectral_formula"):
        dggs_stream.RasterArrowStream(two_band_tif, RES, nir_band=2).to_dict()


def test_spectral_formula_rejects_categorical(dggs_stream, classes_tif):
    with pytest.raises(RuntimeError, match="continuous scans only"):
        dggs_stream.RasterArrowStream(
            classes_tif, RES, categorical=True, spectral_formula="ndvi"
        ).to_dict()


# -----------------------------------------------------------------------------
# sampling
# -----------------------------------------------------------------------------


def test_center_sampling_is_the_default(dggs_stream, two_band_tif):
    default = sort(dggs_stream.read_raster(two_band_tif, RES))
    center = sort(dggs_stream.read_raster(two_band_tif, RES, sampling="center"))
    assert center.equals(default)


@pytest.mark.parametrize("pattern", ["rgss", "hex", "16point"])
def test_sub_pixel_sampling_spreads_each_pixel_over_cells(dggs_stream, two_band_tif, pattern):
    center = dggs_stream.read_raster(two_band_tif, RES)
    sampled = dggs_stream.read_raster(two_band_tif, RES, sampling=pattern)
    # Sample weights sum to one per pixel, so the totals are unchanged...
    assert sampled["count"].sum() == pytest.approx(center["count"].sum())
    assert sampled["sum"].sum() == pytest.approx(center["sum"].sum())
    # ...but pixels straddling a cell edge now contribute to both sides.
    assert not sort(sampled).select("h3_index", "count").equals(
        sort(center).select("h3_index", "count")
    )


def test_unknown_sampling_is_an_error(dggs_stream, two_band_tif):
    # Core falls back to centre sampling for unknown names; the adapter refuses them.
    with pytest.raises(RuntimeError, match="Invalid sampling 'bilinear'"):
        dggs_stream.RasterArrowStream(two_band_tif, RES, sampling="bilinear").to_dict()


# -----------------------------------------------------------------------------
# overlap_rule
# -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def overlapping_mosaic(tmp_path_factory) -> str:
    """Two 32x24 rasters of 1.0 and 3.0, the second shifted east by half its width."""
    d = tmp_path_factory.mktemp("mosaic")
    write_geotiff(d / "a.tif", W, H, lambda c, r: 1.0)
    write_geotiff(d / "b.tif", W, H, lambda c, r: 3.0, origin=(ORIGIN[0] + PIXEL * 16, ORIGIN[1]))
    return f"{d / 'a.tif'},{d / 'b.tif'}"


def test_overlap_rules(dggs_stream, overlapping_mosaic):
    def scan(rule):
        return dggs_stream.read_raster(overlapping_mosaic, RES, overlap_rule=rule)

    pixels, overlap = 2 * W * H, (W - 16) * H
    cutline, first, average = scan("cutline"), scan("first"), scan("average")
    # Cutline and first keep one sample per ground pixel; average keeps both tiles' samples.
    assert cutline["count"].sum() == pixels - overlap
    assert first["count"].sum() == pixels - overlap
    assert average["count"].sum() == pixels
    # `first` takes the overlap from the first tile, so 1.0 covers more of it than 3.0 does.
    assert first["sum"].sum() == (W * H) * 1.0 + (W * H - overlap) * 3.0
    assert sort(scan(None)).equals(sort(cutline))


def test_unknown_overlap_rule_is_an_error(dggs_stream, overlapping_mosaic):
    with pytest.raises(RuntimeError, match="Invalid overlap_rule 'max'"):
        dggs_stream.RasterArrowStream(overlapping_mosaic, RES, overlap_rule="max").to_dict()


def test_partitions_follow_the_overlap_rule(dggs_stream, overlapping_mosaic):
    whole = sort(
        dggs_stream.read_raster(overlapping_mosaic, RES, overlap_rule="first")
    )
    specs = dggs_stream.plan_partitions(overlapping_mosaic, target_chunks=1, overlap_rule="first")
    assert len(specs) == 2
    parts = [
        dggs_stream.RasterArrowStream(
            overlapping_mosaic, RES, partition=spec, overlap_rule="first"
        ).to_pyarrow()
        for spec in specs
    ]
    rows = pl.concat([frame(p) for p in parts])
    final = rows.filter(pl.col("partial_state").is_null()).drop("partial_state")
    merged = frame(dggs_stream.merge_partials(rows.filter(pl.col("partial_state").is_not_null())))
    assert sort(pl.concat([final, merged])).equals(whole)

    # A plan made under another rule does not match these scans.
    cutline_spec = dggs_stream.plan_partitions(overlapping_mosaic, target_chunks=1)[0]
    with pytest.raises(RuntimeError):
        dggs_stream.RasterArrowStream(
            overlapping_mosaic, RES, partition=cutline_spec, overlap_rule="first"
        ).to_pyarrow()


# -----------------------------------------------------------------------------
# min_majority_fraction
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("fraction", [0.0, 0.5, 0.75, 1.0])
def test_min_majority_fraction_filters_cells(dggs_stream, classes_tif, fraction):
    everything = dggs_stream.read_raster(classes_tif, RES, categorical=True)
    kept = dggs_stream.read_raster(
        classes_tif, RES, categorical=True, min_majority_fraction=fraction
    )
    expected = everything.filter(pl.col("majority_fraction") >= fraction)
    assert 0 < expected.height or fraction == 1.0
    assert sort(kept).equals(sort(expected))


def test_min_majority_fraction_rejects_continuous(dggs_stream, gradient_tif):
    with pytest.raises(RuntimeError, match="categorical scans only"):
        dggs_stream.RasterArrowStream(gradient_tif, RES, min_majority_fraction=0.5).to_dict()


@pytest.mark.parametrize("fraction", [-0.1, 1.5, float("nan")])
def test_min_majority_fraction_range(dggs_stream, classes_tif, fraction):
    with pytest.raises(ValueError, match="min_majority_fraction must be (between 0 and 1|finite)"):
        dggs_stream.RasterArrowStream(
            classes_tif, RES, categorical=True, min_majority_fraction=fraction
        ).to_dict()


# -----------------------------------------------------------------------------
# Every scan path carries the options
# -----------------------------------------------------------------------------


def test_options_reach_every_scan_path(dggs_stream, two_band_tif):
    expected = sort(dggs_stream.read_raster(two_band_tif, RES, band=2))
    lazy = dggs_stream.scan_raster(two_band_tif, RES, band=2)
    if isinstance(lazy, pl.LazyFrame):  # Polars >= 1.4 (register_io_source)
        assert sort(lazy.collect()).equals(expected)
    stream = dggs_stream.RasterArrowStream(two_band_tif, RES, band=2)
    assert sort(stream.to_polars()).equals(expected)
    assert sort(pl.concat(list(stream.iter_batches(7)))).equals(expected)
    # `to_dict` goes through JSON, which carries `wkb` as hex text.
    no_wkb = expected.drop("wkb")
    as_dict = pl.DataFrame(stream.to_dict()).select(no_wkb.columns).cast(dict(no_wkb.schema))
    assert sort(as_dict).equals(no_wkb)


# -----------------------------------------------------------------------------
# parquet_to_pmtiles
# -----------------------------------------------------------------------------


def test_parquet_to_pmtiles(dggs_stream, gradient_tif, tmp_path):
    parquet = tmp_path / "cells.parquet"
    rows = dggs_stream.raster_to_parquet(gradient_tif, parquet, RES)
    out = tmp_path / "cells.pmtiles"
    assert dggs_stream.parquet_to_pmtiles(parquet, out) == rows
    assert out.read_bytes()[:7] == b"PMTiles"


def test_parquet_to_pmtiles_named_hex_column(dggs_stream, gradient_tif, tmp_path):
    parquet = tmp_path / "hex.parquet"
    df = dggs_stream.read_raster(gradient_tif, RES, columns=["h3_hex", "mean"])
    df.rename({"h3_hex": "Cell_ID"}).write_parquet(parquet)
    out = tmp_path / "hex.pmtiles"
    assert dggs_stream.parquet_to_pmtiles(parquet, out, h3_column="cell_id") == df.height


def test_parquet_to_pmtiles_missing_column(dggs_stream, gradient_tif, tmp_path):
    parquet = tmp_path / "cells.parquet"
    dggs_stream.raster_to_parquet(gradient_tif, parquet, RES)
    with pytest.raises(RuntimeError, match="Cell column 'nope' not found"):
        dggs_stream.parquet_to_pmtiles(parquet, tmp_path / "x.pmtiles", h3_column="nope")


@pytest.mark.parametrize("layout", ["interleaved", "planar"])
def test_multiband_matches_single_band_scans(dggs_stream, layout):
    import pyarrow as pa

    fixtures = Path(__file__).resolve().parents[3] / "dggs-stream-core/tests/fixtures"
    path = fixtures / f"multiband-12-{layout}.tif"
    selection = [3, 1]
    options = {"bands": selection, "quantiles": "p50"}
    lazy = dggs_stream.scan_raster(path, 12, **options)
    def collect(scan):
        return scan.collect() if isinstance(scan, pl.LazyFrame) else scan.to_polars()

    rows = collect(lazy)
    if isinstance(lazy, pl.LazyFrame):
        schema = lazy.collect_schema() if hasattr(lazy, "collect_schema") else lazy.schema
        assert schema == rows.schema
    arrow = dggs_stream.RasterArrowStream(path, 12, **options)
    assert pa.schema(arrow).equals(pa.table(arrow).schema)
    metrics = ["mean", "stddev", "count", "min", "max", "sum", "p50"]
    assert rows.columns == ["h3_index", "h3_hex", "resolution", "wkb"] + [
        f"band_{band}_{metric}" for band in selection for metric in metrics
    ]
    observed_cells = set()
    for band in selection:
        single = collect(dggs_stream.scan_raster(path, 12, band=band, quantiles="p50"))
        reference = {r["h3_index"]: r for r in single.iter_rows(named=True)}
        observed_cells.update(reference)
        for row in rows.iter_rows(named=True):
            expected = reference.get(row["h3_index"])
            for metric in metrics:
                value = row[f"band_{band}_{metric}"]
                want = expected[metric] if expected else (0.0 if metric in ("count", "sum") else None)
                if want is None:
                    assert value is None
                else:
                    assert value == pytest.approx(want)
    assert observed_cells == set(rows["h3_index"])
    projection = ["band_1_mean", "band_3_count"]
    subset = collect(dggs_stream.scan_raster(path, 12, columns=projection, limit=3, **options))
    assert subset.columns == projection
    assert subset.shape == (3, 2)
    if isinstance(lazy, pl.LazyFrame):
        pushed = lazy.select(projection).limit(3).collect()
        assert pushed.columns == projection
        assert pushed.shape == (3, 2)


@pytest.mark.parametrize("bands", [[], [3, 3], [0], [True], "3,1"])
def test_invalid_multiband_selection(dggs_stream, two_band_tif, bands):
    with pytest.raises((ValueError, TypeError), match="bands"):
        dggs_stream.scan_raster(two_band_tif, bands=bands)


def test_multiband_rejects_conflicting_or_missing_band(dggs_stream, two_band_tif):
    import pyarrow as pa

    with pytest.raises(RuntimeError, match="bands"):
        pa.schema(dggs_stream.RasterArrowStream(two_band_tif, bands=[1, 2], band=1))
    with pytest.raises(RuntimeError, match="band"):
        dggs_stream.RasterArrowStream(two_band_tif, bands=[3]).__arrow_c_stream__()
