"""Every grid (`h3`, `a5`, `s2`, `healpix`) through the Python API: scans, Polars
pushdown, partitions, exports and scalar helpers. Expected cells come from indexing
each pixel centre with the scalar `lonlat_to_cell`, a path separate from the scan's."""

from __future__ import annotations

import json
from collections import defaultdict

import pytest

pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")

from conftest import ORIGIN, PIXEL, class_value, frame, gradient_value  # noqa: E402

GRIDS = ["h3", "a5", "s2", "healpix"]
#: A resolution per grid whose cells hold a handful of 0.0005° pixels.
RES = {"h3": 9, "a5": 14, "s2": 15, "healpix": 14}


try:
    from polars.io.plugins import register_io_source  # noqa: F401

    HAS_IO_SOURCE = True
except ImportError:
    HAS_IO_SOURCE = False


class _EagerScans:
    """`raster_h3` whose `scan_raster` / `scan_raster_h3` always return a `LazyFrame`.

    Without `register_io_source` (Polars < 1.4) they return a `RasterH3ArrowStream`; it
    is read here, so the grid checks below still run (pushdown is then Polars' own)."""

    def __init__(self, module):
        self._module = module

    def __getattr__(self, name):
        return getattr(self._module, name)

    def _lazy(self, fn, *args, **kwargs):
        out = fn(*args, **kwargs)
        return out if isinstance(out, pl.LazyFrame) else frame(pa.table(out)).lazy()

    def scan_raster(self, *args, **kwargs):
        return self._lazy(self._module.scan_raster, *args, **kwargs)

    def scan_raster_h3(self, *args, **kwargs):
        return self._lazy(self._module.scan_raster_h3, *args, **kwargs)


@pytest.fixture
def raster_h3(raster_h3):
    return raster_h3 if HAS_IO_SOURCE else _EagerScans(raster_h3)


def expected(raster_h3, grid, width, height, value):
    acc = defaultdict(list)
    for r in range(height):
        for c in range(width):
            lng = ORIGIN[0] + PIXEL * (c + 0.5)
            lat = ORIGIN[1] - PIXEL * (r + 0.5)
            acc[raster_h3.lonlat_to_cell(lng, lat, RES[grid], grid)].append(value(c, r))
    return acc


@pytest.mark.parametrize("grid", GRIDS)
def test_continuous_scan_matches_pixel_centres(raster_h3, gradient_tif, grid):
    df = frame(pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, RES[grid], grid=grid)))
    index, hex_ = f"{grid}_index", f"{grid}_hex"
    assert df.columns[:2] == [index, hex_]
    want = expected(raster_h3, grid, 64, 48, gradient_value)
    assert len(want) > 4
    assert set(df[index]) == set(want)
    for row in df.iter_rows(named=True):
        values = want[row[index]]
        assert row["count"] == len(values)
        assert row["sum"] == sum(values)
        assert row["min"] == min(values) and row["max"] == max(values)
        assert row[hex_] == format(row[index], "x")
        assert row["resolution"] == RES[grid]
        assert raster_h3.cell_to_wkb(row[index], grid) == row["wkb"]


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("format", ["wide", "long"])
def test_categorical_scan_matches_pixel_centres(raster_h3, classes_tif, grid, format):
    df = frame(
        pa.table(
            raster_h3.RasterH3ArrowStream(
                classes_tif, RES[grid], grid=grid, categorical=True, format=format
            )
        )
    )
    index = f"{grid}_index"
    want = expected(raster_h3, grid, 48, 48, class_value)
    assert set(df[index]) == set(want)
    for row in df.iter_rows(named=True):
        classes = want[row[index]]
        assert row["total_count"] == len(classes)
        if format == "long":
            assert row["count"] == classes.count(row["category"])
        else:
            # The histogram holds each class's fraction of the cell's samples (4 decimals).
            hist = {int(k): v for k, v in json.loads(row["histogram"]).items()}
            assert hist.keys() == set(classes)
            for k, fraction in hist.items():
                assert fraction == pytest.approx(classes.count(k) / len(classes), abs=1e-4)


@pytest.mark.parametrize("grid", GRIDS)
def test_polars_pushdown_is_the_same_on_every_grid(raster_h3, gradient_tif, grid):
    index, hex_ = f"{grid}_index", f"{grid}_hex"
    lazy = raster_h3.scan_raster(gradient_tif, RES[grid], grid=grid)
    schema = lazy.collect_schema()
    assert schema.names()[:2] == [index, hex_]
    assert schema[index] == pl.UInt64 and schema[hex_] == pl.String
    full = lazy.collect().sort(index)

    projected = lazy.select(index, "mean").collect().sort(index)
    assert projected.columns == [index, "mean"]
    assert projected.equals(full.select(index, "mean"))

    threshold = full["count"].median()
    filtered = lazy.filter(pl.col("count") >= threshold).collect().sort(index)
    assert filtered.equals(full.filter(pl.col("count") >= threshold))
    assert 0 < filtered.height < full.height

    assert lazy.head(3).collect().height == 3
    assert lazy.select(pl.len()).collect().item() == full.height
    wanted = full[hex_][0]
    assert lazy.filter(pl.col(hex_) == wanted).collect()[index].to_list() == [full[index][0]]


@pytest.mark.parametrize("grid", GRIDS)
def test_multi_resolution_and_resolution_filter(raster_h3, gradient_tif, grid):
    res = [RES[grid] - 2, RES[grid]]
    lazy = raster_h3.scan_raster_h3(gradient_tif, res, grid=grid)
    df = lazy.collect()
    assert sorted(df["resolution"].unique()) == res
    coarse = lazy.filter(pl.col("resolution") == res[0]).collect()
    assert coarse.height == df.filter(pl.col("resolution") == res[0]).height
    for cell in coarse[f"{grid}_index"]:
        assert raster_h3.cell_resolution(cell, grid) == res[0]


@pytest.mark.parametrize("grid", ["a5", "s2", "healpix"])
def test_resolution_filter_above_h3_range(raster_h3, gradient_tif, grid):
    """Resolutions past H3's 15 survive a pushed-down `resolution` predicate."""
    res = [RES[grid], 17]
    lazy = raster_h3.scan_raster_h3(gradient_tif, res, grid=grid)
    fine = lazy.filter(pl.col("resolution") == 17).collect()
    assert fine.height > 0 and set(fine["resolution"]) == {17}
    assert lazy.filter(pl.col("resolution") > 15).collect().height == fine.height
    assert lazy.filter(pl.col("resolution") <= 15).collect().height > 0


@pytest.mark.parametrize("grid", GRIDS)
def test_mean_filter_sees_rounded_means(raster_h3, gradient_tif, grid):
    """With `decimals`, a mean predicate compares the rounded means the rows show."""
    lazy = raster_h3.scan_raster_h3(gradient_tif, RES[grid], grid=grid, decimals=1)
    df = lazy.collect()
    raw = raster_h3.scan_raster_h3(gradient_tif, RES[grid], grid=grid).collect()
    joined = raw.join(df, on=f"{grid}_index", suffix="_shown")
    # A mean that rounds up to the threshold must be kept.
    candidates = joined.filter(pl.col("mean") < pl.col("mean_shown"))
    assert candidates.height > 0
    threshold = candidates["mean_shown"][0]
    kept = lazy.filter(pl.col("mean") >= threshold).collect()
    assert kept.height == df.filter(pl.col("mean") >= threshold).height


@pytest.mark.parametrize("grid,finest", [("h3", 15), ("a5", 30), ("s2", 30), ("healpix", 29)])
def test_resolution_range_follows_the_grid(raster_h3, gradient_tif, grid, finest):
    assert raster_h3.GRIDS[grid] == finest
    stream = raster_h3.RasterH3ArrowStream(gradient_tif, finest, grid=grid, limit=1)
    assert frame(pa.table(stream)).height == 1
    with pytest.raises(RuntimeError, match=f"between 0 and {finest}"):
        pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, finest + 1, grid=grid))


def test_unknown_grid_is_rejected(raster_h3, gradient_tif):
    with pytest.raises(ValueError, match="unknown grid"):
        raster_h3.RasterH3ArrowStream(gradient_tif, 9, grid="s9")
    with pytest.raises(TypeError):
        raster_h3.scan_raster_h3(gradient_tif, 9, grid=2)
    # The default and an explicit H3 are the same scan.
    a = frame(pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, 9)))
    b = frame(pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, 9, grid="H3")))
    assert a.equals(b)


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("categorical", [False, True])
def test_partitions_merge_to_the_single_scan(raster_h3, tmp_path, grid, categorical):
    from conftest import write_geotiff

    tif = tmp_path / "strips.tif"
    if categorical:
        write_geotiff(tif, 48, 48, class_value, dtype="u8", rows_per_strip=4)
    else:
        write_geotiff(tif, 48, 48, gradient_value, rows_per_strip=4)
    index = f"{grid}_index"
    kwargs = dict(grid=grid, categorical=categorical)
    single = frame(pa.table(raster_h3.RasterH3ArrowStream(tif, RES[grid], **kwargs))).sort(index)
    specs = raster_h3.plan_partitions(tif, target_chunks=3)
    assert len(specs) > 1
    parts = [
        frame(pa.table(raster_h3.RasterH3ArrowStream(tif, RES[grid], partition=s, **kwargs)))
        for s in specs
    ]
    rows = pl.concat(parts)
    state = raster_h3.PARTIAL_STATE_COLUMN
    final = rows.filter(pl.col(state).is_null()).drop(state)
    partial = rows.filter(pl.col(state).is_not_null())
    assert partial.height > 0
    merged = frame(raster_h3.merge_partials(partial, grid=grid, categorical=categorical))
    combined = pl.concat([final, merged.select(final.columns)]).sort(index)
    keys = [index, "total_count" if categorical else "count"]
    assert combined.select(keys).equals(single.select(keys))


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("categorical", [False, True])
def test_parquet_and_pmtiles_exports(raster_h3, gradient_tif, classes_tif, tmp_path, grid, categorical):
    tif = classes_tif if categorical else gradient_tif
    index = f"{grid}_index"
    scanned = frame(
        pa.table(raster_h3.RasterH3ArrowStream(tif, RES[grid], grid=grid, categorical=categorical))
    )

    out = tmp_path / "cells.parquet"
    written = raster_h3.raster_to_parquet(tif, out, RES[grid], grid=grid, categorical=categorical)
    df = pl.read_parquet(out)
    assert written == df.height == scanned.height
    assert set(df[index]) == set(scanned[index])

    direct = tmp_path / "direct.pmtiles"
    assert raster_h3.raster_to_pmtiles(tif, direct, RES[grid], grid=grid, categorical=categorical) == written
    assert direct.read_bytes()[:7] == b"PMTiles"

    # The grid is detected from the cell column's name...
    tiles = tmp_path / "cells.pmtiles"
    assert raster_h3.parquet_to_pmtiles(out, tiles) == written
    assert raster_h3.parquet_to_pmtiles(out, tiles, cell_column=index.upper()) == written
    # ...and given explicitly when the name does not say.
    renamed = tmp_path / "renamed.parquet"
    df.select(pl.col(index).alias("id"), pl.exclude(index)).write_parquet(renamed)
    assert raster_h3.parquet_to_pmtiles(renamed, tiles, cell_column="id", grid=grid) == written
    if grid != "h3":
        # Read as H3, another grid's ids are not cells.
        assert raster_h3.parquet_to_pmtiles(renamed, tiles, cell_column="id") == 0


def test_parquet_to_pmtiles_keeps_the_h3_column_alias(raster_h3, gradient_tif, tmp_path):
    out = tmp_path / "cells.parquet"
    written = raster_h3.raster_to_parquet(gradient_tif, out, 9)
    tiles = tmp_path / "cells.pmtiles"
    assert raster_h3.parquet_to_pmtiles(out, tiles, h3_column="h3_index") == written
    with pytest.raises(ValueError, match="not both"):
        raster_h3.parquet_to_pmtiles(out, tiles, h3_column="h3_index", cell_column="geometry")


POINTS = [(-122.4194, 37.7749), (151.21, -33.86), (-21.9, 64.1), (179.9, 0.1), (0.0, -89.5)]


@pytest.mark.parametrize("grid", GRIDS)
def test_scalar_helpers_round_trip(raster_h3, grid):
    finest = raster_h3.GRIDS[grid]
    # The a5 crate indexes some points at resolution 30 to a resolution-29 cell, so the
    # round trip is checked one level up for A5.
    deepest = finest - 1 if grid == "a5" else finest
    for lon, lat in POINTS:
        for res in (0, 5, deepest):
            cell = raster_h3.lonlat_to_cell(lon, lat, res, grid)
            assert cell and raster_h3.cell_is_valid(cell, grid)
            assert raster_h3.cell_resolution(cell, grid) == res
            hex_ = raster_h3.cell_to_string(cell, grid)
            assert raster_h3.string_to_cell(hex_, grid) == cell
            assert raster_h3.cell_is_valid(hex_, grid)
            centre = (raster_h3.cell_to_lng(cell, grid), raster_h3.cell_to_lat(cell, grid))
            assert raster_h3.lonlat_to_cell(*centre, res, grid) == cell
            wkb = raster_h3.cell_to_wkb(cell, grid)
            assert wkb is not None and wkb == raster_h3.cell_to_wkb(hex_, grid)
            if res:
                parent = raster_h3.cell_to_parent(cell, res - 1, grid)
                assert raster_h3.cell_resolution(parent, grid) == res - 1
                if grid != "a5":
                    # An A5 parent need not hold every point of its children.
                    assert parent == raster_h3.lonlat_to_cell(lon, lat, res - 1, grid)
                assert raster_h3.cell_to_parent(hex_, res - 1, grid) == format(parent, "x")
            assert raster_h3.cell_to_parent(cell, res + 1, grid) is None
        assert raster_h3.lonlat_to_cell(lon, lat, finest + 1, grid) is None
    assert raster_h3.lonlat_to_cell(0.0, 91.0, 3, grid) is None
    assert not raster_h3.cell_is_valid(0, grid)
    assert not raster_h3.cell_is_valid("not hex", grid)
    assert raster_h3.cell_to_string(0, grid) is None
    assert raster_h3.cell_to_wkb(0, grid) is None
    assert raster_h3.cell_resolution(0, grid) == -1


def test_h3_scalars_agree_with_the_h3_specific_helpers(raster_h3):
    for lon, lat in POINTS:
        cell = raster_h3.lonlat_to_cell(lon, lat, 7)
        assert raster_h3.h3_is_valid(cell)
        assert raster_h3.cell_to_string(cell) == raster_h3.h3_to_string(cell)
        assert raster_h3.cell_to_wkb(cell) == raster_h3.h3_to_wkb(cell)
        assert raster_h3.cell_to_parent(cell, 3) == raster_h3.h3_cell_to_parent(cell, 3)


def test_s2_ids_with_the_top_bit_set_survive_as_uint64(raster_h3):
    # S2 faces 4 and 5 set bit 63; the ids stay unsigned end to end.
    cell = raster_h3.lonlat_to_cell(0.0, -89.5, 12, "s2")
    assert cell >= 1 << 63
    assert raster_h3.string_to_cell(raster_h3.cell_to_string(cell, "s2"), "s2") == cell


@pytest.mark.parametrize("grid", GRIDS)
def test_polygon_to_cells(raster_h3, grid):
    import struct

    ring = [(-122.45, 37.75), (-122.40, 37.75), (-122.40, 37.80), (-122.45, 37.80), (-122.45, 37.75)]
    wkb = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(ring))
    wkb += b"".join(struct.pack("<2d", *p) for p in ring)
    res = RES[grid] - 2
    cells = raster_h3.polygon_to_cells(wkb, res, grid=grid)
    assert cells and cells == sorted(set(cells))
    for cell in cells:
        lng, lat = raster_h3.cell_to_lng(cell, grid), raster_h3.cell_to_lat(cell, grid)
        assert -122.45 <= lng <= -122.40 and 37.75 <= lat <= 37.80
    assert raster_h3.lonlat_to_cell(-122.425, 37.775, res, grid) in cells
    assert raster_h3.polygon_to_cells(wkb, raster_h3.GRIDS[grid] + 1, grid=grid) is None
    assert raster_h3.polygon_to_cells(b"junk", res, grid=grid) is None
    with pytest.raises(ValueError, match="containment"):
        raster_h3.polygon_to_cells(wkb, res, "sideways", grid=grid)


@pytest.mark.parametrize("grid", GRIDS)
def test_auto_resolution(raster_h3, gradient_tif, classes_tif, tmp_path, grid):
    matched = raster_h3.auto_resolution(gradient_tif, grid)
    finest = raster_h3.GRIDS[grid]
    assert 0 < matched < finest
    # 0.0005 degree pixels: the matched cells hold about one pixel each.
    df = raster_h3.scan_raster(gradient_tif, "auto", grid=grid).collect()
    assert df["resolution"].unique().to_list() == [matched]
    assert df["count"].sum() == 64 * 48
    assert 0.3 < 64 * 48 / df.height < 4.0

    for text, offset in [("auto+1", 1), ("AUTO-2", -2), ("auto + 1", 1), ("auto+60", 60)]:
        want = max(0, min(finest, matched + offset))
        assert raster_h3.auto_resolution(gradient_tif, grid, offset=offset) == want
        stream = raster_h3.RasterH3ArrowStream(gradient_tif, text, grid=grid, limit=1)
        assert frame(pa.table(stream))["resolution"].to_list() == [want]

    # Categorical scans and the exporters take it too.
    cat = raster_h3.read_raster(classes_tif, "auto", grid=grid, categorical=True)
    assert cat["resolution"].unique().to_list() == [raster_h3.auto_resolution(classes_tif, grid)]
    out = tmp_path / "auto.parquet"
    written = raster_h3.raster_to_parquet(gradient_tif, out, "auto", grid=grid)
    assert written == df.height
    index = f"{grid}_index"
    assert set(pl.read_parquet(out)[index]) == set(df[index])
    assert raster_h3.raster_to_pmtiles(gradient_tif, tmp_path / "auto.pmtiles", "auto-1", grid=grid) > 0

    # A filter on `resolution` still gives the right rows.
    lazy = raster_h3.scan_raster(gradient_tif, "auto", grid=grid)
    assert lazy.filter(pl.col("resolution") == matched).collect().height == df.height
    assert lazy.filter(pl.col("resolution") == matched + 1).collect().height == 0

    for bad in ["auto+", "auto1", "automatic", "auto+1.5"]:
        with pytest.raises(RuntimeError, match="auto"):
            pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, bad, grid=grid))
    with pytest.raises(RuntimeError):
        raster_h3.auto_resolution(tmp_path / "missing.tif", grid)
    with pytest.raises(TypeError):
        raster_h3.auto_resolution(gradient_tif, grid, offset=1.5)


def test_auto_resolution_is_comparable_across_grids(raster_h3, gradient_tif):
    # Cells per raster agree within the grids' aperture steps, unlike a fixed number.
    counts = {
        grid: raster_h3.scan_raster(gradient_tif, "auto", grid=grid).collect().height
        for grid in GRIDS
    }
    assert max(counts.values()) / min(counts.values()) < 6.0, counts


NODATA = -9999.0


def _holes(c, r):
    """NoData in the left third and along a diagonal; fractional values elsewhere."""
    if c < 20 or (c + r) % 9 == 0:
        return NODATA
    return float((c * 7 + r * 3) % 11) + 0.375


@pytest.fixture(scope="module")
def holes_tif(tmp_path_factory):
    from conftest import write_geotiff

    path = tmp_path_factory.mktemp("rasters") / "holes.tif"
    write_geotiff(path, 60, 40, _holes)
    return path


def _expected_holes(raster_h3, grid):
    acc = expected(raster_h3, grid, 60, 40, _holes)
    return {cell: [v for v in values if v != NODATA] for cell, values in acc.items()}


@pytest.mark.parametrize("grid", GRIDS)
def test_nodata_policy_emit_and_decimals(raster_h3, holes_tif, tmp_path, grid):
    index = f"{grid}_index"
    want = _expected_holes(raster_h3, grid)
    empty = {cell for cell, values in want.items() if not values}
    assert 2 < len(empty) < len(want)
    scan = lambda **kw: (  # noqa: E731
        raster_h3.scan_raster(holes_tif, RES[grid], grid=grid, nodata=NODATA, **kw)
        .collect()
        .sort(index)
    )

    omit = scan()
    assert set(omit[index]) == set(want) - empty
    assert scan(nodata_policy="omit").equals(omit)

    # emit: every cell a pixel falls in; nulls where there is no valid sample.
    emit = scan(nodata_policy="emit")
    assert set(emit[index]) == set(want)
    for row in emit.iter_rows(named=True):
        values = want[row[index]]
        assert row["count"] == len(values)
        if values:
            assert row["mean"] == pytest.approx(sum(values) / len(values))
            assert (row["min"], row["max"]) == (min(values), max(values))
        else:
            assert row["mean"] is None and row["min"] is None and row["max"] is None
            assert row["stddev"] is None and row["sum"] == 0.0
            assert row["wkb"] is not None
    assert emit.filter(pl.col("count") > 0).select(index, "count", "min", "max").equals(
        omit.select(index, "count", "min", "max")
    )

    # emit with a value, rounded to one decimal: the value is written as given.
    filled = scan(nodata_policy="emit", emit_nodata_value=-1.25, decimals=1)
    assert filled.height == emit.height
    for row in filled.iter_rows(named=True):
        values = want[row[index]]
        if values:
            assert row["mean"] == round(sum(values) / len(values) + 1e-12, 1)
            assert row["count"] == len(values)
        else:
            assert (row["mean"], row["min"], row["max"]) == (-1.25, -1.25, -1.25)

    # decimals alone: same cells, rounded statistics.
    rounded = scan(decimals=0)
    assert rounded[index].to_list() == omit[index].to_list()
    assert rounded["count"].to_list() == omit["count"].to_list()
    assert all(v == int(v) for v in rounded["mean"])
    assert any(v != int(v) for v in omit["mean"])

    # Filters still hold under emit (a count filter is applied by Polars, not pushed).
    lazy = raster_h3.scan_raster(
        holes_tif, RES[grid], grid=grid, nodata=NODATA, nodata_policy="emit"
    )
    assert lazy.filter(pl.col("count") >= 1).collect().height == omit.height
    assert lazy.filter(pl.col("count") == 0).collect().height == len(empty)
    assert lazy.filter(pl.col("mean").is_null()).select(pl.len()).collect().item() == len(empty)

    # The arrow stream carries the nulls too.
    table = pa.table(
        raster_h3.RasterH3ArrowStream(
            holes_tif, RES[grid], grid=grid, nodata=NODATA, nodata_policy="emit"
        )
    )
    assert table.column("mean").null_count == len(empty)

    # Exporters: Parquet writes NaN for a missing statistic.
    out = tmp_path / "emit.parquet"
    written = raster_h3.raster_to_parquet(
        holes_tif, out, RES[grid], grid=grid, nodata_policy="emit", decimals=1
    )
    # The exporter has no `nodata=` override; the fixture declares none, so nothing is
    # NoData there and every cell has samples.
    assert written == len(want)
    assert raster_h3.raster_to_pmtiles(
        holes_tif, tmp_path / "emit.pmtiles", RES[grid], grid=grid, nodata_policy="emit"
    ) == len(want)


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("format", ["wide", "long"])
def test_nodata_policy_emit_categorical(raster_h3, tmp_path, grid, format):
    from conftest import write_geotiff

    tif = tmp_path / "classes_holes.tif"
    write_geotiff(tif, 60, 40, lambda c, r: 0 if c < 20 else (c // 5 + r // 5) % 3 + 1, dtype="u8")
    index = f"{grid}_index"
    total = "total_count"
    key = "majority_class" if format == "wide" else "category"
    scan = lambda **kw: raster_h3.scan_raster(  # noqa: E731
        tif, RES[grid], grid=grid, categorical=True, format=format, nodata=0, **kw
    ).collect()
    omit = scan()
    assert omit.filter(pl.col(total) == 0).height == 0
    emit = scan(nodata_policy="emit")
    empties = emit.filter(pl.col(total) == 0)
    assert empties.height > 2
    assert emit[index].n_unique() == omit[index].n_unique() + empties.height
    assert empties[key].null_count() == empties.height
    assert emit.filter(pl.col(total) > 0)[key].null_count() == 0

    filled = scan(nodata_policy="emit", emit_nodata_value=255)
    assert filled.filter(pl.col(total) == 0)[key].unique().to_list() == [255]
    assert filled[key].null_count() == 0


def test_output_option_errors(raster_h3, gradient_tif, tmp_path):
    def scan(**kw):
        return pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, 9, **kw))

    with pytest.raises(RuntimeError, match="nodata_policy"):
        scan(nodata_policy="drop")
    with pytest.raises(RuntimeError, match="decimals"):
        scan(decimals=16)
    with pytest.raises(RuntimeError, match="emit_nodata_value"):
        scan(emit_nodata_value=1.0)
    with pytest.raises(RuntimeError, match="min_count"):
        scan(nodata_policy="emit", min_count=2)
    with pytest.raises(TypeError):
        scan(decimals=1.5)
    with pytest.raises(ValueError):
        scan(nodata_policy="emit", emit_nodata_value=float("nan"))
    with pytest.raises(RuntimeError, match="emit_nodata_value"):
        raster_h3.raster_to_parquet(
            gradient_tif, tmp_path / "c.parquet", 9, categorical=True, nodata_policy="emit"
        )
    assert (
        raster_h3.raster_to_parquet(
            gradient_tif,
            tmp_path / "c.parquet",
            9,
            categorical=True,
            nodata_policy="emit",
            emit_nodata_value=0,
        )
        > 0
    )


def _plane(lng, lat):
    return 1000.0 + 4000.0 * (lng - ORIGIN[0]) - 2500.0 * (lat - ORIGIN[1])


@pytest.fixture(scope="module")
def plane_tif(tmp_path_factory):
    from conftest import write_geotiff

    path = tmp_path_factory.mktemp("rasters") / "plane.tif"
    write_geotiff(
        path,
        64,
        48,
        lambda c, r: _plane(ORIGIN[0] + PIXEL * (c + 0.5), ORIGIN[1] - PIXEL * (r + 0.5)),
    )
    return path


@pytest.mark.parametrize("grid", GRIDS)
def test_transfer_sample(raster_h3, plane_tif, grid):
    index = f"{grid}_index"
    res = raster_h3.auto_resolution(plane_tif, grid, offset=2)
    centres = raster_h3.scan_raster(plane_tif, res, grid=grid).collect()
    assert raster_h3.scan_raster(plane_tif, res, grid=grid, transfer="assign_centers").collect().height == centres.height
    for method, tolerance in [("nearest", 2.1), ("bilinear", 2e-3), ("bicubic", 2e-3), ("lanczos", 0.2)]:
        df = raster_h3.scan_raster(
            plane_tif, res, grid=grid, transfer="sample", sample_method=method
        ).collect()
        # Cells two steps finer than the pixels: sampling fills what centres leave empty.
        assert df.height > 3 * centres.height
        assert df[index].n_unique() == df.height
        assert df["count"].unique().to_list() == [1.0]
        worst = 0.0
        for cell, mean in zip(df[index], df["mean"]):
            lng, lat = raster_h3.cell_to_lng(cell, grid), raster_h3.cell_to_lat(cell, grid)
            col, row = (lng - ORIGIN[0]) / PIXEL, (ORIGIN[1] - lat) / PIXEL
            assert 0 <= col < 64 and 0 <= row < 48
            if 3.5 < col < 60.5 and 3.5 < row < 44.5:
                worst = max(worst, abs(mean - _plane(lng, lat)))
        assert worst < tolerance, (grid, method, worst)
    # The method name alone selects sampling; pushdown still works.
    lazy = raster_h3.scan_raster(plane_tif, res, grid=grid, transfer="bicubic")
    assert lazy.select(pl.len()).collect().item() == df.height
    assert lazy.select(index, "mean").head(5).collect().shape == (5, 2)
    table = pa.table(raster_h3.RasterH3ArrowStream(plane_tif, res, grid=grid, transfer="sample", limit=7))
    assert table.num_rows == 7


@pytest.mark.parametrize("grid", GRIDS)
def test_transfer_overlay_conserves_mass(raster_h3, gradient_tif, tmp_path, grid):
    index = f"{grid}_index"
    total = sum(gradient_value(c, r) for r in range(48) for c in range(64))
    for offset in (-2, 0):
        res = raster_h3.auto_resolution(gradient_tif, grid, offset=offset)
        overlay = raster_h3.scan_raster(gradient_tif, res, grid=grid, transfer="overlay").collect()
        assert overlay["count"].sum() == pytest.approx(64 * 48, rel=1e-6)
        assert overlay["sum"].sum() == pytest.approx(total, rel=1e-6)
        assert (overlay["mean"] >= overlay["min"] - 1e-9).all()
        preserved = raster_h3.scan_raster(
            gradient_tif, res, grid=grid, transfer="overlay", mass_preserve=True
        ).collect()
        assert preserved["sum"].sum() == pytest.approx(total, rel=1e-9)
        assert preserved["count"].sum() == pytest.approx(64 * 48, rel=1e-9)
        touched = raster_h3.scan_raster(
            gradient_tif, res, grid=grid, transfer="overlay", overlay_weighted=False
        ).collect()
        assert touched.height == overlay.height
        assert (touched["count"] == touched["count"].round()).all()
        centres = raster_h3.scan_raster(gradient_tif, res, grid=grid).collect()
        assert set(centres[index]) <= set(overlay[index])

    # Exporters take the same options; the Parquet file holds the overlay cells.
    out = tmp_path / "overlay.parquet"
    written = raster_h3.raster_to_parquet(
        gradient_tif, out, res, grid=grid, transfer="overlay", mass_preserve=True
    )
    assert written == preserved.height
    assert pl.read_parquet(out)["sum_value"].sum() == pytest.approx(total, rel=1e-9)
    assert raster_h3.raster_to_pmtiles(
        gradient_tif, tmp_path / "sample.pmtiles", res, grid=grid, transfer="sample"
    ) > 0


@pytest.mark.parametrize("grid", GRIDS)
def test_transfer_categorical(raster_h3, classes_tif, grid):
    res = raster_h3.auto_resolution(classes_tif, grid, offset=2)
    nearest = raster_h3.scan_raster(
        classes_tif, res, grid=grid, categorical=True, transfer="sample", sample_method="nearest"
    ).collect()
    assert nearest.height > 3 * 48 * 48
    for cell, cls in zip(nearest[f"{grid}_index"], nearest["majority_class"]):
        lng, lat = raster_h3.cell_to_lng(cell, grid), raster_h3.cell_to_lat(cell, grid)
        c, r = int((lng - ORIGIN[0]) / PIXEL), int((ORIGIN[1] - lat) / PIXEL)
        assert cls == class_value(c, r)
    # Polars wraps the scan's error in its own exception type.
    with pytest.raises(Exception, match="nearest"):
        raster_h3.scan_raster(classes_tif, res, grid=grid, categorical=True, transfer="sample").collect()
    overlay = raster_h3.scan_raster(
        classes_tif, raster_h3.auto_resolution(classes_tif, grid, offset=-2), grid=grid,
        categorical=True, transfer="overlay",
    ).collect()
    assert overlay["total_count"].sum() == pytest.approx(48 * 48, rel=1e-6)


def test_transfer_option_errors(raster_h3, gradient_tif):
    def scan(**kw):
        return pa.table(raster_h3.RasterH3ArrowStream(gradient_tif, 9, **kw))

    for kw, needle in [
        (dict(transfer="smear"), "transfer"),
        (dict(sample_method="bilinear"), "sample_method"),
        (dict(transfer="overlay", sample_method="bilinear"), "sample_method"),
        (dict(mass_preserve=True), "mass_preserve"),
        (dict(transfer="overlay", overlay_weighted=False, mass_preserve=True), "mass_preserve"),
        (dict(transfer="overlay", sampling="5point"), "sampling"),
    ]:
        with pytest.raises(RuntimeError, match=needle):
            scan(**kw)
    with pytest.raises(TypeError):
        scan(transfer="overlay", mass_preserve=1)


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("transfer", ["assign_centers", "overlay"])
def test_profile_raster(raster_h3, gradient_tif, grid, transfer):
    rows = raster_h3.scan_raster_h3(gradient_tif, RES[grid], grid=grid, transfer=transfer).collect().height
    out = raster_h3.profile_raster(gradient_tif, RES[grid], grid=grid, transfer=transfer)
    profile = out["profile"]
    assert out["rows"] == rows == profile["cells"]
    assert profile["pixels_read"] == 64 * 48
    assert profile["samples_indexed"] > 0
    assert profile["elapsed_ms"] > 0
    assert 0 <= profile["stall_pct"] <= 100
    names = {p["name"] for p in profile["phases"]}
    assert {"fetch", "tiff_decode", "kernel", "horizon", "serialize"} <= names
    kernel = next(p for p in profile["phases"] if p["name"] == "kernel")
    assert kernel["wall_ms"] > 0
    # Only the pixel-centre kernel is split further; sample and overlay report it whole.
    if transfer == "assign_centers":
        split = {c["name"] for c in kernel["children"]}
        assert {"crs_transform", "grid_index"} <= split <= {"crs_transform", "grid_index", "accumulate"}
    with pytest.raises(TypeError, match="unknown scan option"):
        raster_h3.profile_raster(gradient_tif, 8, nope=1)


def _bins(c, r):
    """A continuous field with fractions: 0.1 .. 9.85 in steps of 0.25."""
    return ((c * 7 + r * 13) % 40) * 0.25 + 0.1


@pytest.fixture(scope="module")
def bins_tif(tmp_path_factory):
    from conftest import write_geotiff

    path = tmp_path_factory.mktemp("rasters") / "bins.tif"
    write_geotiff(path, 64, 48, _bins)
    return path


@pytest.mark.parametrize("grid", GRIDS)
def test_binned_histogram(raster_h3, bins_tif, grid):
    import polars as pl

    edges = [0.0, 2.0, 4.0, 6.0, 8.0]
    want = defaultdict(lambda: [0.0] * 4)
    for cell, values in expected(raster_h3, grid, 64, 48, _bins).items():
        for v in values:
            if v <= 8.0:
                want[cell][min(int(v // 2), 3)] += 1.0
    lf = raster_h3.scan_raster_h3(bins_tif, RES[grid], grid=grid, histogram="4:0:8")
    assert lf.collect_schema()["histogram"] == pl.List(
        pl.Struct({"lower": pl.Float64, "upper": pl.Float64, "weight": pl.Float64})
    )
    df = lf.collect()
    got = {}
    for cell, hist in zip(df[f"{grid}_index"], df["histogram"]):
        rows = hist.to_list()
        assert [(e["lower"], e["upper"]) for e in rows] == list(zip(edges, edges[1:]))
        got[cell] = [e["weight"] for e in rows]
    assert got == dict(want)

    # Explicit edges are the same bins; fractions sum to one, densities integrate to one.
    same = raster_h3.scan_raster_h3(bins_tif, RES[grid], grid=grid, histogram=edges).collect()
    assert same["histogram"].to_list() == df["histogram"].to_list()
    for normalize, width in (("fraction", 1.0), ("density", 2.0)):
        scaled = raster_h3.scan_raster_h3(
            bins_tif, RES[grid], grid=grid, histogram=edges, histogram_normalize=normalize
        ).collect()
        totals = scaled.select(
            pl.col("histogram").list.eval(pl.element().struct.field("weight")).list.sum()
        )["histogram"]
        assert all(abs(t * width - 1.0) < 1e-12 for t in totals)

    # Area weights spread each pixel over the cells it overlaps; every bin keeps its pixels.
    area = raster_h3.scan_raster_h3(
        bins_tif, RES[grid], grid=grid, histogram="4:0:8", histogram_weight="area"
    ).collect()
    per_bin = [0.0] * 4
    for hist in area["histogram"]:
        for i, e in enumerate(hist.to_list()):
            per_bin[i] += e["weight"]
    counted = [sum(w[i] for w in want.values()) for i in range(4)]
    assert all(abs(a / c - 1.0) < 1e-6 for a, c in zip(per_bin, counted))


@pytest.mark.parametrize("grid", GRIDS)
def test_value_histogram(raster_h3, classes_tif, grid):
    import polars as pl
    import pyarrow as pa

    want = {
        cell: sorted((v, float(values.count(v))) for v in set(values))
        for cell, values in expected(raster_h3, grid, 48, 48, class_value).items()
    }
    df = raster_h3.scan_raster_h3(classes_tif, RES[grid], grid=grid, histogram="values").collect()
    assert df.schema["histogram"] == pl.List(pl.Struct({"class": pl.Int64, "weight": pl.Float64}))
    got = {
        cell: [(e["class"], e["weight"]) for e in hist.to_list()]
        for cell, hist in zip(df[f"{grid}_index"], df["histogram"])
    }
    assert got == want
    # The Arrow stream carries the same list of structs, and JSON the same numbers.
    stream = raster_h3.RasterH3ArrowStream(classes_tif, RES[grid], grid=grid, histogram="values")
    table = pa.table(stream)
    assert pa.types.is_list(table.schema.field("histogram").type)
    assert pa.types.is_struct(table.schema.field("histogram").type.value_type)
    assert sorted(map(str, table.column("histogram").to_pylist())) == sorted(
        map(str, df["histogram"].to_list())
    )
    # Without a spec the column is still JSON text (class fractions).
    plain = raster_h3.scan_raster_h3(classes_tif, RES[grid], grid=grid, categorical=True).collect()
    assert plain.schema["histogram"] == pl.String
    assert set(plain[f"{grid}_index"]) == set(want)
    assert all(text.startswith("{") for text in plain["histogram"])


def test_histogram_option_errors(raster_h3, classes_tif):
    for options, message in [
        (dict(histogram="0:0:1"), "bins"),
        (dict(histogram=[2, 1]), "ascending"),
        (dict(histogram="values", histogram_normalize="density"), "density"),
        (dict(histogram_weight="mass"), "weight"),
        (dict(histogram_weight="area", transfer="sample"), "overlay"),
        (dict(histogram="2:0:9", remap={1: 2}), "remap"),
    ]:
        with pytest.raises(Exception, match=message):
            raster_h3.scan_raster_h3(classes_tif, 9, **options).collect()
    with pytest.raises(TypeError, match="numbers"):
        raster_h3.scan_raster_h3(classes_tif, 9, histogram=["a", "b"])
