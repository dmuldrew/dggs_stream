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
