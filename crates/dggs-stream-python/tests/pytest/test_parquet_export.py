"""`raster_to_parquet` output read back with Polars' native Parquet reader (not
`use_pyarrow=True`): parquet-rs 60 footers made it fail with "Invalid thrift: bad data".
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from conftest import RES  # noqa: E402


@pytest.mark.parametrize("geoparquet", [True, False])
@pytest.mark.parametrize("categorical", [False, True])
def test_native_polars_reads_exporter_output(
    dggs_stream, gradient_tif, classes_tif, tmp_path, geoparquet, categorical
):
    tif = classes_tif if categorical else gradient_tif
    out = tmp_path / "cells.parquet"
    written = dggs_stream.raster_to_parquet(
        tif, out, RES, categorical=categorical, geoparquet=geoparquet
    )
    assert written > 0

    df = pl.read_parquet(out)
    assert df.height == written
    assert ("geometry" in df.columns) == geoparquet
    assert df["h3_index"].n_unique() == written

    lazy = pl.scan_parquet(out).select(pl.len()).collect().item()
    assert lazy == written


@pytest.mark.parametrize("layout", ["interleaved", "planar"])
@pytest.mark.parametrize("geo", [True, False])
def test_multiband_export_matches_scan(dggs_stream, tmp_path, layout, geo):
    import json
    from pathlib import Path

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    fixtures = Path(__file__).resolve().parents[3] / "dggs-stream-core/tests/fixtures"
    path = fixtures / f"multiband-12-{layout}.tif"
    out = tmp_path / "subdir" / "cells.parquet"
    written = dggs_stream.raster_to_parquet(path, out, 12, bands=[3, 1], geoparquet=geo)
    expected = pa.table(dggs_stream.RasterArrowStream(path, 12, bands=[3, 1])).sort_by("h3_index")
    actual = pq.read_table(out).sort_by("h3_index")
    assert written == len(expected) > 2048
    assert pq.ParquetFile(out).num_row_groups > 1
    assert actual.schema.equals(expected.schema)
    assert actual.equals(expected)
    assert actual["band_1_mean"].null_count > 0
    assert pl.read_parquet(out).height == written
    metadata = actual.schema.metadata or {}
    assert (b"geo" in metadata) == geo
    if geo:
        spec = json.loads(metadata[b"geo"])
        assert spec["primary_column"] == "wkb"
        geometry = spec["columns"]["wkb"]
        assert geometry["encoding"] == "WKB"
        assert geometry["crs"]["id"] == {"authority": "OGC", "code": "CRS84"}
        west, south, east, north = geometry["bbox"]
        assert west < east and south < north
