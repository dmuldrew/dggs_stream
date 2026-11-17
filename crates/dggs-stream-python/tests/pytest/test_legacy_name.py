"""The package's former name, `raster_h3`, still imports, as an alias of `dggs_stream`."""

from __future__ import annotations

import importlib
import sys

import pytest

import dggs_stream


def _fresh_import(name: str):
    sys.modules.pop(name, None)
    return importlib.import_module(name)


def test_raster_h3_is_dggs_stream_and_warns() -> None:
    with pytest.warns(DeprecationWarning, match="dggs_stream"):
        legacy = _fresh_import("raster_h3")
    assert legacy is dggs_stream
    assert legacy.scan_raster_h3 is dggs_stream.scan_raster_h3


def test_spark_registers_the_former_data_source_name() -> None:
    # The data source classes are defined (on a stand-in base) even without PySpark.
    spark = pytest.importorskip("dggs_stream.spark")
    assert spark.LEGACY_SHORT_NAME == "raster_h3"
    assert spark.LegacyRasterH3DataSource.name() == "raster_h3"
    assert spark.RasterH3DataSource.name() == "dggs_stream"
