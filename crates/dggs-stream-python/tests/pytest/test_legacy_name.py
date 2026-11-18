"""Spark data source registration names."""

import pytest


def test_spark_registers_the_former_data_source_name() -> None:
    # The data source classes are defined (on a stand-in base) even without PySpark.
    spark = pytest.importorskip("dggs_stream.spark")
    assert spark.LEGACY_SHORT_NAME == "raster_h3"
    assert spark.LegacyRasterH3DataSource.name() == "raster_h3"
    assert spark.RasterH3DataSource.name() == "dggs_stream"
