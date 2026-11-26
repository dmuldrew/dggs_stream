"""Filter pushdown into the `dggs_stream` Spark data source (Spark 4.1+).

`resolution` comparisons and `partial_state IS [NOT] NULL` are pushed into the scan (they
leave Spark's plan) and give the same rows as Spark evaluating them; filters on statistics
stay in Spark's plan, because on partial rows they would see one partition's share of a
cell.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

import dggs_stream
import dggs_stream.spark as spark_ds

RESOLUTIONS = "7,8,9"


def load(spark, path, pushdown):
    spark_ds.register(spark, pushdown=pushdown)
    return (
        spark.read.format("dggs_stream")
        .option("resolutions", RESOLUTIONS)
        .option("target_chunks", 8)
        .load(path)
    )


def plan(df) -> str:
    return df._jdf.queryExecution().optimizedPlan().toString()


def rows(df):
    """Rows grouped by (resolution, h3_index, is partial): an edge cell has one partial row
    per partition that reaches it. Scanning fewer resolutions changes floating-point
    summation order, so values are compared within 1e-9 relative and partial states (which
    encode those sums) by presence; merges of them are compared below."""
    out = {}
    for row in df.toArrow().to_pylist():
        state = row.pop("partial_state")
        out.setdefault((row["resolution"], row["h3_index"], state is not None), []).append(row)
    for group in out.values():
        group.sort(key=lambda r: (r["count"], r["mean"]))
    return out


def assert_same_rows(got, want):
    assert got.keys() == want.keys(), (
        f"{len(want.keys() - got.keys())} groups missing, {len(got.keys() - want.keys())} extra"
    )
    for key, expected_group in want.items():
        assert len(got[key]) == len(expected_group), f"{key}: {len(got[key])} rows, expected {len(expected_group)}"
        for expected, actual in zip(expected_group, got[key]):
            for column, x in expected.items():
                y = actual[column]
                if isinstance(x, float) and isinstance(y, float):
                    assert abs(x - y) <= 1e-9 * max(1.0, abs(x)), f"{column} of {key}: {y} != {x}"
                else:
                    assert x == y, f"{column} of {key}: {y!r} != {x!r}"


@pytest.mark.parametrize(
    "condition, pushed",
    [
        ("resolution = 9", True),
        ("resolution IN (7, 9)", True),
        ("resolution >= 8", True),
        ("resolution < 8 AND resolution > 6", True),
        ("partial_state IS NULL", True),
        ("partial_state IS NOT NULL AND resolution = 8", True),
        ("count >= 20", False),
        ("mean > 300 AND resolution = 9", False),  # resolution pushed, mean kept
        ("resolution = 9 OR count > 50", False),  # an OR is never split
    ],
)
def test_pushed_filters_leave_the_plan_and_keep_the_rows(spark, terrain, condition, pushed):
    expected = rows(load(spark, terrain, pushdown=False).where(condition))
    df = load(spark, terrain, pushdown=True).where(condition)
    assert ("Filter" not in plan(df)) is pushed, plan(df)
    assert_same_rows(rows(df), expected)
    assert expected  # every condition keeps some rows


def test_reusing_a_source_without_pushdown_keeps_final_and_partial_rows(spark, terrain):
    # Enable pushdown on the session so `pushdown=False` must override it, whatever ran before.
    previous = spark.conf.get(spark_ds._PUSHDOWN_CONF, "false")
    spark.conf.set(spark_ds._PUSHDOWN_CONF, "true")
    try:
        parts = load(spark, terrain, pushdown=False)
        partial = parts.where("partial_state IS NOT NULL")
        assert "Filter" in plan(partial), plan(partial)
        assert partial.count() > 0
        got = spark_ds.merge_partials(parts).toArrow()
    finally:
        spark.conf.set(spark_ds._PUSHDOWN_CONF, previous)
    expected = pa.table(dggs_stream.RasterArrowStream(terrain, resolutions=[7, 8, 9]))

    def keyed(table):
        return {(row["resolution"], row["h3_index"], False): [row] for row in table.to_pylist()}

    assert got.num_rows == expected.num_rows > 0
    assert_same_rows(keyed(got), keyed(expected))


def test_resolution_filter_on_a_view_merges_like_a_narrower_scan(spark, terrain):
    """Pushed through SQL too; merging the pushed scan equals a res-9-only scan."""
    spark_ds.register(spark, pushdown=True)
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY VIEW push_parts USING dggs_stream "
        f"OPTIONS (path '{terrain}', resolutions '{RESOLUTIONS}', target_chunks '8')"
    )
    parts = spark.sql("SELECT * FROM push_parts WHERE resolution = 9")
    assert "Filter" not in plan(parts)
    got = spark_ds.merge_partials(parts).toArrow()
    want = pa.table(dggs_stream.RasterArrowStream(terrain, resolutions=[9]))
    def as_rows(t):
        return {(r["resolution"], r["h3_index"], False): [r] for r in t.to_pylist()}

    assert_same_rows(as_rows(got), as_rows(want))


@pytest.mark.parametrize(("grid", "resolution"), [("h3", 9), ("a5", 14), ("s2", 15), ("healpix", 14)])
def test_merge_uses_grid_index_in_python_and_sql(spark, terrain, grid, resolution):
    spark_ds.register(spark, pushdown=True)
    index = f"{grid}_index"
    columns = [index, "resolution", "count", "mean"]
    parts = (
        spark.read.format("dggs_stream")
        .option("grid", grid.upper())
        .option("resolutions", str(resolution))
        .option("columns", ",".join(columns))
        .option("target_chunks", 8)
        .load(terrain)
    )
    from pyarrow import compute as pc

    scanned = parts.toArrow()
    partials = scanned.filter(pc.is_valid(scanned["partial_state"]))
    assert partials.num_rows > 0
    if grid == "s2":
        assert pc.any(pc.less(partials[index], 0)).as_py()

    parts.createOrReplaceTempView("grid_parts")
    want = pa.table(dggs_stream.RasterArrowStream(
        terrain, resolutions=[resolution], grid=grid, columns=columns,
    ))
    expected = {(r["resolution"], r[index], False): [r] for r in want.to_pylist()}
    for mode, result in (
        ("DataFrame", spark_ds.merge_partials(parts, columns=columns)),
        ("SQL", spark.sql("SELECT * FROM dggs_stream_merge(TABLE(grid_parts), 'buckets=4')")),
    ):
        got = result.toArrow()
        key_pos = got.schema.get_field_index(index)
        got = got.set_column(key_pos, index, got[index].cast(pa.uint64(), safe=False))
        assert got.num_rows == len(expected) > 0, (grid, mode, got.num_rows, len(expected))
        assert_same_rows(
            {(r["resolution"], r[index], False): [r] for r in got.to_pylist()}, expected,
        )


@pytest.fixture(scope="module")
def land_cover(tmp_path_factory):
    """`terrain`'s layout with uint8 classes 1-8 in place of elevations."""
    import dggs_stream_spark_demo as demo

    path = str(tmp_path_factory.mktemp("rasters") / "land_cover.tif")
    demo.write_sample(path, side=512, tile=64, categorical=True)
    return path


def test_sql_merge_infers_categorical_from_categorical_only_columns(spark, land_cover):
    """A projection keeping only `entropy` merges without repeating `categorical=true`."""
    spark_ds.register(spark, pushdown=True)
    columns = ["s2_index", "resolution", "entropy"]
    parts = (
        spark.read.format("dggs_stream")
        .option("grid", "s2")
        .option("categorical", "true")
        .option("resolutions", "15")
        .option("columns", ",".join(columns))
        .option("target_chunks", 8)
        .load(land_cover)
    )
    from pyarrow import compute as pc

    scanned = parts.toArrow()
    assert pc.sum(pc.is_valid(scanned["partial_state"])).as_py() > 0
    parts.createOrReplaceTempView("land_cover_parts")
    got = spark.sql("SELECT * FROM dggs_stream_merge(TABLE(land_cover_parts))").toArrow()
    got = got.set_column(0, "s2_index", got["s2_index"].cast(pa.uint64(), safe=False))
    want = pa.table(dggs_stream.RasterArrowStream(
        land_cover, resolutions=[15], grid="s2", categorical=True, columns=columns,
    ))
    def by_cell(table):
        return {(r["resolution"], r["s2_index"], False): [r] for r in table.to_pylist()}

    assert got.num_rows == want.num_rows > 0
    assert_same_rows(by_cell(got), by_cell(want))


def test_filters_that_exclude_everything_scan_nothing(spark, terrain):
    df = load(spark, terrain, pushdown=True)
    assert df.where("resolution = 12").count() == 0
    assert df.where("partial_state IS NULL AND partial_state IS NOT NULL").count() == 0


def test_register_follows_the_session_setting(spark, terrain):
    spark.conf.set(spark_ds._PUSHDOWN_CONF, "false")
    spark_ds.register(spark)
    df = spark.read.format("dggs_stream").option("resolutions", "9").load(terrain).where("resolution = 9")
    assert "Filter" in plan(df)  # not pushed, and the reader still works
    assert df.count() > 0

    spark.conf.set(spark_ds._PUSHDOWN_CONF, "true")
    spark_ds.register(spark)
    df = spark.read.format("dggs_stream").option("resolutions", "9").load(terrain).where("resolution = 9")
    assert "Filter" not in plan(df)


def test_partitioned_weighted_overlay(spark, terrain):
    columns = ["h3_index", "resolution", "count", "sum", "mean"]
    options = dict(resolutions=[8], transfer="overlay", columns=columns, valid_coverage_threshold=0.75)
    distributed = spark_ds.read(spark, terrain, target_chunks=8, **options).toArrow()
    serial = pa.table(dggs_stream.RasterArrowStream(terrain, **options))
    def keyed(table):
        return {(r["resolution"], r["h3_index"], False): [r] for r in table.to_pylist()}
    assert distributed.num_rows == serial.num_rows > 0
    assert_same_rows(keyed(distributed), keyed(serial))


def test_cached_overlay_merges_only_boundaries(spark, terrain):
    spark_ds.register(spark)
    columns = ["h3_index", "resolution", "count", "sum", "mean"]
    cached = (spark.read.format("dggs_stream").option("resolutions", 9)
              .option("transfer", "overlay").option("target_chunks", 8)
              .option("columns", ",".join(columns)).load(terrain).persist())
    try:
        assert cached.filter("partial_state IS NULL").count() > 0
        assert cached.filter("partial_state IS NOT NULL").count() > 0
        result = spark_ds.merge_partials(cached, columns=columns)
        assert "Union" in plan(result)
        actual = result.toArrow()
        serial = pa.table(dggs_stream.RasterArrowStream(terrain, 9, transfer="overlay", columns=columns))
        assert actual.num_rows == serial.num_rows
        def keyed(table):
            return {(r["resolution"], r["h3_index"], False): [r] for r in table.to_pylist()}
        assert_same_rows(keyed(actual), keyed(serial))
    finally:
        cached.unpersist(blocking=True)
