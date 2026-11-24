"""Measure local Spark overlay shuffle with a generated tiled raster, Spark 4 and PyArrow.

Run with the same Python and release native library on driver and workers.
Prints JSON per case. The driver also scans each partition to measure unique
ownership and uncompressed partial payload size outside the timed Spark jobs.
"""
import argparse
import json
import os
import random
import sys
from collections import Counter, defaultdict
from contextlib import contextmanager
from itertools import product
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

import numpy as np
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import dggs_stream as ds
import dggs_stream.spark as adapter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dggs_stream_spark_demo import write_sample

COLUMNS = ["resolution", "h3_index", "count", "sum", "mean"]


def ownership(path, resolution, chunks):
    owners = Counter()
    payload = Counter()
    final_rows = 0
    specs = ds.plan_partitions(path, target_chunks=chunks)
    for spec in specs:
        table = pa.table(ds.RasterArrowStream(path, resolution, partition=spec, transfer="overlay", columns=COLUMNS))
        final_rows += table["partial_state"].null_count
        cells = table["h3_index"].to_pylist()
        owners.update(set(cells))
        for cell, state in zip(cells, table["partial_state"].to_pylist()):
            payload[cell] += len(state) if state is not None else 0
    single = [c for c, n in owners.items() if n == 1]
    boundary = [c for c, n in owners.items() if n > 1]
    return boundary, dict(
        partitions=len(specs), cells=len(owners), final_rows=final_rows, single_cells=len(single),
        single_state_bytes=sum(payload[c] for c in single), state_bytes=payload.total(),
    )


def pass_batches(batches):
    yield from batches


def split_merge(cached):
    finals = cached.filter("partial_state IS NULL").drop("partial_state")
    edges = cached.filter("partial_state IS NOT NULL")
    return finals.unionByName(adapter.merge_partials(edges, columns=COLUMNS))


def verify(frame, serial):
    table = frame.toArrow().sort_by("h3_index")
    for name in ("h3_index", "resolution"):
        np.testing.assert_array_equal(table[name], serial[name])
    for name in ("count", "sum", "mean"):
        np.testing.assert_allclose(table[name], serial[name], rtol=1e-9, atol=1e-12)


def task_metrics(log_dir):
    stages = {}
    metrics = defaultdict(Counter)
    for file in sorted(log_dir.glob("**/events_*")):
        with file.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event["Event"] == "SparkListenerStageSubmitted":
                    stages[event["Stage Info"]["Stage ID"]] = (event.get("Properties") or {}).get("spark.jobGroup.id")
                if event["Event"] != "SparkListenerTaskEnd" or event["Task End Reason"]["Reason"] != "Success":
                    continue
                group = stages[event["Stage ID"]]
                task = event["Task Metrics"]
                write = task["Shuffle Write Metrics"]
                read = task["Shuffle Read Metrics"]
                metrics[group].update(
                    written=write["Shuffle Bytes Written"], local_read=read["Local Bytes Read"],
                    remote_read=read["Remote Bytes Read"], write_ns=write["Shuffle Write Time"],
                    fetch_wait_ms=read["Fetch Wait Time"], task_ms=task["Executor Run Time"],
                )
    return metrics


def measure_case(spark, path, resolution, chunks, args, rng):
    raw = (spark.read.format("dggs_stream").option("resolutions", resolution)
           .option("target_chunks", chunks).option("transfer", "overlay")
           .option("columns", ",".join(COLUMNS)).load(path))
    # The two controls consume identical partial rows via an opaque Arrow
    # identity function. Only the exchange and resulting task layout differ.
    plans = dict(merge=adapter.merge_partials(raw, columns=COLUMNS),
                 exchange=raw.repartition("h3_index").mapInArrow(pass_batches, raw.schema),
                 direct=raw.mapInArrow(pass_batches, raw.schema))
    boundary, info = ownership(path, resolution, chunks)
    if args.probe:
        # An oracle for this all-valid fixture. The driver already knows
        # the exact IDs shared by multiple partitions. Singles' partial
        # statistic columns are their final statistics with these options.
        shared = F.expr("h3_index IN (" + ",".join(map(str, boundary)) + ")") if boundary else F.lit(False)
        flagged = raw.withColumn("partial_state", F.when(shared, F.col("partial_state")))
        plans = dict(merge=plans["merge"],
                     flagged=adapter.merge_partials(flagged, columns=COLUMNS),
                     split=None)

    if args.native:
        plans = dict(merge=plans["merge"], cached=None)

    @contextmanager
    def result(mode):
        if mode not in ("split", "cached"):
            yield plans[mode]
            return
        cached = (flagged if mode == "split" else raw).persist()
        try:
            yield split_merge(cached) if mode == "split" else adapter.merge_partials(cached, columns=COLUMNS)
        finally:
            cached.unpersist(blocking=True)

    runs = defaultdict(list)
    for repeat in range(args.repeats + 1):
        modes = list(plans)
        rng.shuffle(modes)
        for mode in modes:
            label = f"{resolution}-{chunks}-{mode}-{repeat}"
            spark.sparkContext.setJobGroup(label, label)
            start = perf_counter()
            with result(mode) as frame:
                frame.write.format("noop").mode("overwrite").save()
            elapsed = perf_counter() - start
            if repeat:
                runs[mode].append((label, elapsed))

    if args.probe or args.native:
        spark.sparkContext.setJobGroup("verify", "compare with serial overlay")
        serial = pa.table(ds.RasterArrowStream(path, resolution, transfer="overlay", columns=COLUMNS))
        serial = serial.sort_by("h3_index")
        for mode in plans:
            with result(mode) as frame:
                verify(frame, serial)
    return info, runs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--side", type=int, default=1024)
    parser.add_argument("--tile", type=int, default=128)
    parser.add_argument("--repeats", type=int, default=3)
    detail = parser.add_mutually_exclusive_group()
    detail.add_argument("--native", action="store_true", help="compare uncached and caller-persisted native merge on the fine four-partition case")
    detail.add_argument("--probe", action="store_true", help="compare ideal interior classification and a cached boundary-only exchange in the fine four-partition case")
    args = parser.parse_args()
    if min(args.side, args.tile, args.repeats) < 1 or args.side % args.tile:
        parser.error("sizes and repeats must be positive, and side must be divisible by tile")
    with TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "terrain.tif")
        write_sample(path, side=args.side, tile=args.tile)
        logs = Path(tmp) / "events"
        logs.mkdir()
        spark = (SparkSession.builder.master("local[2]").appName("overlay-shuffle")
                 .config("spark.ui.showConsoleProgress", "false")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.sql.adaptive.enabled", "false")
                 .config("spark.eventLog.enabled", "true")
                 .config("spark.eventLog.compress", "false")
                 .config("spark.eventLog.dir", logs.as_uri()).getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        spark_version = spark.version
        adapter.register(spark)
        rng = random.Random(42)
        results = []
        try:
            cases = [(11, 16)] if args.probe or args.native else product([9, 11], [1, 4, 16])
            for resolution, chunks in cases:
                info, runs = measure_case(spark, path, resolution, chunks, args, rng)
                results.append((resolution, chunks, info, runs))
                print(f"Finished H3 {resolution}, target_chunks={chunks}", file=sys.stderr, flush=True)
        finally:
            spark.stop()
        metrics = task_metrics(logs)
        for resolution, chunks, info, runs in results:
            merge = [metrics[label] for label, _ in runs["merge"]]
            if not all(m["written"] and m["written"] == m["local_read"] + m["remote_read"] for m in merge):
                raise ValueError("incomplete Spark exchange metrics")
            medians = {
                mode: {key: median(metrics[label][key] for label, _ in data) for key in merge[0]}
                for mode, data in runs.items()
            }
            print(json.dumps(dict(
                side=args.side, tile=args.tile, spark_version=spark_version, resolution=resolution,
                target_chunks=chunks, **info, median_metrics=medians,
                runs_s={mode: [round(t, 4) for _, t in data] for mode, data in runs.items()},
            )))


if __name__ == "__main__":
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    main()
