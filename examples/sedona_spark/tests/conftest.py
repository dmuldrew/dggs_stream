"""Fixtures for running the Apache Sedona (Spark) demo's pipeline under pytest.

These tests need PySpark, Apache Sedona (its Python package and jars) and the `dggs_stream`
package, as in the `sedona` image built from `Dockerfile.sedona`:

    docker build -f Dockerfile.sedona -t dggs_stream-sedona .
    docker run --rm -v "$PWD/examples/sedona_spark:/opt/dggs_stream/examples" dggs_stream-sedona \\
        sh -c "pip3 install --break-system-packages pytest && python3 -m pytest -v /opt/dggs_stream/examples/tests"

Outside such an environment the whole module is skipped.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

DEMO_DIR = Path(__file__).resolve().parents[1]

for module in ("pyspark", "sedona.spark", "dggs_stream", "numpy"):
    pytest.importorskip(module)

# The executors' Python workers unpickle the demo's `mapInArrow` functions by module name,
# so the demo directory must be importable there as well as here.
sys.path.insert(0, str(DEMO_DIR))
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(DEMO_DIR), os.environ.get("PYTHONPATH")]))

import dggs_stream_spark_demo as demo  # noqa: E402


@pytest.fixture(scope="session")
def sedona():
    """One Spark session with Sedona's functions, shared by every test: local mode, or the
    cluster named by SPARK_MASTER (see docker-compose.yml's spark-cluster profile)."""
    spark = demo.sedona_session(os.environ.get("SPARK_MASTER", "local[2]"), shuffle_partitions=4)
    spark.sparkContext.setLogLevel("ERROR")
    # Executors in other containers or machines import the demo's mapInArrow functions.
    spark.sparkContext.addPyFile(str(DEMO_DIR / "dggs_stream_spark_demo.py"))
    yield spark
    spark.stop()


@pytest.fixture
def terrain(tmp_path):
    """Write the demo's synthetic tiled terrain; returns a factory of paths.

    On a cluster, executors must read the rasters too: set DGGS_STREAM_SHARED_DIR to a
    directory every node mounts at the same path, and they are written there."""
    shared = os.environ.get("DGGS_STREAM_SHARED_DIR")
    if shared:
        tmp_path = Path(shared) / "tests" / tmp_path.name
        tmp_path.mkdir(parents=True, exist_ok=True)

    def write(name: str = "terrain.tif", side: int = 512, tile: int = 64, categorical: bool = False) -> str:
        path = str(tmp_path / name)
        demo.write_sample(path, side=side, tile=tile, categorical=categorical)
        return path

    return write
