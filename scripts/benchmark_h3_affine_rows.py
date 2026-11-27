"""Compare two release CLI builds on affine H3 center-assignment scans.

Run with baseline and candidate binary paths. Requires GDAL's gdal_translate.
Each pixel holds a distinct positive integer so categorical output identifies its cell.
"""
import argparse
import math
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


def fixture(directory, size):
    source = directory / "ids.asc"
    step = 0.0003
    with source.open("w") as writer:
        writer.write(f"ncols {size}\nnrows {size}\nxllcorner -122\nyllcorner 38\ncellsize {step}\n")
        for row in range(size):
            first = size * row + 1
            writer.write(" ".join(map(str, range(first, first + size))) + "\n")
    bounds = [-122, 38 + size * step, -122 + size * step, 38]
    mercator = [
        6378137 * (math.radians(v) if i % 2 == 0 else math.asinh(math.tan(math.radians(v))))
        for i, v in enumerate(bounds)
    ]
    for epsg, extent in [(4326, bounds), (3857, mercator)]:
        target = directory / f"{epsg}.tif"
        subprocess.run(["gdal_translate", "-q", "-ot", "UInt32", "-a_srs", f"EPSG:{epsg}",
                        "-a_ullr", *map(str, extent), "-co", "TILED=YES", source, target], check=True)
        yield target


def compare(binaries, raster, resolution, size, repeats):
    args = [raster, "--grid", "h3", "-r", resolution, "--transfer", "assign_centers",
            "--decode-workers", "1", "--aggregation-workers", "1"]
    verify = [*args, "-c", "--format", "long", "--columns", "category,resolution,h3_index,count"]
    outputs = [sorted(subprocess.check_output([binary, "jsonl", *verify]).splitlines())
               for binary in binaries]
    expected = size * size * len(resolution.split(","))
    if len(outputs[0]) != expected or outputs[0] != outputs[1]:
        raise ValueError(f"{raster.stem} {resolution}: pixel assignments differ or are missing")
    del outputs
    print(f"{raster.stem} {resolution}: {expected} matching assignments", flush=True)
    time_scans(binaries, raster, resolution, repeats, ["--columns", "h3_index,count,sum"])


def time_scans(binaries, raster, resolution, repeats, selection):
    args = [raster, "--grid", "h3", "-r", resolution, "--transfer", "assign_centers",
            "--decode-workers", "1", "--aggregation-workers", "1", *selection]
    elapsed = [[], []]
    for rep in range(repeats + 1):
        for index in (rep % 2, 1 - rep % 2):
            start = time.perf_counter()
            subprocess.run([binaries[index], "ipc", *args],
                           check=True, stdout=subprocess.DEVNULL)
            if rep:
                elapsed[index].append(1000 * (time.perf_counter() - start))
    before, after = map(statistics.median, elapsed)
    print(f"{raster.stem} {resolution} | "
          f"{before:.2f} -> {after:.2f} ms | {(1 - after / before) * 100:+.1f}%", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--size", type=int, default=512)
    parser.add_argument("--repeats", type=int, default=9)
    parser.add_argument("--fuel-source", type=Path, help="Optional categorical raster or pre-cut window")
    opts = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        for tif in fixture(Path(tmp), opts.size):
            for levels in ["7", "11", "7,11"]:
                compare([opts.baseline.resolve(), opts.candidate.resolve()], tif, levels,
                        opts.size, opts.repeats)
        if opts.fuel_source:
            for levels in ["7", "11", "7,11"]:
                time_scans([opts.baseline.resolve(), opts.candidate.resolve()], opts.fuel_source,
                           levels, opts.repeats, ["-c", "--columns", "h3_index,majority_class,total_count"])
