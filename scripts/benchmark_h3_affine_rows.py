"""Compare scalar and row-hoisted H3 indexing in this experimental branch.

Build dggs-stream-cli in release mode, then run this script with its binary path.
Requires gdal_create and gdal_translate. Pass --fuel-source with the Hawaii
LF2024_FBFM40_HI_wgs84_cog.tif to include the categorical benchmark.

To measure the indexing phase directly, also build a binary with
--features dggs-stream-core/stream-profile and pass it as --instrumented.
Use a separate target directory or copy the ordinary binary before rebuilding.
"""

import argparse
import os
from pathlib import Path
from statistics import median
import subprocess
from tempfile import TemporaryDirectory
from time import perf_counter

RESOLUTIONS = ["7", "11", "7,11"]
SIZE = 512


def scan(binary, args, candidate, **kwargs):
    env = os.environ.copy()
    env.pop("DGGS_BENCH_H3_ROW", None)
    if candidate:
        env["DGGS_BENCH_H3_ROW"] = "1"
    return subprocess.run(
        [binary, *args, "--decode-workers", "1", "--aggregation-workers", "1"],
        env=env, check=True, **kwargs,
    )


def verify(binary, directory):
    asc = directory / "ids.asc"
    tif = directory / "ids.tif"
    with asc.open("w") as writer:
        writer.write(f"ncols {SIZE}\nnrows {SIZE}\nxllcorner -122\nyllcorner 38.8464\ncellsize 0.0003\n")
        for row in range(SIZE):
            first = SIZE * row + 1
            writer.write(" ".join(map(str, range(first, first + SIZE))) + "\n")
    subprocess.run(["gdal_translate", "-q", "-ot", "UInt32", "-a_srs", "EPSG:4326",
                    "-co", "TILED=YES", asc, tif], check=True)
    for resolutions in RESOLUTIONS:
        args = ["jsonl", tif, "-r", resolutions, "-c", "--format", "long",
                "--columns", "category,resolution,h3_index"]
        reference = sorted(scan(binary, args, False, stdout=subprocess.PIPE).stdout.splitlines())
        candidate = sorted(scan(binary, args, True, stdout=subprocess.PIPE).stdout.splitlines())
        if len(reference) != SIZE**2 * len(resolutions.split(",")) or reference != candidate:
            raise ValueError(f"assignment mismatch at {resolutions}")
        print(f"{resolutions}: {len(reference)} matching pixel assignments", flush=True)


def benchmark(binary, raster, selection, instrumented):
    for resolutions in RESOLUTIONS:
        args = ["ipc", raster, "-r", resolutions, *selection]
        times = [[], []]
        for rep in range(10):
            for candidate in [rep % 2, 1 - rep % 2]:
                start = perf_counter()
                scan(binary, args, candidate, stdout=subprocess.DEVNULL)
                if rep:  # First pair warms the filesystem cache.
                    times[candidate].append(1000 * (perf_counter() - start))
        baseline, candidate = map(median, times)
        print(f"{raster.name} {resolutions}: median wall ms {baseline:.2f} -> {candidate:.2f}", flush=True)
        for candidate in [False, True]:
            report = scan(instrumented or binary, [*args, "--profile"], candidate,
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE).stderr.decode()
            print(next(line for line in report.splitlines() if "grid_index" in line))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--fuel-source", type=Path)
    parser.add_argument("--instrumented", type=Path)
    args = parser.parse_args()
    with TemporaryDirectory() as tmp:
        directory = Path(tmp)
        verify(args.binary.resolve(), directory)
        raster = directory / "continuous.tif"
        subprocess.run(["gdal_create", "-q", "-of", "GTiff", "-outsize", str(SIZE), str(SIZE),
                        "-ot", "Byte", "-burn", "7", "-a_srs", "EPSG:4326", "-a_ullr",
                        "-122", "39", "-121.8464", "38.8464", "-co", "TILED=YES", raster], check=True)
        benchmark(args.binary.resolve(), raster, ["--columns", "h3_index,count"], args.instrumented)
        if args.fuel_source:
            fuel = directory / "fuel.tif"
            subprocess.run(["gdal_translate", "-q", "-srcwin", "17700", "9300", str(SIZE),
                            str(SIZE), "-co", "TILED=YES", args.fuel_source, fuel], check=True)
            benchmark(args.binary.resolve(), fuel, ["-c", "--columns", "h3_index,majority_class,total_count"],
                      args.instrumented)


if __name__ == "__main__":
    main()
