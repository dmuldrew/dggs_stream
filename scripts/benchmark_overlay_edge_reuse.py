"""Measure weighted overlay on WGS84 and Web Mercator rasters. Requires GDAL (gdal_translate and gdalwarp).

Compare release CLI binaries built from main and the serial A5 cache branch.
"""
import argparse
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

CASES = [("h3", 11, "match"), ("a5", 18, "match"), ("h3", 9, "coarse"), ("a5", 14, "coarse"),
         ("a5", 18, "mercator")]


def fixtures(root):
    source = root / "ids.asc"
    with source.open("w") as writer:
        writer.write("ncols 512\nnrows 512\nxllcorner -122\nyllcorner 38.8464\ncellsize 0.0003\n")
        for row in range(512):
            writer.write(" ".join(str(row * 512 + col + 1) for col in range(512)) + "\n")
    for name, flags in [
        ("match", ["-srcwin", "0", "0", "256", "256"]),
        ("coarse", ["-outsize", "1024", "1024", "-a_ullr", "-122", "39", "-121.6928", "38.6928"]),
        ("seam", ["-a_ullr", "179.872", "76.064", "180.128", "75.808"]),
    ]:
        subprocess.run(["gdal_translate", "-q", "-ot", "UInt32", "-a_srs", "EPSG:4326", "-co",
                        "TILED=YES", *flags, source, root / f"{name}.tif"], check=True)

    subprocess.run(["gdalwarp", "-q", "-t_srs", "EPSG:3857", "-r", "near", "-ts", "256", "256",
                    "-co", "TILED=YES", root / "match.tif", root / "mercator.tif"], check=True)


def options(root, grid, level, name, workers):
    return [root / f"{name}.tif", "--grid", grid, "-r", str(level), "--transfer", "overlay",
            "--aggregation-workers", str(workers), "--decode-workers", "1",
            "--columns", f"{grid}_index,count,sum"]


def verify(root, binaries):
    for grid, fine, coarse in [("h3", 11, 9), ("a5", 18, 14)]:
        for workers in [1, 4]:
            for level, name, bbox in [(fine, "match", None), (coarse, "coarse", None),
                                      (fine, "mercator", None), (coarse, "mercator", None),
                                      (fine, "mercator", "-121.99,38.94,-121.95,38.98"),
                                      (fine, "match", "-121.99,38.94,-121.95,38.98"),
                                      (coarse, "seam", None), (coarse, "seam", "179.97,75.94,-179.97,76.02")]:
                args = options(root, grid, level, name, workers)
                if bbox:
                    args.extend(["--bbox", bbox])
                rows = [sorted(subprocess.check_output([binary, "jsonl", *args]).splitlines())
                        for binary in binaries]
                if not rows[0] or rows[0] != rows[1]:
                    raise ValueError(f"Parity failure: {args}")
                print("matched", grid, level, name, workers, bbox, len(rows[0]), flush=True)


def measure(root, binaries):
    for grid, level, name in CASES:
        for workers in [1, 4]:
            args = ["ipc", *options(root, grid, level, name, workers)]
            times = [[], []]
            for rep in range(10):
                for idx in (rep % 2, 1 - rep % 2):
                    start = time.perf_counter()
                    subprocess.run([binaries[idx], *args], check=True, stdout=subprocess.DEVNULL)
                    if rep:
                        times[idx].append((time.perf_counter() - start) * 1000)
            print("median ms", name, grid, level, workers, *(statistics.median(t) for t in times), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp)
        fixtures(directory)
        bins = [args.baseline.resolve(), args.candidate.resolve()]
        verify(directory, bins)
        measure(directory, bins)
