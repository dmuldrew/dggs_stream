"""Benchmark local warm-cache overlay IPC scans on macOS or Linux (requires gdal_create)."""

import argparse
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def measure(command):
    start = time.perf_counter()
    with subprocess.Popen(command, stdout=subprocess.DEVNULL) as proc:
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, command)
    rss_bytes = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    return time.perf_counter() - start, rss_bytes / 2**20


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", default="target/release/dggs-stream-cli")
    parser.add_argument("--baseline-cli", help="Optional baseline binary for paired comparisons")
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    versions = [("current", args.cli)]
    if args.baseline_cli:
        versions.insert(0, ("baseline", args.baseline_cli))
    print("version,size,h3_resolution,mass_preserve,median_seconds,median_peak_rss_mib")
    with tempfile.TemporaryDirectory() as tmp:
        for size, resolution in [(1024, 6), (128, 9)]:
            path = Path(tmp) / f"overlay-{size}.tif"
            subprocess.run(
                [
                    "gdal_create", "-q", "-of", "GTiff", "-outsize", str(size), str(size),
                    "-a_srs", "EPSG:4326", "-a_ullr", "-122", "38", "-121", "37",
                    "-ot", "Float32", "-burn", "7", "-co", "TILED=YES",
                    "-co", "COMPRESS=DEFLATE", str(path),
                ],
                check=True,
            )
            for preserve in [False, True]:
                scan = ["ipc", str(path), "-r", str(resolution), "--transfer", "overlay",
                        "-o", os.devnull]
                if preserve:
                    scan.append("--mass-preserve")
                samples = {label: [] for label, _ in versions}
                for _, binary in versions:
                    measure([binary, *scan])
                for i in range(args.runs):
                    for label, binary in (versions if i % 2 == 0 else reversed(versions)):
                        samples[label].append(measure([binary, *scan]))
                for label, runs in samples.items():
                    elapsed = statistics.median(seconds for seconds, _ in runs)
                    rss = statistics.median(mib for _, mib in runs)
                    print(f"{label},{size},{resolution},{preserve},{elapsed:.4f},{rss:.1f}",
                          flush=True)


if __name__ == "__main__":
    main()
