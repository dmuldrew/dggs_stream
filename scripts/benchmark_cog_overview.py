"""Compare native and preview reads of a COG over loopback HTTP range requests."""

import argparse
import json
import re
import statistics
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def benchmark(binary, raster, resolution, repeats):
    lock = threading.Lock()
    transferred = 0
    size = raster.stat().st_size

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            nonlocal transferred
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
            if match is None:
                self.send_error(400, "expected a byte range")
                return
            start, stop = map(int, match.groups())
            stop = min(stop, size - 1)
            if start > stop:
                self.send_error(416)
                return
            length = stop - start + 1
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{stop}/{size}")
            self.send_header("Content-Length", str(length))
            self.end_headers()
            with lock:
                transferred += length
            with raster.open("rb") as src:
                src.seek(start)
                self.wfile.write(src.read(length))

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}/input.tif"
        try:
            for mode in ["assign_centers", "interpolate"]:
                samples = {"native": [], "auto": []}
                results = {}
                for rep in range(repeats):
                    for policy in (["native", "auto"] if rep % 2 == 0 else ["auto", "native"]):
                        before = transferred
                        start = time.perf_counter()
                        run = subprocess.run(
                            [binary, "jsonl", url, "-r", str(resolution), "--transfer", mode,
                             "--overview", policy, "--columns", "h3_index,count,mean"],
                            check=True, capture_output=True, text=True,
                        )
                        samples[policy].append((time.perf_counter() - start, transferred - before))
                        results[policy] = {r["h3_index"]: r for r in map(json.loads, run.stdout.splitlines())}
                        if rep == 0 and policy == "auto":
                            print(run.stderr.strip())
                for policy, timings in samples.items():
                    seconds, sizes = zip(*timings)
                    rows = results[policy]
                    print(mode, policy, f"{statistics.median(seconds):.3f}s",
                          f"{statistics.median(sizes):,.0f} body bytes", f"{len(rows)} cells",
                          f'{sum(r["count"] for r in rows.values()):,.0f} observations')
                common = results["native"].keys() & results["auto"].keys()
                errors = [abs(results["native"][k]["mean"] - results["auto"][k]["mean"]) for k in common]
                if errors:
                    print(f"{len(common)} common cells, mean absolute difference {statistics.mean(errors):.6g}")
        finally:
            server.shutdown()
            thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("raster", type=Path)
    parser.add_argument("--resolution", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    benchmark(args.binary, args.raster, args.resolution, args.repeats)
