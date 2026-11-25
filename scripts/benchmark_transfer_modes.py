"""Compare centre assignment, sub-pixel sampling, overlay and interpolation across
cell-to-pixel area ratios: wall time, peak memory and accuracy against overlay.

Needs only the Python standard library and the release example:

    cargo build --release -p dggs-stream-core --example transfer_compare
    python3 scripts/benchmark_transfer_modes.py --runs 5 --markdown

The script writes tiled, Deflate-compressed Float32 GeoTIFFs (EPSG:4326, 1/2048 degree
pixels, about 43 m x 54 m near 37.5 N) holding a smooth gradient plus a 0.01 degree
checkerboard, so cells see both gentle variation and sharp edges. Each H3 resolution gets
a raster sized to keep the cell count manageable, all with the same pixel size, so the
cell-to-pixel area ratio is set by the resolution alone.

Accuracy uses weighted overlay, which clips every cell against every pixel exactly, as the
reference. For each mode and resolution it reports the share of overlay's cells the mode
leaves out, the mean absolute error of the cell means (the field spans 0 to 150), and the
mean relative error of the covered area (`count`, in pixels; not meaningful for
interpolation, whose count is always 1).
"""

import argparse
import math
import os
import statistics
import struct
import subprocess
import sys
import tempfile
import time
import zlib
from array import array
from pathlib import Path

PIXEL_DEG = 1 / 2048
WEST, NORTH = -122.0, 38.0
TILE = 256

# H3 resolution -> raster size in pixels (square). Same pixel size throughout.
DEFAULT_CASES = {7: 2048, 8: 2048, 9: 2048, 10: 1024, 11: 512, 12: 256, 13: 128}
DEFAULT_MODES = ["center", "rgss", "9point", "16point", "overlay", "bilinear", "auto"]


def field(lon, lat):
    gradient = 100.0 * (lon - WEST)  # 0..100 across one degree
    checker = 50.0 * ((math.floor(lon / 0.01) + math.floor(lat / 0.01)) % 2)
    return gradient + checker


def write_geotiff(path, size):
    """Tiled, Deflate-compressed, single-band Float32 GeoTIFF in EPSG:4326."""
    tiles_across = (size + TILE - 1) // TILE
    tiles = []
    for ty in range(tiles_across):
        for tx in range(tiles_across):
            values = array("f", bytes(4 * TILE * TILE))
            for r in range(TILE):
                row = ty * TILE + r
                if row >= size:
                    break
                lat = NORTH - (row + 0.5) * PIXEL_DEG
                base = r * TILE
                for c in range(TILE):
                    col = tx * TILE + c
                    if col >= size:
                        break
                    values[base + c] = field(WEST + (col + 0.5) * PIXEL_DEG, lat)
            if sys.byteorder != "little":
                values.byteswap()
            tiles.append(zlib.compress(values.tobytes(), 6))

    entries = []  # (tag, type, values)
    n = len(tiles)
    entries += [
        (256, 4, [size]), (257, 4, [size]), (258, 3, [32]), (259, 3, [8]),
        (262, 3, [1]), (277, 3, [1]), (284, 3, [1]),
        (322, 3, [TILE]), (323, 3, [TILE]), (324, 4, [0] * n),
        (325, 4, [len(t) for t in tiles]), (339, 3, [3]),
        (33550, 12, [PIXEL_DEG, PIXEL_DEG, 0.0]),
        (33922, 12, [0.0, 0.0, 0.0, WEST, NORTH, 0.0]),
        (34735, 3, [1, 1, 0, 3, 1024, 0, 1, 2, 1025, 0, 1, 1, 2048, 0, 1, 4326]),
    ]
    entries.sort()
    fmt = {3: "H", 4: "I", 12: "d"}
    ifd_offset = 8
    extra_offset = ifd_offset + 2 + 12 * len(entries) + 4
    extra = bytearray()
    records = []
    offsets_slot = None
    for tag, typ, vals in entries:
        raw = struct.pack("<" + fmt[typ] * len(vals), *vals)
        if len(raw) <= 4:
            records.append(struct.pack("<HHI", tag, typ, len(vals)) + raw.ljust(4, b"\0"))
        else:
            if len(extra) % 2:
                extra += b"\0"
            if tag == 324:
                offsets_slot = len(extra)
            records.append(struct.pack("<HHII", tag, typ, len(vals), extra_offset + len(extra)))
            extra += raw
    data_offset = extra_offset + len(extra)
    offsets, position = [], data_offset
    for t in tiles:
        offsets.append(position)
        position += len(t)
    packed = struct.pack("<" + "I" * n, *offsets)
    if n == 1:  # a single offset is stored inline
        records = [
            r if struct.unpack("<H", r[:2])[0] != 324 else r[:8] + packed for r in records
        ]
    else:
        extra[offsets_slot:offsets_slot + len(packed)] = packed
    with open(path, "wb") as out:
        out.write(b"II*\0" + struct.pack("<I", ifd_offset))
        out.write(struct.pack("<H", len(records)) + b"".join(records) + struct.pack("<I", 0))
        out.write(extra)
        for t in tiles:
            out.write(t)


def measure(command):
    start = time.perf_counter()
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as proc:
        stdout = proc.stdout.read()
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, command)
    label, cells, scan_ms, own_rss_kib = stdout.split()
    if own_rss_kib != "-":  # the scan's own peak (Linux), excluding the forked parent
        rss_mib = int(own_rss_kib) / 1024
    else:  # elsewhere the child's rusage, which can include the parent's pre-exec pages
        rss_mib = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024) / 2**20
    return label, int(cells), float(scan_ms), (time.perf_counter() - start) * 1e3, rss_mib


def read_cells(path):
    cells = {}
    with open(path) as f:
        for line in f:
            cell, count, mean = line.split(",")
            cells[int(cell)] = (float(count), float(mean))
    return cells


def accuracy(cells, reference, interpolated):
    missing = sum(1 for c in reference if c not in cells) / len(reference)
    common = [c for c in reference if c in cells]
    mean_err = statistics.fmean(abs(cells[c][1] - reference[c][1]) for c in common)
    area_err = None
    if not interpolated:
        area_err = statistics.fmean(
            abs(cells[c][0] - reference[c][0]) / reference[c][0]
            for c in common if reference[c][0] > 0
        )
    return missing, mean_err, area_err


def h3_area_m2(res):
    # Mean H3 cell area: the authalic sphere's area over the number of cells.
    return 510_065_621_724_000 / (2 + 120 * 7 ** res)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--example", default="target/release/examples/transfer_compare")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--resolutions", default=",".join(map(str, DEFAULT_CASES)),
                        help="H3 resolutions to run (7-13)")
    parser.add_argument("--modes", default=",".join(DEFAULT_MODES))
    parser.add_argument("--markdown", action="store_true", help="Print a Markdown table")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    resolutions = [int(r) for r in args.resolutions.split(",")]
    if any(r not in DEFAULT_CASES for r in resolutions):
        parser.error(f"resolutions must be among {sorted(DEFAULT_CASES)}")
    modes = args.modes.split(",")
    interp_names = {"nearest", "bilinear", "bicubic", "lanczos"}

    pixel_m2 = (PIXEL_DEG * 111_320 * math.cos(math.radians(37.5))) * (PIXEL_DEG * 110_950)
    columns = ["resolution", "raster_px", "cell_px_ratio", "mode", "cells", "scan_ms",
               "peak_rss_mib", "missing_pct", "mean_abs_err", "area_err_pct"]
    if not args.markdown:
        print(",".join(columns), flush=True)
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        rasters = {}
        for res in resolutions:
            size = DEFAULT_CASES[res]
            if size not in rasters:
                rasters[size] = Path(tmp) / f"field-{size}.tif"
                write_geotiff(rasters[size], size)
        for res in resolutions:
            raster = str(rasters[DEFAULT_CASES[res]])
            ratio = h3_area_m2(res) / pixel_m2
            commands = {m: [args.example, raster, "h3", str(res), m] for m in modes}
            for command in commands.values():  # warm the page cache and binary
                measure(command)
            samples = {m: [] for m in modes}
            for i in range(args.runs):
                for m in (modes if i % 2 == 0 else reversed(modes)):
                    samples[m].append(measure(commands[m]))
            # Accuracy runs come after every timed run, so the timed children never fork
            # from a parent holding the cell tables.
            reference_csv = Path(tmp) / f"ref-{res}.csv"
            subprocess.run([args.example, raster, "h3", str(res), "overlay", str(reference_csv)],
                           check=True, stdout=subprocess.DEVNULL)
            reference = read_cells(reference_csv)
            for m in modes:
                cells_csv = Path(tmp) / f"{m}-{res}.csv"
                subprocess.run([*commands[m], str(cells_csv)], check=True,
                               stdout=subprocess.DEVNULL)
                label = samples[m][0][0]
                missing, mean_err, area_err = accuracy(
                    read_cells(cells_csv), reference,
                    m in interp_names or label == "auto:bilinear",
                )
                rows.append({
                    "resolution": res,
                    "raster_px": DEFAULT_CASES[res],
                    "cell_px_ratio": ratio,
                    "mode": label,
                    "cells": samples[m][0][1],
                    "scan_ms": statistics.median(s[2] for s in samples[m]),
                    "peak_rss_mib": statistics.median(s[4] for s in samples[m]),
                    "missing_pct": 100 * missing,
                    "mean_abs_err": mean_err,
                    "area_err_pct": None if area_err is None else 100 * area_err,
                })
                if not args.markdown:
                    r = rows[-1]
                    print(",".join("" if r[k] is None else str(r[k]) for k in columns),
                          flush=True)
                cells_csv.unlink()
            del reference
            reference_csv.unlink()

    if args.markdown:
        print("| H3 res | Cell / pixel area | Mode | Cells | Scan ms | Peak RSS MiB "
              "| Cells missing | Mean abs. error | Area error |")
        print("| ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for r in rows:
            area = "—" if r["area_err_pct"] is None else f"{r['area_err_pct']:.2f}%"
            print(f"| {r['resolution']} | {r['cell_px_ratio']:.3g} | {r['mode']} | "
                  f"{r['cells']:,} | {r['scan_ms']:.1f} | {r['peak_rss_mib']:.1f} | "
                  f"{r['missing_pct']:.1f}% | {r['mean_abs_err']:.3f} | {area} |")


if __name__ == "__main__":
    main()
