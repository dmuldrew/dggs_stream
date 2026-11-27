"""Compare centre assignment, sub-pixel sampling, overlay and interpolation across
cell-to-pixel area ratios: wall time, peak memory and accuracy against overlay.

Needs only the Python standard library and the release example:

    cargo build --release -p dggs-stream-core --example transfer_compare
    python3 scripts/benchmark_transfer_modes.py --runs 5 --markdown
    python3 scripts/benchmark_transfer_modes.py --sweep ratio --kind categorical --markdown

The script writes tiled, Deflate-compressed Float32 GeoTIFFs in EPSG:4326 near 37.5 N.
Continuous rasters hold a smooth west-east gradient (0-100) plus a 50-unit, 0.01 degree
checkerboard, so cells see both gentle variation and sharp edges. Categorical rasters
hold eight classes: the checkerboard's two colours times four west-east bands.

Two sweeps:

- `--sweep pixels` (default): H3 resolutions 7-13 over rasters with one pixel size
  (1/2048 degree, about 2,350 m2), each sized to keep the cell count manageable. The
  ratio changes by 7x per resolution.
- `--sweep ratio`: one grid and resolution (default S2 level 13, about 1.27 km2) over a
  fixed region of about `--cells` cells, with the pixel size chosen for each requested
  cell-to-pixel area ratio. Only the pixel size changes between rows. The checkerboard
  squares are `--feature-cells` cell edges wide (default 1), so the field varies on the
  same scale relative to the cell on every grid and resolution; value errors are only
  comparable across grids that way. Area errors do not depend on the field.

`--field smooth` swaps the checkerboard for a sinusoid of the same scale (no edges), and
`--field-reference F` adds a second reference: overlay on a raster of the same field F
times finer each way, which approaches the field's true mean (or class shares) over each
cell. Overlay on the scanned raster is exact if each pixel is an area average; the field
reference is the right one if pixels are point samples of a smooth field, the case
interpolation with sub-cell `sampling` targets. Modes like `bicubic+16point` (sub-cell
samples) and `auto+rgss` combine an interpolation or `auto` with a sampling preset.

Accuracy uses weighted overlay, which clips every cell against every pixel exactly, as
the reference. Continuous scans report the share of overlay's cells the mode leaves out,
the mean absolute error of cell means (the field spans 0 to 150) and the mean relative
error of the covered area (`count`, in pixels). Categorical scans report the same missing
share and area error, and as their main error the mean total-variation distance between a
cell's class fractions and overlay's (half the summed absolute differences: 0 is
identical, 1 is disjoint). Interpolation's count is always 1, so it has no area error.
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

WEST, NORTH = -122.0, 38.0
TILE = 256
EARTH_M2 = 510_065_621_724_000
M_PER_DEG_LAT = 110_950.0
M_PER_DEG_LON = 111_320.0 * math.cos(math.radians(37.5))

# --sweep pixels: H3 resolution -> raster size in pixels (square), one pixel size.
PIXEL_CASES = {7: 2048, 8: 2048, 9: 2048, 10: 1024, 11: 512, 12: 256, 13: 128}
PIXEL_DEG = 1 / 2048
DEFAULT_RATIOS = "0.25,0.5,1,1.5,2,3,4,6,8,12,16,24,32,64"
CONTINUOUS_MODES = "center,rgss,16point,overlay,bilinear,auto"
CATEGORICAL_MODES = "center,rgss,16point,overlay,nearest,auto"


def mean_cell_area(grid, res):
    """Mean cell area in m², as GridKind::area_m2 computes it (A5 approximately)."""
    if grid == "h3":
        return EARTH_M2 / (2 + 120 * 7**res)
    if grid == "s2":
        return EARTH_M2 / (6 * 4**res)
    if grid == "healpix":
        return EARTH_M2 / (12 * 4**res)
    if grid == "a5":  # 12 pentagons at resolution 0, then 60 * 4**(res - 1)
        return EARTH_M2 / (12 if res == 0 else 60 * 4 ** (res - 1))
    raise ValueError(grid)


def make_field(kind, west, width_deg, checker_deg=0.01, shape="checker"):
    """The test field at a point.

    `checker`: a gradient plus a checkerboard of `checker_deg` squares (sharp edges).
    `smooth`: a sinusoid whose half-wavelength is `checker_deg` (no edges), the case
    interpolation reconstructs best. Categorical fields threshold the same shapes.
    """
    def smooth(lon, lat):
        k = math.pi / checker_deg
        return math.sin(k * (lon - west)) * math.sin(k * (lat - NORTH))

    def continuous(lon, lat):
        if shape == "smooth":
            return 500.0 + 160.0 * smooth(lon, lat)
        gradient = 100.0 * (lon - west) / width_deg
        checker = (math.floor(lon / checker_deg) + math.floor(lat / checker_deg)) % 2
        return gradient + 50.0 * checker

    def categorical(lon, lat):
        if shape == "smooth":  # four classes with curved boundaries
            return float(1 + min(3, int(2.0 * (smooth(lon, lat) + 1.0))))
        band = min(3, int(4 * (lon - west) / width_deg))
        checker = (math.floor(lon / checker_deg) + math.floor(lat / checker_deg)) % 2
        return float(1 + checker + 2 * band)

    return continuous if kind == "continuous" else categorical


def write_geotiff(path, size, pixel_deg, value):
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
                lat = NORTH - (row + 0.5) * pixel_deg
                base = r * TILE
                for c in range(TILE):
                    col = tx * TILE + c
                    if col >= size:
                        break
                    values[base + c] = value(WEST + (col + 0.5) * pixel_deg, lat)
            if sys.byteorder != "little":
                values.byteswap()
            tiles.append(zlib.compress(values.tobytes(), 6))

    n = len(tiles)
    entries = sorted([
        (256, 4, [size]), (257, 4, [size]), (258, 3, [32]), (259, 3, [8]),
        (262, 3, [1]), (277, 3, [1]), (284, 3, [1]),
        (322, 3, [TILE]), (323, 3, [TILE]), (324, 4, [0] * n),
        (325, 4, [len(t) for t in tiles]), (339, 3, [3]),
        (33550, 12, [pixel_deg, pixel_deg, 0.0]),
        (33922, 12, [0.0, 0.0, 0.0, WEST, NORTH, 0.0]),
        (34735, 3, [1, 1, 0, 3, 1024, 0, 1, 2, 1025, 0, 1, 1, 2048, 0, 1, 4326]),
    ])
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
    offsets, position = [], extra_offset + len(extra)
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
    label, cells, scan_ms, own_rss_kib, ratio = stdout.split()
    if own_rss_kib != "-":  # the scan's own peak (Linux), excluding the forked parent
        rss_mib = int(own_rss_kib) / 1024
    else:  # elsewhere the child's rusage, which can include the parent's pre-exec pages
        rss_mib = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024) / 2**20
    wall_ms = (time.perf_counter() - start) * 1e3
    return label, int(cells), float(scan_ms), wall_ms, rss_mib, float(ratio)


def read_cells(path, kind):
    cells = {}
    with open(path) as f:
        for line in f:
            cell, total, rest = line.rstrip("\n").split(",", 2)
            if kind == "continuous":
                cells[int(cell)] = (float(total), float(rest))
            else:
                weights = {}
                for item in filter(None, rest.split(";")):
                    cls, weight = item.split(":")
                    weights[int(cls)] = float(weight)
                cells[int(cell)] = (float(total), weights)
    return cells


def accuracy(cells, reference, kind, interpolated):
    missing = sum(1 for c in reference if c not in cells) / len(reference)
    common = [c for c in reference if c in cells]
    if not common:
        return missing, None, None
    if kind == "continuous":
        error = statistics.fmean(abs(cells[c][1] - reference[c][1]) for c in common)
    else:
        def distance(a, b):
            ta, tb = a[0] or 1.0, b[0] or 1.0
            classes = a[1].keys() | b[1].keys()
            return 0.5 * sum(abs(a[1].get(k, 0) / ta - b[1].get(k, 0) / tb) for k in classes)

        error = statistics.fmean(distance(cells[c], reference[c]) for c in common)
    area_err = None
    if not interpolated:
        area_err = statistics.fmean(
            abs(cells[c][0] - reference[c][0]) / reference[c][0]
            for c in common if reference[c][0] > 0
        )
    return missing, error, area_err


def build_cases(args, tmp):
    """(grid, resolution, raster path) per row group, writing each raster once."""
    cases = []
    if args.sweep == "pixels":
        if args.grid != "h3":
            sys.exit("--sweep pixels runs on H3 only")
        resolutions = [int(r) for r in args.resolutions.split(",")]
        if any(r not in PIXEL_CASES for r in resolutions):
            sys.exit(f"--sweep pixels resolutions must be among {sorted(PIXEL_CASES)}")
        written = {}
        field = make_field(args.kind, WEST, 1.0)  # gradient over one degree, as before
        for res in resolutions:
            size = PIXEL_CASES[res]
            if size not in written:
                written[size] = Path(tmp) / f"pixels-{size}.tif"
                write_geotiff(written[size], size, PIXEL_DEG, field)
            cases.append(("h3", res, written[size], None))
        return cases
    res = args.resolution
    cell_m2 = mean_cell_area(args.grid, res)
    region_m2 = args.cells * cell_m2
    # A square region in degrees, wide enough to hold about --cells cells.
    side_deg = math.sqrt(region_m2 / (M_PER_DEG_LON * M_PER_DEG_LAT))
    # Checkerboard squares are --feature-cells cell edges wide, so every grid and resolution
    # sees the same ratio of feature size to cell size. A fixed size would make a fine grid's
    # cells nearly uniform and a coarse grid's cells straddle every edge, and the value
    # errors would then measure the test field rather than the modes.
    checker_deg = args.feature_cells * math.sqrt(cell_m2) / math.sqrt(M_PER_DEG_LON * M_PER_DEG_LAT)
    field = make_field(args.kind, WEST, side_deg, checker_deg, args.field)
    for ratio in (float(r) for r in args.ratios.split(",")):
        pixel_m2 = cell_m2 / ratio
        pixel_deg = math.sqrt(pixel_m2 / (M_PER_DEG_LON * M_PER_DEG_LAT))
        size = max(1, round(side_deg / pixel_deg))
        path = Path(tmp) / f"ratio-{ratio:g}.tif"
        write_geotiff(path, size, side_deg / size, field)
        fine = None
        if args.field_reference > 1:
            # The same field sampled F times finer each way: overlay on it approaches the
            # field's true mean (or class shares) over each cell.
            fine = Path(tmp) / f"ratio-{ratio:g}-fine.tif"
            f = args.field_reference
            write_geotiff(fine, size * f, side_deg / (size * f), field)
        cases.append((args.grid, res, path, fine))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--example", default="target/release/examples/transfer_compare")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--sweep", choices=["pixels", "ratio"], default="pixels")
    parser.add_argument("--kind", choices=["continuous", "categorical"], default="continuous")
    parser.add_argument("--grid", default=None, choices=["h3", "a5", "s2", "healpix"],
                        help="h3 for --sweep pixels; default s2 for ratio")
    parser.add_argument("--resolutions", default=",".join(map(str, PIXEL_CASES)),
                        help="--sweep pixels: H3 resolutions to run (7-13)")
    parser.add_argument("--resolution", type=int, default=13, help="--sweep ratio: resolution")
    parser.add_argument("--ratios", default=DEFAULT_RATIOS,
                        help="--sweep ratio: cell-to-pixel area ratios")
    parser.add_argument("--cells", type=int, default=40_000,
                        help="--sweep ratio: approximate cells in the region")
    parser.add_argument("--feature-cells", type=float, default=1.0,
                        help="--sweep ratio: checkerboard square (or smooth half-wave) width "
                             "in cell edge lengths")
    parser.add_argument("--field", choices=["checker", "smooth"], default="checker",
                        help="--sweep ratio: test field shape")
    parser.add_argument("--field-reference", type=int, default=0, metavar="F",
                        help="--sweep ratio: also score every mode against overlay on a raster "
                             "of the same field F times finer each way (its true cell means)")
    parser.add_argument("--modes", default=None, help="Comma-separated modes")
    parser.add_argument("--markdown", action="store_true", help="Print a Markdown table")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    args.grid = args.grid or ("h3" if args.sweep == "pixels" else "s2")
    modes = (args.modes or (CONTINUOUS_MODES if args.kind == "continuous"
                            else CATEGORICAL_MODES)).split(",")
    interp_names = {"nearest", "bilinear", "bicubic", "lanczos"}

    columns = ["grid", "resolution", "cell_px_ratio", "mode", "cells", "scan_ms",
               "peak_rss_mib", "missing_pct", "error", "area_err_pct",
               "field_missing_pct", "field_error"]
    if not args.markdown:
        print(",".join(columns), flush=True)
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        for grid, res, raster, fine in build_cases(args, tmp):
            raster = str(raster)
            commands = {m: [args.example, raster, grid, str(res), args.kind, m] for m in modes}
            for command in commands.values():  # warm the page cache and binary
                measure(command)
            samples = {m: [] for m in modes}
            for i in range(args.runs):
                for m in (modes if i % 2 == 0 else reversed(modes)):
                    samples[m].append(measure(commands[m]))
            # Accuracy runs come after every timed run, so the timed children never fork
            # from a parent holding the cell tables.
            reference_csv = Path(tmp) / "reference.csv"
            subprocess.run([args.example, raster, grid, str(res), args.kind, "overlay",
                            str(reference_csv)], check=True, stdout=subprocess.DEVNULL)
            reference = read_cells(reference_csv, args.kind)
            field_reference = None
            if fine is not None:
                field_csv = Path(tmp) / "field.csv"
                subprocess.run([args.example, str(fine), grid, str(res), args.kind, "overlay",
                                str(field_csv)], check=True, stdout=subprocess.DEVNULL)
                field_reference = read_cells(field_csv, args.kind)
                field_csv.unlink()
                os.unlink(fine)
            for m in modes:
                cells_csv = Path(tmp) / "cells.csv"
                subprocess.run([*commands[m], str(cells_csv)], check=True,
                               stdout=subprocess.DEVNULL)
                label = samples[m][0][0]
                base = label.split(":")[-1].split("+")[0]
                interpolated = base in interp_names
                cells = read_cells(cells_csv, args.kind)
                missing, error, area_err = accuracy(cells, reference, args.kind, interpolated)
                field_missing = field_error = None
                if field_reference is not None:
                    field_missing, field_error, _ = accuracy(
                        cells, field_reference, args.kind, True)
                del cells
                rows.append({
                    "grid": grid,
                    "resolution": res,
                    "cell_px_ratio": samples[m][0][5],
                    "mode": label,
                    "cells": samples[m][0][1],
                    "scan_ms": statistics.median(s[2] for s in samples[m]),
                    "peak_rss_mib": statistics.median(s[4] for s in samples[m]),
                    "missing_pct": 100 * missing,
                    "error": error,
                    "area_err_pct": None if area_err is None else 100 * area_err,
                    "field_missing_pct": None if field_missing is None else 100 * field_missing,
                    "field_error": field_error,
                })
                if not args.markdown:
                    r = rows[-1]
                    print(",".join("" if r[k] is None else str(r[k]) for k in columns),
                          flush=True)
                cells_csv.unlink()
            del reference, field_reference
            reference_csv.unlink()

    if args.markdown:
        error_name = "Mean abs. error" if args.kind == "continuous" else "Class-fraction error"
        field = args.field_reference > 1
        extra = " | Error vs. field |" if field else " |"
        print(f"| Grid | Res | Cell / pixel area | Mode | Cells | Scan ms | Peak RSS MiB "
              f"| Cells missing | {error_name} | Area error{extra}")
        print("| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: |"
              + (" ---: |" if field else ""))
        for r in rows:
            area = "—" if r["area_err_pct"] is None else f"{r['area_err_pct']:.2f}%"
            error = "—" if r["error"] is None else f"{r['error']:.3f}"
            tail = " |"
            if field:
                tail = " | — |" if r["field_error"] is None else f" | {r['field_error']:.3f} |"
            print(f"| {r['grid']} | {r['resolution']} | {r['cell_px_ratio']:.3g} | {r['mode']} | "
                  f"{r['cells']:,} | {r['scan_ms']:.1f} | {r['peak_rss_mib']:.1f} | "
                  f"{r['missing_pct']:.1f}% | {error} | {area}{tail}")


if __name__ == "__main__":
    main()
