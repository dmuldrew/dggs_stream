#!/usr/bin/env python3
"""Render a GitHub-flavoured Markdown comparison table from a Criterion output tree."""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_TIME_UNITS = ("ns", "µs", "ms", "s")


@dataclass
class BenchmarkRow:
    name: str
    status: str  # "slower", "faster", "noise", "new", "not_run"
    base_mean_ns: Optional[float]
    head_mean_ns: Optional[float]
    change_point: Optional[float]
    change_lower: Optional[float]
    change_upper: Optional[float]
    # Criterion's recorded confidence level for the change interval (e.g. 0.95).
    confidence_level: Optional[float] = None

    @property
    def sort_key(self) -> tuple[int, float, str]:
        if self.status == "slower":
            assert self.change_point is not None
            return (0, -self.change_point, self.name)
        if self.status == "faster":
            assert self.change_point is not None
            return (1, self.change_point, self.name)
        if self.status == "noise":
            assert self.change_point is not None
            return (2, -abs(self.change_point), self.name)
        if self.status == "new":
            return (3, 0.0, self.name)
        return (4, 0.0, self.name)


def _non_negative_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float value: {raw!r}") from exc
    if not math.isfinite(value) or value < 0.0:
        raise argparse.ArgumentTypeError(f"noise threshold must be non-negative: {raw!r}")
    return value


def format_duration_ns(ns: Optional[float]) -> str:
    if ns is None or not math.isfinite(ns) or ns < 0.0:
        return "—"
    if ns == 0.0:
        return "0 ns"
    idx = 0
    while idx < len(_TIME_UNITS) - 1 and ns >= 1000.0 ** (idx + 1):
        idx += 1
    value = ns / (1000.0 ** idx)
    decimals = max(0, 2 - int(math.floor(math.log10(value))))
    rounded = round(value, decimals)
    if rounded >= 1000.0 and idx < len(_TIME_UNITS) - 1:
        idx += 1
        value = rounded / 1000.0
        decimals = max(0, 2 - int(math.floor(math.log10(value))))
        rounded = round(value, decimals)
    else:
        decimals = max(0, 2 - int(math.floor(math.log10(rounded))))
    return f"{rounded:.{decimals}f} {_TIME_UNITS[idx]}"


def format_percent(fraction: Optional[float]) -> str:
    if fraction is None or not math.isfinite(fraction):
        return "—"
    pct = fraction * 100.0
    if abs(pct) < 0.005:
        pct = 0.0
    return f"{pct:+.2f}%"


def _load_json(path: Path) -> Optional[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _read_mean_point(estimates_path: Path) -> Optional[float]:
    data = _load_json(estimates_path)
    if data is None:
        return None
    mean = data.get("mean")
    if not isinstance(mean, dict):
        return None
    point = mean.get("point_estimate")
    if isinstance(point, (int, float)) and not isinstance(point, bool):
        val = float(point)
        if math.isfinite(val):
            return val
    return None


def _read_mean_change(
    estimates_path: Path,
) -> Optional[tuple[float, float, float, Optional[float]]]:
    data = _load_json(estimates_path)
    if data is None:
        return None
    mean = data.get("mean")
    if not isinstance(mean, dict):
        return None
    point = mean.get("point_estimate")
    ci = mean.get("confidence_interval")
    if not isinstance(ci, dict):
        return None
    lower = ci.get("lower_bound")
    upper = ci.get("upper_bound")
    coords = (point, lower, upper)
    if all(
        isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v))
        for v in coords
    ):
        level = ci.get("confidence_level")
        if not (
            isinstance(level, (int, float))
            and not isinstance(level, bool)
            and 0.0 < float(level) < 1.0
        ):
            level = None
        return float(point), float(lower), float(upper), (None if level is None else float(level))
    return None


def _read_benchmark_name(bench_dir: Path, criterion_dir: Path, baseline: str) -> str:
    for subdir in ("new", baseline):
        meta = _load_json(bench_dir / subdir / "benchmark.json")
        if meta is not None:
            full_id = meta.get("full_id")
            if isinstance(full_id, str) and full_id.strip():
                return full_id.strip()
    return bench_dir.relative_to(criterion_dir).as_posix()


def _discover_benchmark_dirs(criterion_dir: Path, baseline: str) -> list[Path]:
    discovered: set[Path] = set()
    for root, dirs, files in os.walk(criterion_dir):
        root_path = Path(root)
        if (
            "benchmark.json" in files
            and root_path != criterion_dir
            and root_path.name in ("new", baseline)
        ):
            discovered.add(root_path.parent)
            # No benchmark subdirs live inside Criterion output leaves.
            dirs[:] = []
    return sorted(discovered)


def collect_rows(
    criterion_dir: Path,
    baseline: str = "base",
    since: Optional[Path] = None,
    noise_threshold: float = 0.05,
) -> list[BenchmarkRow]:
    if not criterion_dir.is_dir():
        return []

    if since is None:
        since_mtime_ns: Optional[float] = None
    elif since.exists():
        since_mtime_ns = float(since.stat().st_mtime_ns)
    else:
        since_mtime_ns = math.inf

    rows: list[BenchmarkRow] = []
    for bench_dir in _discover_benchmark_dirs(criterion_dir, baseline):
        new_estimates = bench_dir / "new" / "estimates.json"
        base_estimates = bench_dir / baseline / "estimates.json"
        change_estimates = bench_dir / "change" / "estimates.json"

        ran = False
        new_mtime_ns: Optional[int] = None
        if new_estimates.is_file():
            new_mtime_ns = new_estimates.stat().st_mtime_ns
            ran = since_mtime_ns is None or new_mtime_ns >= since_mtime_ns

        name = _read_benchmark_name(bench_dir, criterion_dir, baseline)
        has_base = base_estimates.is_file()

        if ran:
            head_mean = _read_mean_point(new_estimates)
            fresh_change = (
                has_base
                and new_mtime_ns is not None
                and change_estimates.is_file()
                and change_estimates.stat().st_mtime_ns >= new_mtime_ns
            )
            change_stats = _read_mean_change(change_estimates) if fresh_change else None
            base_mean = _read_mean_point(base_estimates) if has_base else None
            if change_stats is not None and base_mean is not None and head_mean is not None:
                point, lower, upper, level = change_stats
                if lower > noise_threshold:
                    status = "slower"
                elif upper < -noise_threshold:
                    status = "faster"
                else:
                    status = "noise"
                rows.append(
                    BenchmarkRow(
                        name=name,
                        status=status,
                        base_mean_ns=base_mean,
                        head_mean_ns=head_mean,
                        change_point=point,
                        change_lower=lower,
                        change_upper=upper,
                        confidence_level=level,
                    )
                )
            else:
                rows.append(
                    BenchmarkRow(
                        name=name,
                        status="new",
                        base_mean_ns=None,
                        head_mean_ns=head_mean,
                        change_point=None,
                        change_lower=None,
                        change_upper=None,
                    )
                )
        elif has_base:
            rows.append(
                BenchmarkRow(
                    name=name,
                    status="not_run",
                    base_mean_ns=_read_mean_point(base_estimates),
                    head_mean_ns=None,
                    change_point=None,
                    change_lower=None,
                    change_upper=None,
                )
            )

    rows.sort(key=lambda row: row.sort_key)
    return rows


def _format_cell_name(name: str) -> str:
    escaped = name.replace("|", "\\|").replace("`", "'")
    return f"`{escaped}`"


def render_markdown(
    rows: list[BenchmarkRow],
    criterion_dir: Path,
    noise_threshold: float = 0.05,
    title: str = "Criterion benchmark comparison",
    base_ref: Optional[str] = None,
    head_ref: Optional[str] = None,
) -> str:
    if not rows:
        return f"No Criterion results found in {criterion_dir}\n"

    counts = {
        "slower": sum(1 for r in rows if r.status == "slower"),
        "faster": sum(1 for r in rows if r.status == "faster"),
        "noise": sum(1 for r in rows if r.status == "noise"),
        "new": sum(1 for r in rows if r.status == "new"),
        "not_run": sum(1 for r in rows if r.status == "not_run"),
    }

    lines = [f"## {title}", ""]
    refs = []
    if base_ref:
        refs.append(f"Base: `{base_ref}`")
    if head_ref:
        refs.append(f"Head: `{head_ref}`")
    if refs:
        lines.append(" · ".join(refs))
        lines.append("")

    lines.append(
        f"{counts['slower']} slower, {counts['faster']} faster, "
        f"{counts['noise']} within noise, {counts['new']} new, {counts['not_run']} not run"
    )
    lines.append("")
    threshold_pct = f"{noise_threshold * 100.0:g}%"
    # Label intervals with the level Criterion recorded (`--confidence-level`);
    # fall back to a generic label when it is missing or differs between rows.
    levels = {r.confidence_level for r in rows if r.change_lower is not None}
    ci_label = "CI"
    if len(levels) == 1 and None not in levels:
        ci_label = f"{next(iter(levels)) * 100.0:g}% CI"
    lines.append(
        "> Shared runners are noisy; these timings are informational only and never fail the build "
        f"(noise threshold: ±{threshold_pct} on the {ci_label} of the mean change)."
    )
    lines.append("")
    lines.append(f"| Benchmark | Base mean | PR mean | Change (mean) | {ci_label} | Result |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | :--- |")

    labels = {
        "slower": "🔴 slower",
        "faster": "🟢 faster",
        "noise": "⚪ within noise",
        "new": "🆕 new",
        "not_run": "⏭️ not run",
    }

    for row in rows:
        base_str = format_duration_ns(row.base_mean_ns)
        head_str = format_duration_ns(row.head_mean_ns)
        change_str = format_percent(row.change_point)
        if row.change_lower is not None and row.change_upper is not None:
            ci_str = f"[{format_percent(row.change_lower)}, {format_percent(row.change_upper)}]"
        else:
            ci_str = "—"
        lines.append(
            f"| {_format_cell_name(row.name)} | {base_str} | {head_str} | "
            f"{change_str} | {ci_str} | {labels[row.status]} |"
        )

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("criterion_dir", type=Path, help="Path to Criterion output directory")
    parser.add_argument("--baseline", default="base", help="Baseline name (default: base)")
    parser.add_argument(
        "--since",
        type=Path,
        default=None,
        help="Marker file whose mtime separates head runs from baseline-only runs",
    )
    parser.add_argument(
        "--noise-threshold",
        type=_non_negative_float,
        default=0.05,
        help="Relative noise threshold on the mean change CI (default: 0.05)",
    )
    parser.add_argument(
        "--title",
        default="Criterion benchmark comparison",
        help="Markdown heading title",
    )
    parser.add_argument("--base-ref", default=None, help="Optional base commit or ref label")
    parser.add_argument("--head-ref", default=None, help="Optional head commit or ref label")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    rows = collect_rows(
        args.criterion_dir,
        baseline=args.baseline,
        since=args.since,
        noise_threshold=args.noise_threshold,
    )
    output = render_markdown(
        rows,
        args.criterion_dir,
        noise_threshold=args.noise_threshold,
        title=args.title,
        base_ref=args.base_ref,
        head_ref=args.head_ref,
    )
    print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
