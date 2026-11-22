"""Unit tests for the Criterion benchmark summary script."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Optional

from criterion_summary import format_duration_ns, format_percent, main


def _write_json(path: Path, payload: dict, mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(path, (mtime, mtime))


def _write_benchmark_meta(bench_dir: Path, subdir: str, full_id: Optional[str], mtime: float) -> None:
    payload = {"full_id": full_id} if full_id is not None else {"group_id": "group"}
    _write_json(bench_dir / subdir / "benchmark.json", payload, mtime)


def _write_estimates(bench_dir: Path, subdir: str, mean_ns: float, mtime: float) -> None:
    payload = {
        "mean": {
            "point_estimate": mean_ns,
            "standard_error": mean_ns * 0.01,
            "confidence_interval": {
                "confidence_level": 0.95,
                "lower_bound": mean_ns * 0.98,
                "upper_bound": mean_ns * 1.02,
            },
        }
    }
    _write_json(bench_dir / subdir / "estimates.json", payload, mtime)


def _write_change(
    bench_dir: Path, point: float, lower: float, upper: float, mtime: float
) -> None:
    payload = {
        "mean": {
            "point_estimate": point,
            "standard_error": 0.01,
            "confidence_interval": {
                "confidence_level": 0.95,
                "lower_bound": lower,
                "upper_bound": upper,
            },
        }
    }
    _write_json(bench_dir / "change" / "estimates.json", payload, mtime)


class CriterionSummaryTests(unittest.TestCase):
    def _run_main(self, argv: list[str]) -> tuple[int, str]:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = main(argv)
        return code, buf.getvalue()

    def test_classification_and_sorting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / ".head-start"
            marker.write_text("", encoding="utf-8")
            t_base = 1_000_000.0
            t_marker = 1_000_100.0
            t_head = 1_000_200.0
            t_change = 1_000_201.0
            os.utime(marker, (t_marker, t_marker))

            # Two regressions to check sorting by largest change first.
            slower_big = root / "streaming" / "slow_big"
            _write_benchmark_meta(slower_big, "new", "streaming/slow_big", t_head)
            _write_estimates(slower_big, "base", 1000.0, t_base)
            _write_estimates(slower_big, "new", 1250.0, t_head)
            _write_change(slower_big, 0.25, 0.18, 0.32, t_change)

            slower_small = root / "streaming" / "slow_small"
            _write_benchmark_meta(slower_small, "new", "streaming/slow_small", t_head)
            _write_estimates(slower_small, "base", 2000.0, t_base)
            _write_estimates(slower_small, "new", 2160.0, t_head)
            _write_change(slower_small, 0.08, 0.06, 0.10, t_change)

            # Improvement (upper < -0.05).
            faster = root / "streaming" / "fast"
            _write_benchmark_meta(faster, "new", "streaming/fast", t_head)
            _write_estimates(faster, "base", 5_000_000.0, t_base)
            _write_estimates(faster, "new", 4_000_000.0, t_head)
            _write_change(faster, -0.20, -0.25, -0.15, t_change)

            # Within noise: point estimate > 5%, but CI straddles threshold.
            noise = root / "streaming" / "noisy"
            _write_benchmark_meta(noise, "new", "streaming/noisy", t_head)
            _write_estimates(noise, "base", 500.0, t_base)
            _write_estimates(noise, "new", 530.0, t_head)
            _write_change(noise, 0.06, -0.01, 0.13, t_change)

            # New benchmark (no baseline).
            new_bench = root / "streaming" / "brand_new"
            _write_benchmark_meta(new_bench, "new", "streaming/brand_new", t_head)
            _write_estimates(new_bench, "new", 42_000.0, t_head)

            # Not run in head (ran before marker, has baseline).
            not_run = root / "streaming" / "old_only"
            _write_benchmark_meta(not_run, "new", "streaming/old_only", t_base)
            _write_estimates(not_run, "base", 900.0, t_base)
            _write_estimates(not_run, "new", 900.0, t_base)

            code, out = self._run_main(
                [
                    str(root),
                    "--baseline",
                    "base",
                    "--since",
                    str(marker),
                    "--noise-threshold",
                    "0.05",
                    "--base-ref",
                    "base-sha",
                    "--head-ref",
                    "head-sha",
                ]
            )
            self.assertEqual(code, 0)
            self.assertIn("Base: `base-sha` · Head: `head-sha`", out)
            self.assertIn(
                "2 slower, 1 faster, 1 within noise, 1 new, 1 not run",
                out,
            )

            table_rows = [
                line for line in out.splitlines() if line.startswith("| `")
            ]
            ordered_names = [line.split("|")[1].strip().strip("`") for line in table_rows]
            self.assertEqual(
                ordered_names,
                [
                    "streaming/slow_big",
                    "streaming/slow_small",
                    "streaming/fast",
                    "streaming/noisy",
                    "streaming/brand_new",
                    "streaming/old_only",
                ],
            )
            self.assertIn("🔴 slower", table_rows[0])
            self.assertIn("+25.00%", table_rows[0])
            self.assertIn("🟢 faster", table_rows[2])
            self.assertIn("⚪ within noise", table_rows[3])
            self.assertIn("🆕 new", table_rows[4])
            self.assertIn("⏭️ not run", table_rows[5])

    def test_stale_change_treated_as_new(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            t_old_change = 1_000_050.0
            t_head = 1_000_200.0

            bench = root / "group" / "case"
            _write_benchmark_meta(bench, "new", None, t_head)
            _write_estimates(bench, "base", 100.0, 1_000_000.0)
            _write_estimates(bench, "new", 150.0, t_head)
            # Stale change dir written before new/estimates.json
            _write_change(bench, 0.50, 0.40, 0.60, t_old_change)

            code, out = self._run_main([str(root)])
            self.assertEqual(code, 0)
            self.assertIn("0 slower, 0 faster, 0 within noise, 1 new, 0 not run", out)
            # Fallback to relative path when full_id is missing.
            self.assertIn("| `group/case` | — | 150 ns | — | — | 🆕 new |", out)

    def test_missing_and_empty_dirs_exit_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "does-not-exist"
            code, out = self._run_main([str(missing)])
            self.assertEqual(code, 0)
            self.assertIn(f"No Criterion results found in {missing}", out)

            empty = Path(tmp) / "empty"
            empty.mkdir()
            code, out = self._run_main([str(empty)])
            self.assertEqual(code, 0)
            self.assertIn(f"No Criterion results found in {empty}", out)

    def test_missing_since_marker_treats_base_as_not_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bench = root / "group" / "only_base"
            _write_benchmark_meta(bench, "base", "group/only_base", 1_000_000.0)
            _write_estimates(bench, "base", 2500.0, 1_000_000.0)

            code, out = self._run_main(
                [str(root), "--since", str(root / "missing-marker")]
            )
            self.assertEqual(code, 0)
            self.assertIn("0 slower, 0 faster, 0 within noise, 0 new, 1 not run", out)
            self.assertIn("| `group/only_base` | 2.50 µs | — | — | — | ⏭️ not run |", out)

    def test_formatting(self):
        cases = [
            (0.0, "0 ns"),
            (0.5432, "0.543 ns"),
            (5.432, "5.43 ns"),
            (54.32, "54.3 ns"),
            (543.2, "543 ns"),
            (99.96, "100 ns"),
            (999.9, "1.00 µs"),
            (1_234.0, "1.23 µs"),
            (12_340.0, "12.3 µs"),
            (123_400.0, "123 µs"),
            (999_900.0, "1.00 ms"),
            (12_340_000.0, "12.3 ms"),
            (1_234_000_000.0, "1.23 s"),
            (None, "—"),
        ]
        for ns, expected in cases:
            self.assertEqual(format_duration_ns(ns), expected, msg=f"ns={ns}")

        self.assertEqual(format_percent(0.0421), "+4.21%")
        self.assertEqual(format_percent(-0.1234), "-12.34%")
        self.assertEqual(format_percent(0.0), "+0.00%")
        self.assertEqual(format_percent(-0.00001), "+0.00%")
        self.assertEqual(format_percent(None), "—")


if __name__ == "__main__":
    unittest.main()
