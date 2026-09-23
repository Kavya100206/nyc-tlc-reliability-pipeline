"""
tests/test_pipeline.py — Unit + integration tests for pipeline.py (Phase 5)

Fast tests (no real data required):
  TestMonthValidation   — MONTH_RE regex rejects bad formats before any I/O
  TestValidSteps        — VALID_STEPS contains expected stage names
  TestGitSha            — _get_git_sha() returns a non-empty string
  TestCLIArgs           — invalid --month / --step cause exit code 1 without running stages
  TestIdempotency       — validate.run_validate skips when outputs exist (force=False)
                          and runs when force=True (raises FileNotFoundError on missing raw)

Integration tests (require real data — @pytest.mark.integration):
  TestIntegration       — full metrics stage runs end-to-end and produces non-empty CSVs
                          warm pipeline completes in < 5 seconds

Run fast tests:
  pytest tests/test_pipeline.py

Run integration tests:
  pytest tests/test_pipeline.py -m integration
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

# Ensure repo root is on sys.path for src.* imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline import MONTH_RE, VALID_STEPS, _get_git_sha, run_pipeline


# ── Month format validation ────────────────────────────────────────────────────

class TestMonthValidation:
    """MONTH_RE must accept valid months and reject invalid ones before any I/O."""

    @pytest.mark.parametrize("valid_month", [
        "2025-01", "2024-12", "2023-06", "2025-10",
    ])
    def test_valid_months_accepted(self, valid_month):
        assert MONTH_RE.match(valid_month), f"Expected {valid_month!r} to match MONTH_RE"

    @pytest.mark.parametrize("bad_month", [
        "01-2025",    # wrong order
        "2025-1",     # missing leading zero
        "2025-13",    # month 13 doesn't exist
        "2025-00",    # month 00 doesn't exist
        "25-01",      # 2-digit year
        "2025/01",    # wrong separator
        "2025-01-15", # full date, not month
        "not-a-date",
        "",
    ])
    def test_invalid_months_rejected(self, bad_month):
        assert not MONTH_RE.match(bad_month), f"Expected {bad_month!r} to NOT match MONTH_RE"


# ── VALID_STEPS ────────────────────────────────────────────────────────────────

class TestValidSteps:
    def test_contains_all_stages(self):
        assert "ingest" in VALID_STEPS
        assert "validate" in VALID_STEPS
        assert "metrics" in VALID_STEPS

    def test_no_unexpected_stages(self):
        assert VALID_STEPS == frozenset({"ingest", "validate", "metrics"})


# ── git SHA ────────────────────────────────────────────────────────────────────

class TestGitSha:
    def test_returns_string(self):
        sha = _get_git_sha()
        assert isinstance(sha, str)

    def test_not_empty(self):
        sha = _get_git_sha()
        assert len(sha) > 0

    def test_is_either_hex_or_unknown(self):
        sha = _get_git_sha()
        # Either a valid short SHA (hex chars) or the fallback string "unknown"
        is_hex = all(c in "0123456789abcdef" for c in sha)
        assert is_hex or sha == "unknown", f"Unexpected SHA format: {sha!r}"


# ── CLI arg validation ─────────────────────────────────────────────────────────

class TestCLIArgs:
    """
    These tests invoke the CLI as a subprocess to verify that argument validation
    runs BEFORE any pipeline work. They check exit codes, not pipeline output.
    """
    REPO_ROOT = str(Path(__file__).parent.parent)

    def _run(self, args: list) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "src/pipeline.py"] + args,
            capture_output=True,
            text=True,
            cwd=self.REPO_ROOT,
        )

    def test_invalid_month_format_exits_1(self):
        result = self._run(["--month", "01-2025"])
        assert result.returncode == 1, (
            f"Expected exit 1 for invalid month format. Got {result.returncode}.\n"
            f"stderr: {result.stderr}"
        )

    def test_invalid_month_no_leading_zero_exits_1(self):
        result = self._run(["--month", "2025-1"])
        assert result.returncode == 1

    def test_invalid_step_exits_1(self):
        # argparse rejects unknown --step choices before MONTH_RE check
        result = self._run(["--month", "2025-01", "--step", "bogus_stage"])
        assert result.returncode != 0, "Expected non-zero exit for invalid --step"

    def test_missing_month_exits_nonzero(self):
        result = self._run([])
        assert result.returncode != 0, "Expected non-zero exit when --month is missing"

    def test_help_exits_0(self):
        result = self._run(["--help"])
        assert result.returncode == 0

    def test_help_lists_steps(self):
        result = self._run(["--help"])
        for step in ["ingest", "validate", "metrics"]:
            assert step in result.stdout, f"Expected {step!r} in --help output"


# ── Idempotency unit tests ─────────────────────────────────────────────────────

class TestIdempotency:
    """
    Test that validate.run_validate respects the force=False skip logic and
    correctly raises FileNotFoundError when force=True bypasses the skip.

    Uses tmp_path (pytest fixture) to create isolated fake output files.
    Does NOT require the real 3.5M-row Parquet — all inputs are synthetic.
    """

    def _make_fake_outputs(self, proc_dir: Path, month: str = "2025-01"):
        """Create minimal fake validated Parquet + report to satisfy the skip check."""
        import pandas as pd
        pd.DataFrame({"col": [1, 2, 3]}).to_parquet(
            proc_dir / f"trips_validated_{month}.parquet"
        )
        report = {
            "month": month,
            "counts": {
                "rows_in_raw": 100,
                "rows_valid_out": 90,
                "rows_valid_pct": 90.0,
                "rows_hard_flagged": 10,
                "rows_hard_flagged_pct": 10.0,
                "rows_soft_only_flagged": 5,
                "rows_soft_only_flagged_pct": 5.0,
            },
            "flag_counts": {},
            "profile": {},
            "known_unknown_assumption_limitation": [],
        }
        (proc_dir / f"validation_report_{month}.json").write_text(
            json.dumps(report)
        )
        return report

    def test_skip_returns_existing_report(self, tmp_path):
        """With force=False and both outputs existing, run_validate returns existing report."""
        from src.validate import run_validate

        proc_dir = tmp_path / "processed"
        proc_dir.mkdir()
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        ref_dir = tmp_path / "reference"
        ref_dir.mkdir()

        expected_report = self._make_fake_outputs(proc_dir)

        result = run_validate(
            month="2025-01",
            raw_dir=raw_dir,
            ref_dir=ref_dir,
            processed_dir=proc_dir,
            force=False,
        )

        assert result["month"] == "2025-01"
        assert result["counts"]["rows_valid_out"] == expected_report["counts"]["rows_valid_out"]

    def test_skip_does_not_touch_raw_file(self, tmp_path):
        """Skip path must not read or require the raw Parquet (it may not exist on CI)."""
        from src.validate import run_validate

        proc_dir = tmp_path / "processed"
        proc_dir.mkdir()
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        ref_dir = tmp_path / "reference"
        ref_dir.mkdir()

        self._make_fake_outputs(proc_dir)
        # raw_dir is empty — no Parquet file present

        # Should succeed without reading raw_dir at all
        result = run_validate(
            month="2025-01",
            raw_dir=raw_dir,
            ref_dir=ref_dir,
            processed_dir=proc_dir,
            force=False,
        )
        assert result["month"] == "2025-01"

    def test_force_bypasses_skip_and_fails_on_missing_raw(self, tmp_path):
        """With force=True, the skip is ignored → FileNotFoundError when raw Parquet is absent."""
        from src.validate import run_validate

        proc_dir = tmp_path / "processed"
        proc_dir.mkdir()
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        ref_dir = tmp_path / "reference"
        ref_dir.mkdir()

        # Both outputs exist — skip would normally fire
        self._make_fake_outputs(proc_dir)

        # force=True bypasses skip → tries to load raw Parquet → FileNotFoundError
        with pytest.raises(FileNotFoundError, match="Raw Parquet not found"):
            run_validate(
                month="2025-01",
                raw_dir=raw_dir,
                ref_dir=ref_dir,
                processed_dir=proc_dir,
                force=True,
            )

    def test_metrics_skip_returns_existing_paths(self, tmp_path):
        """With force=False and all 4 CSVs existing, run_metrics skips model build."""
        from src.metrics import run_metrics

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()

        # Write the 4 expected CSV files
        month = "2025-01"
        csv_names = [
            f"metrics_{month}.csv",
            f"metrics_duration_speed_{month}.csv",
            f"metrics_duration_speed_by_zone_{month}.csv",
            f"metrics_fare_{month}.csv",
        ]
        for name in csv_names:
            (outputs_dir / name).write_text("col1,col2\n1,2\n")

        # Should return immediately without building DuckDB model
        result = run_metrics(
            month=month,
            processed_dir=tmp_path / "processed",  # doesn't need to exist for skip
            ref_dir=tmp_path / "reference",
            outputs_dir=outputs_dir,
            force=False,
        )

        assert len(result) == 4
        for path in result.values():
            assert Path(path).exists()


# ── Integration tests ─────────────────────────────────────────────────────────

@pytest.mark.integration
class TestIntegration:
    """
    End-to-end integration tests. Require real data files to be present.
    Skipped automatically if the validated Parquet is not found.

    Run with:
      pytest tests/test_pipeline.py -m integration
    """
    MONTH = "2025-01"
    VALIDATED_PARQUET = Path("data/processed/trips_validated_2025-01.parquet")
    VALIDATION_REPORT = Path("data/processed/validation_report_2025-01.json")

    def _check_prerequisites(self):
        if not self.VALIDATED_PARQUET.exists() or not self.VALIDATION_REPORT.exists():
            pytest.skip(
                f"Validated Parquet or report not present — run ingest + validate first.\n"
                f"  Expected: {self.VALIDATED_PARQUET}\n"
                f"  Expected: {self.VALIDATION_REPORT}"
            )

    def test_metrics_stage_produces_all_four_csvs(self):
        """Run metrics stage with --force and assert all 4 CSVs exist and are non-empty."""
        self._check_prerequisites()

        exit_code = run_pipeline(self.MONTH, force=True, step="metrics")
        assert exit_code == 0, "Pipeline should exit 0 on success"

        expected_csvs = [
            Path(f"outputs/metrics_{self.MONTH}.csv"),
            Path(f"outputs/metrics_duration_speed_{self.MONTH}.csv"),
            Path(f"outputs/metrics_duration_speed_by_zone_{self.MONTH}.csv"),
            Path(f"outputs/metrics_fare_{self.MONTH}.csv"),
        ]
        for path in expected_csvs:
            assert path.exists(), f"Expected output CSV not found: {path}"
            assert path.stat().st_size > 0, f"Output CSV is empty: {path}"

    def test_metrics_csv_has_expected_columns(self):
        """Summary CSV must contain metric_name, value, and kpi_component columns."""
        import csv
        self._check_prerequisites()
        run_pipeline(self.MONTH, force=False, step="metrics")  # warm run, should skip

        summary_path = Path(f"outputs/metrics_{self.MONTH}.csv")
        assert summary_path.exists()

        with open(summary_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) > 0, "Summary CSV should have at least one data row"
        required_cols = {"metric_name", "value", "kpi_component", "unit"}
        actual_cols = set(rows[0].keys())
        missing = required_cols - actual_cols
        assert not missing, f"Summary CSV missing columns: {missing}"

    def test_reliability_pct_is_reasonable(self):
        """Data reliability must be between 80% and 99% for Jan 2025 data."""
        import csv
        self._check_prerequisites()

        summary_path = Path(f"outputs/metrics_{self.MONTH}.csv")
        assert summary_path.exists()

        with open(summary_path) as f:
            rows = {row["metric_name"]: row for row in csv.DictReader(f)}

        assert "data_reliability_pct" in rows, "data_reliability_pct must be in summary CSV"
        pct = float(rows["data_reliability_pct"]["value"])
        assert 80.0 < pct < 99.0, (
            f"data_reliability_pct={pct:.2f}% is outside expected range [80%, 99%]. "
            "This suggests a validation rule is over-flagging or under-flagging."
        )

    def test_warm_pipeline_completes_fast(self):
        """A fully-warm run (all outputs exist) should complete in under 5 seconds."""
        self._check_prerequisites()

        # Ensure all outputs exist first
        required_outputs = [
            Path(f"outputs/metrics_{self.MONTH}.csv"),
            Path(f"outputs/metrics_duration_speed_{self.MONTH}.csv"),
            Path(f"outputs/metrics_duration_speed_by_zone_{self.MONTH}.csv"),
            Path(f"outputs/metrics_fare_{self.MONTH}.csv"),
        ]
        if not all(p.exists() for p in required_outputs):
            run_pipeline(self.MONTH, force=True, step="metrics")

        t0 = time.time()
        exit_code = run_pipeline(self.MONTH, force=False, step="metrics")
        elapsed = time.time() - t0

        assert exit_code == 0
        assert elapsed < 5.0, (
            f"Warm run took {elapsed:.1f}s — expected < 5s. "
            "The idempotency skip is not firing correctly."
        )

    def test_log_file_created_per_run(self, tmp_path, monkeypatch):
        """Each pipeline run should create a new log file in logs/."""
        self._check_prerequisites()
        # Use absolute path — pytest may change cwd, but LOG_DIR is relative to repo root
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        before = set(log_dir.glob(f"pipeline_{self.MONTH}_*.log"))

        run_pipeline(self.MONTH, force=False, step="metrics")

        after = set(log_dir.glob(f"pipeline_{self.MONTH}_*.log"))
        new_logs = after - before
        assert len(new_logs) == 1, (
            f"Expected exactly 1 new log file per run. Got: {len(new_logs)}.\n"
            f"New files: {new_logs}"
        )
