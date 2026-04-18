"""Unit tests for scripts/check_score_regression.py (#237)."""

import json

import pytest

from scripts.check_score_regression import run_checks


def _write_fixtures(tmp_path, summary_overrides=None, report_overrides=None):
    """Write minimal valid summary + report JSON, with optional field overrides."""
    summary = {
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "regions": ["singapore"],
        "skipped_regions": [],
        "total_known_cases": 13,
        "min_known_cases_target": 5,
        "metrics_summary": {
            "precision_at_50": {"mean": 0.50},
            "precision_at_100": {"mean": 0.50},
            "recall_at_200": {"mean": 1.0},
        },
    }
    report = {
        "schema_version": "1.0",
        "windows": [
            {
                "window_id": "singapore-integration-public",
                "metrics": {
                    "auroc": 0.85,
                    "precision_at_50": 0.50,
                    "recall_at_200": 1.0,
                    "labeled_count": 39,
                    "positive_count": 13,
                },
                "error_analysis": {"false_negatives": [], "false_positives": []},
            }
        ],
    }
    if summary_overrides:
        _deep_update(summary, summary_overrides)
    if report_overrides:
        _deep_update(report, report_overrides)

    summary_path = tmp_path / "summary.json"
    report_path = tmp_path / "report.json"
    summary_path.write_text(json.dumps(summary))
    report_path.write_text(json.dumps(report))
    return summary_path, report_path


def _deep_update(base, overrides):
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v


class TestPassingRun:
    def test_clean_metrics_produce_no_violations(self, tmp_path):
        s, r = _write_fixtures(tmp_path)
        assert run_checks(s, r) == []


class TestPrecisionAt50:
    def test_below_floor_is_violation(self, tmp_path):
        s, r = _write_fixtures(
            tmp_path, summary_overrides={"metrics_summary": {"precision_at_50": {"mean": 0.10}}}
        )
        violations = run_checks(s, r)
        assert any("precision_at_50" in v and "floor" in v for v in violations)

    def test_above_ceiling_is_violation(self, tmp_path):
        """P@50 = 1.0 on real data signals label inflation (issue #229 regression)."""
        s, r = _write_fixtures(
            tmp_path, summary_overrides={"metrics_summary": {"precision_at_50": {"mean": 1.0}}}
        )
        violations = run_checks(s, r)
        assert any("precision_at_50" in v and "ceiling" in v for v in violations)

    def test_at_floor_boundary_passes(self, tmp_path):
        s, r = _write_fixtures(
            tmp_path, summary_overrides={"metrics_summary": {"precision_at_50": {"mean": 0.25}}}
        )
        assert run_checks(s, r) == []

    def test_at_ceiling_boundary_passes(self, tmp_path):
        s, r = _write_fixtures(
            tmp_path, summary_overrides={"metrics_summary": {"precision_at_50": {"mean": 0.95}}}
        )
        assert run_checks(s, r) == []


class TestAuroc:
    def test_below_floor_is_violation(self, tmp_path):
        """AUROC < 0.65 means worse than random — issue #231 regression."""
        s, r = _write_fixtures(
            tmp_path,
            report_overrides={
                "windows": [
                    {
                        "metrics": {"auroc": 0.55, "labeled_count": 20, "positive_count": 10},
                        "error_analysis": {"false_negatives": []},
                    }
                ]
            },
        )
        violations = run_checks(s, r)
        assert any("auroc" in v and "floor" in v for v in violations)

    def test_above_ceiling_is_violation(self, tmp_path):
        s, r = _write_fixtures(
            tmp_path,
            report_overrides={
                "windows": [
                    {
                        "metrics": {"auroc": 1.0, "labeled_count": 20, "positive_count": 10},
                        "error_analysis": {"false_negatives": []},
                    }
                ]
            },
        )
        violations = run_checks(s, r)
        assert any("auroc" in v and "ceiling" in v for v in violations)

    def test_auroc_none_with_no_negatives_is_skipped(self, tmp_path):
        """AUROC=None when labeled_count==positive_count must not be a violation."""
        s, r = _write_fixtures(
            tmp_path,
            report_overrides={
                "windows": [
                    {
                        "metrics": {"auroc": None, "labeled_count": 10, "positive_count": 10},
                        "error_analysis": {"false_negatives": []},
                    }
                ]
            },
        )
        violations = run_checks(s, r)
        assert not any("auroc" in v for v in violations)


class TestRecallAt200:
    def test_below_floor_is_violation(self, tmp_path):
        s, r = _write_fixtures(
            tmp_path, summary_overrides={"metrics_summary": {"recall_at_200": {"mean": 0.40}}}
        )
        violations = run_checks(s, r)
        assert any("recall_at_200" in v for v in violations)

    def test_no_ceiling_on_recall(self, tmp_path):
        s, r = _write_fixtures(
            tmp_path, summary_overrides={"metrics_summary": {"recall_at_200": {"mean": 1.0}}}
        )
        assert run_checks(s, r) == []


class TestFalseNegatives:
    def test_near_zero_confidence_is_violation(self, tmp_path):
        """A vessel scoring 0.05 is genuinely near-zero — must fail."""
        s, r = _write_fixtures(
            tmp_path,
            report_overrides={
                "windows": [
                    {
                        "metrics": {"auroc": 0.85, "labeled_count": 10, "positive_count": 5},
                        "error_analysis": {
                            "false_negatives": [{"mmsi": "123", "confidence": 0.05}],
                            "false_positives": [],
                        },
                    }
                ]
            },
        )
        violations = run_checks(s, r)
        assert any("false_negatives" in v for v in violations)

    def test_above_near_zero_threshold_is_not_a_violation(self, tmp_path):
        """Vessels scoring 0.47–0.69 (seed mode threshold artefact) must not fail."""
        s, r = _write_fixtures(
            tmp_path,
            report_overrides={
                "windows": [
                    {
                        "metrics": {"auroc": 0.85, "labeled_count": 10, "positive_count": 5},
                        "error_analysis": {
                            "false_negatives": [
                                {"mmsi": "111", "confidence": 0.47},
                                {"mmsi": "222", "confidence": 0.69},
                            ],
                            "false_positives": [],
                        },
                    }
                ]
            },
        )
        violations = run_checks(s, r)
        assert not any("false_negatives" in v for v in violations)

    def test_zero_false_negatives_passes(self, tmp_path):
        s, r = _write_fixtures(tmp_path)
        assert run_checks(s, r) == []


class TestSeedModeAllPositive:
    """Seed-mode dataset: labeled_count == positive_count for all windows."""

    def _seed_fixtures(self, tmp_path, p50=1.0):
        """All-positive dataset matching CI seed-mode output."""
        summary = {
            "generated_at_utc": "2026-01-01T00:00:00+00:00",
            "regions": ["singapore"],
            "skipped_regions": [],
            "total_known_cases": 10,
            "metrics_summary": {
                "precision_at_50": {"mean": p50},
                "recall_at_200": {"mean": 1.0},
            },
        }
        report = {
            "schema_version": "1.0",
            "windows": [
                {
                    "window_id": "singapore-integration-public",
                    "metrics": {
                        "auroc": None,
                        "labeled_count": 10,
                        "positive_count": 10,
                    },
                    "error_analysis": {
                        # 7 vessels at 0.47–0.69 — backtest threshold artefact
                        "false_negatives": [{"mmsi": str(i), "confidence": 0.50} for i in range(7)],
                        "false_positives": [],
                    },
                }
            ],
        }
        s = tmp_path / "summary.json"
        r = tmp_path / "report.json"
        s.write_text(json.dumps(summary))
        r.write_text(json.dumps(report))
        return s, r

    def test_seed_mode_passes_with_p50_1(self, tmp_path):
        """P@50=1.0 and AUROC=None and threshold-artefact FNs must all pass."""
        s, r = self._seed_fixtures(tmp_path, p50=1.0)
        assert run_checks(s, r) == []

    def test_seed_mode_still_fails_on_p50_floor(self, tmp_path):
        """Even in seed mode a broken scorer can score positives near-zero → P@50 floor enforced."""
        s, r = self._seed_fixtures(tmp_path, p50=0.10)
        violations = run_checks(s, r)
        assert any("precision_at_50" in v and "floor" in v for v in violations)

    def test_seed_mode_near_zero_fn_still_fails(self, tmp_path):
        """Near-zero confidence (< 0.1) is a real bug even in seed mode."""
        import json as _json

        s, r = self._seed_fixtures(tmp_path)
        report = _json.loads(r.read_text())
        report["windows"][0]["error_analysis"]["false_negatives"].append(
            {"mmsi": "999", "confidence": 0.05}
        )
        r.write_text(_json.dumps(report))
        violations = run_checks(s, r)
        assert any("false_negatives" in v for v in violations)


class TestTotalKnownCases:
    def test_too_few_cases_is_violation(self, tmp_path):
        s, r = _write_fixtures(tmp_path, summary_overrides={"total_known_cases": 2})
        violations = run_checks(s, r)
        assert any("total_known_cases" in v and "floor" in v for v in violations)

    def test_too_many_cases_is_violation(self, tmp_path):
        s, r = _write_fixtures(tmp_path, summary_overrides={"total_known_cases": 600})
        violations = run_checks(s, r)
        assert any("total_known_cases" in v and "ceiling" in v for v in violations)


class TestMissingFiles:
    def test_missing_summary_exits_2(self, tmp_path):
        _, r = _write_fixtures(tmp_path)
        with pytest.raises(SystemExit) as exc:
            run_checks(tmp_path / "nonexistent.json", r)
        assert exc.value.code == 2

    def test_missing_report_exits_2(self, tmp_path):
        s, _ = _write_fixtures(tmp_path)
        with pytest.raises(SystemExit) as exc:
            run_checks(s, tmp_path / "nonexistent.json")
        assert exc.value.code == 2
