"""
Integration test: verify causal_effects.parquet written by the pipeline.

Run after `docker compose run --rm pipeline` (or `scripts/run_pipeline.py`)
has completed at least one scoring cycle:

    uv run pytest tests/test_causal_effects_output.py -v

The test reads `data/processed/singapore_causal_effects.parquet` (or the path
set in the CAUSAL_EFFECTS_PATH env var) and asserts the structural and
semantic expectations described in docs/local-e2e-test.md Step 3.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

CAUSAL_EFFECTS_PATH = os.getenv(
    "CAUSAL_EFFECTS_PATH",
    "data/processed/singapore_causal_effects.parquet",
)

EXPECTED_REGIMES = {"OFAC Iran", "OFAC Russia", "UN DPRK"}

REQUIRED_COLUMNS = {
    "regime",
    "label",
    "n_treated",
    "n_control",
    "att_estimate",
    "att_ci_lower",
    "att_ci_upper",
    "p_value",
    "is_significant",
    "calibrated_weight",
}


@pytest.fixture(scope="module")
def causal_df() -> pl.DataFrame:
    path = Path(CAUSAL_EFFECTS_PATH)
    if not path.exists():
        pytest.skip(
            f"{CAUSAL_EFFECTS_PATH} not found — run the pipeline first: "
            "docker compose run --rm pipeline"
        )
    return pl.read_parquet(path)


def test_causal_effects_has_required_columns(causal_df):
    """All expected output columns must be present."""
    assert REQUIRED_COLUMNS <= set(causal_df.columns), (
        f"Missing columns: {REQUIRED_COLUMNS - set(causal_df.columns)}"
    )


def test_causal_effects_has_three_regimes(causal_df):
    """One row per sanction regime (OFAC Iran, OFAC Russia, UN DPRK)."""
    assert causal_df.height == 3, f"Expected 3 regime rows, got {causal_df.height}"
    labels = set(causal_df["label"].to_list())
    assert labels == EXPECTED_REGIMES, f"Unexpected regime labels: {labels}"


def test_causal_effects_p_values_in_range(causal_df):
    """p-values must be in [0, 1]."""
    p = causal_df["p_value"]
    assert p.min() >= 0.0
    assert p.max() <= 1.0


def test_causal_effects_ci_ordered(causal_df):
    """CI lower bound must be ≤ upper bound for every regime."""
    for row in causal_df.iter_rows(named=True):
        assert row["att_ci_lower"] <= row["att_ci_upper"], (
            f"CI inverted for {row['label']}: [{row['att_ci_lower']}, {row['att_ci_upper']}]"
        )


def test_causal_effects_calibrated_weight_in_range(causal_df):
    """Calibrated weight must stay within the [0.20, 0.65] guardrails."""
    w = causal_df["calibrated_weight"]
    assert w.min() >= 0.20, f"calibrated_weight below floor: {w.min()}"
    assert w.max() <= 0.65, f"calibrated_weight above cap: {w.max()}"


def test_causal_effects_weight_consistent_across_regimes(causal_df):
    """All rows share the same calibrated_weight (it is a pipeline-level scalar)."""
    weights = causal_df["calibrated_weight"].unique()
    assert weights.len() == 1, (
        f"Expected a single calibrated_weight value, got: {weights.to_list()}"
    )


def test_causal_effects_counts_non_negative(causal_df):
    """Treated and control vessel counts must be ≥ 0."""
    assert causal_df["n_treated"].min() >= 0
    assert causal_df["n_control"].min() >= 0
