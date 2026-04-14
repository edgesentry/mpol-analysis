"""Unit tests for vessel name matching in _build_labels_for_watchlist (#232)."""

import polars as pl

from scripts.run_public_backtest_batch import _build_labels_for_watchlist, _normalize_vessel_name


def _make_positives(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mmsi": [r.get("mmsi", "") for r in rows],
            "imo": [r.get("imo", "") for r in rows],
            "name": [r.get("name", "") for r in rows],
            "entity_type": [r.get("entity_type", "Vessel") for r in rows],
            "evidence_source": [r.get("evidence_source", "us_ofac_sdn") for r in rows],
        }
    )


def _make_watchlist(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mmsi": [r.get("mmsi", "") for r in rows],
            "imo": [r.get("imo", "") for r in rows],
            "vessel_name": [r.get("vessel_name", "") for r in rows],
            "vessel_type": [r.get("vessel_type", "Tanker") for r in rows],
            "flag": [r.get("flag", "PA") for r in rows],
            "confidence": [r.get("confidence", 0.5) for r in rows],
            "behavioral_deviation_score": [0.5] * len(rows),
            "graph_risk_score": [0.5] * len(rows),
            "identity_score": [0.5] * len(rows),
            "top_signals": ["[]"] * len(rows),
            "last_lat": [1.0] * len(rows),
            "last_lon": [103.0] * len(rows),
            "last_seen": ["2026-01-01"] * len(rows),
            "ais_gap_count_30d": [0] * len(rows),
            "ais_gap_max_hours": [0.0] * len(rows),
            "position_jump_count": [0] * len(rows),
            "sts_candidate_count": [0] * len(rows),
            "flag_changes_2y": [0] * len(rows),
            "name_changes_2y": [0] * len(rows),
            "owner_changes_2y": [0] * len(rows),
            "sanctions_distance": [99] * len(rows),
            "shared_address_centrality": [0] * len(rows),
            "sts_hub_degree": [0] * len(rows),
            "cluster_label": [-1] * len(rows),
            "baseline_noise_score": [0.0] * len(rows),
        }
    )


class TestNormalizeVesselName:
    def test_uppercases_and_strips(self):
        expr = _normalize_vessel_name(pl.lit("  ocean voyager  "))
        result = pl.select(expr).item()
        assert result == "OCEAN VOYAGER"

    def test_removes_punctuation(self):
        expr = _normalize_vessel_name(pl.lit("MT. STAR-1"))
        result = pl.select(expr).item()
        assert result == "MT STAR1"

    def test_collapses_whitespace(self):
        expr = _normalize_vessel_name(pl.lit("DARK   SHIP"))
        result = pl.select(expr).item()
        assert result == "DARK SHIP"


class TestBuildLabelsNameMatch:
    def test_name_match_finds_vessel_not_in_mmsi_imo(self):
        """A vessel in the watchlist should be labeled positive if its name matches
        a Vessel-type sanctions entry even when MMSI/IMO don't match."""
        positives = _make_positives(
            [
                # Name-only sanctions entry (no MMSI/IMO)
                {"mmsi": "", "imo": "", "name": "SHADOW EAGLE", "entity_type": "Vessel"},
            ]
        )
        watchlist = _make_watchlist(
            [
                {
                    "mmsi": "123456789",
                    "imo": "IMO1234567",
                    "vessel_name": "SHADOW EAGLE",
                    "confidence": 0.7,
                },
                {
                    "mmsi": "999999999",
                    "imo": "IMO9999999",
                    "vessel_name": "CLEAN VESSEL",
                    "confidence": 0.1,
                },
            ]
        )
        labels = _build_labels_for_watchlist(watchlist, positives, max_known_cases=50)
        positives_found = labels.filter(pl.col("label") == "positive")
        assert positives_found.height == 1
        assert positives_found["mmsi"][0] == "123456789"

    def test_name_normalization_handles_case_and_punctuation(self):
        """Name matching is case-insensitive and strips punctuation."""
        positives = _make_positives(
            [{"mmsi": "", "imo": "", "name": "mt. shadow-1", "entity_type": "Vessel"}]
        )
        watchlist = _make_watchlist(
            [
                {
                    "mmsi": "111000111",
                    "imo": "IMO1110001",
                    "vessel_name": "MT SHADOW1",
                    "confidence": 0.6,
                },
            ]
        )
        labels = _build_labels_for_watchlist(watchlist, positives, max_known_cases=50)
        assert labels.filter(pl.col("label") == "positive").height == 1

    def test_name_match_does_not_duplicate_mmsi_match(self):
        """When a vessel is found by both MMSI and name, it appears exactly once."""
        positives = _make_positives(
            [
                {
                    "mmsi": "123456789",
                    "imo": "",
                    "name": "DOUBLE MATCH",
                    "entity_type": "Vessel",
                }
            ]
        )
        watchlist = _make_watchlist(
            [
                {
                    "mmsi": "123456789",
                    "imo": "IMO1234567",
                    "vessel_name": "DOUBLE MATCH",
                    "confidence": 0.8,
                },
            ]
        )
        labels = _build_labels_for_watchlist(watchlist, positives, max_known_cases=50)
        assert labels.filter(pl.col("label") == "positive").height == 1

    def test_non_vessel_entity_type_excluded_from_name_match(self):
        """Company/Organization-type entries must not be matched by vessel name."""
        positives = _make_positives(
            [
                {"mmsi": "", "imo": "", "name": "SHADOW CORP", "entity_type": "Company"},
            ]
        )
        watchlist = _make_watchlist(
            [
                {
                    "mmsi": "888000888",
                    "imo": "IMO8880008",
                    "vessel_name": "SHADOW CORP",
                    "confidence": 0.6,
                },
            ]
        )
        labels = _build_labels_for_watchlist(watchlist, positives, max_known_cases=50)
        assert labels.filter(pl.col("label") == "positive").height == 0

    def test_empty_vessel_name_not_matched(self):
        """Watchlist vessels with empty vessel_name are not matched by name."""
        positives = _make_positives([{"mmsi": "", "imo": "", "name": "", "entity_type": "Vessel"}])
        watchlist = _make_watchlist(
            [{"mmsi": "777000777", "imo": "IMO7770007", "vessel_name": "", "confidence": 0.5}]
        )
        labels = _build_labels_for_watchlist(watchlist, positives, max_known_cases=50)
        assert labels.filter(pl.col("label") == "positive").height == 0
