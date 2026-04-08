"""
Trade flow mismatch feature engineering.

Joins AIS-implied vessel routes against UN Comtrade bilateral trade statistics
to detect vessels moving cargo between country pairs with no declared trade.

Output columns (one row per MMSI):
    mmsi, route_cargo_mismatch, declared_vs_estimated_cargo_value

Usage:
    uv run python src/features/trade_mismatch.py
    uv run python src/features/trade_mismatch.py --comtrade-api-key YOUR_KEY
"""

import os

import duckdb
import httpx
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
COMTRADE_API_KEY = os.getenv("COMTRADE_API_KEY", "")

# UN Comtrade+ REST API (free tier: 500 req/day)
COMTRADE_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

# Crude oil HS code; most relevant commodity for tanker-dominated shadow fleet
CRUDE_OIL_HS = "2709"

# Sanctioned states commonly associated with shadow fleet crude exports
SANCTIONED_EXPORTERS = {"KP", "IR", "VE", "SY", "CU", "RU"}

# Tanker ship_type range (AIS)
TANKER_TYPES = set(range(80, 90))

# Reporter country code for Singapore (UN Comtrade M49)
SG_REPORTER = "702"


# ---------------------------------------------------------------------------
# Comtrade download
# ---------------------------------------------------------------------------


def download_comtrade(
    reporter: str,
    partner_codes: list[str],
    hs_code: str,
    year: int,
    db_path: str = DEFAULT_DB_PATH,
    api_key: str = COMTRADE_API_KEY,
) -> int:
    """Download bilateral trade data from UN Comtrade and cache in DuckDB trade_flow.

    Returns number of rows inserted.
    """
    if not api_key:
        print("  COMTRADE_API_KEY not set — skipping download. Export will use cached data only.")
        return 0

    params = {
        "reporterCode": reporter,
        "partnerCode": ",".join(partner_codes),
        "cmdCode": hs_code,
        "period": str(year),
        "motCode": "0",  # all modes of transport
        "flowCode": "M",  # imports
        "maxRecords": "500",
    }
    headers = {"Ocp-Apim-Subscription-Key": api_key}

    with httpx.Client(timeout=60) as client:
        resp = client.get(COMTRADE_URL, params=params, headers=headers)
        resp.raise_for_status()

    data = resp.json().get("data", [])
    if not data:
        return 0

    rows = [
        {
            "reporter": str(r.get("reporterCode", "")),
            "partner": str(r.get("partnerCode", "")),
            "hs_code": hs_code,
            "period": str(r.get("period", "")),
            "trade_value_usd": float(r.get("primaryValue") or 0),
            "route_key": f"{r.get('reporterCode')}-{r.get('partnerCode')}-{hs_code}-{r.get('period')}",
        }
        for r in data
    ]
    df = pl.DataFrame(rows)  # noqa: F841 — referenced by DuckDB via `FROM df`
    con = duckdb.connect(db_path)
    try:
        before = con.execute("SELECT count(*) FROM trade_flow").fetchone()[0]  # type: ignore[index]
        con.execute("""
            INSERT OR IGNORE INTO trade_flow
                (reporter, partner, hs_code, period, trade_value_usd, route_key)
            SELECT reporter, partner, hs_code, period, trade_value_usd, route_key
            FROM df
        """)
        inserted = con.execute("SELECT count(*) FROM trade_flow").fetchone()[0] - before  # type: ignore[index]
    finally:
        con.close()
    return inserted


# ---------------------------------------------------------------------------
# Route inference from AIS
# ---------------------------------------------------------------------------


def _infer_vessel_routes(db_path: str) -> pl.DataFrame:
    """Infer likely origin country for each tanker from its flag state.

    Simplified proxy: a tanker flagged to a sanctioned country is assumed to
    carry cargo from that country.  Full route inference (port detection +
    voyage segmentation) is left for Phase B.
    """
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute("""
            SELECT m.mmsi,
                   COALESCE(m.flag, '') AS flag,
                   COALESCE(m.ship_type, 0) AS ship_type
            FROM vessel_meta m
            WHERE m.mmsi IS NOT NULL
        """).pl()
    finally:
        con.close()
    return df


def _load_trade_flows(db_path: str, reporter: str, hs_code: str) -> pl.DataFrame:
    """Load cached Comtrade rows for a given reporter + HS code."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT partner, SUM(trade_value_usd) AS total_usd
            FROM trade_flow
            WHERE reporter = ? AND hs_code = ?
            GROUP BY partner
        """,
            [reporter, hs_code],
        ).pl()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------


def compute_trade_features(
    db_path: str = DEFAULT_DB_PATH,
    reporter: str = SG_REPORTER,
    hs_code: str = CRUDE_OIL_HS,
) -> pl.DataFrame:
    """Compute trade flow mismatch features.

    route_cargo_mismatch:
        1.0  — tanker flagged to a sanctioned state with no Comtrade trade
                flow from that state to the reporter country
        0.5  — tanker flagged to a sanctioned state but some trade flow exists
        0.0  — flag is not sanctioned OR not a tanker

    declared_vs_estimated_cargo_value:
        ratio of Comtrade total import value for the flag country to a
        per-vessel estimate (total / vessel count with that flag).
        0.0 if no Comtrade data available.
    """
    vessel_df = _infer_vessel_routes(db_path)
    trade_df = _load_trade_flows(db_path, reporter, hs_code)

    # trade_by_partner: partner (ISO M49) → total USD
    trade_map: dict[str, float] = {}
    if not trade_df.is_empty():
        trade_map = dict(zip(trade_df["partner"].to_list(), trade_df["total_usd"].to_list()))

    # Count vessels per flag (to estimate per-vessel share)
    flag_counts = vessel_df.group_by("flag").agg(pl.len().alias("vessel_count"))
    flag_count_map: dict[str, int] = dict(
        zip(
            flag_counts["flag"].to_list(),
            flag_counts["vessel_count"].to_list(),
        )
    )

    rows = []
    for row in vessel_df.iter_rows(named=True):
        mmsi = row["mmsi"]
        flag = row["flag"]
        ship_type = row["ship_type"]
        is_tanker = ship_type in TANKER_TYPES

        if not is_tanker or flag not in SANCTIONED_EXPORTERS:
            rows.append(
                {
                    "mmsi": mmsi,
                    "route_cargo_mismatch": 0.0,
                    "declared_vs_estimated_cargo_value": 0.0,
                }
            )
            continue

        trade_val = trade_map.get(flag, 0.0)
        mismatch = 0.0 if trade_val > 0 else 1.0

        count = flag_count_map.get(flag, 1)
        per_vessel = trade_val / count if trade_val > 0 else 0.0

        rows.append(
            {
                "mmsi": mmsi,
                "route_cargo_mismatch": mismatch,
                "declared_vs_estimated_cargo_value": per_vessel,
            }
        )

    return (
        pl.DataFrame(
            rows,
            schema={
                "mmsi": pl.Utf8,
                "route_cargo_mismatch": pl.Float32,
                "declared_vs_estimated_cargo_value": pl.Float32,
            },
        )
        if rows
        else pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "route_cargo_mismatch": pl.Float32,
                "declared_vs_estimated_cargo_value": pl.Float32,
            }
        )
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute trade flow mismatch features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--reporter", default=SG_REPORTER)
    parser.add_argument("--hs-code", default=CRUDE_OIL_HS)
    parser.add_argument("--comtrade-api-key", default=COMTRADE_API_KEY)
    args = parser.parse_args()

    if args.comtrade_api_key:
        print("Downloading Comtrade crude oil data …")
        n = download_comtrade(
            args.reporter,
            list(SANCTIONED_EXPORTERS),
            args.hs_code,
            2024,
            args.db,
            args.comtrade_api_key,
        )
        print(f"  {n} rows inserted")

    result = compute_trade_features(args.db, args.reporter, args.hs_code)
    print(f"Trade mismatch features: {len(result)} vessels")
    print(result.head())
