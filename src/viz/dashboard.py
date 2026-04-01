"""Streamlit dashboard for ranked candidate watchlist."""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import pydeck as pdk
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DEFAULT_WATCHLIST_PATH = os.getenv("WATCHLIST_OUTPUT_PATH", "data/processed/candidate_watchlist.parquet")
DEFAULT_VALIDATION_PATH = os.getenv("VALIDATION_METRICS_PATH", "data/processed/validation_metrics.json")


@st.cache_data(show_spinner=False)
def load_watchlist(path: str) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_validation_metrics(path: str) -> dict | None:
    candidate = Path(path)
    if not candidate.exists():
        return None
    try:
        return json.loads(candidate.read_text())
    except Exception:
        return None


def _color_for_confidence(value: float) -> list[int]:
    if value >= 0.7:
        return [220, 38, 38, 180]
    if value >= 0.4:
        return [245, 158, 11, 180]
    return [34, 197, 94, 180]


def _map_frame(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("last_lat").is_not_null() & pl.col("last_lon").is_not_null()).with_columns(
        pl.col("confidence").map_elements(_color_for_confidence, return_dtype=pl.List(pl.Int64)).alias("color")
    )


def main() -> None:
    st.set_page_config(page_title="MPOL Watchlist", layout="wide")
    st.title("Shadow Fleet Candidate Watchlist")

    watchlist = load_watchlist(DEFAULT_WATCHLIST_PATH)
    if watchlist.is_empty():
        st.warning("candidate_watchlist.parquet not found or empty. Run src/score/watchlist.py first.")
        return

    metrics = load_validation_metrics(DEFAULT_VALIDATION_PATH)

    with st.sidebar:
        st.header("Filters")
        min_confidence = st.slider("Minimum confidence", 0.0, 1.0, 0.4, 0.05)
        vessel_types = sorted(watchlist["vessel_type"].unique().to_list())
        selected_types = st.multiselect("Vessel types", vessel_types, default=vessel_types)
        top_n = st.slider("Top N rows", 10, min(500, watchlist.height), min(50, watchlist.height))

    filtered = watchlist.filter(
        (pl.col("confidence") >= min_confidence) & (pl.col("vessel_type").is_in(selected_types))
    ).head(top_n)

    col1, col2, col3 = st.columns(3)
    col1.metric("Candidates", filtered.height)
    col2.metric("High confidence", filtered.filter(pl.col("confidence") >= 0.75).height)
    col3.metric("Avg confidence", f"{filtered['confidence'].mean():.2f}")

    if metrics:
        st.subheader("Validation")
        v1, v2, v3 = st.columns(3)
        v1.metric("Precision@50", f"{metrics.get('precision_at_50', 0.0):.2f}")
        v2.metric("Recall@200", f"{metrics.get('recall_at_200', 0.0):.2f}")
        auroc = metrics.get("auroc")
        v3.metric("AUROC", "N/A" if auroc is None else f"{auroc:.2f}")

    left, right = st.columns([3, 2])

    with left:
        st.subheader("Map")
        map_df = _map_frame(filtered)
        if map_df.is_empty():
            st.info("No candidate positions available for the current filters.")
        else:
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df.to_dicts(),
                get_position="[last_lon, last_lat]",
                get_fill_color="color",
                get_radius=20000,
                pickable=True,
            )
            view_state = pdk.ViewState(
                latitude=float(map_df["last_lat"].mean()),
                longitude=float(map_df["last_lon"].mean()),
                zoom=4,
            )
            st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "{vessel_name}\nConfidence: {confidence}"}))

    with right:
        st.subheader("Ranked table")
        display_df = filtered.select([
            "mmsi",
            "vessel_name",
            "vessel_type",
            "flag",
            "confidence",
            "top_signals",
        ]).to_pandas()
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.subheader("Top signals")
    top_row = filtered.head(1)
    if top_row.height:
        record = top_row.row(0, named=True)
        st.markdown(f"**{record['vessel_name']}** ({record['mmsi']})")
        try:
            st.json(json.loads(record["top_signals"]))
        except Exception:
            st.code(record["top_signals"])


if __name__ == "__main__":
    main()

