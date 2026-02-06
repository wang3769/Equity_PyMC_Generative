# src/ingest/macro_fred.py
from __future__ import annotations

import os
import requests
import pandas as pd

from dotenv import load_dotenv

# 1. Load the variables from .env into the system environment
load_dotenv()


FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def fetch_fred_series(series_id: str, start: str, end: str, api_key: str) -> pd.DataFrame:
    """
    Fetch a FRED series into a DataFrame with columns: dt, value
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end,
    }
    r = requests.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["observations"]

    df = pd.DataFrame({
        "dt": [x["date"] for x in data],
        series_id.lower(): [None if x["value"] == "." else float(x["value"]) for x in data],
    })
    return df


def build_macro_frame(start: str, end: str) -> pd.DataFrame:
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise EnvironmentError("Missing env var FRED_API_KEY (required for FRED API calls).")

    dgs10 = fetch_fred_series("DGS10", start, end, api_key)
    dgs2 = fetch_fred_series("DGS2", start, end, api_key)

    # Optional: effective fed funds rate (proxy RF); series often "DFF"
    # We'll try DFF; if it fails, we skip gracefully.
    try:
        fedfunds = fetch_fred_series("DFF", start, end, api_key)  # effective fed funds rate
    except Exception:
        fedfunds = pd.DataFrame({"dt": [], "dff": []})

    macro = dgs10.merge(dgs2, on="dt", how="outer").merge(fedfunds, on="dt", how="outer")
    macro = macro.sort_values("dt")

    # curve slope: 10Y - 2Y
    macro["curve_slope"] = macro["dgs10"] - macro["dgs2"]

    # rename dff -> fedfunds if present
    if "dff" in macro.columns:
        macro = macro.rename(columns={"dff": "fedfunds"})
    else:
        macro["fedfunds"] = None

    # forward fill for missing days (FRED series can have gaps)
    macro[["dgs10", "dgs2", "curve_slope", "fedfunds"]] = macro[["dgs10", "dgs2", "curve_slope", "fedfunds"]].ffill()

    return macro[["dt", "dgs10", "dgs2", "curve_slope", "fedfunds"]]
