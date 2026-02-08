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

    # Credit spreads (OAS): High Yield and Investment Grade
    # HY OAS: BAMLH0A0HYM2, IG OAS: BAMLC0A0CM
    hy_oas = fetch_fred_series("BAMLH0A0HYM2", start, end, api_key)  # hy_oas
    ig_oas = fetch_fred_series("BAMLC0A0CM", start, end, api_key)    # ig_oas

    # Optional: effective fed funds rate (proxy RF); series often "DFF"
    try:
        fedfunds = fetch_fred_series("DFF", start, end, api_key)
    except Exception:
        fedfunds = pd.DataFrame({"dt": [], "dff": []})

    macro = (
        dgs10.merge(dgs2, on="dt", how="outer")
             .merge(hy_oas, on="dt", how="outer")
             .merge(ig_oas, on="dt", how="outer")
             .merge(fedfunds, on="dt", how="outer")
    )
    macro = macro.sort_values("dt")

    # curve slope: 10Y - 2Y
    macro["curve_slope"] = macro["dgs10"] - macro["dgs2"]

    # credit spread: HY OAS - IG OAS
    # column names are lower-cased series ids
    macro["credit_spread"] = macro["bamlh0a0hym2"] - macro["bamlc0a0cm"]

    # rename dff -> fedfunds if present
    if "dff" in macro.columns:
        macro = macro.rename(columns={"dff": "fedfunds"})
    else:
        macro["fedfunds"] = None

    # forward fill for missing days (FRED series can have gaps)
    ffill_cols = ["dgs10", "dgs2", "curve_slope", "fedfunds", "bamlh0a0hym2", "bamlc0a0cm", "credit_spread"]
    for c in ffill_cols:
        if c not in macro.columns:
            macro[c] = None
    macro[ffill_cols] = macro[ffill_cols].ffill()

    return macro[[
        "dt",
        "dgs10",
        "dgs2",
        "curve_slope",
        "fedfunds",
        "bamlh0a0hym2",   # HY OAS (raw)
        "bamlc0a0cm",     # IG OAS (raw)
        "credit_spread",
    ]]
