# src/ingest/prices_yf.py
from __future__ import annotations
import pandas as pd
import yfinance as yf

def download_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
    )

    if df is None or len(df) == 0:
        raise ValueError(f"No data returned for ticker={ticker}")

    # --- IMPORTANT: flatten MultiIndex columns like ('Close','SPY') -> 'Close'
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()

    # Canonical lowercase schema
    df = df.rename(columns={
        "Date": "dt",
        "Close": "close",
        "Volume": "volume",
    })

    if "dt" not in df.columns or "close" not in df.columns or "volume" not in df.columns:
        raise ValueError(f"{ticker}: unexpected columns after download: {df.columns.tolist()}")

    df["dt"] = pd.to_datetime(df["dt"]).dt.strftime("%Y-%m-%d")
    df["ticker"] = ticker

    return df[["ticker", "dt", "close", "volume"]]

# the new version is to handle yfinance multiindex columns and be a tuple.
