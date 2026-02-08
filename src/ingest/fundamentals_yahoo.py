# src/ingest/fundamentals_yahoo.py
from __future__ import annotations

import time
import datetime as dt
import pandas as pd
import yfinance as yf


FIELDS = {
    "marketCap": "market_cap",
    "trailingPE": "trailing_pe",
    "priceToBook": "price_to_book",
    "profitMargins": "profit_margins",
    "operatingMargins": "operating_margins",
    "returnOnEquity": "return_on_equity",
}


def fetch_fundamentals_snapshot(ticker: str, asof: str | None = None, pause_s: float = 0.25) -> dict:
    """
    Pull a single snapshot of fundamentals from Yahoo via yfinance Ticker.info.
    This is NOT point-in-time safe historically; treat as a prototyping proxy.
    """
    if asof is None:
        asof = dt.date.today().strftime("%Y-%m-%d")

    tk = yf.Ticker(ticker)
    info = tk.info  # network call + parse

    row = {"ticker": ticker, "asof": asof}
    for k, outk in FIELDS.items():
        v = info.get(k, None)
        # coerce to float where possible
        try:
            row[outk] = None if v is None else float(v)
        except Exception:
            row[outk] = None

    # light rate-limit to avoid hammering Yahoo
    time.sleep(pause_s)
    return row


def fetch_many(tickers: list[str], asof: str | None = None) -> pd.DataFrame:
    rows = []
    for t in tickers:
        try:
            rows.append(fetch_fundamentals_snapshot(t, asof=asof))
            print(f"✓ fundamentals {t}")
        except Exception as e:
            print(f"✗ fundamentals {t}: {e}")
    return pd.DataFrame(rows)
