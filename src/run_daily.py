# src/run_daily.py
from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import text

from src.store.db import get_engine, init_tables
from src.ingest.prices_yf import download_prices
from src.ingest.macro_fred import build_macro_frame
from src.transform.signals import build_signals

from src.ingest.fundamentals_yahoo import fetch_many as fetch_fundamentals_many
import datetime as dt



TICKERS = ["AMZN", "META", "GOOG", "AAPL", "MSFT", "NVDA", "ORCL",
           "ADBE", "CRM", "NOW", "INTU", "PANW", "WDAY",
           "SNOW", "DDOG", "CRWD", "NET", "MDB", "PLTR", "SHOP", "UBER",
           "ANBN", "PINS", "PYPL", "TEAM", "HUBS", "OKTA", "ZS", "DT", "MNDY","ASAN",
           "SMAR", "FRSH", "INFA", "DUOL", "U", "FIG", "KLAR",
           "UNH","TSM","BABA", "LRCX"]  # add others if yfinance resolves them
MARKET = "SPY"

def _to_prices_table(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"Close": "close", "Volume": "volume"}) if "Close" in df.columns else df

def main(start="2018-01-01", end="2026-01-01", db_url: str | None = None):
    engine = get_engine(db_url)
    init_tables(engine)

    # 1) macro (FRED)
    macro = build_macro_frame(start, end)

    # 2) prices for tickers + market
    # src/run_daily.py (replace the loop that builds frames)

    frames = []
    failed = []

    for t in [MARKET] + TICKERS:
        try:
            df = download_prices(t, start, end)  # should return ticker, dt, close, volume
            # HARD normalize (defensive)
            df = df.rename(columns={c: c.lower() for c in df.columns})
            needed = {"ticker", "dt", "close", "volume"}
            if not needed.issubset(df.columns):
                raise ValueError(f"{t}: missing columns {needed - set(df.columns)}; got {df.columns.tolist()}")

            df = df[["ticker", "dt", "close", "volume"]].copy()
            frames.append(df)
            print(f"✓ prices {t}: {len(df)} rows")
        except Exception as e:
            failed.append((t, str(e)))
            print(f"✗ prices {t}: {e}")

    if not frames:
        raise RuntimeError("No price data downloaded for any ticker. Check internet / yfinance / tickers.")

    prices = pd.concat(frames, ignore_index=True)

    # Now assert (optional)
    assert {"ticker", "dt", "close", "volume"}.issubset(prices.columns), prices.columns

    if failed:
        print("\nDownload failures:")
        for t, msg in failed:
            print(f"  - {t}: {msg}")

    # 3) normalize columns for storage
    prices_tbl = prices[["ticker", "dt", "close", "volume"]].copy()
    macro_tbl = macro.copy()

    # 2.5) fundamentals snapshot (Yahoo), the fundamentals is only today
    asof = dt.date.today().strftime("%Y-%m-%d")
    fund = fetch_fundamentals_many(TICKERS, asof=asof)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fundamentals_snapshot WHERE asof = :asof"), {"asof": asof})
    fund.to_sql("fundamentals_snapshot", con=engine, if_exists="append", index=False)
    print(f"✓ saved fundamentals_snapshot rows: {len(fund)} (asof={asof})")


    # 4) write raw tables (replace is fine for prototype)
    prices_tbl.to_sql("prices_daily", con=engine, if_exists="replace", index=False)
    macro_tbl.to_sql("macro_daily", con=engine, if_exists="replace", index=False)

    try:
        news_daily = pd.read_sql("SELECT ticker, dt, news_sent_7d FROM news_daily", con=engine)
        print(f"✓ loaded news_daily: {len(news_daily)} rows")
    except Exception:
        news_daily = None
        print("ℹ news_daily not found; using news_sent_7d = 0")

    # 5) build signals (9 inputs; news empty)
    signals = build_signals(
    prices_tbl,
    macro_tbl,
    fundamentals=fund,
    news_daily=news_daily,
    market_ticker=MARKET,
    )
    print("news_sent_7d stats:", signals["news_sent_7d"].describe())

    # 6) store signals + model frame
    signals.to_sql("signals_daily", con=engine, if_exists="replace", index=False)

    os.makedirs("data", exist_ok=True)
    signals.to_parquet("data/model_frame.parquet", index=False)
    print("✓ saved data/model_frame.parquet", signals.shape)

if __name__ == "__main__":
    today_str = dt.date.today().strftime("%Y-%m-%d")
    main(end = today_str)
