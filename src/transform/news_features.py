from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from src.store.db import get_engine


def build_news_daily(db_url: str | None = None):
    engine = get_engine(db_url)

    raw = pd.read_sql("SELECT ticker, dt, content_hash FROM news_raw", con=engine)
    if len(raw) == 0:
        print("No news_raw rows; skipping news_daily.")
        return

    scored = pd.read_sql("SELECT content_hash, model_name, sent_score FROM news_scored", con=engine)
    if len(scored) == 0:
        print("No news_scored rows; skipping news_daily.")
        return

    # Use latest model_name present (or filter explicitly if you want)
    # For now: pick the most frequent model_name
    model_name = scored["model_name"].value_counts().idxmax()
    scored = scored[scored["model_name"] == model_name][["content_hash", "sent_score"]].copy()

    m = raw.merge(scored, on="content_hash", how="inner")

    daily = (
        m.groupby(["ticker", "dt"])
         .agg(
             news_count_1d=("content_hash", "count"),
             news_sent_1d=("sent_score", "mean"),
         )
         .reset_index()
         .sort_values(["ticker", "dt"])
    )

    daily["news_sent_7d"] = (
        daily.groupby("ticker")["news_sent_1d"]
             .rolling(7, min_periods=1).mean()
             .reset_index(level=0, drop=True)
    )

    daily.to_sql("news_daily", con=engine, if_exists="replace", index=False)
    print(f"✓ wrote news_daily using model={model_name}, rows={len(daily)}")
