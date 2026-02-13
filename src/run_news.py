from __future__ import annotations

import datetime as dt
from src.store.db import get_engine, init_tables
from src.ingest.news_newsapi import fetch_news_for_tickers
from src.nlp.score_news import score_and_store_news
from src.transform.news_features import build_news_daily

from dotenv import load_dotenv
load_dotenv()

TICKERS = ["AMZN", "META", "GOOG", "AAPL", "MSFT", "NVDA", "ORCL",
           "ADBE", "CRM", "NOW", "INTU", "PANW", "WDAY",
           "SNOW", "DDOG", "CRWD", "NET", "MDB", "PLTR", "SHOP", "UBER",
           "ANBN", "PINS", "PYPL", "TEAM", "HUBS", "OKTA", "ZS", "DT", "MNDY","ASAN",
           "SMAR", "FRSH", "INFA", "DUOL", "U", "FIG", "KLAR",
           "UNH","TSM","BABA", "LRCX"]  # your set

TICKER_TO_NAME = {
    "GOOG": "Alphabet",
    "META": "Meta Platforms",
    "CRM": "Salesforce",
    "TSM": "TSMC",
}

def main(date_from: str, date_to: str, db_url: str | None = None):
    engine = get_engine(db_url)
    init_tables(engine)

    news = fetch_news_for_tickers(TICKERS, TICKER_TO_NAME, date_from=date_from, date_to=date_to, max_pages=5)
    if len(news):
        # basic dedupe within the batch
        news = news.drop_duplicates(subset=["ticker", "content_hash", "published_at"])
        news.to_sql("news_raw", con=engine, if_exists="append", index=False)
        print(f"✓ inserted news_raw rows: {len(news)}")
    else:
        print("No news fetched.")

    score_and_store_news(db_url=db_url)
    build_news_daily(db_url=db_url)

if __name__ == "__main__":
    # last 7 days by default
    today = dt.date.today()
    date_to = today.strftime("%Y-%m-%d")
    date_from = (today - dt.timedelta(days=7)).strftime("%Y-%m-%d")
    main(date_from, date_to)
