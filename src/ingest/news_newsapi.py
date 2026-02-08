from __future__ import annotations

import os
import time
import hashlib
import requests
import pandas as pd


NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def fetch_newsapi(
    query: str,
    date_from: str,
    date_to: str,
    api_key: str,
    language: str = "en",
    page_size: int = 100,
    max_pages: int = 5,
    pause_s: float = 1.0,
) -> pd.DataFrame:
    """
    Fetch articles from NewsAPI 'everything' endpoint for a query in [date_from, date_to].
    date_from/date_to: YYYY-MM-DD
    """
    rows = []
    for page in range(1, max_pages + 1):
        params = {
            "q": query,
            "from": date_from,
            "to": date_to,
            "language": language,
            "sortBy": "publishedAt",
            "pageSize": page_size,
            "page": page,
            "apiKey": api_key,
        }
        r = requests.get(NEWSAPI_ENDPOINT, params=params, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"NewsAPI error {r.status_code}: {r.text}")
        j = r.json()

        articles = j.get("articles", [])
        if not articles:
            break

        for a in articles:
            url = a.get("url") or ""
            published_at = a.get("publishedAt") or ""
            title = a.get("title") or ""
            description = a.get("description") or ""
            source = (a.get("source") or {}).get("name") or ""
            # dt derived from publishedAt (first 10 chars of ISO timestamp)
            dt = published_at[:10] if len(published_at) >= 10 else None

            rows.append(
                {
                    "published_at": published_at,
                    "dt": dt,
                    "source": source,
                    "title": title,
                    "description": description,
                    "url": url,
                    "content_hash": _sha1(url) if url else _sha1(title + "|" + published_at),
                }
            )

        # Respect rate limits / politeness
        time.sleep(pause_s)

        # NewsAPI gives totalResults; stop early if we likely exhausted pages
        if len(articles) < page_size:
            break

    return pd.DataFrame(rows)


def build_query_simple(ticker: str, company_name: str | None = None) -> str:
    """
    Strategy 1: 'TICKER OR CompanyName' (company_name optional).
    """
    if company_name:
        return f'("{ticker}" OR "{company_name}")'
    return f'"{ticker}"'


def fetch_news_for_tickers(
    tickers: list[str],
    ticker_to_name: dict[str, str] | None,
    date_from: str,
    date_to: str,
    max_pages: int = 5,
) -> pd.DataFrame:
    api_key = os.environ.get("NEWSAPI_KEY")
    if not api_key:
        raise EnvironmentError("Missing env var NEWSAPI_KEY")

    all_rows = []
    for t in tickers:
        q = build_query_simple(t, (ticker_to_name or {}).get(t))
        try:
            df = fetch_newsapi(q, date_from, date_to, api_key=api_key, max_pages=max_pages)
            if len(df):
                df["ticker"] = t
            all_rows.append(df)
            print(f"✓ news {t}: {len(df)} articles")
        except Exception as e:
            print(f"✗ news {t}: {e}")

    out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    # drop rows missing dt (bad timestamps)
    if len(out):
        out = out.dropna(subset=["dt"])
    return out
