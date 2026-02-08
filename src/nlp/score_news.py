from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from src.store.db import get_engine
from src.nlp.finbert_score import FinBertScorer, MODEL_NAME


def _load_scored_hashes(engine, model_name: str) -> set[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT content_hash FROM news_scored WHERE model_name = :m"),
            {"m": model_name},
        ).fetchall()
    return {r[0] for r in rows}


def score_and_store_news(model_name: str = MODEL_NAME, db_url: str | None = None, batch_size: int = 32):
    engine = get_engine(db_url)
    scorer = FinBertScorer(model_name=model_name)

    # Load raw news
    news = pd.read_sql("SELECT * FROM news_raw", con=engine)
    if len(news) == 0:
        print("No news_raw rows to score.")
        return

    # Build text to score (title + description)
    news["text"] = (news["title"].fillna("") + ". " + news["description"].fillna("")).str.strip()
    news = news[news["text"].str.len() > 0].copy()

    scored_hashes = _load_scored_hashes(engine, model_name)
    todo = news[~news["content_hash"].isin(scored_hashes)][["content_hash", "text"]].drop_duplicates("content_hash")

    if len(todo) == 0:
        print("✓ No new articles to score.")
        return

    rows = []
    texts = todo["text"].tolist()
    hashes = todo["content_hash"].tolist()

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        batch_hashes = hashes[i : i + batch_size]

        p_pos, p_neg, p_neu, score = scorer.score_texts(batch_texts)

        for h, pos, neg, neu, s in zip(batch_hashes, p_pos, p_neg, p_neu, score):
            rows.append(
                {
                    "content_hash": h,
                    "model_name": model_name,
                    "sent_pos": float(pos),
                    "sent_neg": float(neg),
                    "sent_neu": float(neu),
                    "sent_score": float(s),
                }
            )

    scored_df = pd.DataFrame(rows)
    scored_df.to_sql("news_scored", con=engine, if_exists="append", index=False)
    print(f"✓ Scored and stored {len(scored_df)} articles using {model_name}")
