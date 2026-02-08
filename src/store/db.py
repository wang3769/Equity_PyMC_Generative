# src/store/db.py
from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

DEFAULT_SQLITE_PATH = os.path.join(DATA_DIR, "finance.db")
DEFAULT_DB_URL = f"sqlite:///{DEFAULT_SQLITE_PATH}"


def get_engine(db_url: str | None = None) -> Engine:
    """
    Use SQLite locally by default. Later on Azure, set DB_URL to e.g.:
      - Azure Database for PostgreSQL: postgresql+psycopg2://...
      - Azure SQL: mssql+pyodbc://...
    """
    return create_engine(db_url or DEFAULT_DB_URL, future=True)


def init_tables(engine: Engine) -> None:
    """
    Minimal schema. Keep it portable across SQLite/Postgres by using simple types.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS prices_daily (
      ticker TEXT NOT NULL,
      dt     TEXT NOT NULL,     -- ISO date YYYY-MM-DD
      close  REAL,
      volume REAL,
      PRIMARY KEY (ticker, dt)
    );

    CREATE TABLE IF NOT EXISTS macro_daily (
      dt          TEXT NOT NULL PRIMARY KEY,
      dgs10       REAL,
      dgs2        REAL,
      curve_slope REAL,
      fedfunds    REAL
    );

    CREATE TABLE IF NOT EXISTS signals_daily (
      ticker TEXT NOT NULL,
      dt     TEXT NOT NULL,

      beta_mkt        REAL,
      log_mktcap      REAL,  -- proxy for now
      value_z         REAL,  -- proxy (0)
      mom_12_1        REAL,
      vol_20d         REAL,
      illiq_amihud    REAL,
      quality_z       REAL,  -- proxy (0)
      macro_sens      REAL,  -- proxy: curve_slope zscore
      credit_sens     REAL,  -- proxy: VIX not used here (set 0)
      news_sent_7d    REAL,  -- empty (0)

      ret_1d_fwd      REAL,

      PRIMARY KEY (ticker, dt)
    );
  CREATE TABLE IF NOT EXISTS fundamentals_snapshot (
      ticker TEXT NOT NULL,
      asof   TEXT NOT NULL,   -- ISO date YYYY-MM-DD of snapshot pull
      market_cap      REAL,
      trailing_pe     REAL,
      price_to_book   REAL,
      profit_margins  REAL,
      operating_margins REAL,
      return_on_equity REAL,
      PRIMARY KEY (ticker, asof)
    );
    CREATE TABLE IF NOT EXISTS news_raw (
      ticker        TEXT,
      published_at  TEXT,    -- ISO datetime from NewsAPI
      dt            TEXT,    -- YYYY-MM-DD derived
      source        TEXT,
      title         TEXT,
      description   TEXT,
      url           TEXT,
      content_hash  TEXT     -- sha1(url)
    );

    CREATE TABLE IF NOT EXISTS news_scored (
      content_hash  TEXT,
      model_name    TEXT,
      sent_pos      REAL,
      sent_neg      REAL,
      sent_neu      REAL,
      sent_score    REAL     -- p_pos - p_neg
    );

    CREATE TABLE IF NOT EXISTS news_daily (
      ticker         TEXT,
      dt             TEXT,
      news_count_1d  REAL,
      news_sent_1d   REAL,
      news_sent_7d   REAL
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

# clearly this write on daily basis. This is a clear room for improvement I believe. Keep it for now, 02/05/2026