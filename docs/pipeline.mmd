flowchart TB
  %% =========================
  %% Environment / Secrets
  %% =========================
  ENV[.env / Environment Variables]:::sec
  ENV -->|NEWSAPI_KEY| NEWSAPI
  ENV -->|FRED_KEY (optional)| FRED
  ENV -->|PYTENSOR_FLAGS cxx= (optional)| PYMC

  %% =========================
  %% Storage
  %% =========================
  DB[(SQLite: data/finance.db)]:::db
  PARQ[[Parquet: data/model_frame.parquet]]:::file
  NC[[NetCDF: data/idata.nc]]:::file
  JSON[[JSON: data/posterior_summaries.json]]:::file

  %% =========================
  %% NEWS PIPELINE
  %% =========================
  subgraph NEWS_PIPELINE["News pipeline (daily/periodic)"]
    RUN_NEWS["src/run_news.py\n(orchestrator)"]:::script
    NEWSAPI["NewsAPI\n/v2/everything"]:::api
    NEWS_INGEST["src/ingest/news_newsapi.py\nfetch_news_for_tickers()"]:::mod
    FINBERT["FinBERT (HF)\nProsusAI/finbert"]:::model
    SCORE["src/nlp/finbert_score.py\nFinBertScorer.score_texts()"]:::mod
    SCORE_WRAP["src/nlp/score_news.py\nscore_and_store_news()"]:::mod
    NEWS_FEATS["src/transform/news_features.py\nbuild_news_daily()"]:::mod

    RUN_NEWS --> NEWS_INGEST --> NEWSAPI
    RUN_NEWS --> SCORE_WRAP --> SCORE --> FINBERT
    RUN_NEWS --> NEWS_FEATS
  end

  %% News tables
  NEWS_INGEST -->|append| DB
  SCORE_WRAP -->|append| DB
  NEWS_FEATS -->|replace| DB

  %% =========================
  %% MARKET DATA PIPELINE
  %% =========================
  subgraph MARKET_PIPELINE["Market data pipeline (daily)"]
    RUN_DAILY["src/run_daily.py\n(orchestrator)"]:::script
    YF["yfinance\nprices (and optional fundamentals)"]:::api
    PRICES_INGEST["src/extract.py\ndownload_prices()"]:::mod
    MACRO_INGEST["(optional) macro ingest\n(FRED series)"]:::mod
    FUND_INGEST["(optional) fundamentals_yahoo\nTicker.info snapshot"]:::mod
    SIGNALS["src/transform/signals.py\nbuild_signals(prices, macro, fundamentals, news_daily)"]:::mod
  end

  RUN_DAILY --> PRICES_INGEST --> YF
  RUN_DAILY --> MACRO_INGEST --> FRED
  RUN_DAILY --> FUND_INGEST --> YF

  %% Read news_daily for merge
  RUN_DAILY -->|read news_daily| DB
  RUN_DAILY --> SIGNALS

  %% Market tables
  PRICES_INGEST -->|replace/append| DB
  MACRO_INGEST -->|replace/append| DB
  FUND_INGEST -->|append| DB

  %% Signals output
  SIGNALS -->|replace| DB
  RUN_DAILY -->|write| PARQ

  %% =========================
  %% MODEL TRAINING
  %% =========================
  subgraph TRAINING["Training / inference (batch)"]
    TRAIN["src/train_pymc.py\nfit_model() + diagnostics() + export_posterior()"]:::script
    PYMC["PyMC + PyTensor\n(ADVI now; NUTS later)"]:::model
    ARVIZ["ArviZ\n(summary/ppc)"]:::lib
  end

  PARQ --> TRAIN --> PYMC
  TRAIN --> ARVIZ
  TRAIN -->|write| NC
  TRAIN -->|write| JSON

  %% =========================
  %% DB Tables (conceptual)
  %% =========================
  DB --- T1["prices_daily\n(ticker, dt, close, volume, ...)"]:::table
  DB --- T2["macro_daily\n(dt, curve_slope, ...)"]:::table
  DB --- T3["fundamentals_snapshot\n(ticker, asof, market_cap, trailing_pe, margins, ...)"]:::table
  DB --- T4["news_raw\n(ticker, published_at, dt, title, url, content_hash, ...)"]:::table
  DB --- T5["news_scored\n(content_hash, model_name, sent_pos/neg/neu, sent_score)"]:::table
  DB --- T6["news_daily\n(ticker, dt, news_count_1d, news_sent_1d, news_sent_7d)"]:::table
  DB --- T7["signals_daily/model_frame\n(ticker, dt, ret_1d, 10 features)"]:::table

  %% =========================
  %% Styling
  %% =========================
  classDef script fill:#eef,stroke:#335,stroke-width:1px;
  classDef mod fill:#f7f7ff,stroke:#667,stroke-width:1px;
  classDef api fill:#fff3e6,stroke:#a65,stroke-width:1px;
  classDef model fill:#e8fff1,stroke:#2a7,stroke-width:1px;
  classDef db fill:#f0fff7,stroke:#2a7,stroke-width:1px;
  classDef table fill:#f0fff7,stroke:#2a7,stroke-dasharray: 3 3;
  classDef file fill:#fff,stroke:#999,stroke-width:1px;
  classDef lib fill:#fff,stroke:#777,stroke-width:1px;
  classDef sec fill:#ffecec,stroke:#a33,stroke-width:1px;
