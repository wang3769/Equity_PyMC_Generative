# src/transform/signals.py
from __future__ import annotations

import numpy as np
import pandas as pd

"""
FEATURE DEFINITIONS (daily, cross-sectionally standardized)

Each feature is designed to capture a well-established economic or market effect.
Unless otherwise noted, features are computed using only information available
at or before date t and are intended to predict the next-day return (t+1).

All features are z-scored cross-sectionally by date before modeling to improve
numerical stability and comparability across signals.
"""

FEATURE_COLS = [
    # beta_mkt:
    # Rolling market beta (systematic risk exposure).
    # Computed as a rolling covariance of asset returns vs market returns (e.g., SPY),
    # divided by market return variance. Captures sensitivity to broad market moves.
    "beta_mkt",

    # log_mktcap:
    # Size proxy representing firm scale.
    # In the absence of true market capitalization, this is approximated as the
    # logarithm of rolling dollar trading volume. Intended to capture size-related
    # return effects (small vs large firms).
    "log_mktcap",

    # value_z:
    # Value signal (placeholder in current pipeline).
    # Intended to represent valuation metrics such as book-to-market, earnings yield,
    # or free-cash-flow yield. Currently set to zero until fundamental data is added.
    "value_z",

    # mom_12_1:
    # Momentum signal (12–1 momentum).
    # Defined as the 12-month return minus the most recent 1-month return.
    # Captures medium-term trend persistence while avoiding short-term reversal effects.
    "mom_12_1",

    # vol_20d:
    # Short-term realized volatility.
    # Computed as the rolling 20-day standard deviation of daily returns.
    # Typically negatively associated with future returns cross-sectionally.
    "vol_20d",

    # illiq_amihud:
    # Liquidity / illiquidity proxy based on the Amihud measure.
    # Defined as the rolling average of |return| divided by dollar trading volume.
    # Higher values indicate higher trading costs and lower liquidity.
    "illiq_amihud",

    # quality_z:
    # Profitability / quality signal (placeholder in current pipeline).
    # Intended to capture firm quality metrics such as ROE, ROIC, or operating margins.
    # Currently set to zero until fundamental data is added.
    "quality_z",

    # macro_sens:
    # Macro environment proxy.
    # Currently implemented as a standardized yield-curve slope (e.g., 10Y–2Y Treasury).
    # Serves as a common macro regime indicator shared across assets on a given date.
    "macro_sens",

    # credit_sens:
    # Credit / risk appetite proxy (placeholder in current pipeline).
    # Intended to reflect sensitivity to credit conditions (e.g., HY–IG spreads).
    # Currently set to zero; future versions may substitute VIX or credit spread data.
    "credit_sens",

    # news_sent_7d:
    # News and event sentiment signal (placeholder in current pipeline).
    # Intended to represent aggregated sentiment from news or events over a recent window
    # (e.g., past 7 days). Currently set to zero; populated when news pipeline is enabled.
    "news_sent_7d",
]


def _safe_log(x: pd.Series, eps: float = 1e-12) -> pd.Series:
    return np.log(np.maximum(x, eps))

def rolling_beta(asset_ret: pd.Series, mkt_ret: pd.Series, window: int = 60) -> pd.Series:
    cov = asset_ret.rolling(window).cov(mkt_ret)
    var = mkt_ret.rolling(window).var()
    return cov / (var + 1e-12)

def build_signals(
    prices: pd.DataFrame,
    macro: pd.DataFrame,
    fundamentals: pd.DataFrame | None = None,
    news_daily: pd.DataFrame | None = None,
    market_ticker: str = "SPY",
) -> pd.DataFrame:

    """
    prices columns: ticker, dt, close, volume
    macro columns: dt, curve_slope, ...
    """
    p = prices.copy()
    if fundamentals is not None and len(fundamentals) > 0:
        p = attach_latest_fundamentals(p, fundamentals)
    # Attach news sentiment (daily per ticker). If missing, default to 0.
    if news_daily is not None and len(news_daily) > 0:
        nd = news_daily[["ticker", "dt", "news_sent_7d"]].copy()
        nd["dt"] = nd["dt"].astype(str)
        p = p.merge(nd, on=["ticker", "dt"], how="left")
        p["news_sent_7d"] = p["news_sent_7d"].fillna(0.0)
    else:
        p["news_sent_7d"] = 0.0
    p["dt"] = p["dt"].astype(str)
    p = p.sort_values(["ticker", "dt"])

    # Daily returns
    p["ret_1d"] = p.groupby("ticker")["close"].pct_change()

    # Momentum 12-1
    p["ret_21d"] = p.groupby("ticker")["close"].pct_change(21)
    p["ret_252d"] = p.groupby("ticker")["close"].pct_change(252)
    p["mom_12_1"] = p["ret_252d"] - p["ret_21d"]

    # Volatility
    p["vol_20d"] = p.groupby("ticker")["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)

    # Liquidity proxies
    p["dollar_vol"] = p["close"] * p["volume"]
    p["dollar_vol_20d"] = p.groupby("ticker")["dollar_vol"].rolling(20).mean().reset_index(level=0, drop=True)
    p["illiq_amihud"] = (p["ret_1d"].abs() / (p["dollar_vol"] + 1e-12))
    p["illiq_amihud"] = p.groupby("ticker")["illiq_amihud"].rolling(20).mean().reset_index(level=0, drop=True)

    # Size proxy (placeholder): log rolling dollar volume
    p["log_mktcap"] = _safe_log(p["dollar_vol_20d"])

    # Market returns for beta
    mkt = p[p["ticker"] == market_ticker][["dt", "ret_1d"]].rename(columns={"ret_1d": "mkt_ret"})
    p = p.merge(mkt, on="dt", how="left")

    # Beta
    p["beta_mkt"] = p.groupby("ticker", group_keys=False).apply(
        lambda g: rolling_beta(g["ret_1d"], g["mkt_ret"], window=60)
    )

    # Macro proxy: curve slope zscore (same for all tickers by date)
# Macro proxies: curve slope + credit spread (same for all tickers by date)
    m = macro[["dt", "curve_slope", "credit_spread"]].copy()
    m["dt"] = m["dt"].astype(str)

    m["curve_slope_z"] = (m["curve_slope"] - m["curve_slope"].mean()) / (m["curve_slope"].std() + 1e-12)
    m["credit_spread_z"] = (m["credit_spread"] - m["credit_spread"].mean()) / (m["credit_spread"].std() + 1e-12)

    p = p.merge(m[["dt", "curve_slope_z", "credit_spread_z"]], on="dt", how="left")
    p["macro_sens"] = p["curve_slope_z"]
    p["credit_sens"] = p["credit_spread_z"].fillna(0.0)

    # Proxies / empty for now
    #p["value_z"] = 0.0
    #p["quality_z"] = 0.0
    # Expect macro to contain credit_spread (daily)
    #p["news_sent_7d"] = 0.0
        # --- log_mktcap: prefer true market cap; fallback to liquidity proxy ---
    if "market_cap" in p.columns:
        p["log_mktcap"] = np.where(
            p["market_cap"].notna() & (p["market_cap"] > 0),
            _safe_log(p["market_cap"]),
            _safe_log(p["dollar_vol_20d"])
        )
    else:
        p["log_mktcap"] = _safe_log(p["dollar_vol_20d"])

    # --- value_z: prefer -log(trailingPE); fallback -log(price_to_book); else 0 ---
    value_raw = None
    if "trailing_pe" in p.columns:
        value_raw = np.where(
            p["trailing_pe"].notna() & (p["trailing_pe"] > 0),
            -_safe_log(p["trailing_pe"]),
            np.nan
        )
    if value_raw is None or np.all(pd.isna(value_raw)):
        if "price_to_book" in p.columns:
            value_raw = np.where(
                p["price_to_book"].notna() & (p["price_to_book"] > 0),
                -_safe_log(p["price_to_book"]),
                np.nan
            )
    p["value_raw"] = value_raw
    p["value_z"] = 0.0  # will be overwritten after merge + z-score step if available

    # --- quality_z: use profit_margins; fallback operating_margins; fallback ROE ---
    quality_raw = None
    for col in ["profit_margins", "operating_margins", "return_on_equity"]:
        if col in p.columns:
            q = p[col].astype(float)
            if quality_raw is None:
                quality_raw = q
            else:
                # if we already have something, fill missing from fallback
                quality_raw = quality_raw.where(quality_raw.notna(), q)
    p["quality_raw"] = quality_raw
    p["quality_z"] = 0.0  # overwrite later if available


    # Target: next-day return
    p["ret_1d_fwd"] = p.groupby("ticker")["ret_1d"].shift(-1)

    # Keep only needed output
    out = p[["ticker", "dt", "ret_1d_fwd"] + FEATURE_COLS + ["value_raw", "quality_raw"]].rename(columns={"ret_1d_fwd": "ret_1d"})

    out = out.dropna(subset=["ret_1d", "beta_mkt", "mom_12_1", "vol_20d", "illiq_amihud", "macro_sens", "log_mktcap"])

    # Cross-sectional z-score each day for stability
    for c in FEATURE_COLS:
        # value_z and quality_z will be handled from raw fields below
        if c in ["value_z", "quality_z"]:
            continue
        mu = out.groupby("dt")[c].transform("mean")
        sd = out.groupby("dt")[c].transform("std")
        out[c] = (out[c] - mu) / (sd + 1e-12)

    # value_z from value_raw (if present and has variance)
    if "value_raw" in out.columns:
        mu = out.groupby("dt")["value_raw"].transform("mean")
        sd = out.groupby("dt")["value_raw"].transform("std")
        out["value_z"] = (out["value_raw"] - mu) / (sd + 1e-12)
        out["value_z"] = out["value_z"].fillna(0.0)

    # quality_z from quality_raw
    if "quality_raw" in out.columns:
        mu = out.groupby("dt")["quality_raw"].transform("mean")
        sd = out.groupby("dt")["quality_raw"].transform("std")
        out["quality_z"] = (out["quality_raw"] - mu) / (sd + 1e-12)
        out["quality_z"] = out["quality_z"].fillna(0.0)

    # drop raw helpers
    out = out.drop(columns=[c for c in ["value_raw", "quality_raw"] if c in out.columns])

    print("Signals rows per ticker:")
    print(out.groupby("ticker").size().sort_values(ascending=False).head(20))
    return out

# seperate function to attach fundamentals later when we have them, 
def attach_latest_fundamentals(panel: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame:
    """
    fundamentals: ticker, asof, market_cap, trailing_pe, price_to_book, profit_margins, ...
    Attaches the latest snapshot PER ticker (max asof).
    """
    f = fundamentals.copy()
    f["asof"] = f["asof"].astype(str)
    f = f.sort_values(["ticker", "asof"])
    f_latest = f.groupby("ticker", as_index=False).tail(1)  # last snapshot per ticker
    return panel.merge(f_latest.drop(columns=["asof"]), on="ticker", how="left")
