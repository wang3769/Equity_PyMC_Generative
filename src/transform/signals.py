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

def build_signals(prices: pd.DataFrame, macro: pd.DataFrame, market_ticker: str = "SPY") -> pd.DataFrame:
    """
    prices columns: ticker, dt, close, volume
    macro columns: dt, curve_slope, ...
    """
    p = prices.copy()
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
    m = macro[["dt", "curve_slope"]].copy()
    m["curve_slope_z"] = (m["curve_slope"] - m["curve_slope"].mean()) / (m["curve_slope"].std() + 1e-12)
    p = p.merge(m[["dt", "curve_slope_z"]], on="dt", how="left")
    p["macro_sens"] = p["curve_slope_z"]

    # Proxies / empty for now
    p["value_z"] = 0.0
    p["quality_z"] = 0.0
    p["credit_sens"] = 0.0
    p["news_sent_7d"] = 0.0

    # Target: next-day return
    p["ret_1d_fwd"] = p.groupby("ticker")["ret_1d"].shift(-1)

    # Keep only needed output
    out = p[["ticker", "dt", "ret_1d_fwd"] + FEATURE_COLS].rename(columns={"ret_1d_fwd": "ret_1d"})

    # Drop rows with missing due to rolling windows / merges
    out = out.dropna(subset=["ret_1d", "beta_mkt", "mom_12_1", "vol_20d", "illiq_amihud", "macro_sens", "log_mktcap"])

    # Cross-sectional z-score each day for stability (common in equity modeling)
    # NOTE: Do NOT zscore ret_1d (target). Only features.
    for c in FEATURE_COLS:
        mu = out.groupby("dt")[c].transform("mean")
        sd = out.groupby("dt")[c].transform("std")
        out[c] = (out[c] - mu) / (sd + 1e-12)

    print("Signals rows per ticker:")
    print(out.groupby("ticker").size().sort_values(ascending=False).head(20))
    return out
