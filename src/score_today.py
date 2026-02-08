# src/score_today.py
from __future__ import annotations

import json
import pandas as pd
import arviz as az
from scipy.stats import norm

FEATURE_COLS = [
    "beta_mkt",
    "log_mktcap",
    "value_z",
    "mom_12_1",
    "vol_20d",
    "illiq_amihud",
    "quality_z",
    "macro_sens",
    "credit_sens",
    "news_sent_7d",
]

POST_PATH = "data/posterior_summaries.json"
IDATA_PATH = "data/idata.nc"
FRAME_PATH = "data/model_frame.parquet"
OUT_PATH = "data/today_scores.csv"


def _load_post(path: str = POST_PATH) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def _as_float_map_from_json(value, keys: list[str], name: str) -> dict[str, float]:
    """
    Accepts either:
      - dict-like {key: number}
      - list-like [v0, v1, ...] aligned with keys
    Returns:
      - dict {key: float}
    """
    if isinstance(value, dict):
        return {str(k): float(v) for k, v in value.items()}
    if isinstance(value, list):
        if len(value) != len(keys):
            raise ValueError(f"{name} list length mismatch: {len(value)} vs {len(keys)}")
        return {str(k): float(v) for k, v in zip(keys, value)}
    raise TypeError(f"Unsupported {name} type: {type(value)}")


def _get_sigma(post: dict) -> float:
    for k in ["sigma_mean", "sigma", "sigma_mean_"]:
        if k in post and post[k] is not None:
            try:
                s = float(post[k])
                if s > 0:
                    return s
            except Exception:
                pass
    return 1e-6


def score_today(
    frame_path: str = FRAME_PATH,
    post_path: str = POST_PATH,
    idata_path: str = IDATA_PATH,
    out_path: str = OUT_PATH,
):
    # Load latest features per ticker
    df = pd.read_parquet(frame_path).copy()
    df["dt"] = df["dt"].astype(str)
    df = df.sort_values(["ticker", "dt"])
    latest = df.groupby("ticker", group_keys=False).tail(1).reset_index(drop=True)

    # Load posterior summaries + idata to recover asset/feature ordering when JSON stores lists
    post = _load_post(post_path)
    idata = az.from_netcdf(idata_path)

    assets = [str(x) for x in idata.posterior.coords["asset"].values] if "asset" in idata.posterior.coords else []
    features = [str(x) for x in idata.posterior.coords["feature"].values] if "feature" in idata.posterior.coords else []

    # Beta map
    beta_val = post.get("beta_mean", post.get("beta"))
    if beta_val is None:
        raise KeyError("posterior_summaries.json missing beta_mean/beta")

    # Prefer idata feature ordering; fallback to FEATURE_COLS
    beta_keys = features if features else FEATURE_COLS
    beta_map = _as_float_map_from_json(beta_val, beta_keys, "beta")

    # Alpha map
    alpha_val = post.get("alpha_mean", post.get("alpha"))
    if alpha_val is None:
        raise KeyError("posterior_summaries.json missing alpha_mean/alpha")

    # Prefer idata asset ordering; fallback to sorted tickers in latest frame
    alpha_keys = assets if assets else sorted(latest["ticker"].unique().tolist())
    alpha_map = _as_float_map_from_json(alpha_val, alpha_keys, "alpha")

    sigma = _get_sigma(post)

    # Filter to tickers the model knows (if available)
    if assets:
        latest = latest[latest["ticker"].isin(assets)].copy()

    # Ensure all feature columns exist
    for c in FEATURE_COLS:
        if c not in latest.columns:
            latest[c] = 0.0

    # Compute mu = alpha + X beta
    mu = []
    for _, r in latest.iterrows():
        a = float(alpha_map.get(r["ticker"], 0.0))
        xb = 0.0
        for c in FEATURE_COLS:
            xb += float(r[c]) * float(beta_map.get(c, 0.0))
        mu.append(a + xb)

    latest["mu_1d"] = mu
    latest["sigma"] = sigma
    latest["z_score"] = latest["mu_1d"] / (sigma + 1e-12)
    latest["p_pos"] = norm.cdf(latest["mu_1d"] / (sigma + 1e-12))

    def label(z: float) -> str:
        if z > 0.5:
            return "undervalued"
        if z < -0.5:
            return "overvalued"
        return "neutral"

    latest["label"] = latest["z_score"].apply(label)

    out_cols = ["ticker", "dt", "mu_1d", "sigma", "z_score", "p_pos", "label"] + FEATURE_COLS
    out = latest[out_cols].sort_values("z_score", ascending=False)

    out.to_csv(out_path, index=False)
    print(f"✓ wrote {out_path}")
    print(out[["ticker", "dt", "mu_1d", "z_score", "p_pos", "label"]].head(50).to_string(index=False))


if __name__ == "__main__":
    score_today()


