"""
You don't use Parquet instead of SQL; rather, Parquet is an efficient file format for storing data, while SQL is the language used to query that data. 
You can run SQL queries directly on data stored in Parquet files using various query engines and big data frameworks. 
The choice to use Parquet, typically within a data lake architecture, is driven by the need for performance, efficiency, and scalability 
when dealing with large-scale analytical workloads (OLAP), which traditional, row-oriented SQL databases are not optimized for. 
"""

# src/train_pymc.py
from __future__ import annotations

import os
import json
import numpy as np
import pandas as pd
import pymc as pm
import arviz as az

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
TARGET = "ret_1d"


def fit_model(df: pd.DataFrame, draws=1500, tune=1500, chains=4, target_accept=0.9, seed=42):
    df = df.copy()

    # Basic sanity
    missing = [c for c in [TARGET, "ticker", "dt"] + FEATURE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in model_frame: {missing}")

    # Drop NA rows
    df = df.dropna(subset=[TARGET] + FEATURE_COLS).reset_index(drop=True)

    # Encode tickers
    tickers = df["ticker"].astype("category")
    asset_idx = tickers.cat.codes.values
    asset_names = tickers.cat.categories.tolist()

    X = df[FEATURE_COLS].to_numpy(dtype=np.float64)
    y = df[TARGET].to_numpy(dtype=np.float64)

    # Optional: time split for evaluation later (keep simple for now)
    # You can add this in Phase 1.5.

    with pm.Model(coords={"feature": FEATURE_COLS, "asset": asset_names}) as model:
        s_beta = pm.HalfNormal("s_beta", sigma=0.5)
        s_alpha = pm.HalfNormal("s_alpha", sigma=0.02)
        sigma = pm.HalfNormal("sigma", sigma=0.02)

        beta = pm.Normal("beta", mu=0.0, sigma=s_beta, dims=("feature",))
        alpha = pm.Normal("alpha", mu=0.0, sigma=s_alpha, dims=("asset",))

        mu = alpha[asset_idx] + pm.math.dot(X, beta)

        # Fix nu to avoid slow/inaccurate inference of nu under ADVI at first
        nu = 8.0
        pm.StudentT("y", nu=nu, mu=mu, sigma=sigma, observed=y)

        # FAST inference
        approx = pm.fit(n=30_000, method="advi", random_seed=seed)
        idata = approx.sample(draws=2000)

        # Posterior predictive
        ppc = pm.sample_posterior_predictive(idata, var_names=["y"], random_seed=seed)
        idata.extend(ppc)

    return model, idata


def export_posterior(idata, out_path="data/posterior_summaries.json"):
    import os, json
    post = idata.posterior

    beta_mean = post["beta"].mean(dim=("chain", "draw")).values.tolist()
    beta_std  = post["beta"].std(dim=("chain", "draw")).values.tolist()

    alpha_mean = post["alpha"].mean(dim=("chain", "draw")).values.tolist()

    sigma_mean = float(post["sigma"].mean(dim=("chain", "draw")).values)
    s_beta_mean = float(post["s_beta"].mean(dim=("chain", "draw")).values)
    s_alpha_mean = float(post["s_alpha"].mean(dim=("chain", "draw")).values)

    # nu only exists if you used StudentT likelihood
    nu_mean = None
    if "nu" in post.data_vars:
        nu_mean = float(post["nu"].mean(dim=("chain", "draw")).values)

    payload = {
        "feature_cols": FEATURE_COLS,
        "beta_mean": beta_mean,
        "beta_std": beta_std,
        "alpha_mean": alpha_mean,
        "sigma_mean": sigma_mean,
        "s_beta_mean": s_beta_mean,
        "s_alpha_mean": s_alpha_mean,
        "nu_mean": nu_mean,
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"✓ wrote {out_path}")



def diagnostics(idata):
    import numpy as np
    import arviz as az

    # Keep only variables that actually exist in the posterior
    wanted = ["beta", "sigma", "nu", "s_beta", "s_alpha", "alpha"]
    present = set(idata.posterior.data_vars)
    var_names = [v for v in wanted if v in present]

    summ = az.summary(idata, var_names=var_names, round_to=4)
    print(summ[["mean", "sd", "hdi_3%", "hdi_97%", "r_hat", "ess_bulk"]])

    # Posterior predictive quick check (works for both Normal and StudentT)
    y_obs = idata.observed_data["y"].values
    y_ppc = idata.posterior_predictive["y"].values  # (chain, draw, obs)

    obs_mean, obs_std = float(np.mean(y_obs)), float(np.std(y_obs))
    ppc_mean, ppc_std = float(np.mean(y_ppc)), float(np.std(y_ppc))

    print("\nPosterior predictive quick check:")
    print(f"  observed mean/std: {obs_mean:.6f} / {obs_std:.6f}")
    print(f"  ppc mean/std     : {ppc_mean:.6f} / {ppc_std:.6f}")



def main():
    df = pd.read_parquet("data/model_frame.parquet")
    # ---- SPEED MODE (prototype) ----
    # 1) use a smaller time window
    df = df[df["dt"] >= "2023-01-01"].copy()

    # 2) optionally keep fewer tickers
    keep = ["AAPL", "MSFT", "AMZN", "GOOG", "META", "NVDA", "CRM", "TSM"]
    df = df[df["ticker"].isin(keep)].copy()

    # 3) cap rows per ticker to keep runtime predictable
    df = (df.sort_values(["ticker", "dt"])
            .groupby("ticker", group_keys=False)
            .tail(500))  # 500 rows per ticker
    
    # Useful: see usable rows per ticker after rolling windows
    print("Rows per ticker (top):")
    print(df.groupby("ticker").size().sort_values(ascending=False).head(20))

    model, idata = fit_model(df)
    diagnostics(idata)
    export_posterior(idata, "data/posterior_summaries.json")

    # Save full inference trace (recommended)
    az.to_netcdf(idata, "data/idata.nc")
    print("✓ wrote data/idata.nc")


if __name__ == "__main__":
    main()
