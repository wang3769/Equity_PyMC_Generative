from __future__ import annotations

import numpy as np
import pandas as pd
import arviz as az
from scipy.stats import spearmanr

TARGET = "ret_1d"  # in your model_frame

def daily_ic(df: pd.DataFrame) -> pd.Series:
    def _ic(g):
        c = spearmanr(g["pred_mu"], g[TARGET]).correlation
        return np.nan if c is None else c
    return df.groupby("dt").apply(_ic).dropna()

def main(split_date="2024-01-01"):
    df = pd.read_parquet("data/model_frame.parquet").copy()
    df["dt"] = df["dt"].astype(str)
    df = df.sort_values(["ticker", "dt"])

    idata = az.from_netcdf("data/idata.nc")

    # tickers used in training (from model coords)
    assets = list(idata.posterior.coords["asset"].values)

    # filter to those assets, then apply the same tail(500) rule
    df_fit = (
        df[df["ticker"].isin(assets)]
        .groupby("ticker", group_keys=False)
        .tail(500)
        .reset_index(drop=True)
    )

    y_ppc_mean = idata.posterior_predictive["y"].mean(dim=("chain","draw")).values
    assert len(y_ppc_mean) == len(df_fit), (len(y_ppc_mean), len(df_fit))
    df_fit["pred_mu"] = y_ppc_mean


    # use df_fit (not df) for evaluation
    test = df_fit[df_fit["dt"] >= split_date].copy()
    if len(test) == 0:
        raise RuntimeError(f"No rows after split_date={split_date}. Adjust split_date.")

    ic = daily_ic(test)

    print(f"Test rows: {len(test):,} from {test['dt'].min()} to {test['dt'].max()}")
    print("\nDaily IC (Spearman rank corr):")
    print(f"  mean: {ic.mean():.4f}")
    print(f"  std : {ic.std():.4f}")
    print(f"  t   : {ic.mean() / (ic.std()/np.sqrt(len(ic))):.2f}  (n_days={len(ic)})")

    # Optional: decile spread (top-bottom) by day
    def decile_spread(g):
        g = g.sort_values("pred_mu")
        n = len(g)
        if n < 10:
            return np.nan
        bot = g.iloc[: max(1, n//10)][TARGET].mean()
        top = g.iloc[-max(1, n//10):][TARGET].mean()
        return top - bot

    spread = test.groupby("dt").apply(decile_spread).dropna()
    print("\nTop-bottom decile spread:")
    print(f"  mean: {spread.mean():.4f}")
    print(f"  std : {spread.std():.4f}")

if __name__ == "__main__":
    main()
