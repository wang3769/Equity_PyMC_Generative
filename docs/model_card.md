# Equity Bayesian Model (PyMC)

## What this system does
This dashboard shows **model-implied next-day expected returns** and a **relative score** for each ticker, derived from a Bayesian cross-sectional return model.

## Data sources
- Prices/volume: Yahoo Finance (yfinance)
- Macro: FRED (10Y, 2Y, credit OAS)
- News sentiment: NewsAPI + FinBERT (ProsusAI/finbert), aggregated to 7-day rolling sentiment

## Features (10)
- beta_mkt
- log_mktcap
- value_z
- mom_12_1
- vol_20d
- illiq_amihud
- quality_z
- macro_sens
- credit_sens
- news_sent_7d

## Model
Bayesian hierarchical regression with per-ticker intercepts and shared feature coefficients. Trained with variational inference (ADVI).

## How to interpret scores
- `mu_1d`: model-implied expected next-day return
- `p_pos`: probability of next-day return > 0 under the model’s predictive distribution
- `z_score`: risk-adjusted score (mu / sigma)
- `label`: rank-based: top group = undervalued, bottom group = overvalued (relative to universe)
