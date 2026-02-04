# Equity_PyMC_Generative
Overview

This project implements a Bayesian generative model for equity returns using PyMC for probabilistic inference and PyTorch for downstream, production-style computation.

The primary goal is learning and system design, not alpha maximization.

The model:

Explicitly defines how returns are generated

Treats uncertainty as first-class

Uses economically grounded signals

Separates inference from fast prediction

Mirrors how real quantitative systems evolve from research to production

Motivation

Traditional ML approaches treat equity returns as labels to predict.

This project instead asks:

What probabilistic process could plausibly generate observed returns, given noisy signals and changing market conditions?

By answering this question explicitly, the model:

Avoids overfitting in small and noisy datasets

Produces uncertainty-aware forecasts

Supports principled extensions (regimes, events, optimization)

Provides a strong foundation for Bayesian optimization later

Model Summary
Observed Data

For asset i at time t:

Target
: realized return

Inputs (10 total)

Market excess return (beta exposure)

Size (log market cap / SMB)

Value (book-to-market, earnings yield)

Momentum (12-1 or 6-1 return)

Volatility (realized or implied)

Liquidity (volume / Amihud proxy)

Profitability / quality (ROE, ROIC)

Macro sensitivity (rates, yield curve, inflation)

Credit / risk appetite (credit spreads)

News / event / sentiment signal (noisy)

All inputs are standardized and treated as imperfect measurements, not ground truth.