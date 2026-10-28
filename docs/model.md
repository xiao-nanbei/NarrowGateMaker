# Textbook quotation model

[English](model.md) | [简体中文](model.zh-CN.md)

Last materially synchronized: 2026-10-28

The example implements the finite-horizon AS approximation:

```text
reservation = mid - inventory * gamma * sigma^2 * remaining
half_spread = gamma * sigma^2 * remaining / 2 + log(1 + gamma/kappa) / gamma
bid = reservation - half_spread
ask = reservation + half_spread
```

Here inventory is in fictional asset units; mid and quotes are price per unit. Sigma is absolute price volatility per square root of the chosen time unit, not percentage volatility or a high–low range. Gamma is positive risk aversion, kappa is positive inverse-price sensitivity, and remaining is nonnegative time to the horizon. Positive inventory shifts both quotes down; negative inventory shifts them up. Bid rounds down and ask rounds up to the configured price tick.

The reference is [Avellaneda and Stoikov, High-frequency trading in a limit order book (2008)](https://math.nyu.edu/inmemoriam/avellaneda/HighFrequencyTrading.pdf). This is a small implementation of its quotation approximation, not a replication of its stochastic execution model or empirical evaluation.

The command-line example chooses fictional normalized inputs directly in its source. There is no model fit, signal, dynamic parameter selection or production configuration. The replay uses a separate simplified trade-through rule, not the exponential order-arrival model used to derive the formula. Read the [backtest limitations](backtest_rules.md) before interpreting output.
