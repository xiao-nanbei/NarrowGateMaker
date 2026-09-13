"""Shared market identity helpers for cross-market data and live streams.

``symbol`` is not a unique market identifier.  BTCUSDT spot, perpetuals on
different venues, and dated futures can all share the same display symbol.  A
venue-aware identity keeps those streams separate before any consensus or
lead/lag feature is calculated.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from data_paths import data_root


PERP_MARKET = "perp"
SPOT_MARKET = "spot"
FUTURE_MARKET = "future"
OPTION_MARKET = "option"
ETF_MARKET = "etf"

BINANCE_VENUE = "binance"
BITGET_VENUE = "bitget"
BYBIT_VENUE = "bybit"
OKX_VENUE = "okx"

EXECUTION_ROLE = "execution"
REFERENCE_ROLE = "reference"
ANCHOR_ROLE = "anchor"
STABLECOIN_ANCHOR_ROLE = "stablecoin_anchor"


@dataclass(frozen=True)
class MarketSpec:
    symbol: str
    market_type: str
    role: str
    venue: str = BINANCE_VENUE
    quote_currency: str = ""
    settlement_currency: str = ""
    contract_multiplier: float = 1.0
    expiry_ns: Optional[int] = None
    instrument_id: str = ""

    @property
    def instrument_type(self) -> str:
        """Preferred public name; ``market_type`` remains for compatibility."""
        return self.market_type

    @property
    def market_id(self) -> str:
        return market_key(self.venue, self.market_type, self.symbol)


def normalize_venue(venue: Optional[str] = None, default: str = BINANCE_VENUE) -> str:
    value = str(venue or default or BINANCE_VENUE).strip().lower()
    return value.replace(" ", "_")


def normalize_symbol(symbol: Optional[str] = None, default: str = "BTCUSDC") -> str:
    value = (symbol or default or "BTCUSDC").strip().upper()
    return value.replace("/", "").replace("-", "")


def market_key(venue: str, market_type: str, symbol: str) -> str:
    """Return the canonical venue-aware market identity."""
    return (
        f"{normalize_venue(venue)}:"
        f"{str(market_type or PERP_MARKET).strip().lower()}:"
        f"{normalize_symbol(symbol)}"
    )


def default_reference_symbol(execution_symbol: str) -> str:
    execution_symbol = normalize_symbol(execution_symbol)
    return "BTCUSDC" if execution_symbol == "BTCUSDT" else "BTCUSDT"


def market_raw_dir(root: Path, market_type: str) -> Path:
    return data_root(root) / ("raw_spot" if market_type == SPOT_MARKET else "raw")


def market_bars_dir(root: Path, market_type: str) -> Path:
    return data_root(root) / ("bars_1s_spot" if market_type == SPOT_MARKET else "bars_1s")


def build_market_specs(
    execution_symbol: str,
    market_stage: str = "minimal",
    reference_symbol: Optional[str] = None,
    stablecoin_anchor_symbol: Optional[str] = "USDCUSDT",
) -> list[MarketSpec]:
    """Return ordered market roles for the selected cross-market stage."""
    execution = normalize_symbol(execution_symbol)
    reference = normalize_symbol(reference_symbol, default_reference_symbol(execution))
    stage = (market_stage or "minimal").strip().lower()

    # execution perp 永远是主交易/回测标的；reference/spot 只提供 quote-time 特征，
    # 不代表可以直接打开 multi_market policy 当作收益开关。
    specs = [MarketSpec(execution, PERP_MARKET, EXECUTION_ROLE, BINANCE_VENUE)]
    if reference != execution:
        specs.append(MarketSpec(reference, PERP_MARKET, REFERENCE_ROLE, BINANCE_VENUE))

    if stage in {"enhanced", "full"}:
        specs.append(MarketSpec(execution, SPOT_MARKET, ANCHOR_ROLE, BINANCE_VENUE))
        if reference != execution:
            specs.append(MarketSpec(reference, SPOT_MARKET, ANCHOR_ROLE, BINANCE_VENUE))
        stablecoin = normalize_symbol(stablecoin_anchor_symbol, "USDCUSDT")
        if stablecoin and stablecoin not in {execution, reference}:
            specs.append(MarketSpec(
                stablecoin,
                SPOT_MARKET,
                STABLECOIN_ANCHOR_ROLE,
                BINANCE_VENUE,
                quote_currency="USDT",
                settlement_currency="USDT",
            ))

    return specs


def build_external_reference_specs(
    symbol: str = "BTCUSDT",
    *,
    venues: tuple[str, ...] = (BITGET_VENUE, BYBIT_VENUE, OKX_VENUE),
    include_spot: bool = True,
    include_perp: bool = True,
) -> list[MarketSpec]:
    """Return independent external reference markets as first-class specs.

    Spot and perpetual instruments remain separate factors.  They share a
    venue failure domain and must not be counted as six independent votes.
    """
    normalized_symbol = normalize_symbol(symbol, "BTCUSDT")
    specs: list[MarketSpec] = []
    for venue in venues:
        normalized_venue = normalize_venue(venue)
        if include_perp:
            specs.append(MarketSpec(
                normalized_symbol,
                PERP_MARKET,
                REFERENCE_ROLE,
                normalized_venue,
                quote_currency="USDT",
                settlement_currency="USDT",
                contract_multiplier=0.01 if normalized_venue == OKX_VENUE else 1.0,
                instrument_id=(
                    "BTC-USDT-SWAP" if normalized_venue == OKX_VENUE else normalized_symbol
                ),
            ))
        if include_spot:
            specs.append(MarketSpec(
                normalized_symbol,
                SPOT_MARKET,
                REFERENCE_ROLE,
                normalized_venue,
                quote_currency="USDT",
                settlement_currency="USDT",
                instrument_id="BTC-USDT" if normalized_venue == OKX_VENUE else normalized_symbol,
            ))
    return specs
