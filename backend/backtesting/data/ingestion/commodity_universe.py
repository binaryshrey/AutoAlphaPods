"""Curated Yahoo Finance commodity universe for prototyping.

This module is intentionally opinionated:

- `COMMODITY_FUTURES_TICKERS` contains liquid commodity futures symbols that are
  commonly available on Yahoo Finance.
- `COMMODITY_ETF_TICKERS` contains commodity-focused exchange-traded products and
  trusts. Some are commodity pools or grantor trusts rather than 1940 Act ETFs,
  but they trade as exchange-traded commodity vehicles.

This is a bootstrap universe for ingestion and early research. It is not a
survivorship-bias-free institutional security master.
"""

from __future__ import annotations

from typing import Dict

TickerBucket = Dict[str, Dict[str, str]]


COMMODITY_FUTURES_TICKERS: TickerBucket = {
    "energy": {
        "wti_crude_oil": "CL=F",
        "brent_crude_oil": "BZ=F",
        "natural_gas": "NG=F",
        "rbob_gasoline": "RB=F",
        "heating_oil": "HO=F",
    },
    "precious_metals": {
        "gold": "GC=F",
        "silver": "SI=F",
        "platinum": "PL=F",
        "palladium": "PA=F",
    },
    "industrial_metals": {
        "copper": "HG=F",
    },
    "grains_and_oilseeds": {
        "corn": "ZC=F",
        "soybeans": "ZS=F",
        "wheat": "ZW=F",
        "oats": "ZO=F",
        "rough_rice": "ZR=F",
    },
    "softs": {
        "coffee": "KC=F",
        "cotton": "CT=F",
        "sugar": "SB=F",
        "cocoa": "CC=F",
        "orange_juice": "OJ=F",
    },
    "livestock": {
        "live_cattle": "LE=F",
        "lean_hogs": "HE=F",
        "feeder_cattle": "GF=F",
    },
}


COMMODITY_ETF_TICKERS: TickerBucket = {
    "broad_commodity_baskets": {
        "invesco_db_commodity_index_tracking_fund": "DBC",
        "ishares_gsci_commodity_dynamic_roll_strategy_etf": "COMT",
        "invesco_optimum_yield_diversified_commodity_strategy_no_k1_etf": "PDBC",
        "united_states_commodity_index_fund": "USCI",
        "uscf_summerhaven_dynamic_commodity_strategy_no_k1_fund": "SDCI",
        "uscf_sustainable_commodity_strategy_fund": "ZSC",
    },
    "energy": {
        "united_states_oil_fund": "USO",
        "united_states_12_month_oil_fund": "USL",
        "united_states_brent_oil_fund": "BNO",
        "united_states_natural_gas_fund": "UNG",
        "united_states_12_month_natural_gas_fund": "UNL",
        "united_states_gasoline_fund": "UGA",
        "invesco_db_energy_fund": "DBE",
        "invesco_db_oil_fund": "DBO",
    },
    "precious_metals": {
        "spdr_gold_shares": "GLD",
        "ishares_gold_trust": "IAU",
        "abrdn_physical_gold_shares_etf": "SGOL",
        "ishares_silver_trust": "SLV",
        "abrdn_physical_silver_shares_etf": "SIVR",
        "abrdn_physical_platinum_shares_etf": "PPLT",
        "abrdn_physical_palladium_shares_etf": "PALL",
    },
    "industrial_metals": {
        "united_states_copper_index_fund": "CPER",
        "invesco_db_base_metals_fund": "DBB",
    },
    "agriculture": {
        "invesco_db_agriculture_fund": "DBA",
        "teucrium_corn_fund": "CORN",
        "teucrium_wheat_fund": "WEAT",
        "teucrium_soybean_fund": "SOYB",
        "teucrium_sugar_fund": "CANE",
    },
}


def flatten_ticker_buckets(buckets: TickerBucket) -> Dict[str, Dict[str, str]]:
    """Flatten a nested category->name->symbol mapping for iteration."""

    flat: Dict[str, Dict[str, str]] = {}
    for category, entries in buckets.items():
        for instrument_name, symbol in entries.items():
            flat[instrument_name] = {
                "symbol": symbol,
                "category": category,
            }
    return flat


ALL_COMMODITY_TICKERS: Dict[str, Dict[str, str]] = {
    **flatten_ticker_buckets(COMMODITY_FUTURES_TICKERS),
    **flatten_ticker_buckets(COMMODITY_ETF_TICKERS),
}
