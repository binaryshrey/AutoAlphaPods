"""Curated Yahoo Finance currency universe for USD cross research.

This is a bootstrap FX universe for ingestion and prototyping. Each entry
represents a currency with USD as the reference currency, but the Yahoo symbol
may be quoted either as USD per currency unit or currency units per USD.
"""

from __future__ import annotations

from typing import Dict

CurrencyDefinition = Dict[str, str]
CurrencyBucket = Dict[str, Dict[str, CurrencyDefinition]]


CURRENCY_USD_TICKERS: CurrencyBucket = {
    "majors": {
        "euro": {
            "symbol": "EURUSD=X",
            "currency_code": "EUR",
            "quote_direction": "usd_per_currency",
        },
        "british_pound": {
            "symbol": "GBPUSD=X",
            "currency_code": "GBP",
            "quote_direction": "usd_per_currency",
        },
        "japanese_yen": {
            "symbol": "USDJPY=X",
            "currency_code": "JPY",
            "quote_direction": "currency_per_usd",
        },
        "swiss_franc": {
            "symbol": "USDCHF=X",
            "currency_code": "CHF",
            "quote_direction": "currency_per_usd",
        },
        "canadian_dollar": {
            "symbol": "USDCAD=X",
            "currency_code": "CAD",
            "quote_direction": "currency_per_usd",
        },
        "australian_dollar": {
            "symbol": "AUDUSD=X",
            "currency_code": "AUD",
            "quote_direction": "usd_per_currency",
        },
        "new_zealand_dollar": {
            "symbol": "NZDUSD=X",
            "currency_code": "NZD",
            "quote_direction": "usd_per_currency",
        },
        "swedish_krona": {
            "symbol": "USDSEK=X",
            "currency_code": "SEK",
            "quote_direction": "currency_per_usd",
        },
        "norwegian_krone": {
            "symbol": "USDNOK=X",
            "currency_code": "NOK",
            "quote_direction": "currency_per_usd",
        },
        "danish_krone": {
            "symbol": "USDDKK=X",
            "currency_code": "DKK",
            "quote_direction": "currency_per_usd",
        },
        "singapore_dollar": {
            "symbol": "USDSGD=X",
            "currency_code": "SGD",
            "quote_direction": "currency_per_usd",
        },
        "hong_kong_dollar": {
            "symbol": "USDHKD=X",
            "currency_code": "HKD",
            "quote_direction": "currency_per_usd",
        },
    },
    "americas": {
        "mexican_peso": {
            "symbol": "USDMXN=X",
            "currency_code": "MXN",
            "quote_direction": "currency_per_usd",
        },
        "brazilian_real": {
            "symbol": "USDBRL=X",
            "currency_code": "BRL",
            "quote_direction": "currency_per_usd",
        },
        "chilean_peso": {
            "symbol": "USDCLP=X",
            "currency_code": "CLP",
            "quote_direction": "currency_per_usd",
        },
        "colombian_peso": {
            "symbol": "USDCOP=X",
            "currency_code": "COP",
            "quote_direction": "currency_per_usd",
        },
        "peruvian_sol": {
            "symbol": "USDPEN=X",
            "currency_code": "PEN",
            "quote_direction": "currency_per_usd",
        },
    },
    "europe_middle_east_africa": {
        "czech_koruna": {
            "symbol": "USDCZK=X",
            "currency_code": "CZK",
            "quote_direction": "currency_per_usd",
        },
        "hungarian_forint": {
            "symbol": "USDHUF=X",
            "currency_code": "HUF",
            "quote_direction": "currency_per_usd",
        },
        "polish_zloty": {
            "symbol": "USDPLN=X",
            "currency_code": "PLN",
            "quote_direction": "currency_per_usd",
        },
        "romanian_leu": {
            "symbol": "USDRON=X",
            "currency_code": "RON",
            "quote_direction": "currency_per_usd",
        },
        "turkish_lira": {
            "symbol": "USDTRY=X",
            "currency_code": "TRY",
            "quote_direction": "currency_per_usd",
        },
        "south_african_rand": {
            "symbol": "USDZAR=X",
            "currency_code": "ZAR",
            "quote_direction": "currency_per_usd",
        },
        "israeli_shekel": {
            "symbol": "USDILS=X",
            "currency_code": "ILS",
            "quote_direction": "currency_per_usd",
        },
    },
    "asia": {
        "chinese_yuan_onshore": {
            "symbol": "USDCNY=X",
            "currency_code": "CNY",
            "quote_direction": "currency_per_usd",
        },
        "indian_rupee": {
            "symbol": "USDINR=X",
            "currency_code": "INR",
            "quote_direction": "currency_per_usd",
        },
        "south_korean_won": {
            "symbol": "USDKRW=X",
            "currency_code": "KRW",
            "quote_direction": "currency_per_usd",
        },
        "taiwan_dollar": {
            "symbol": "USDTWD=X",
            "currency_code": "TWD",
            "quote_direction": "currency_per_usd",
        },
        "thai_baht": {
            "symbol": "USDTHB=X",
            "currency_code": "THB",
            "quote_direction": "currency_per_usd",
        },
        "malaysian_ringgit": {
            "symbol": "USDMYR=X",
            "currency_code": "MYR",
            "quote_direction": "currency_per_usd",
        },
        "indonesian_rupiah": {
            "symbol": "USDIDR=X",
            "currency_code": "IDR",
            "quote_direction": "currency_per_usd",
        },
        "philippine_peso": {
            "symbol": "USDPHP=X",
            "currency_code": "PHP",
            "quote_direction": "currency_per_usd",
        },
        "vietnamese_dong": {
            "symbol": "USDVND=X",
            "currency_code": "VND",
            "quote_direction": "currency_per_usd",
        },
    },
}


def flatten_currency_buckets(buckets: CurrencyBucket) -> Dict[str, Dict[str, str]]:
    """Flatten a nested category->name->metadata mapping for iteration."""

    flat: Dict[str, Dict[str, str]] = {}
    for category, entries in buckets.items():
        for instrument_name, metadata in entries.items():
            flat[instrument_name] = {
                "category": category,
                **metadata,
            }
    return flat


ALL_CURRENCY_TICKERS: Dict[str, Dict[str, str]] = flatten_currency_buckets(
    CURRENCY_USD_TICKERS
)
