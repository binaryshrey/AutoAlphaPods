"""Normalize raw Yahoo currency CSVs and upload them to Supabase."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

import requests

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[3]))

from backtesting.data.ingestion.currency_universe import ALL_CURRENCY_TICKERS


DEFAULT_RAW_ROOT = (
    Path(__file__).resolve().parents[2] / "storage" / "raw" / "yfinance" / "currencies"
)
DEFAULT_TABLE = "currency_price_bars"

YAHOO_COLUMN_PATTERN = re.compile(r"\('(.+)', '(.+)'\)")


@dataclass(frozen=True)
class InstrumentContext:
    category: str
    instrument_name: str
    currency_code: str
    yahoo_symbol: str
    quote_direction: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload normalized currency Yahoo Finance CSVs to Supabase."
    )
    parser.add_argument("--supabase-url", required=True, help="Supabase project URL.")
    parser.add_argument("--supabase-key", required=True, help="Supabase anon or service key.")
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="Destination public table name.",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=DEFAULT_RAW_ROOT,
        help="Root directory containing yfinance currency CSVs.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows per REST insert.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Normalize the files without posting to Supabase.",
    )
    return parser.parse_args()


def parse_instrument_context(csv_path: Path, raw_root: Path) -> InstrumentContext:
    relative = csv_path.relative_to(raw_root)
    parts = relative.parts
    if len(parts) < 2:
        raise ValueError(f"Unexpected CSV path layout: {csv_path}")

    category = parts[0]
    instrument_name = csv_path.stem
    metadata = ALL_CURRENCY_TICKERS.get(instrument_name)
    if metadata is None:
        raise ValueError(f"Instrument '{instrument_name}' missing from currency universe")

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        first_row = next(reader, None)

    if first_row is None or not first_row.get("symbol"):
        raise ValueError(f"Could not determine symbol for {csv_path}")

    return InstrumentContext(
        category=category,
        instrument_name=instrument_name,
        currency_code=metadata["currency_code"],
        yahoo_symbol=first_row["symbol"],
        quote_direction=metadata["quote_direction"],
    )


def parse_yahoo_columns(fieldnames: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for fieldname in fieldnames:
        if fieldname in {"timestamp", "symbol"}:
            mapping[fieldname] = fieldname
            continue

        match = YAHOO_COLUMN_PATTERN.fullmatch(fieldname)
        if not match:
            continue

        yahoo_name = match.group(1)
        normalized = yahoo_name.lower().replace(" ", "_")
        mapping[fieldname] = normalized

    return mapping


def field_name_for(column_map: Dict[str, str], normalized_name: str) -> str | None:
    for raw_name, mapped_name in column_map.items():
        if mapped_name == normalized_name:
            return raw_name
    return None


def coerce_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def coerce_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))


def invert_rate(value: float | None) -> float | None:
    if value in (None, 0):
        return None
    return 1.0 / value


def normalized_usd_rates(
    *,
    quote_direction: str,
    open_value: float | None,
    high_value: float | None,
    low_value: float | None,
    close_value: float | None,
    adj_close_value: float | None,
) -> Dict[str, float | None]:
    if quote_direction == "usd_per_currency":
        return {
            "usd_per_currency_open": open_value,
            "usd_per_currency_high": high_value,
            "usd_per_currency_low": low_value,
            "usd_per_currency_close": close_value,
            "usd_per_currency_adj_close": adj_close_value,
        }

    if quote_direction != "currency_per_usd":
        raise ValueError(f"Unsupported quote direction: {quote_direction}")

    return {
        "usd_per_currency_open": invert_rate(open_value),
        "usd_per_currency_high": invert_rate(low_value),
        "usd_per_currency_low": invert_rate(high_value),
        "usd_per_currency_close": invert_rate(close_value),
        "usd_per_currency_adj_close": invert_rate(adj_close_value),
    }


def normalize_csv(csv_path: Path, raw_root: Path) -> Iterator[dict]:
    context = parse_instrument_context(csv_path, raw_root)

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        column_map = parse_yahoo_columns(reader.fieldnames or [])

        for row in reader:
            open_value = coerce_float(row.get(field_name_for(column_map, "open")))
            high_value = coerce_float(row.get(field_name_for(column_map, "high")))
            low_value = coerce_float(row.get(field_name_for(column_map, "low")))
            close_value = coerce_float(row.get(field_name_for(column_map, "close")))
            adj_close_value = coerce_float(
                row.get(field_name_for(column_map, "adj_close"))
            )

            yield {
                "source": "yfinance",
                "category": context.category,
                "instrument_name": context.instrument_name,
                "currency_code": context.currency_code,
                "quote_currency": "USD",
                "yahoo_symbol": context.yahoo_symbol,
                "quote_direction": context.quote_direction,
                "timestamp": row["timestamp"],
                "raw_open": open_value,
                "raw_high": high_value,
                "raw_low": low_value,
                "raw_close": close_value,
                "raw_adj_close": adj_close_value,
                "volume": coerce_int(row.get(field_name_for(column_map, "volume"))),
                **normalized_usd_rates(
                    quote_direction=context.quote_direction,
                    open_value=open_value,
                    high_value=high_value,
                    low_value=low_value,
                    close_value=close_value,
                    adj_close_value=adj_close_value,
                ),
            }


def iter_normalized_rows(raw_root: Path) -> Iterator[dict]:
    for csv_path in sorted(raw_root.rglob("*.csv")):
        yield from normalize_csv(csv_path, raw_root)


def batched(rows: Iterator[dict], size: int) -> Iterator[List[dict]]:
    batch: List[dict] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def post_batch(
    *,
    supabase_url: str,
    supabase_key: str,
    table: str,
    batch: List[dict],
) -> requests.Response:
    url = f"{supabase_url.rstrip('/')}/rest/v1/{table}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    return requests.post(url, headers=headers, data=json.dumps(batch), timeout=60)


def main() -> int:
    args = parse_args()
    if not args.raw_root.exists():
        raise SystemExit(f"Raw root does not exist: {args.raw_root}")

    rows = iter_normalized_rows(args.raw_root)
    batches = batched(rows, args.batch_size)
    total_rows = 0
    total_batches = 0

    for batch in batches:
        total_batches += 1
        total_rows += len(batch)
        if args.dry_run:
            continue

        response = post_batch(
            supabase_url=args.supabase_url,
            supabase_key=args.supabase_key,
            table=args.table,
            batch=batch,
        )
        if response.status_code >= 300:
            raise SystemExit(
                "\n".join(
                    [
                        f"Supabase upload failed on batch {total_batches}.",
                        f"HTTP {response.status_code}",
                        response.text,
                    ]
                )
            )

    print(
        json.dumps(
            {
                "raw_root": str(args.raw_root),
                "table": args.table,
                "batches": total_batches,
                "rows": total_rows,
                "dry_run": args.dry_run,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
