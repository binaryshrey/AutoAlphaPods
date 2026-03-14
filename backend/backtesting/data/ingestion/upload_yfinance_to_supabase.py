"""Normalize raw Yahoo commodity CSVs and upload them to Supabase."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

import requests


DEFAULT_RAW_ROOT = Path(__file__).resolve().parents[2] / "storage" / "raw" / "yfinance"
DEFAULT_TABLE = "commodity_price_bars"

YAHOO_COLUMN_PATTERN = re.compile(r"\('(.+)', '(.+)'\)")


@dataclass(frozen=True)
class InstrumentContext:
    asset_class: str
    universe: str
    category: str
    instrument_name: str
    symbol: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload normalized commodity Yahoo Finance CSVs to Supabase."
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
        help="Root directory containing yfinance CSVs.",
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
    if len(parts) < 3:
        raise ValueError(f"Unexpected CSV path layout: {csv_path}")

    universe = parts[0]
    category = parts[1]
    instrument_name = csv_path.stem
    asset_class = "future" if universe == "futures" else "etf"

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        first_row = next(reader, None)

    if first_row is None or not first_row.get("symbol"):
        raise ValueError(f"Could not determine symbol for {csv_path}")

    return InstrumentContext(
        asset_class=asset_class,
        universe=universe,
        category=category,
        instrument_name=instrument_name,
        symbol=first_row["symbol"],
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


def coerce_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def coerce_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))


def normalize_csv(csv_path: Path, raw_root: Path) -> Iterator[dict]:
    context = parse_instrument_context(csv_path, raw_root)

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        column_map = parse_yahoo_columns(reader.fieldnames or [])

        for row in reader:
            yield {
                "source": "yfinance",
                "asset_class": context.asset_class,
                "universe": context.universe,
                "category": context.category,
                "instrument_name": context.instrument_name,
                "symbol": context.symbol,
                "timestamp": row["timestamp"],
                "open": coerce_float(row.get(field_name_for(column_map, "open"))),
                "high": coerce_float(row.get(field_name_for(column_map, "high"))),
                "low": coerce_float(row.get(field_name_for(column_map, "low"))),
                "close": coerce_float(row.get(field_name_for(column_map, "close"))),
                "adj_close": coerce_float(row.get(field_name_for(column_map, "adj_close"))),
                "volume": coerce_int(row.get(field_name_for(column_map, "volume"))),
                "dividends": coerce_float(row.get(field_name_for(column_map, "dividends"))),
                "capital_gains": coerce_float(
                    row.get(field_name_for(column_map, "capital_gains"))
                ),
                "stock_splits": coerce_float(
                    row.get(field_name_for(column_map, "stock_splits"))
                ),
            }


def field_name_for(column_map: Dict[str, str], normalized_name: str) -> str | None:
    for raw_name, mapped_name in column_map.items():
        if mapped_name == normalized_name:
            return raw_name
    return None


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
