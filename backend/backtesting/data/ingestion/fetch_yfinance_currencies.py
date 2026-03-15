"""Download historical currency data from Yahoo Finance.

Bootstrap only:

- Useful for prototyping the ingestion layer and early FX research.
- Not sufficient for production point-in-time backtesting.
- Yahoo FX series are a convenience feed, not a revision-aware institutional
  FX vendor.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, Tuple

import pandas as pd
import yfinance as yf

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[3]))

from backtesting.data.ingestion.currency_universe import CURRENCY_USD_TICKERS

CurrencyMetadata = Dict[str, str]

DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parents[2] / "storage" / "raw" / "yfinance" / "currencies"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Finance currency history for currencies versus USD."
    )
    parser.add_argument(
        "--group",
        default="all",
        help="Which currency group to fetch: all or a specific bucket name.",
    )
    parser.add_argument(
        "--period",
        default="max",
        help="Yahoo Finance period, e.g. 1y, 5y, max. Ignored if start/end are set.",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        help="Yahoo Finance interval, e.g. 1d, 1wk, 1mo.",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Optional ISO start date, e.g. 2010-01-01.",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Optional ISO end date, e.g. 2025-12-31.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.35,
        help="Pause between requests to reduce transient Yahoo throttling.",
    )
    parser.add_argument(
        "--require-history-on-or-before",
        default=None,
        help=(
            "Optional ISO date. Skip symbols whose earliest available history "
            "starts after this date."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Base output directory for CSV files and the manifest.",
    )
    return parser.parse_args()


def iter_universe(group: str) -> Iterator[Tuple[str, str, CurrencyMetadata]]:
    if group == "all":
        selected = CURRENCY_USD_TICKERS.items()
    else:
        if group not in CURRENCY_USD_TICKERS:
            choices = ", ".join(sorted(CURRENCY_USD_TICKERS))
            raise SystemExit(f"Unknown group '{group}'. Available groups: {choices}")
        selected = [(group, CURRENCY_USD_TICKERS[group])]

    for category, instruments in selected:
        for instrument_name, metadata in instruments.items():
            yield category, instrument_name, metadata


def download_history(
    symbol: str,
    *,
    period: str,
    interval: str,
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    kwargs = {
        "interval": interval,
        "auto_adjust": False,
        "actions": True,
    }
    if start or end:
        kwargs["start"] = start
        kwargs["end"] = end
    else:
        kwargs["period"] = period

    history = yf.download(
        tickers=symbol,
        progress=False,
        threads=False,
        **kwargs,
    )
    if history is None:
        return pd.DataFrame()

    history = history.copy()
    if history.empty:
        return history

    history.columns = [str(column) for column in history.columns]
    history.index.name = "timestamp"
    history.reset_index(inplace=True)
    history["symbol"] = symbol
    return history


def write_dataset(
    frame: pd.DataFrame,
    *,
    output_dir: Path,
    category: str,
    instrument_name: str,
) -> Path:
    target_dir = output_dir / category
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{instrument_name}.csv"
    frame.to_csv(target_path, index=False)
    return target_path


def first_history_date(frame: pd.DataFrame) -> date | None:
    if frame.empty:
        return None

    first_timestamp = pd.to_datetime(frame.iloc[0]["timestamp"]).date()
    return first_timestamp


def main() -> int:
    args = parse_args()
    required_history_date = (
        date.fromisoformat(args.require_history_on_or_before)
        if args.require_history_on_or_before
        else None
    )
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance",
        "dataset": "currencies_vs_usd",
        "group": args.group,
        "period": args.period,
        "interval": args.interval,
        "start": args.start,
        "end": args.end,
        "require_history_on_or_before": args.require_history_on_or_before,
        "datasets": [],
        "failures": [],
    }

    for category, instrument_name, metadata in iter_universe(args.group):
        symbol = metadata["symbol"]
        try:
            history = download_history(
                symbol,
                period=args.period,
                interval=args.interval,
                start=args.start,
                end=args.end,
            )
            if history.empty:
                manifest["failures"].append(
                    {
                        "instrument_name": instrument_name,
                        "currency_code": metadata["currency_code"],
                        "symbol": symbol,
                        "reason": "empty_history",
                    }
                )
            else:
                earliest_available_date = first_history_date(history)
                if (
                    required_history_date
                    and earliest_available_date
                    and earliest_available_date > required_history_date
                ):
                    manifest["failures"].append(
                        {
                            "instrument_name": instrument_name,
                            "currency_code": metadata["currency_code"],
                            "symbol": symbol,
                            "reason": "insufficient_history",
                            "first_available_date": earliest_available_date.isoformat(),
                        }
                    )
                    time.sleep(args.pause_seconds)
                    continue

                output_path = write_dataset(
                    history,
                    output_dir=args.output_dir,
                    category=category,
                    instrument_name=instrument_name,
                )
                manifest["datasets"].append(
                    {
                        "instrument_name": instrument_name,
                        "currency_code": metadata["currency_code"],
                        "symbol": symbol,
                        "category": category,
                        "quote_direction": metadata["quote_direction"],
                        "rows": int(len(history)),
                        "first_available_date": (
                            earliest_available_date.isoformat()
                            if earliest_available_date
                            else None
                        ),
                        "path": str(output_path),
                    }
                )
        except Exception as exc:  # pragma: no cover - network/runtime failure path
            manifest["failures"].append(
                {
                    "instrument_name": instrument_name,
                    "currency_code": metadata["currency_code"],
                    "symbol": symbol,
                    "reason": str(exc),
                }
            )

        time.sleep(args.pause_seconds)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "group": args.group,
                "saved_datasets": len(manifest["datasets"]),
                "failed_datasets": len(manifest["failures"]),
                "manifest_path": str(manifest_path),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
