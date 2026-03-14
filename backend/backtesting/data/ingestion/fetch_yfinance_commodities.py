"""Download historical commodity data from Yahoo Finance.

Bootstrap only:

- Useful for prototyping the ingestion layer and synthetic tests.
- Not sufficient for production point-in-time backtesting.
- Yahoo futures series are not a substitute for a proper futures master,
  contract rolls, and revision-aware vendor data.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, Tuple

import pandas as pd
import yfinance as yf

from backtesting.data.ingestion.commodity_universe import (
    COMMODITY_ETF_TICKERS,
    COMMODITY_FUTURES_TICKERS,
)

TickerBuckets = Dict[str, Dict[str, str]]

DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parents[2] / "storage" / "raw" / "yfinance"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Finance commodity futures and commodity ETP history."
    )
    parser.add_argument(
        "--universe",
        choices=("futures", "etfs", "all"),
        default="all",
        help="Which commodity universe to fetch.",
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


def iter_universe(universe: str) -> Iterator[Tuple[str, str, str, str]]:
    selected: Iterable[Tuple[str, TickerBuckets]]
    if universe == "futures":
        selected = [("futures", COMMODITY_FUTURES_TICKERS)]
    elif universe == "etfs":
        selected = [("etfs", COMMODITY_ETF_TICKERS)]
    else:
        selected = [
            ("futures", COMMODITY_FUTURES_TICKERS),
            ("etfs", COMMODITY_ETF_TICKERS),
        ]

    for universe_name, buckets in selected:
        for category, instruments in buckets.items():
            for instrument_name, symbol in instruments.items():
                yield universe_name, category, instrument_name, symbol


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
    universe_name: str,
    category: str,
    instrument_name: str,
) -> Path:
    target_dir = output_dir / universe_name / category
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
        "universe": args.universe,
        "period": args.period,
        "interval": args.interval,
        "start": args.start,
        "end": args.end,
        "require_history_on_or_before": args.require_history_on_or_before,
        "datasets": [],
        "failures": [],
    }

    for universe_name, category, instrument_name, symbol in iter_universe(args.universe):
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
                    universe_name=universe_name,
                    category=category,
                    instrument_name=instrument_name,
                )
                manifest["datasets"].append(
                    {
                        "instrument_name": instrument_name,
                        "symbol": symbol,
                        "universe": universe_name,
                        "category": category,
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
                "saved_datasets": len(manifest["datasets"]),
                "failed_datasets": len(manifest["failures"]),
                "manifest_path": str(manifest_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
