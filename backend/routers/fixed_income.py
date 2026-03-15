import re
import time
from typing import Any

import yfinance as yf
from fastapi import APIRouter

router = APIRouter(prefix="/fixed-income", tags=["Fixed Income"])

_BASE_TICKERS = {
    "irx": "^IRX",
    "fvx": "^FVX",
    "tnx": "^TNX",
    "tyx": "^TYX",
}

_TENORS = [
    {"label": "1M", "name": "1M T-Bill", "years": 1 / 12},
    {"label": "3M", "name": "3M T-Bill", "years": 0.25},
    {"label": "6M", "name": "6M T-Bill", "years": 0.5},
    {"label": "1Y", "name": "1Y Note", "years": 1},
    {"label": "2Y", "name": "2Y Note", "years": 2},
    {"label": "5Y", "name": "5Y Note", "years": 5},
    {"label": "10Y", "name": "10Y Note", "years": 10},
    {"label": "20Y", "name": "20Y Bond", "years": 20},
    {"label": "30Y", "name": "30Y Bond", "years": 30},
    {"label": "10Y TIPS", "name": "10Y TIPS", "years": 10, "tips": True},
]

_SPREAD_SERIES = {
    "ig_oas": {"ticker": "BAMLC0A0CM", "label": "IG OAS", "unit": "bps"},
    "hy_oas": {"ticker": "BAMLH0A0HYM2", "label": "HY OAS", "unit": "bps"},
    "sofr": {"ticker": "SOFR", "label": "SOFR", "unit": "%"},
    "fed_funds": {"ticker": "FEDFUNDS", "label": "Fed Funds", "unit": "%"},
    "ted": {"ticker": "TEDRATE", "label": "TED Spread", "unit": "%"},
    "breakeven_10y": {
        "ticker": "T10YIE",
        "label": "Breakeven 10Y",
        "unit": "%",
    },
    "real_10y": {"ticker": "DFII10", "label": "Real Yield 10Y", "unit": "%"},
}

_CACHE: dict[str, Any] = {"timestamp": 0.0, "payload": None}
_CACHE_TTL = 60.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except Exception:
        return default
    if num != num:
        return default
    return num


def _normalize_yield(value: float) -> float:
    if value > 20:
        return value / 10.0
    return value


def _fetch_history(ticker: str, period: str, interval: str) -> list[float]:
    try:
        hist = yf.Ticker(ticker).history(period=period, interval=interval)
    except Exception:
        return []
    if hist is None or hist.empty or "Close" not in hist.columns:
        return []
    values = [_safe_float(v, None) for v in hist["Close"].tolist()]
    return [v for v in values if isinstance(v, float)]


def _download_base_series() -> dict[str, list[float]]:
    try:
        raw = yf.download(
            tickers=list(_BASE_TICKERS.values()),
            period="5d",
            interval="30m",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception:
        raw = None

    series: dict[str, list[float]] = {}
    if raw is None:
        return series

    for key, ticker in _BASE_TICKERS.items():
        try:
            if hasattr(raw.columns, "levels"):
                closes = raw[ticker]["Close"]
            else:
                closes = raw["Close"]
            values = [_safe_float(v, None) for v in closes.tolist()]
            series[key] = [v for v in values if isinstance(v, float)]
        except Exception:
            series[key] = []
    return series


def _interpolate(points: list[tuple[float, float]], x: float) -> float:
    points = sorted(points, key=lambda p: p[0])
    if not points:
        return 0.0
    if x <= points[0][0]:
        return points[0][1]
    if x >= points[-1][0]:
        return points[-1][1]
    for i in range(len(points) - 1):
        x1, y1 = points[i]
        x2, y2 = points[i + 1]
        if x1 <= x <= x2:
            if x2 == x1:
                return y1
            ratio = (x - x1) / (x2 - x1)
            return y1 + ratio * (y2 - y1)
    return points[-1][1]


def _signal_from_change(change: float, positive: str, negative: str) -> str:
    return positive if change >= 0 else negative


def _latest_and_change(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    last = values[-1]
    prev = values[-2] if len(values) > 1 else last
    return last, last - prev


def _build_payload() -> dict[str, Any]:
    base_series = _download_base_series()
    irx = base_series.get("irx", [])
    fvx = base_series.get("fvx", [])
    tnx = base_series.get("tnx", [])
    tyx = base_series.get("tyx", [])

    min_len = min(len(irx), len(fvx), len(tnx), len(tyx)) if irx and fvx and tnx and tyx else 0
    if min_len > 12:
        start = min_len - 12
    else:
        start = 0

    history_points = []
    for idx in range(start, min_len):
        base_points = [
            (0.25, _normalize_yield(irx[idx])),
            (5.0, _normalize_yield(fvx[idx])),
            (10.0, _normalize_yield(tnx[idx])),
            (30.0, _normalize_yield(tyx[idx])),
        ]
        history_points.append(base_points)

    tenor_histories: dict[str, list[float]] = {t["label"]: [] for t in _TENORS if not t.get("tips")}
    for base_points in history_points:
        for tenor in _TENORS:
            if tenor.get("tips"):
                continue
            label = tenor["label"]
            tenor_histories[label].append(_interpolate(base_points, tenor["years"]))

    tips_history = _fetch_history("DFII10", period="3mo", interval="1d")
    if len(tips_history) > 12:
        tips_history = tips_history[-12:]

    cards = []
    for tenor in _TENORS:
        label = tenor["label"]
        if tenor.get("tips"):
            series = tips_history
            last, delta = _latest_and_change(series)
        else:
            series = tenor_histories.get(label, [])
            last, delta = _latest_and_change(series)
        cards.append(
            {
                "id": label,
                "tenor": label,
                "name": tenor["name"],
                "yield": round(last, 3),
                "change_bps": round(delta * 100, 2),
                "sparkline": series,
            }
        )

    curve_tenors = [t["label"] for t in _TENORS if not t.get("tips")]
    today = []
    yesterday = []
    for tenor in curve_tenors:
        series = tenor_histories.get(tenor, [])
        last, delta = _latest_and_change(series)
        today.append(round(last, 3))
        yesterday.append(round(last - delta, 3))

    spreads = {}
    for key, meta in _SPREAD_SERIES.items():
        series = _fetch_history(meta["ticker"], period="3mo", interval="1d")
        last, delta = _latest_and_change(series)
        spreads[key] = {
            "value": round(last, 3),
            "change": round(delta, 4),
            "unit": meta["unit"],
        }

    def to_bps(value: float, unit: str) -> float:
        return value if unit == "bps" else value * 100

    two_y = next((c for c in cards if c["tenor"] == "2Y"), {"yield": 0, "change_bps": 0})
    ten_y = next((c for c in cards if c["tenor"] == "10Y"), {"yield": 0, "change_bps": 0})
    thirty_y = next((c for c in cards if c["tenor"] == "30Y"), {"yield": 0, "change_bps": 0})

    spreads_cards = [
        {
            "id": "2s10s",
            "label": "2s10s",
            "value": round((ten_y["yield"] - two_y["yield"]) * 100, 2),
            "unit": "bps",
            "change_bps": round(ten_y["change_bps"] - two_y["change_bps"], 2),
            "signal": _signal_from_change(
                ten_y["change_bps"] - two_y["change_bps"],
                "Steepening",
                "Flattening",
            ),
        },
        {
            "id": "10s30s",
            "label": "10s30s",
            "value": round((thirty_y["yield"] - ten_y["yield"]) * 100, 2),
            "unit": "bps",
            "change_bps": round(thirty_y["change_bps"] - ten_y["change_bps"], 2),
            "signal": _signal_from_change(
                thirty_y["change_bps"] - ten_y["change_bps"],
                "Steepening",
                "Flattening",
            ),
        },
        {
            "id": "ig_oas",
            "label": "IG OAS",
            "value": round(spreads["ig_oas"]["value"], 2),
            "unit": spreads["ig_oas"]["unit"],
            "change_bps": round(to_bps(spreads["ig_oas"]["change"], spreads["ig_oas"]["unit"]), 2),
            "signal": _signal_from_change(
                spreads["ig_oas"]["change"],
                "Widening",
                "Tightening",
            ),
        },
        {
            "id": "hy_oas",
            "label": "HY OAS",
            "value": round(spreads["hy_oas"]["value"], 2),
            "unit": spreads["hy_oas"]["unit"],
            "change_bps": round(to_bps(spreads["hy_oas"]["change"], spreads["hy_oas"]["unit"]), 2),
            "signal": _signal_from_change(
                spreads["hy_oas"]["change"],
                "Widening",
                "Tightening",
            ),
        },
        {
            "id": "real_10y",
            "label": "Real 10Y",
            "value": round(spreads["real_10y"]["value"], 2),
            "unit": spreads["real_10y"]["unit"],
            "change_bps": round(to_bps(spreads["real_10y"]["change"], spreads["real_10y"]["unit"]), 2),
            "signal": _signal_from_change(
                spreads["real_10y"]["change"],
                "Rising",
                "Falling",
            ),
        },
        {
            "id": "breakeven_10y",
            "label": "Breakeven 10Y",
            "value": round(spreads["breakeven_10y"]["value"], 2),
            "unit": spreads["breakeven_10y"]["unit"],
            "change_bps": round(to_bps(spreads["breakeven_10y"]["change"], spreads["breakeven_10y"]["unit"]), 2),
            "signal": _signal_from_change(
                spreads["breakeven_10y"]["change"],
                "Inflation Up",
                "Inflation Down",
            ),
        },
        {
            "id": "ted",
            "label": "TED Spread",
            "value": round(spreads["ted"]["value"], 2),
            "unit": spreads["ted"]["unit"],
            "change_bps": round(to_bps(spreads["ted"]["change"], spreads["ted"]["unit"]), 2),
            "signal": _signal_from_change(
                spreads["ted"]["change"],
                "Risk-off",
                "Risk-on",
            ),
        },
        {
            "id": "sofr",
            "label": "SOFR",
            "value": round(spreads["sofr"]["value"], 2),
            "unit": spreads["sofr"]["unit"],
            "change_bps": round(to_bps(spreads["sofr"]["change"], spreads["sofr"]["unit"]), 2),
            "signal": _signal_from_change(
                spreads["sofr"]["change"],
                "Hawkish",
                "Easing",
            ),
        },
    ]

    strip = [
        {
            "id": "10y",
            "label": "10Y UST",
            "value": round(ten_y["yield"], 2),
            "unit": "%",
            "change_bps": round(ten_y["change_bps"], 2),
        },
        {
            "id": "2y",
            "label": "2Y UST",
            "value": round(two_y["yield"], 2),
            "unit": "%",
            "change_bps": round(two_y["change_bps"], 2),
        },
        {
            "id": "2s10s",
            "label": "2s10s",
            "value": round((ten_y["yield"] - two_y["yield"]) * 100, 2),
            "unit": "bps",
            "change_bps": round(ten_y["change_bps"] - two_y["change_bps"], 2),
        },
        {
            "id": "ig_oas",
            "label": "IG OAS",
            "value": round(spreads["ig_oas"]["value"], 2),
            "unit": spreads["ig_oas"]["unit"],
            "change_bps": round(to_bps(spreads["ig_oas"]["change"], spreads["ig_oas"]["unit"]), 2),
        },
        {
            "id": "hy_oas",
            "label": "HY OAS",
            "value": round(spreads["hy_oas"]["value"], 2),
            "unit": spreads["hy_oas"]["unit"],
            "change_bps": round(to_bps(spreads["hy_oas"]["change"], spreads["hy_oas"]["unit"]), 2),
        },
        {
            "id": "fed_funds",
            "label": "Fed Funds",
            "value": round(spreads["fed_funds"]["value"], 2),
            "unit": spreads["fed_funds"]["unit"],
            "change_bps": round(to_bps(spreads["fed_funds"]["change"], spreads["fed_funds"]["unit"]), 2),
        },
    ]

    news = _fetch_news()

    return {
        "cards": cards,
        "curve": {"tenors": curve_tenors, "today": today, "yesterday": yesterday},
        "spreads": spreads_cards,
        "strip": strip,
        "news": news,
    }


def _fetch_news() -> list[dict[str, str]]:
    import requests

    rss_url = "https://news.google.com/rss/search?q=treasury+bonds+fed&hl=en-US&gl=US&ceid=US:en"
    try:
        res = requests.get(
            rss_url,
            headers={"Accept": "application/xml,text/xml", "User-Agent": "FalseMarkets/1.0"},
            timeout=10,
        )
        res.raise_for_status()
    except Exception:
        return []

    xml = res.text
    items = []
    for block in xml.split("<item>")[1:]:
        segment = block.split("</item>")[0]
        title = _extract_tag(segment, "title")
        link = _extract_tag(segment, "link")
        pub_date = _extract_tag(segment, "pubDate")
        source = _extract_source(segment)
        if not title or not link:
            continue
        items.append(
            {
                "title": title,
                "link": link,
                "tag": _detect_tag(title),
                "publisher": source or "Google News",
                "timeAgo": _format_time_ago(pub_date),
            }
        )
    return items[:10]


def _decode_entities(value: str) -> str:
    return (
        value.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )


def _strip_cdata(value: str) -> str:
    return value.replace("<![CDATA[", "").replace("]]>", "").strip()


def _extract_tag(block: str, tag: str) -> str:
    match = re.search(rf"<{tag}>([\s\S]*?)</{tag}>", block, re.IGNORECASE)
    if not match:
        return ""
    return _decode_entities(_strip_cdata(match.group(1)))


def _extract_source(block: str) -> str:
    match = re.search(r"<source[^>]*>([\s\S]*?)</source>", block, re.IGNORECASE)
    if not match:
        return ""
    return _decode_entities(_strip_cdata(match.group(1)))


def _format_time_ago(raw: str) -> str:
    try:
        dt = time.mktime(time.strptime(raw, "%a, %d %b %Y %H:%M:%S %Z"))
    except Exception:
        return ""
    diff = time.time() - dt
    minutes = max(int(diff / 60), 1)
    if minutes < 60:
        return f"{minutes}m ago"
    hours = int(minutes / 60)
    if hours < 24:
        return f"{hours}h ago"
    days = int(hours / 24)
    return f"{days}d ago"


def _detect_tag(title: str) -> str:
    lower = title.lower()
    if "fed" in lower:
        return "FED"
    if "10-year" in lower or "10y" in lower:
        return "10Y"
    if "curve" in lower:
        return "CURVE"
    if "credit" in lower or "spread" in lower:
        return "CREDIT"
    if "high yield" in lower or "junk" in lower:
        return "HY"
    if "tips" in lower:
        return "TIPS"
    if "mbs" in lower or "mortgage" in lower:
        return "MBS"
    if "global" in lower or "europe" in lower or "china" in lower:
        return "GLOBAL"
    return "FED"


@router.get("/dashboard")
async def fixed_income_dashboard():
    now = time.time()
    cached = _CACHE.get("payload")
    if cached and now - _CACHE["timestamp"] < _CACHE_TTL:
        return cached

    payload = _build_payload()
    _CACHE["timestamp"] = now
    _CACHE["payload"] = payload
    return payload
