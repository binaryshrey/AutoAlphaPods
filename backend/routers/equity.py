import re
import time
from typing import Any

import yfinance as yf
from fastapi import APIRouter

router = APIRouter(prefix="/equity", tags=["Equity"])

_INDEX_CONFIG = [
    {"ticker": "^GSPC", "symbol": "SPX", "name": "S&P 500"},
    {"ticker": "^NDX", "symbol": "NDX", "name": "NASDAQ 100"},
    {"ticker": "^DJI", "symbol": "DJIA", "name": "Dow Jones"},
    {"ticker": "^RUT", "symbol": "RUT", "name": "Russell 2000"},
    {"ticker": "^VIX", "symbol": "VIX", "name": "CBOE Volatility"},
    {"ticker": "^SOX", "symbol": "SOX", "name": "Semiconductors"},
    {"ticker": "BKX", "symbol": "BKX", "name": "KBW Banks"},
    {"ticker": "XBI", "symbol": "XBI", "name": "Biotech"},
    {"ticker": "GLD", "symbol": "GLD", "name": "Gold ETF"},
    {"ticker": "TLT", "symbol": "TLT", "name": "20Y+ Treasuries"},
]

_SECTOR_CONFIG = [
    {"ticker": "XLK", "name": "Technology"},
    {"ticker": "XLF", "name": "Financials"},
    {"ticker": "XLV", "name": "Health Care"},
    {"ticker": "XLY", "name": "Consumer Disc."},
    {"ticker": "XLI", "name": "Industrials"},
    {"ticker": "XLE", "name": "Energy"},
    {"ticker": "XLB", "name": "Materials"},
    {"ticker": "XLU", "name": "Utilities"},
    {"ticker": "XLRE", "name": "Real Estate"},
    {"ticker": "XLC", "name": "Comm Services"},
    {"ticker": "XLP", "name": "Consumer Staples"},
]

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


def _latest_and_change(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    last = values[-1]
    prev = values[-2] if len(values) > 1 else last
    return last, last - prev


def _download_history(tickers: list[str], period: str, interval: str) -> Any:
    try:
        return yf.download(
            tickers=tickers,
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception:
        return None


def _extract_series(raw: Any, ticker: str, field: str) -> list[float]:
    if raw is None:
        return []
    try:
        if hasattr(raw.columns, "levels"):
            series = raw[ticker][field]
        else:
            series = raw[field]
    except Exception:
        return []
    values = [_safe_float(v, None) for v in series.tolist()]
    return [v for v in values if isinstance(v, float)]


def _fetch_index_cards() -> list[dict[str, Any]]:
    tickers = [item["ticker"] for item in _INDEX_CONFIG]
    raw = _download_history(tickers, period="5d", interval="30m")

    cards = []
    for meta in _INDEX_CONFIG:
        ticker = meta["ticker"]
        closes = _extract_series(raw, ticker, "Close")
        volumes = _extract_series(raw, ticker, "Volume")
        last, delta = _latest_and_change(closes)
        prev = last - delta
        change_pct = (delta / prev * 100) if prev else 0.0
        cards.append(
            {
                "id": meta["symbol"],
                "symbol": meta["symbol"],
                "name": meta["name"],
                "price": round(last, 2),
                "change_percent": round(change_pct, 2),
                "change_abs": round(delta, 2),
                "sparkline": closes[-40:],
                "volume": volumes[-1] if volumes else 0.0,
            }
        )
    return cards


def _fetch_sector_performance() -> list[dict[str, Any]]:
    tickers = [item["ticker"] for item in _SECTOR_CONFIG]
    raw = _download_history(tickers, period="5d", interval="1d")
    sectors = []
    for meta in _SECTOR_CONFIG:
        closes = _extract_series(raw, meta["ticker"], "Close")
        last, delta = _latest_and_change(closes)
        prev = last - delta
        change_pct = (delta / prev * 100) if prev else 0.0
        sectors.append(
            {
                "symbol": meta["ticker"],
                "name": meta["name"],
                "change_percent": round(change_pct, 2),
            }
        )
    return sectors


def _fetch_fear_greed() -> float:
    try:
        import requests

        res = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            timeout=10,
        )
        if res.ok:
            payload = res.json()
            return _safe_float(payload.get("fear_and_greed", {}).get("now", {}).get("value", 0))
    except Exception:
        return 0.0
    return 0.0


def _fetch_adv_dec_ratio() -> float:
    raw = _download_history(["^ADVN", "^ADVD"], period="5d", interval="1d")
    adv = _extract_series(raw, "^ADVN", "Close")
    dec = _extract_series(raw, "^ADVD", "Close")
    adv_last = adv[-1] if adv else 0.0
    dec_last = dec[-1] if dec else 0.0
    if dec_last == 0:
        return 0.0
    return round(adv_last / dec_last, 2)


def _build_indicators(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    vix = next((c for c in cards if c["symbol"] == "VIX"), None)
    fear = _fetch_fear_greed()
    adv_dec = _fetch_adv_dec_ratio()

    indicators = [
        {
            "id": "vix",
            "label": "VIX",
            "value": vix["price"] if vix else 0.0,
            "unit": "",
            "change": vix["change_abs"] if vix else 0.0,
            "signal": "Risk-on" if (vix and vix["change_abs"] < 0) else "Risk-off",
        },
        {
            "id": "fear_greed",
            "label": "Fear & Greed",
            "value": round(fear, 0),
            "unit": "index",
            "change": 0.0,
            "signal": "Neutral",
        },
        {
            "id": "put_call",
            "label": "Put/Call",
            "value": 0.95,
            "unit": "ratio",
            "change": 0.0,
            "signal": "Neutral",
        },
        {
            "id": "aaii",
            "label": "AAII Bull %",
            "value": 32.0,
            "unit": "%",
            "change": 0.0,
            "signal": "Cautious",
        },
        {
            "id": "adv_dec",
            "label": "Advance/Decline",
            "value": adv_dec,
            "unit": "ratio",
            "change": 0.0,
            "signal": "Breadth",
        },
        {
            "id": "high_low",
            "label": "52W High/Low",
            "value": 1.1,
            "unit": "ratio",
            "change": 0.0,
            "signal": "Stable",
        },
        {
            "id": "margin_debt",
            "label": "Margin Debt",
            "value": 0.0,
            "unit": "bn",
            "change": 0.0,
            "signal": "N/A",
        },
        {
            "id": "short_interest",
            "label": "Short Interest",
            "value": 2.4,
            "unit": "%",
            "change": 0.0,
            "signal": "Neutral",
        },
    ]
    return indicators


def _fetch_forward_pe() -> float:
    try:
        info = yf.Ticker("SPY").info
        return _safe_float(info.get("forwardPE", 0.0))
    except Exception:
        return 0.0


def _build_strip(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def pick(symbol: str) -> dict[str, Any] | None:
        return next((c for c in cards if c["symbol"] == symbol), None)

    spx = pick("SPX")
    ndx = pick("NDX")
    dji = pick("DJIA")
    rut = pick("RUT")
    vix = pick("VIX")
    forward_pe = _fetch_forward_pe()
    adv_dec = _fetch_adv_dec_ratio()

    def strip_item(label: str, card: dict[str, Any] | None):
        if not card:
            return {"label": label, "value": 0.0, "change": 0.0, "unit": ""}
        return {
            "label": label,
            "value": card["price"],
            "change": card["change_percent"],
            "unit": "%",
        }

    strip = [
        strip_item("SPX", spx),
        strip_item("NASDAQ", ndx),
        strip_item("DOW", dji),
        strip_item("RUSSELL", rut),
        {"label": "VIX", "value": vix["price"] if vix else 0.0, "change": vix["change_percent"] if vix else 0.0, "unit": "%"},
        {"label": "Forward P/E", "value": round(forward_pe, 1), "change": 0.0, "unit": "x"},
        {"label": "Adv/Dec", "value": adv_dec, "change": 0.0, "unit": "ratio"},
    ]
    return strip


def _fetch_earnings() -> list[dict[str, Any]]:
    return [
        {"symbol": "AAPL", "name": "Apple", "eps_est": 1.28, "eps_act": 1.31, "beat": True, "timing": "AMC"},
        {"symbol": "MSFT", "name": "Microsoft", "eps_est": 2.78, "eps_act": 2.74, "beat": False, "timing": "AMC"},
        {"symbol": "NVDA", "name": "NVIDIA", "eps_est": 5.42, "eps_act": 5.61, "beat": True, "timing": "BMO"},
        {"symbol": "JPM", "name": "JPMorgan", "eps_est": 3.85, "eps_act": 3.92, "beat": True, "timing": "BMO"},
        {"symbol": "XOM", "name": "Exxon Mobil", "eps_est": 2.14, "eps_act": 2.05, "beat": False, "timing": "BMO"},
    ]


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
    if "spx" in lower or "s&p" in lower:
        return "SPX"
    if "nasdaq" in lower or "ndx" in lower:
        return "NDX"
    if "fed" in lower:
        return "FED"
    if "bank" in lower or "financial" in lower:
        return "FINS"
    if "sox" in lower or "semiconductor" in lower or "nvda" in lower:
        return "SOX"
    if "vix" in lower:
        return "VIX"
    if "xbi" in lower or "biotech" in lower:
        return "XBI"
    for sector in ["xlk", "xlf", "xlv", "xly", "xli", "xle", "xlb", "xlu", "xlre", "xlc", "xlp"]:
        if sector in lower:
            return sector.upper()
    return "SPX"


def _fetch_news() -> list[dict[str, str]]:
    import requests

    rss_url = "https://news.google.com/rss/search?q=stock+market+equity&hl=en-US&gl=US&ceid=US:en"
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


def _build_payload() -> dict[str, Any]:
    cards = _fetch_index_cards()
    sectors = _fetch_sector_performance()
    indicators = _build_indicators(cards)
    strip = _build_strip(cards)
    earnings = _fetch_earnings()
    news = _fetch_news()

    return {
        "cards": cards,
        "sectors": sectors,
        "indicators": indicators,
        "strip": strip,
        "earnings": earnings,
        "news": news,
    }


@router.get("/dashboard")
async def equity_dashboard():
    now = time.time()
    cached = _CACHE.get("payload")
    if cached and now - _CACHE["timestamp"] < _CACHE_TTL:
        return cached

    payload = _build_payload()
    _CACHE["timestamp"] = now
    _CACHE["payload"] = payload
    return payload
