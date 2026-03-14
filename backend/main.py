from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Query
from typing import List
from datetime import datetime

app = FastAPI(title="FastAPI Starter")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Welcome to FastAPI Starter"}

@app.get("/health")
async def health():
    return {"status": "alive"}


RANGE_MAP = {
    "1D": {"period": "1d", "interval": "5m"},
    "1W": {"period": "5d", "interval": "30m"},
    "1M": {"period": "1mo", "interval": "1d"},
    "3M": {"period": "3mo", "interval": "1d"},
    "1Y": {"period": "1y", "interval": "1wk"},
}


@app.get("/yfinance/detail")
async def yfinance_detail(symbol: str = Query(...), range: str = Query("1D")):
    try:
        import yfinance as yf
    except Exception:
        return {"error": "yfinance not installed"}

    symbol = symbol.strip().upper()
    if not symbol:
        return {"error": "symbol required"}

    cfg = RANGE_MAP.get(range, RANGE_MAP["1D"])
    ticker = yf.Ticker(symbol)

    try:
        hist = ticker.history(period=cfg["period"], interval=cfg["interval"])
        fast_info = ticker.fast_info
        full_info = ticker.info if hasattr(ticker, "info") else {}
    except Exception as e:
        return {"error": f"yfinance fetch failed: {e}"}

    def fast_get(key, default=0):
        try:
            if hasattr(fast_info, "get"):
                value = fast_info.get(key)
            else:
                value = fast_info[key]
            return default if value is None else value
        except Exception:
            return default

    prices: List[List[float]] = []
    if hist is not None and not hist.empty:
        for idx, row in hist.iterrows():
            ts = idx.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=None)
            prices.append([int(ts.timestamp() * 1000), float(row["Close"])])

    current_price = float(fast_get("last_price", prices[-1][1] if prices else 0))
    day_high = float(fast_get("day_high", 0))
    day_low = float(fast_get("day_low", 0))
    market_cap = float(fast_get("market_cap", 0))
    volume = float(fast_get("last_volume", 0))
    previous_close = float(fast_get("previous_close", current_price or 0))

    change_abs = current_price - previous_close
    change_pct = (change_abs / previous_close * 100) if previous_close else 0

    if hist is not None and not hist.empty:
        ath = float(hist["High"].max())
        atl = float(hist["Low"].min())
    else:
        ath = 0
        atl = 0

    return {
        "id": symbol.lower(),
        "name": full_info.get("longName") or full_info.get("shortName") or symbol,
        "symbol": symbol,
        "image": None,
        "current_price": current_price,
        "price_change_24h": change_abs,
        "price_change_percentage_24h": change_pct,
        "market_cap": market_cap,
        "total_volume": volume,
        "high_24h": day_high,
        "low_24h": day_low,
        "ath": ath,
        "atl": atl,
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "prices": prices,
        "asset_type": "equity",
    }
