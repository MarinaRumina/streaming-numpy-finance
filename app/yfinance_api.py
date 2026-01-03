# app/yfinance_api.py
import yfinance as yf
import time


def get_quote_fallback(symbol: str) -> dict:
    t = yf.Ticker(symbol)

    # 1) Пытаемся fast_info (самый быстрый и стабильный вариант)
    try:
        fi = t.fast_info
        price = fi.get("last_price") or fi.get("regular_market_price")
        if price:
            return {
                "id": symbol,
                "price": float(price),
                "currency": fi.get("currency"),
                "exchange": fi.get("exchange"),
                "time": int(time.time() * 1000),
                "source": "yfinance_fast_info",
                "delayed": True,
            }
    except Exception:
        pass

    # 2) Fallback: history (медленнее, но почти всегда работает)
    try:
        hist = t.history(period="1d", interval="1m")
        if not hist.empty:
            last = hist.iloc[-1]
            return {
                "id": symbol,
                "price": float(last["Close"]),
                "currency": None,
                "exchange": None,
                "time": int(time.time() * 1000),
                "source": "yfinance_history",
                "delayed": True,
            }
    except Exception:
        pass

    raise RuntimeError("yfinance fallback failed")



def get_history(symbol: str, period: str, interval: str):
    t = yf.Ticker(symbol)
    df = t.history(period=period, interval=interval)
    # JSON-friendly
    return {
        "symbol": symbol.upper(),
        "rows": [
            {
                "time": idx.isoformat(),
                "open": float(row["Open"]) if row["Open"] == row["Open"] else None,
                "high": float(row["High"]) if row["High"] == row["High"] else None,
                "low": float(row["Low"]) if row["Low"] == row["Low"] else None,
                "close": float(row["Close"]) if row["Close"] == row["Close"] else None,
                "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else None,
            }
            for idx, row in df.iterrows()
        ]
    }

def get_info(symbol: str):
    t = yf.Ticker(symbol)
    # WARNING: .info бывает медленный/ломкий у Yahoo, лучше кешировать
    return {"symbol": symbol.upper(), "info": t.info}
