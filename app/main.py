# app/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

from app.stream_manager import StreamManager
from app.yfinance_api import get_history, get_info


app = FastAPI(title="Yahoo Stream REST API", version="1.0.0")
stream = StreamManager()


class SymbolsBody(BaseModel):
    symbols: List[str]


@app.on_event("startup")
async def startup():
    await stream.start()


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/v1/stream/subscribe")
async def subscribe(body: SymbolsBody):
    await stream.subscribe(set(body.symbols))
    return {"subscribed": [s.upper() for s in body.symbols]}


@app.post("/v1/stream/unsubscribe")
async def unsubscribe(body: SymbolsBody):
    await stream.unsubscribe(set(body.symbols))
    return {"unsubscribed": [s.upper() for s in body.symbols]}


from time import time as _time

from fastapi import HTTPException
from time import time as _time

from app.yfinance_api import get_quote_fallback


@app.get("/v1/quote/{symbol}")
async def quote(symbol: str):
    symbol = symbol.upper()

    # 1) Пытаемся отдать realtime из Yahoo WS
    data = await stream.get_latest(symbol)
    if data:
        return {
            **data,
            "source": "yahoo_ws",
            "delayed": False,
        }

    # 2) Fallback: yfinance (fast_info → history)
    try:
        return get_quote_fallback(symbol)

    except Exception:
        raise HTTPException(
            status_code=404,
            detail="No realtime stream for this symbol and yfinance fallback failed."
        )


@app.get("/v1/quotes")
async def quotes(symbols: str):
    sym_set = set(s.strip().upper() for s in symbols.split(",") if s.strip())
    data = await stream.get_latest_many(sym_set)
    return data


@app.get("/v1/history/{symbol}")
async def history(symbol: str, period: str = "5d", interval: str = "1m"):
    return get_history(symbol, period=period, interval=interval)


@app.get("/v1/info/{symbol}")
async def info(symbol: str):
    return get_info(symbol)
