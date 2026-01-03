# app/stream_manager.py
import asyncio
import json
import time
from typing import Dict, Set, Any, Optional

import websockets

from ticker_pb2 import Ticker  # у тебя уже есть
import base64


YAHOO_WS_URL = "wss://streamer.finance.yahoo.com/"


def deserialize(message: str):
    ticker_ = Ticker()
    message_bytes = base64.b64decode(message)
    ticker_.ParseFromString(message_bytes)
    return ticker_.id, ticker_


class StreamManager:
    def __init__(self):
        self._symbols: Set[str] = set()
        self._latest: Dict[str, Dict[str, Any]] = {}  # symbol -> payload
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._task:
            await asyncio.wait([self._task], timeout=5)

    async def subscribe(self, symbols: Set[str]):
        symbols = set(s.upper() for s in symbols)

        async with self._lock:
            new_symbols = symbols - self._symbols
            self._symbols |= symbols

        # если WS уже подключён — отправляем subscribe сразу
        if new_symbols and hasattr(self, "_ws") and self._ws:
            await self._ws.send(
                json.dumps({"subscribe": list(new_symbols)})
            )

    async def unsubscribe(self, symbols: Set[str]):
        async with self._lock:
            self._symbols -= set(s.upper() for s in symbols)

    async def get_latest(self, symbol: str):
        symbol = symbol.upper()
        async with self._lock:
            return self._latest.get(symbol)

    async def get_latest_many(self, symbols: Set[str]):
        async with self._lock:
            return {s: self._latest.get(s) for s in symbols}

    async def _run(self):
        backoff = 1
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                        YAHOO_WS_URL,
                        ping_interval=20,
                        ping_timeout=20
                ) as ws:

                    self._ws = ws  # ← ТУТ правильно

                    backoff = 1

                    async with self._lock:
                        current = list(self._symbols)

                    if current:
                        await ws.send(json.dumps({"subscribe": current}))
                        try:
                            await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            pass

                    while not self._stop.is_set():
                        msg = await ws.recv()
                        _id, ticker = deserialize(msg)

                        # print(
                        #     "WS MESSAGE:",
                        #     "id=", ticker.id,
                        #     "quoteType=", Ticker.QuoteType.Name(ticker.quoteType)
                        # )

                        if ticker.quoteType == Ticker.QuoteType.HEARTBEAT:
                            continue

                        symbol = (ticker.id or "").upper().strip()
                        if not symbol:
                            continue

                        payload = {
                            "id": symbol,
                            "price": float(ticker.price),
                            "time": int(ticker.time) if ticker.time else int(time.time()),
                            "currency": ticker.currency,
                            "exchange": ticker.exchange,
                            "quoteType": Ticker.QuoteType.Name(ticker.quoteType),
                            "marketHours": Ticker.MarketHoursType.Name(ticker.marketHours),
                            "change": float(ticker.change),
                            "changePercent": float(ticker.changePercent),
                            "dayVolume": int(ticker.dayVolume),
                            "dayHigh": float(ticker.dayHigh),
                            "dayLow": float(ticker.dayLow),
                        }

                        async with self._lock:
                            self._latest[symbol] = payload

                # 👇 ВОТ СЮДА. РОВНО СЮДА.
                self._ws = None

            except Exception as e:
                self._ws = None  # ← и ТУТ тоже (на всякий случай)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
