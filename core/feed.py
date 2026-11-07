from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

LOG = logging.getLogger("v7.feed")


def now_ms() -> int:
    return int(time.time() * 1000)


def _as_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"", "null", "none", "nan"}:
            return False
        if s in {"1", "true", "t", "yes", "y", "up", "winner_up", "winner-up"}:
            return True
        if s in {"0", "false", "f", "no", "n", "down", "winner_down", "winner-down"}:
            return False
    return bool(value)


@dataclass
class Market:
    slug: str
    condition_id: str
    up_token: str
    down_token: str
    start_ms: int
    end_ms: int
    question: str
    neg_risk: bool = False


@dataclass
class Tick:
    token_id: str
    mid: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    ready: bool = False
    last_update_ms: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
    elif isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in {"null", "none", "nan"}:
            return None
        try:
            v = float(s)
        except Exception:
            return None
    else:
        try:
            v = float(value)
        except Exception:
            return None

    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


class BinanceFeed:
    """Binance ticker feed with controlled logging to avoid terminal spam."""

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self._spot: float | None = None
        self._best_bid: float | None = None
        self._best_ask: float | None = None
        self._task: asyncio.Task | None = None
        self._closed = False

        self._last_log_spot: float | None = None
        self._last_log_ts = 0.0
        self._log_interval_s = 5.0
        self._log_change_threshold = 0.5

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._closed = False
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        backoff = 1.0
        while not self._closed:
            try:
                import websockets

                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        if self._closed:
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        self._apply_message(msg)
                        backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                LOG.warning("[Binance] websocket reconnect in %.1fs due to %s", backoff, exc)
                if self._closed:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

        self._spot = None
        self._best_bid = None
        self._best_ask = None
        self._last_log_spot = None
        self._last_log_ts = 0.0

    def _should_log_spot(self, spot: float) -> bool:
        now_ts = time.monotonic()
        if self._last_log_spot is None:
            self._last_log_spot = spot
            self._last_log_ts = now_ts
            return True

        delta = abs(spot - self._last_log_spot)
        if delta >= self._log_change_threshold or (now_ts - self._last_log_ts) >= self._log_interval_s:
            self._last_log_spot = spot
            self._last_log_ts = now_ts
            return True

        return False

    def _apply_message(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return

        bid = _parse_float(msg.get("b"))
        ask = _parse_float(msg.get("a"))
        if bid is None and ask is None:
            bid = _parse_float(msg.get("best_bid"))
            ask = _parse_float(msg.get("best_ask"))

        if bid is not None:
            self._best_bid = bid
        if ask is not None:
            self._best_ask = ask

        if bid is not None and ask is not None:
            self._spot = (bid + ask) / 2.0
        elif bid is not None:
            self._spot = bid
        elif ask is not None:
            self._spot = ask

        if self._spot is not None and self._should_log_spot(self._spot):
            LOG.info(
                "[Binance] spot_now=%.8f bid=%.8f ask=%.8f",
                self._spot,
                self._best_bid or 0.0,
                self._best_ask or 0.0,
            )

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    def spot(self) -> float | None:
        return self._spot

    @property
    def best_bid(self) -> float | None:
        return self._best_bid

    @property
    def best_ask(self) -> float | None:
        return self._best_ask


class PolymarketFeed:
    """PM CLOB feed with strict book parsing and explicit ready=True on first valid price."""

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.market_resolution_cache: dict[str, str] = {}
        self.asset_resolution_cache: dict[str, str] = {}
        self._ticks: dict[str, Tick] = {}
        self._ws = None
        self._task: asyncio.Task | None = None
        self._closed = False
        self._subscribe_queue: list[str] = []

        self._last_message_ts = time.monotonic()
        self._watchdog_timeout_s = 120.0
        self._heartbeat_interval_s = 10.0
        self._watchdog_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None

    async def _cancel_task(self, task: asyncio.Task | None) -> None:
        if task is None:
            return
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        task = None

    async def _watchdog_loop(self, ws) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(1.0)
                if time.monotonic() - self._last_message_ts > self._watchdog_timeout_s:
                    LOG.warning(
                        "[PM] watchdog timeout: no message for %.1fs, reconnecting",
                        time.monotonic() - self._last_message_ts,
                    )
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    return
        except asyncio.CancelledError:
            pass

    async def _heartbeat_loop(self, ws) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._heartbeat_interval_s)
                if self._closed:
                    return
                try:
                    if ws.closed:
                        return
                except Exception:
                    return
                try:
                    await ws.send("PING")
                    LOG.debug("[PM] heartbeat sent: PING")
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("[PM] heartbeat failed: %s", exc)
                    return
        except asyncio.CancelledError:
            pass

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._closed = False
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        backoff = 1.0
        while not self._closed:
            try:
                import websockets

                async with websockets.connect(self.ws_url, ping_interval=None, ping_timeout=20) as ws:
                    self._ws = ws
                    self._last_message_ts = time.monotonic()

                    self._watchdog_task = asyncio.create_task(self._watchdog_loop(ws))
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))

                    try:
                        if self._subscribe_queue:
                            await self._send_subscription(ws, self._subscribe_queue)

                        async for raw in ws:
                            if self._closed:
                                break

                            self._last_message_ts = time.monotonic()

                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue

                            self.on_message(msg)
                            backoff = 1.0
                    finally:
                        await self._cancel_task(self._watchdog_task)
                        await self._cancel_task(self._heartbeat_task)
                        self._watchdog_task = None
                        self._heartbeat_task = None

                    self._ws = None
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                LOG.warning("[PM] websocket reconnect in %.1fs due to %s", backoff, exc)
                if self._closed:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

        self._ws = None
        self._last_message_ts = time.monotonic()

    async def _send_subscription(self, ws, token_ids: list[str] | str) -> None:
        if isinstance(token_ids, str):
            raw_ids = [token_ids]
        else:
            raw_ids = list(token_ids)
        ids = [str(t) for t in raw_ids if t]
        ids = list(dict.fromkeys(ids))
        if not ids:
            return
        try:
            payload = {
                "assets_ids": ids,
                "type": "market",
            }
            await ws.send(json.dumps(payload))
            LOG.info("[PM] subscribed tokens=%s", ids)
        except Exception as exc:
            LOG.warning("[PM] subscribe failed tokens=%s err=%s", ids, exc)

    async def stop(self) -> None:
        self._closed = True
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def subscribe(self, token_ids: list[str]) -> list[str]:
        ids = [str(t) for t in token_ids if t]
        if not ids:
            return []

        for token in ids:
            self._ticks.setdefault(
                token,
                Tick(token_id=token, mid=None, best_bid=None, best_ask=None, ready=False),
            )
            if token not in self._subscribe_queue:
                self._subscribe_queue.append(token)

        if self._ws is not None:
            try:
                await self._send_subscription(self._ws, ids)
            except Exception:
                pass

        return ids

    def get_tick(self, token_id: str, force_refresh: bool = False, timeout: float = 5.0) -> Tick | None:
        token = str(token_id)
        tick = self._ticks.get(token)
        if tick is None:
            tick = Tick(token_id=token, mid=None, best_bid=None, best_ask=None, ready=False)
            self._ticks[token] = tick
        return tick

    def _extract_token_from_message(self, msg: dict[str, Any]) -> str | None:
        if not isinstance(msg, dict):
            return None

        for key in ("asset_id", "assetId", "token_id", "tokenId", "token", "id"):
            value = msg.get(key)
            if value is not None:
                return str(value)

        data = msg.get("data")
        if isinstance(data, dict):
            for key in ("asset_id", "assetId", "token_id", "tokenId", "token", "id"):
                value = data.get(key)
                if value is not None:
                    return str(value)

        for key in ("book", "market", "book_data"):
            value = msg.get(key)
            if isinstance(value, dict):
                for k in ("asset_id", "assetId", "token_id", "tokenId", "token", "id"):
                    v = value.get(k)
                    if v is not None:
                        return str(v)

        changes = msg.get("price_changes")
        if isinstance(changes, list):
            for change in changes:
                if not isinstance(change, dict):
                    continue
                for key in ("asset_id", "assetId", "token_id", "tokenId", "token", "id"):
                    value = change.get(key)
                    if value is not None:
                        return str(value)

        return None

    def _extract_size_from_level(self, level: Any) -> float | None:
        if isinstance(level, dict):
            for key in ("size", "quantity", "q", "amount"):
                if key in level:
                    value = _parse_float(level[key])
                    if value is not None:
                        return value
            if "1" in level:
                return _parse_float(level["1"])
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            return _parse_float(level[1])
        return None

    def _parse_levels(self, raw_levels: Any) -> dict[float, float]:
        levels: dict[float, float] = {}
        if isinstance(raw_levels, dict):
            for price_key, size_value in raw_levels.items():
                price = _parse_float(price_key)
                if isinstance(size_value, dict):
                    size = self._extract_size_from_level(size_value)
                else:
                    size = _parse_float(size_value)
                if price is not None and size is not None and size > 0:
                    levels[price] = size
            return levels

        if not isinstance(raw_levels, (list, tuple)):
            return levels

        for level in raw_levels:
            price = self._extract_price_from_level(level)
            size = self._extract_size_from_level(level)
            if price is not None and size is not None and size > 0:
                levels[price] = size
        return levels

    def _refresh_tick_top(self, tick: Tick) -> None:
        tick.best_bid = max(tick.bids) if tick.bids else None
        tick.best_ask = min(tick.asks) if tick.asks else None
        if tick.best_bid is not None and tick.best_ask is not None:
            tick.mid = (tick.best_bid + tick.best_ask) / 2.0
        else:
            tick.mid = None

    def _event_type(self, msg: Any) -> str | None:
        if not isinstance(msg, dict):
            return None

        for key in ("event_type", "eventType", "type"):
            value = msg.get(key)
            if value is not None:
                return str(value).lower()

        for key in ("data", "book", "payload", "snapshot", "result"):
            value = msg.get(key)
            if isinstance(value, dict):
                nested = self._event_type(value)
                if nested is not None:
                    return nested

        return None

    def _looks_like_book(self, obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False

        keys = set(obj.keys())
        if {"bids", "asks"} & keys or "best_bid" in obj or "best_ask" in obj or "mid" in obj:
            return True

        for key in ("data", "book", "payload", "snapshot"):
            value = obj.get(key)
            if isinstance(value, dict) and self._looks_like_book(value):
                return True

        return False

    def _find_book_candidate(self, msg: Any) -> dict[str, Any] | None:
        if not isinstance(msg, dict):
            return None

        keys = set(msg.keys())
        if (
            {"bids", "asks"} & keys
            or "best_bid" in keys
            or "best_ask" in keys
            or "mid" in keys
        ):
            return msg

        for key in ("data", "book", "payload", "snapshot", "result"):
            value = msg.get(key)
            if isinstance(value, dict):
                nested = self._find_book_candidate(value)
                if nested is not None:
                    return nested

        for value in msg.values():
            if isinstance(value, dict):
                nested = self._find_book_candidate(value)
                if nested is not None:
                    return nested

        return None

    def _extract_price_from_level(self, level: Any) -> float | None:
        if level is None:
            return None

        if isinstance(level, (int, float, str)):
            return _parse_float(level)

        if isinstance(level, dict):
            for key in ("price", "p", "bid", "ask", "best_bid", "best_ask"):
                val = level.get(key)
                if val is not None:
                    parsed = _parse_float(val)
                    if parsed is not None:
                        return parsed

            for key in ("0", "1"):
                if key in level:
                    parsed = _parse_float(level[key])
                    if parsed is not None:
                        return parsed

        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return _parse_float(level[0])

        return None

    def _extract_explicit_quote(
        self,
        payload: dict[str, Any],
    ) -> tuple[float | None, float | None, float | None]:
        bid = payload.get("best_bid")
        ask = payload.get("best_ask")
        mid = payload.get("mid")

        if bid is None:
            bid = payload.get("bid")
        if ask is None:
            ask = payload.get("ask")

        if bid is None:
            bid = payload.get("bestBid")
        if ask is None:
            ask = payload.get("bestAsk")
        if mid is None:
            mid = payload.get("midPrice")

        return _parse_float(bid), _parse_float(ask), _parse_float(mid)

    def _extract_price_fields(self, payload: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
        bid, ask, mid = self._extract_explicit_quote(payload)
        if bid is not None or ask is not None or mid is not None:
            return bid, ask, mid

        if bid is None and isinstance(payload.get("bids"), list) and payload["bids"]:
            candidates: list[float] = []
            for item in payload["bids"]:
                price = self._extract_price_from_level(item)
                if price is not None:
                    candidates.append(price)
            if candidates:
                bid = max(candidates)

        if ask is None and isinstance(payload.get("asks"), list) and payload["asks"]:
            candidates: list[float] = []
            for item in payload["asks"]:
                price = self._extract_price_from_level(item)
                if price is not None:
                    candidates.append(price)
            if candidates:
                ask = min(candidates)

        if bid is None and isinstance(payload.get("bids"), dict):
            candidates: list[float] = []
            for v in payload["bids"].values():
                if isinstance(v, dict):
                    price = self._extract_price_from_level(v)
                    if price is not None:
                        candidates.append(price)
                else:
                    price = _parse_float(v)
                    if price is not None:
                        candidates.append(price)
            if candidates:
                bid = max(candidates)

        if ask is None and isinstance(payload.get("asks"), dict):
            candidates: list[float] = []
            for v in payload["asks"].values():
                if isinstance(v, dict):
                    price = self._extract_price_from_level(v)
                    if price is not None:
                        candidates.append(price)
                else:
                    price = _parse_float(v)
                    if price is not None:
                        candidates.append(price)
            if candidates:
                ask = min(candidates)

        return _parse_float(bid), _parse_float(ask), _parse_float(mid)

    def apply_book_update(self, msg: dict[str, Any], snapshot: bool = False) -> None:
        if not isinstance(msg, dict):
            return

        payload = self._find_book_candidate(msg)
        if payload is None:
            payload = msg

        token = self._extract_token_from_message(payload) or self._extract_token_from_message(msg)
        if token is None:
            return

        tick = self.get_tick(token)
        if tick is None:
            return

        raw_bids = payload.get("bids") if isinstance(payload, dict) else None
        raw_asks = payload.get("asks") if isinstance(payload, dict) else None

        if raw_bids is not None or raw_asks is not None:
            if raw_bids is not None:
                tick.bids = self._parse_levels(raw_bids)
            if raw_asks is not None:
                tick.asks = self._parse_levels(raw_asks)
            self._refresh_tick_top(tick)
            bid, ask, mid = self._extract_explicit_quote(payload)
        else:
            bid, ask, mid = self._extract_price_fields(payload)
        if bid is not None:
            tick.best_bid = bid
        if ask is not None:
            tick.best_ask = ask
        if mid is not None:
            tick.mid = mid
        elif tick.best_bid is not None and tick.best_ask is not None:
            tick.mid = (tick.best_bid + tick.best_ask) / 2.0
        else:
            tick.mid = None

        tick.ready = bool(
            tick.bids
            or tick.asks
            or tick.best_bid is not None
            or tick.best_ask is not None
            or tick.mid is not None
        )
        tick.last_update_ms = now_ms()
        tick.raw = payload if isinstance(payload, dict) else msg

    def _apply_level_change(self, tick: Tick, change: dict[str, Any]) -> None:
        price = self._extract_price_from_level(change)
        size = self._extract_size_from_level(change)
        side = str(change.get("side") or change.get("book_side") or "").strip().upper()
        if price is None or size is None:
            return

        if side in {"BUY", "BID", "B"}:
            levels = tick.bids
        elif side in {"SELL", "ASK", "A"}:
            levels = tick.asks
        else:
            return

        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size

    def apply_price_change(self, msg: dict[str, Any]) -> None:
        if not isinstance(msg, dict):
            return

        changes = msg.get("price_changes")
        if not isinstance(changes, list):
            self.apply_book_update(msg)
            return

        fallback_token = self._extract_token_from_message(msg)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for change in changes:
            if not isinstance(change, dict):
                continue
            token = self._extract_token_from_message(change) or fallback_token
            if token is None:
                continue
            grouped.setdefault(token, []).append(change)

        if not grouped:
            self.apply_book_update(msg)
            return

        for token, token_changes in grouped.items():
            tick = self.get_tick(token)
            if tick is None:
                continue

            for change in token_changes:
                self._apply_level_change(tick, change)
            self._refresh_tick_top(tick)

            if len(grouped) == 1:
                bid, ask, mid = self._extract_price_fields(msg)
                if bid is not None:
                    tick.best_bid = bid
                if ask is not None:
                    tick.best_ask = ask
                if mid is not None:
                    tick.mid = mid
                elif tick.best_bid is not None and tick.best_ask is not None:
                    tick.mid = (tick.best_bid + tick.best_ask) / 2.0
                else:
                    tick.mid = None

            tick.ready = bool(
                tick.bids
                or tick.asks
                or tick.best_bid is not None
                or tick.best_ask is not None
                or tick.mid is not None
            )
            tick.last_update_ms = now_ms()
            tick.raw = msg

    def on_message(self, message: Any) -> None:
        if not isinstance(message, dict):
            return

        event_type = self._event_type(message)
        if event_type == "price_change":
            self.apply_price_change(message)
            return

        if event_type in {"book", "snapshot", "order_book"}:
            self.apply_book_update(message, snapshot=True)
            return

        if event_type == "best_bid_ask":
            self.apply_book_update(message)
            return

        if self._looks_like_book(message):
            self.apply_book_update(message, snapshot=True)
            return

        self.apply_book_update(message)

    async def bootstrap_snapshot(self, market: "Market", rest_book_url: str | None = None) -> dict[str, Any]:
        """Best-effort REST snapshot. 404s and empty books should not block trading."""

        snapshots: dict[str, Any] = {}
        token_ids = [market.up_token, market.down_token]

        candidate_urls: list[str] = []
        if rest_book_url:
            candidate_urls.append(rest_book_url)
        candidate_urls.extend([f"https://clob.polymarket.com/book?token_id={token}" for token in token_ids])
        candidate_urls.extend(
            [
                f"https://gamma-api.polymarket.com/markets/{market.slug}/book?token_ids={market.up_token},{market.down_token}",
                f"https://gamma-api.polymarket.com/markets/{market.slug}/book?token_id={market.up_token}",
                f"https://gamma-api.polymarket.com/markets/{market.slug}/book?token_id={market.down_token}",
            ]
        )

        seen_urls: set[str] = set()
        for token in token_ids:
            tick = self.get_tick(token)
            for url in candidate_urls:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                try:
                    req = urllib.request.Request(
                        url,
                        headers={
                            "User-Agent": "v7-discover/1.0",
                            "Accept": "application/json",
                        },
                    )
                    with urllib.request.urlopen(req, timeout=8) as r:
                        payload = json.loads(r.read().decode("utf-8"))
                    book = payload
                    if isinstance(payload, dict):
                        for key in ("book", "data", "result", "snapshot"):
                            if key in payload and isinstance(payload[key], dict):
                                book = payload[key]
                                break
                        if not isinstance(book, dict):
                            book = {}

                        if not ({"bids", "asks", "mid"} & set(book.keys())) and not any(
                            isinstance(v, dict) and ({"bids", "asks", "mid"} & set(v.keys()))
                            for v in book.values()
                        ):
                            book = {}

                        if isinstance(book, dict) and ({"bids", "asks", "mid"} & set(book.keys())):
                            snapshots[token] = book
                            break

                        for candidate in ("up", "down", "yes", "no", "YES", "NO"):
                            value = payload.get(candidate)
                            if isinstance(value, dict) and ({"bids", "asks", "mid"} & set(value.keys())):
                                snapshots[token] = value
                                break
                        if token in snapshots:
                            break

                    elif isinstance(payload, list):
                        for item in payload:
                            if isinstance(item, dict) and ({"bids", "asks", "mid"} & set(item.keys())):
                                snapshots[token] = item
                                break
                        if token in snapshots:
                            break

                except urllib.error.HTTPError as exc:
                    if exc.code == 404:
                        LOG.info(
                            "[PM] REST book snapshot 404 for token=%s url=%s; continuing without snapshot",
                            token,
                            url,
                        )
                        continue
                    LOG.warning("[PM] REST book snapshot failed token=%s url=%s err=%s", token, url, exc)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("[PM] REST book snapshot failed token=%s url=%s err=%s", token, url, exc)

            if token not in snapshots:
                continue

            book = snapshots[token]
            bids = book.get("bids") if isinstance(book, dict) else None
            asks = book.get("asks") if isinstance(book, dict) else None
            mid = book.get("mid") if isinstance(book, dict) else None

            if bids is not None:
                tick.bids = self._parse_levels(bids)
            if asks is not None:
                tick.asks = self._parse_levels(asks)
            self._refresh_tick_top(tick)

            if mid is not None:
                try:
                    tick.mid = float(mid)
                except Exception:
                    pass
            if tick.best_bid is not None and tick.best_ask is not None:
                tick.mid = (tick.best_bid + tick.best_ask) / 2.0

            tick.ready = bool(
                tick.bids
                or tick.asks
                or tick.best_bid is not None
                or tick.best_ask is not None
                or tick.mid is not None
            )
            tick.last_update_ms = now_ms()
            tick.raw = book

        return snapshots


class MarketDiscovery:
    def __init__(self, gamma_url: str, timeout: float = 15.0):
        self.gamma_url = gamma_url.rstrip("/")
        self.timeout = timeout

    @staticmethod
    def slug_for(now_ms_value: int | float) -> tuple[str, int]:
        seconds = int(float(now_ms_value) / 1000.0)
        bucket = seconds - (seconds % 300)
        slug = f"btc-updown-5m-{bucket}"
        return slug, bucket

    def _json_list(self, value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
            return [value]
        if isinstance(value, dict):
            return [value]
        return [value]

    def _normalize_token(self, raw: object) -> str | None:
        if raw is None:
            return None
        if isinstance(raw, dict):
            for key in ("token_id", "tokenId", "id", "value"):
                if key in raw and raw[key] is not None:
                    token = str(raw[key]).strip()
                    return token or None
            return None
        token = str(raw).strip()
        return token or None

    def _request_json(self, url: str, timeout: float | None = None) -> Any:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "v7-discover/1.0",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout if timeout is None else timeout) as r:
            return json.loads(r.read().decode("utf-8"))

    def discover(self, slug: str, retries: int = 3, timeout: float = 15.0) -> Market | None:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                url = f"{self.gamma_url}/markets?slug={urllib.parse.quote(slug)}"
                data = self._request_json(url, timeout=timeout)

                if isinstance(data, dict):
                    data = [data]
                if not data:
                    raise ValueError(f"empty gamma payload for {slug!r}")

                m = data[0]
                raw_tokens = self._json_list(m.get("clobTokenIds") or m.get("clob_token_ids") or [])
                tokens: list[str] = []
                for raw in raw_tokens:
                    token = self._normalize_token(raw)
                    if token:
                        tokens.append(token)

                if len(tokens) < 2:
                    maybe = m.get("tokens")
                    if isinstance(maybe, dict):
                        for val in maybe.values():
                            if isinstance(val, dict):
                                token = self._normalize_token(val.get("token_id") or val.get("tokenId") or val.get("id"))
                                if token:
                                    tokens.append(token)
                        tokens = list(dict.fromkeys(tokens))

                if len(tokens) < 2:
                    raise ValueError(f"insufficient token ids for {slug!r}: found {len(tokens)}")

                start_ms = int(slug.rsplit("-", 1)[-1]) * 1000
                return Market(
                    slug=slug,
                    condition_id=str(m.get("conditionId") or m.get("condition_id") or ""),
                    up_token=str(tokens[0]),
                    down_token=str(tokens[1]),
                    start_ms=start_ms,
                    end_ms=start_ms + 300_000,
                    question=str(m.get("question") or ""),
                    neg_risk=_as_bool(m.get("negRisk") or m.get("neg_risk") or False),
                )
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                LOG.warning("discover %s attempt=%d/%d: %s", slug, attempt, retries, exc)
                if attempt >= retries:
                    LOG.warning("discover %s giving up after %d attempts: %s", slug, retries, last_exc)
                    return None
                time.sleep(min(2 ** (attempt - 1), 8))
        return None

    @staticmethod
    def _side_from_outcome(value: object) -> str | None:
        s = str(value).strip().lower()
        if s in {"up", "yes", "y", "1", "true", "winner_up", "winner-up"}:
            return "UP"
        if s in {"down", "no", "n", "0", "false", "winner_down", "winner-down"}:
            return "DOWN"
        return None

    def resolve(self, market_slug: str) -> str | None:
        """Return the final UP/DOWN winner, or None while not final.

        Gamma does not guarantee a single ``winner`` field.  The canonical
        final state is ``closed``/``resolved`` plus ``outcomes`` and
        ``outcomePrices`` (for example ``["Up","Down"]`` + ``["1","0"]``).
        Never infer a winner from a bare index without the outcomes labels:
        the order is defined by the market, not by a hard-coded index.
        """
        try:
            url = f"{self.gamma_url}/markets?slug={urllib.parse.quote(market_slug)}"
            payload = self._request_json(url, timeout=self.timeout)
            if isinstance(payload, dict):
                payload = [payload]
            if not isinstance(payload, list) or not payload:
                return None

            market = payload[0]
            outcomes = self._json_list(
                market.get("outcomes") or market.get("outcomeNames")
            )
            labels = [str(x).strip() for x in outcomes]

            # Do not honour a winner field before the market is actually
            # final.  A transient/early API value must not settle a live trade.
            status = str(
                market.get("umaResolutionStatus")
                or market.get("resolutionStatus")
                or ""
            ).strip().lower()
            closed = (
                _as_bool(market.get("closed"))
                or _as_bool(market.get("resolved"))
                or _as_bool(market.get("isResolved"))
                or status in {"resolved", "finalized"}
            )
            if not closed:
                return None

            # Explicit winner fields are accepted first, but index fields are
            # resolved through the market's own outcome labels.
            index_keys = {"winningOutcomeIndex", "winnerOutcomeIndex"}
            for key in (
                "winningOutcome",
                "winner",
                "winning_outcome",
                "winnerOutcome",
                "winningOutcomeIndex",
                "winnerOutcomeIndex",
                "winningOutcomeName",
                "winnerName",
                "outcomeWinner",
                "outcomeWinnerName",
            ):
                if key not in market or market.get(key) is None:
                    continue
                value = market.get(key)
                if key in index_keys:
                    try:
                        index = int(float(value))
                    except (TypeError, ValueError):
                        continue
                    if 0 <= index < len(labels):
                        side = self._side_from_outcome(labels[index])
                        if side is not None:
                            return side
                    continue
                side = self._side_from_outcome(value)
                if side is not None:
                    return side

            prices = self._json_list(
                market.get("outcomePrices") or market.get("outcome_prices")
            )
            if len(prices) < 2 or len(labels) < 2:
                return None
            numeric: list[float] = []
            for item in prices[: len(labels)]:
                parsed = _parse_float(item)
                if parsed is None:
                    return None
                numeric.append(parsed)
            if len(numeric) != len(labels):
                return None
            high = max(numeric)
            low = min(numeric)
            if high < 0.98 or low > 0.02:
                return None
            index = numeric.index(high)
            return self._side_from_outcome(labels[index])
        except Exception as exc:  # noqa: BLE001
            LOG.warning("resolve %s failed: %s", market_slug, exc)
            return None

    def fetch(self, slug: str, retries: int = 5) -> Market | None:
        return self.discover(slug, retries=retries)
