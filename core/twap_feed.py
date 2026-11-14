#!/usr/bin/env python3
"""Polymarket RTDS Chainlink TWAP feed.

The official BTC 5-minute markets settle against Chainlink's BTC/USD TWAP,
not against Binance's instantaneous last price.  Polymarket publishes the
same TWAP on its realtime-data WebSocket:

    wss://ws-live-data.polymarket.com
    channel: price.crypto.twap

PolyBolt (the successor to the old RTDS price topics) uses the ``op`` /
``channel`` / ``filter`` envelope.  In particular, the filter is a JSON
object, not a string, and there is no top-level ``action``/``topic``/``type``
in the subscribe frame.  The parser deliberately accepts both the documented
payload and common wrapper variants; it never turns a non-BTC or wrong-window
tick into a settlement price.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

try:  # Keep the module importable for offline tests.
    import websockets
except Exception:  # pragma: no cover - only hit when the dependency is absent
    websockets = None

LOG = logging.getLogger("v7.twap")

DEFAULT_WS_URL = "wss://ws-live-data.polymarket.com"
DEFAULT_TOPIC = "price.crypto.twap"
DEFAULT_SYMBOL = "btcusd"
DEFAULT_WINDOW_SECONDS = 30
_LEGACY_TWAP_TOPICS = {
    "prices.crypto.twap",
    "prices.crypto.chainlink.twap",
    "price.crypto.chainlink.twap",
}


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(str(value).strip())
    except (TypeError, ValueError, InvalidOperation):
        return None
    return out if math.isfinite(out) else None


def _to_ms(value: Any) -> int | None:
    raw = _to_float(value)
    if raw is None or raw <= 0:
        return None
    # RTDS timestamps are normally milliseconds; tolerate seconds.
    return int(raw * 1000) if raw < 10_000_000_000 else int(raw)


def _normalise_symbol(value: Any) -> str:
    """Canonical PolyBolt crypto symbol: ``btcusd``.

    The server accepts canonical USD pairs without separators.  Accept the
    common display forms (``BTC/USD``, ``BTC-USD``) on incoming messages so a
    server-side spelling change cannot silently discard every tick, but always
    subscribe with the canonical no-separator form.
    """
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


@dataclass(frozen=True)
class TwapTick:
    symbol: str
    value: float
    timestamp_ms: int
    window_seconds: int
    source: str
    received_ms: int
    full_accuracy_value: str | None = None


class ChainlinkTwapFeed:
    """Small reconnecting client for Polymarket's Chainlink TWAP topic."""

    def __init__(
        self,
        ws_url: str = DEFAULT_WS_URL,
        *,
        topic: str = DEFAULT_TOPIC,
        symbol: str = DEFAULT_SYMBOL,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        stale_after_s: float = 120.0,
        open_tolerance_ms: int = 60_000,
        close_tolerance_ms: int = 45_000,
        max_ticks: int = 4096,
    ) -> None:
        self.ws_url = str(ws_url)
        self.topic = str(topic)
        self.symbol = _normalise_symbol(symbol)
        self.window_seconds = int(window_seconds)
        self.stale_after_ms = max(1_000, int(float(stale_after_s) * 1000))
        self.open_tolerance_ms = max(1_000, int(open_tolerance_ms))
        self.close_tolerance_ms = max(1_000, int(close_tolerance_ms))
        self._ticks: deque[TwapTick] = deque(maxlen=max(16, int(max_ticks)))
        self._latest: TwapTick | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._connected = False
        self._last_error = ""
        self._last_msg_ms = 0
        self._accepted = 0

    # ------------------------------------------------------------------ public
    @property
    def latest(self) -> TwapTick | None:
        return self._latest

    def is_fresh(self, now_ms: int | None = None) -> bool:
        if self._latest is None:
            return False
        now = _now_ms() if now_ms is None else int(now_ms)
        return now - self._latest.received_ms <= self.stale_after_ms

    def value_near(self, target_ms: int, *, tolerance_ms: int | None = None) -> TwapTick | None:
        """Return the BTC TWAP tick closest to ``target_ms`` within tolerance.

        Settlement is allowed to read a tick a few seconds on either side of
        the boundary, but never a tick from another symbol/window or one
        minutes away.  Ties prefer the at-or-before tick.
        """
        if not self._ticks:
            return None
        tol = self.close_tolerance_ms if tolerance_ms is None else max(1, int(tolerance_ms))
        target = int(target_ms)
        candidates = [t for t in self._ticks if abs(t.timestamp_ms - target) <= tol]
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda t: (abs(t.timestamp_ms - target), 0 if t.timestamp_ms <= target else 1),
        )

    def status(self) -> dict[str, Any]:
        latest = self._latest
        return {
            "connected": self._connected,
            "accepted": self._accepted,
            "buffered": len(self._ticks),
            "last_msg_ms": self._last_msg_ms,
            "last_error": self._last_error,
            "latest_value": latest.value if latest else None,
            "latest_timestamp_ms": latest.timestamp_ms if latest else None,
            "latest_window_seconds": latest.window_seconds if latest else None,
        }

    async def start(self) -> None:
        if websockets is None:
            self._last_error = "websockets dependency missing"
            LOG.error("[TWAP] %s", self._last_error)
            return
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="chainlink-twap-feed")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # --------------------------------------------------------------- networking
    def _subscription_payloads(self) -> list[dict[str, Any]]:
        """Return the current PolyBolt subscription envelope.

        The old RTDS shape (``action`` / ``topic`` / ``type`` / ``filters``)
        is rejected with ``Invalid request body``.  PolyBolt uses exactly:

        ``{"op":"subscribe","subscriptions":[{"channel":...,"filter":{...}}]}``

        and the filter is a JSON object.  Keep one canonical frame here; do
        not send speculative variants, because a malformed frame can make the
        server close the socket and trigger reconnects.
        """
        channel = str(self.topic).strip()
        if channel in _LEGACY_TWAP_TOPICS:
            channel = "price.crypto.twap"
        # The wire protocol uses the singular ``price.*`` channel names even
        # though the SDK exposes ``prices.*`` topic aliases.
        if channel.startswith("prices."):
            channel = "price." + channel[len("prices.") :]
        return [
            {
                "op": "subscribe",
                "subscriptions": [
                    {
                        "channel": channel,
                        "filter": {
                            "symbol": self.symbol,
                            "window_seconds": self.window_seconds,
                        },
                    }
                ],
            }
        ]

    async def _send_subscription(self, ws: Any) -> None:
        """Send the one canonical subscription frame."""
        payload = self._subscription_payloads()[0]
        await ws.send(json.dumps(payload, separators=(",", ":")))
        print(f"[TWAP DEBUG] Sent payload: {payload}")

    @staticmethod
    def _is_invalid_request_body(raw: str | bytes | dict[str, Any] | list[Any]) -> bool:
        """Detect the server's malformed-subscription error without retrying."""
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            if isinstance(raw, str):
                raw = json.loads(raw)
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        if not isinstance(raw, dict):
            return False
        message = raw.get("message") or raw.get("error") or ""
        return "invalid request body" in str(message).lower()

    async def _run(self) -> None:
        backoff_s = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once()
                backoff_s = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"{type(exc).__name__}: {exc}"
                LOG.warning("[TWAP] connection lost: %s", self._last_error)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff_s)
                except asyncio.TimeoutError:
                    pass
                backoff_s = min(60.0, backoff_s * 2.0)
            finally:
                self._connected = False

    async def _connect_once(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets dependency missing")
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=4 * 1024 * 1024,
        ) as ws:
            self._connected = True
            self._last_error = ""
            print("[TWAP DEBUG] WebSocket connected!")
            LOG.info("[TWAP] connected to %s channel=%s", self.ws_url, self.topic)
            await self._send_subscription(ws)
            subscribed_ms = _now_ms()
            connection_accepted = 0
            warned_no_data = False
            raw_debug_count = 0
            while not self._stop.is_set():
                # Warn even if the server sends non-TWAP heartbeats: the
                # condition is zero accepted ticks, not zero raw messages.
                if (
                    not warned_no_data
                    and connection_accepted == 0
                    and _now_ms() - subscribed_ms >= 10_000
                ):
                    warned_no_data = True
                    print(
                        "[TWAP WARNING] 10s 未收到 TWAP 数据 "
                        f"url={self.ws_url} channel={self.topic} "
                        f"symbol={self.symbol!r} window={self.window_seconds}s; "
                        "检查 channel/symbol/auth（PolyBolt 需要 CLOB API 凭证）",
                        flush=True,
                    )
                    LOG.warning(
                        "[TWAP] no data 10s after subscribe channel=%s symbol=%s",
                        self.topic,
                        self.symbol,
                    )
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Keep the socket open; a quiet TWAP channel is normal.
                    continue
                self._last_msg_ms = _now_ms()
                if raw_debug_count < 20:
                    print(f"[TWAP RAW] {raw}")
                    raw_debug_count += 1
                if self._is_invalid_request_body(raw):
                    self._last_error = "subscription rejected: Invalid request body"
                    print(
                        "[TWAP WARNING] subscription rejected: Invalid request body; "
                        "stopping retries. Check the channel/filter frame."
                    )
                    LOG.error("[TWAP] subscription rejected: Invalid request body; stopping")
                    self._stop.set()
                    return
                accepted = self.ingest(raw)
                if accepted:
                    connection_accepted += accepted
                    LOG.info(
                        "[TWAP] accepted=%d latest=%.4f ts=%s window=%ss",
                        accepted,
                        self._latest.value if self._latest else float("nan"),
                        self._latest.timestamp_ms if self._latest else "?",
                        self._latest.window_seconds if self._latest else "?",
                    )

    # ---------------------------------------------------------------- parsing
    def ingest(self, raw: str | bytes | dict[str, Any] | list[Any]) -> int:
        """Parse a raw RTDS message; return number of accepted BTC ticks."""
        if isinstance(raw, (str, bytes)):
            try:
                raw = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                return 0
        accepted = 0
        stack: list[Any] = [raw]
        seen: set[int] = set()
        while stack:
            node = stack.pop()
            node_id = id(node)
            if node_id in seen:
                continue
            seen.add(node_id)
            if isinstance(node, list):
                stack.extend(node)
                continue
            if not isinstance(node, dict):
                continue
            tick = self._parse_tick(node)
            if tick is not None:
                accepted += 1
                continue
            for key in ("payload", "data", "message", "messages", "result", "results"):
                child = node.get(key)
                if isinstance(child, (dict, list)):
                    stack.append(child)
        return accepted

    def _parse_tick(self, node: dict[str, Any]) -> TwapTick | None:
        symbol = node.get("symbol") or node.get("pair") or node.get("asset")
        if not symbol or _normalise_symbol(symbol) != self.symbol:
            return None

        window_raw = node.get("window_seconds", node.get("windowSeconds", node.get("window")))
        if window_raw is not None:
            try:
                window = int(float(window_raw))
            except (TypeError, ValueError):
                return None
            if self.window_seconds and window != self.window_seconds:
                return None
        else:
            window = self.window_seconds

        timestamp = _to_ms(
            node.get("timestamp")
            or node.get("ts")
            or node.get("observed_at_ms")
            or node.get("observationsTimestamp")
            or node.get("observation_timestamp")
        )
        if timestamp is None:
            return None

        full_accuracy = node.get("full_accuracy_value") or node.get("fullAccuracyValue")
        value = self._decimal_price(node.get("value"), full_accuracy)
        if value is None:
            return None

        tick = TwapTick(
            symbol=self.symbol,
            value=value,
            timestamp_ms=timestamp,
            window_seconds=window,
            source=str(node.get("source") or "chainlink"),
            received_ms=_now_ms(),
            full_accuracy_value=str(full_accuracy) if full_accuracy is not None else None,
        )
        self._accept(tick)
        return tick

    @staticmethod
    def _decimal_price(value: Any, full_accuracy: Any) -> float | None:
        candidates: list[tuple[Any, bool]] = [(full_accuracy, True), (value, False)]
        for raw, is_full_accuracy in candidates:
            if raw is None or isinstance(raw, bool):
                continue
            text = str(raw).strip()
            if not text:
                continue
            try:
                dec = Decimal(text)
            except (InvalidOperation, ValueError):
                continue
            # Chainlink full-accuracy values are integers scaled by 1e18.
            if is_full_accuracy and dec == dec.to_integral_value() and abs(dec) > Decimal("1e12"):
                dec = dec / Decimal("1e18")
            out = float(dec)
            if math.isfinite(out) and out > 0:
                return out
        return None

    def _accept(self, tick: TwapTick) -> None:
        if self._ticks and any(
            t.timestamp_ms == tick.timestamp_ms and t.value == tick.value for t in self._ticks
        ):
            return
        self._ticks.append(tick)
        self._accepted += 1
        if self._latest is None or tick.timestamp_ms >= self._latest.timestamp_ms:
            self._latest = tick
