#!/usr/bin/env python3
"""Polymarket RTDS Chainlink TWAP feed.

The official BTC 5-minute markets settle against Chainlink's BTC/USD TWAP,
not against Binance's instantaneous last price.  Polymarket publishes the
same TWAP on its realtime-data WebSocket:

    wss://ws-live-v2.polymarket.com/ws
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
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

try:  # Keep the module importable for offline tests.
    import websockets
except Exception:  # pragma: no cover - only hit when the dependency is absent
    websockets = None

LOG = logging.getLogger("v7.twap")

# PolyBolt is the current official reference-price socket.  The old
# ``ws-live-data`` RTDS endpoint and ``prices.*`` topics are retired.
DEFAULT_WS_URL = "wss://ws-live-v2.polymarket.com/ws"
DEFAULT_TOPIC = "price.crypto.twap"
DEFAULT_SYMBOL = "btcusd"
DEFAULT_WINDOW_SECONDS = 60
DEFAULT_REST_URL = ""
AUTH_RID = "a1"
SUBSCRIBE_RID = "s1"
AUTH_ACK_TIMEOUT_S = 8.0
_LEGACY_TWAP_TOPICS = {
    "prices.crypto.twap",
    "prices.crypto.chainlink.twap",
    "price.crypto.chainlink.twap",
}

# CLOB L2 credential environment variables.  PolyBolt requires the same
# key/secret/passphrase used by the authenticated CLOB REST API.  Accept the
# common aliases so an existing Polymarket bot can reuse its .env unchanged.
_AUTH_ENV_NAMES = {
    "api_key": ("POLYMARKET_CLOB_API_KEY", "POLYMARKET_API_KEY", "CLOB_API_KEY"),
    "secret": ("POLYMARKET_CLOB_API_SECRET", "POLYMARKET_API_SECRET", "CLOB_API_SECRET"),
    "passphrase": (
        "POLYMARKET_CLOB_API_PASSPHRASE",
        "POLYMARKET_API_PASSPHRASE",
        "CLOB_API_PASSPHRASE",
    ),
    "access_token": ("POLYMARKET_CLOB_ACCESS_TOKEN", "POLYMARKET_ACCESS_TOKEN"),
}


def _env_first(names: tuple[str, ...]) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


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
        api_key: str | None = None,
        api_secret: str | None = None,
        passphrase: str | None = None,
        access_token: str | None = None,
        rest_url: str | None = None,
        rest_api_key: str | None = None,
        rest_timeout_s: float = 8.0,
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

        # Auth is a frame, not a URL query parameter.  Values can be passed by
        # main.py (config/env) or read directly from the standard CLOB names.
        self._auth: dict[str, str] = {}
        api_key = (api_key or _env_first(_AUTH_ENV_NAMES["api_key"])).strip()
        api_secret = (api_secret or _env_first(_AUTH_ENV_NAMES["secret"])).strip()
        passphrase = (passphrase or _env_first(_AUTH_ENV_NAMES["passphrase"])).strip()
        access_token = (
            access_token or _env_first(_AUTH_ENV_NAMES["access_token"])
        ).strip()
        if api_key and api_secret and passphrase:
            self._auth = {
                "apiKey": api_key,
                "secret": api_secret,
                "passphrase": passphrase,
            }
            if access_token:
                self._auth["accessToken"] = access_token
        elif access_token:
            # Some deployments issue only a bearer token.  Keep it supported
            # without pretending a partial key/secret pair is valid.
            self._auth = {"accessToken": access_token}

        self.rest_url = (
            rest_url
            if rest_url is not None and str(rest_url).strip()
            else os.getenv("POLYMARKET_TWAP_REST_URL", "")
        ).strip()
        self.rest_api_key = (
            rest_api_key or _env_first(("POLYMARKET_TWAP_REST_KEY",))
        ).strip()
        self.rest_timeout_s = max(1.0, float(rest_timeout_s))
        self._rest_last_error = ""
        self._last_rest_ms = 0

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
            "auth_configured": bool(self._auth),
            "rest_configured": bool(self.rest_url),
            "last_rest_ms": self._last_rest_ms,
            "last_rest_error": self._rest_last_error,
            "latest_value": latest.value if latest else None,
            "latest_timestamp_ms": latest.timestamp_ms if latest else None,
            "latest_window_seconds": latest.window_seconds if latest else None,
        }

    async def fetch_rest_tick(self) -> TwapTick | None:
        """One-shot REST fallback for environments where the WS is blocked.

        Polymarket does not publish an unauthenticated public TWAP REST
        endpoint.  Set ``POLYMARKET_TWAP_REST_URL`` (``chainlink_twap.rest_url``)
        to a provider/Chainlink-gateway URL that returns JSON containing
        ``symbol``, ``value``/``full_accuracy_value`` and ``timestamp``.  The
        URL may contain ``{symbol}`` and ``{window_seconds}`` placeholders.
        If no URL is configured, this returns None instead of inventing a
        Binance substitute.
        """
        url = (self.rest_url or os.getenv("POLYMARKET_TWAP_REST_URL", "")).strip()
        if not url:
            return None
        try:
            url = url.format(symbol=self.symbol, window_seconds=self.window_seconds)
        except (KeyError, ValueError):
            self._rest_last_error = "invalid REST URL template"
            LOG.warning("[TWAP] REST fallback skipped: %s", self._rest_last_error)
            return None

        headers = {"Accept": "application/json", "User-Agent": "polymarket-v7/1.0"}
        if self.rest_api_key:
            headers["Authorization"] = f"Bearer {self.rest_api_key}"

        def _get() -> Any:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=self.rest_timeout_s) as resp:
                body = resp.read(2 * 1024 * 1024)
            return json.loads(body.decode("utf-8", errors="replace"))

        try:
            payload = await asyncio.to_thread(_get)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            self._rest_last_error = f"{type(exc).__name__}: {exc}"
            LOG.warning("[TWAP] REST fallback failed url=%s: %s", url, self._rest_last_error)
            return None

        tick = self._find_tick_in_payload(payload, require_symbol=False)
        if tick is None:
            self._rest_last_error = "REST response contained no BTC TWAP tick"
            LOG.warning("[TWAP] REST fallback response unusable url=%s", url)
            return None
        self._last_rest_ms = _now_ms()
        self._rest_last_error = ""
        self._accept(tick)
        LOG.info(
            "[TWAP] REST fallback accepted value=%.4f ts=%s window=%ss",
            tick.value,
            tick.timestamp_ms,
            tick.window_seconds,
        )
        return tick

    def _find_tick_in_payload(
        self, payload: Any, *, require_symbol: bool = True
    ) -> TwapTick | None:
        stack: list[Any] = [payload]
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
            tick = self._parse_tick(node, require_symbol=require_symbol, accept=False)
            if tick is not None:
                return tick
            for key in ("payload", "data", "message", "messages", "result", "results"):
                child = node.get(key)
                if isinstance(child, (dict, list)):
                    stack.append(child)
        return None

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
                "rid": SUBSCRIBE_RID,
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

    def _auth_frame(self) -> dict[str, Any] | None:
        """Official CLOB-credential authentication frame for PolyBolt.

        PolyBolt reference-price channels are authenticated.  The credentials
        are the same L2 CLOB key/secret/passphrase used by the REST API; they
        belong in the first frame after connecting, never in the URL.
        """
        if not self._auth:
            return None
        return {"op": "auth", "rid": AUTH_RID, "auth": dict(self._auth)}

    async def _await_auth_ack(self, ws: Any, timeout_s: float = AUTH_ACK_TIMEOUT_S) -> None:
        """Wait for ``{"op":"authed","rid":"a1"}`` before subscribing.

        Subscribing before this acknowledgement is the race that made the
        previous client look connected while silently receiving no data.  A
        server-side auth error is terminal; a timeout is retryable.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(1.0, float(timeout_s))
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"TWAP auth acknowledgement timeout rid={AUTH_RID} "
                    f"after {timeout_s:.0f}s"
                )
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise TimeoutError(
                    f"TWAP auth acknowledgement timeout rid={AUTH_RID} "
                    f"after {timeout_s:.0f}s"
                ) from exc

            try:
                msg = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            except (TypeError, ValueError, json.JSONDecodeError):
                print(f"[TWAP RAW] {raw}")
                continue
            if not isinstance(msg, dict):
                continue

            op = str(msg.get("op") or msg.get("type") or "").strip().lower()
            rid = str(msg.get("rid") or "").strip()
            if op == "authed" and (not rid or rid == AUTH_RID):
                print(f"[TWAP DEBUG] Auth acknowledged rid={rid or AUTH_RID}")
                return

            err = msg.get("error") or msg.get("message")
            if op in {"error", "auth_error", "unauthorized"} or (
                err and op not in {"authed"}
            ):
                detail = json.dumps(msg, ensure_ascii=False, default=str)
                print(f"[TWAP ERROR] auth failed: {detail}", flush=True)
                LOG.error("[TWAP] auth failed: %s", detail)
                self._last_error = f"auth failed: {detail}"
                # Bad credentials will not heal on reconnect.  Stop instead
                # of hammering PolyBolt with the same invalid key.
                self._stop.set()
                raise RuntimeError(f"TWAP auth failed: {detail}")

            # Non-auth frames before the ack are diagnostic only; do not feed
            # them to the strategy parser because there is no subscription yet.
            print(f"[TWAP RAW] {raw}")

    async def _send_subscription(self, ws: Any) -> None:
        """Authenticate and wait for the ack, then subscribe.

        The official sequence is mandatory:
        ``auth(rid=a1) -> authed(rid=a1) -> subscribe(rid=s1)``.
        """
        auth_frame = self._auth_frame()
        if auth_frame is not None:
            await ws.send(json.dumps(auth_frame, separators=(",", ":")))
            print(
                "[TWAP DEBUG] Sent auth frame "
                f"(rid={AUTH_RID}, apiKey={self._auth.get('apiKey', '')[:6]}...)"
            )
            await self._await_auth_ack(ws)
        else:
            print(
                "[TWAP WARNING] CLOB credentials missing; subscribing without auth "
                "(PolyBolt will not deliver reference-price data)"
            )
            LOG.warning("[TWAP] auth credentials missing; unauthenticated subscribe")

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
            # No legacy application-level {"action":"ping"} is sent.  This is
            # the RFC6455 transport ping owned by the websockets library; it
            # is required to detect half-open TCP sockets.
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

    def _parse_tick(
        self,
        node: dict[str, Any],
        *,
        require_symbol: bool = True,
        accept: bool = True,
    ) -> TwapTick | None:
        symbol = node.get("symbol") or node.get("pair") or node.get("asset")
        if symbol:
            if _normalise_symbol(symbol) != self.symbol:
                return None
        elif require_symbol:
            return None
        else:
            symbol = self.symbol

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
        if full_accuracy is not None and str(full_accuracy).strip():
            # PolyBolt reference prices are authoritative only in
            # full_accuracy_value (an integer scaled by 1e18).  Never fall
            # back to the lossy float ``value`` when the full value is
            # present but malformed.
            value = self._decimal_price(None, full_accuracy)
        else:
            value = self._decimal_price(node.get("value"), None)
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
        if accept:
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
