"""Settlement decision helpers with an explicit source priority.

This module is deliberately side-effect free so websocket, Gamma, and local
fallback data can be tested independently.  Call :func:`resolve_winner` from
the settlement poller with the current market's cached WS result and the exact
Gamma market payload for that position.
"""
from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping, Sequence
from typing import Any

LOG = logging.getLogger("v7.settlement")


def normalize_outcome(value: Any) -> str | None:
    """Normalize common Polymarket outcome labels to UP/DOWN."""
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().lower().replace(" ", "_")
    if text in {"up", "yes", "y", "1", "true", "winner_up", "winner-up"}:
        return "UP"
    if text in {"down", "no", "n", "0", "false", "winner_down", "winner-down"}:
        return "DOWN"
    return None


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return [value]
        return parsed if isinstance(parsed, list) else [parsed]
    return []


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def winner_from_gamma(payload: Any) -> str | None:
    """Return a winner only for a closed Gamma market with resolved prices.

    Outcome labels are paired with prices by index; no assumptions are made
    about Gamma's ordering of the outcomes.  Prices must be decisively near
    one and zero to avoid settling on transient market quotes.
    """
    if isinstance(payload, list):
        if len(payload) != 1:
            return None
        market = payload[0]
    elif isinstance(payload, Mapping):
        market = payload
    else:
        return None
    if not isinstance(market, Mapping) or not _is_true(market.get("closed")):
        return None

    outcomes = _json_list(market.get("outcomes") or market.get("outcomeNames"))
    prices = _json_list(market.get("outcomePrices") or market.get("outcome_prices"))
    if len(outcomes) != 2 or len(prices) != 2:
        return None
    try:
        numeric = [float(price) for price in prices]
    except (TypeError, ValueError, OverflowError):
        return None
    if not all(math.isfinite(price) for price in numeric):
        return None
    winners = [i for i, price in enumerate(numeric) if price >= 0.98]
    losers = [i for i, price in enumerate(numeric) if price <= 0.02]
    if len(winners) != 1 or len(losers) != 1 or winners[0] == losers[0]:
        return None
    return normalize_outcome(outcomes[winners[0]])


def winner_from_ws_event(message: Any, token_outcomes: Mapping[str, str] | None = None) -> str | None:
    """Extract a winner from a CLOB ``market_resolved`` event.

    Supports direct ``outcome``/``winner`` labels and the CLOB winning-asset
    form when a token-to-outcome map is available.
    """
    if not isinstance(message, Mapping):
        return None
    event_type = str(message.get("event_type") or message.get("eventType") or message.get("type") or "")
    if event_type.lower() != "market_resolved":
        return None
    for key in ("outcome", "winner", "winning_outcome", "winningOutcome", "winnerName"):
        side = normalize_outcome(message.get(key))
        if side:
            return side

    asset = message.get("winning_asset_id") or message.get("winningAssetId")
    if asset is not None and token_outcomes:
        return normalize_outcome(token_outcomes.get(str(asset)))
    return None


def resolve_winner(
    *,
    ws_winner: Any = None,
    gamma_payload: Any = None,
    twap_open: Any = None,
    twap_close: Any = None,
    spot_open: Any = None,
    spot_close: Any = None,
) -> tuple[str | None, str]:
    """Apply WS > Gamma > Chainlink TWAP > local spot resolution priority."""
    side = normalize_outcome(ws_winner)
    if side:
        return side, "ws_market_resolved"

    side = winner_from_gamma(gamma_payload)
    if side:
        return side, "gamma_outcome_prices"

    try:
        start, end = float(twap_open), float(twap_close)
        if math.isfinite(start) and math.isfinite(end) and start > 0 and end > 0:
            if end > start:
                LOG.warning("[Settlement] WARNING: using chainlink_twap_fallback")
                return "UP", "chainlink_twap_fallback"
            if end < start:
                LOG.warning("[Settlement] WARNING: using chainlink_twap_fallback")
                return "DOWN", "chainlink_twap_fallback"
    except (TypeError, ValueError, OverflowError):
        pass

    try:
        start, end = float(spot_open), float(spot_close)
        if math.isfinite(start) and math.isfinite(end) and start > 0 and end > 0:
            if end > start:
                LOG.warning("[Settlement] WARNING: using force_local_spot_fallback")
                return "UP", "force_local_spot_fallback"
            if end < start:
                LOG.warning("[Settlement] WARNING: using force_local_spot_fallback")
                return "DOWN", "force_local_spot_fallback"
    except (TypeError, ValueError, OverflowError):
        pass

    return None, "unresolved"
