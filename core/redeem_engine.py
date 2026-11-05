#!/usr/bin/env python3
"""Polymarket resolution lookup + on-chain redeem + async auto-redeem engine.

Redeem uses ConditionalTokens.redeemPositions on Polygon:
  collateral = USDC.e (6 decimals)
  parentCollectionId = 0x0
  indexSets = [1] for UP/YES, [2] for DOWN/NO

Requires:
  POLYGON_RPC_URL
  POLYMARKET_PRIVATE_KEY
  web3
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

LOG = logging.getLogger("v7.redeem")

GAMMA = "https://gamma-api.polymarket.com/markets"
ZERO32 = "0x" + "00" * 32

CTF_ABI = [
    {
        "inputs": [
            {"internalType": "contract IERC20", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


class RedeemUnavailable(RuntimeError):
    pass


@dataclass
class ResolvedMarket:
    market_slug: str
    closed: bool
    winner: str | None
    condition_id: str | None
    neg_risk: bool
    raw: dict[str, Any]


def _get_json(url: str, timeout: float = 15.0) -> Any:
    req = urllib.request.Request(url, headers={
        "User-Agent": "v7-redeem/1.0",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _as_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            x = json.loads(v)
            return x if isinstance(x, list) else []
        except Exception:  # noqa: BLE001
            return [v]
    return []


def _as_bool(value: Any) -> bool:
    """Normalize booleans from Gamma API; string 'false' must stay False."""
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


def _normalize_winner(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "UP" if value else "DOWN"
    s = str(value).strip().lower()
    if s in {"up", "yes", "y", "1", "true", "winner_up", "winner-up"}:
        return "UP"
    if s in {"down", "no", "n", "0", "false", "winner_down", "winner-down"}:
        return "DOWN"
    return None


def normalize_side(x: Any) -> str | None:
    """Compatibility wrapper for side normalization used by redeem code."""
    return _normalize_winner(x)


class PolymarketResolver:
    """Resolve a 5m market via Gamma. No signing, no keys."""

    def __init__(self, gamma_url: str = GAMMA, timeout: float = 15.0):
        self.gamma_url = gamma_url
        self.timeout = timeout

    def resolve(self, market_slug: str) -> ResolvedMarket:
        url = f"{self.gamma_url}?slug={urllib.parse.quote(market_slug)}"
        data = _get_json(url, self.timeout)
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or not data:
            raise LookupError(f"market not found: {market_slug}")

        m = data[0]
        closed = False
        for key in ("closed", "resolved", "finalized", "isResolved"):
            if key in m:
                closed = _as_bool(m.get(key))
                if closed:
                    break
        if not closed:
            return ResolvedMarket(
                market_slug=market_slug,
                closed=False,
                winner=None,
                condition_id=m.get("conditionId") or m.get("condition_id"),
                neg_risk=_as_bool(m.get("negRisk") or m.get("neg_risk") or False),
                raw=m,
            )

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
            if key in m:
                winner = _normalize_winner(m.get(key))
                if winner:
                    return ResolvedMarket(
                        market_slug=market_slug,
                        closed=True,
                        winner=winner,
                        condition_id=m.get("conditionId") or m.get("condition_id"),
                        neg_risk=_as_bool(m.get("negRisk") or m.get("neg_risk") or False),
                        raw=m,
                    )

        outcomes = [str(x) for x in _as_list(m.get("outcomes"))]
        prices = [float(x) for x in _as_list(m.get("outcomePrices")) if str(x).strip() not in {"", "None", "null"}]
        if outcomes and prices and len(outcomes) == len(prices):
            try:
                idx = max(range(len(prices)), key=lambda i: prices[i])
                if prices[idx] >= 0.99:
                    winner = _normalize_winner(outcomes[idx])
                    if winner:
                        return ResolvedMarket(
                            market_slug=market_slug,
                            closed=True,
                            winner=winner,
                            condition_id=m.get("conditionId") or m.get("condition_id"),
                            neg_risk=_as_bool(m.get("negRisk") or m.get("neg_risk") or False),
                            raw=m,
                        )
            except Exception:  # noqa: BLE001
                pass

        return ResolvedMarket(
            market_slug=market_slug,
            closed=True,
            winner=None,
            condition_id=m.get("conditionId") or m.get("condition_id"),
            neg_risk=_as_bool(m.get("negRisk") or m.get("neg_risk") or False),
            raw=m,
        )


class PolymarketRedeemer:
    """On-chain redeemPositions signer. Lazy web3 import."""

    def __init__(self, *,
                 rpc_url: str | None = None,
                 private_key: str | None = None,
                 conditional_tokens: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
                 collateral_usdc: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                 chain_id: int = 137,
                 gas_multiplier: float = 1.25,
                 receipt_timeout_s: int = 180):
        self.rpc_url = rpc_url or os.getenv("POLYGON_RPC_URL")
        self.private_key = private_key or os.getenv("POLYMARKET_PRIVATE_KEY")
        self.conditional_tokens = conditional_tokens
        self.collateral_usdc = collateral_usdc
        self.chain_id = chain_id
        self.gas_multiplier = gas_multiplier
        self.receipt_timeout_s = receipt_timeout_s
        self._w3 = None
        self._acct = None
        self._ctf = None
        self.reason: str | None = None

        if not self.rpc_url:
            self.reason = "POLYGON_RPC_URL not set"
        elif not self.private_key:
            self.reason = "POLYMARKET_PRIVATE_KEY not set"
        else:
            self._init_web3()

    def _init_web3(self) -> None:
        try:
            from web3 import Web3
            from eth_account import Account
        except Exception as exc:  # noqa: BLE001
            self.reason = f"web3/eth_account not installed: {type(exc).__name__}"
            return
        try:
            w3 = Web3(Web3.HTTPProvider(self.rpc_url, request_kwargs={"timeout": 20}))
            if not w3.is_connected():
                self.reason = "Polygon RPC not connected"
                return
            if w3.eth.chain_id != self.chain_id:
                self.reason = f"wrong chain_id={w3.eth.chain_id}, expected {self.chain_id}"
                return
            acct = Account.from_key(self.private_key)
            self._w3 = w3
            self._acct = acct
            self._ctf = w3.eth.contract(address=Web3.to_checksum_address(self.conditional_tokens), abi=CTF_ABI)
            self.reason = None
        except Exception as exc:  # noqa: BLE001
            self.reason = f"web3 init failed: {type(exc).__name__}: {exc}"
            self._w3 = None
            self._acct = None
            self._ctf = None

    @property
    def available(self) -> bool:
        return self._w3 is not None and self._acct is not None and self._ctf is not None

    @property
    def address(self) -> str | None:
        return self._acct.address if self._acct is not None else None

    def receipt_status(self, tx_hash: str) -> int | None:
        if not self.available:
            return None
        try:
            r = self._w3.eth.get_transaction_receipt(tx_hash)
            return int(r.status)
        except Exception:  # noqa: BLE001
            return None

    def redeem(self, condition_id: str, side: str, *, neg_risk: bool = False) -> str:
        if not self.available:
            raise RedeemUnavailable(self.reason or "redeemer unavailable")
        if neg_risk:
            raise RedeemUnavailable("neg-risk market: use NegRiskAdapter redeem path, not implemented")
        side_u = normalize_side(side)
        if side_u not in ("UP", "DOWN"):
            raise ValueError(f"invalid side: {side}")
        if not condition_id or not condition_id.startswith("0x") or len(condition_id) != 66:
            raise ValueError(f"invalid condition_id: {condition_id}")

        w3, acct, ctf = self._w3, self._acct, self._ctf
        index_sets = [1] if side_u == "UP" else [2]
        fn = ctf.functions.redeemPositions(
            w3.to_checksum_address(self.collateral_usdc),
            ZERO32,
            w3.to_bytes(hexstr=condition_id),
            index_sets,
        )
        nonce = w3.eth.get_transaction_count(acct.address, "pending")
        try:
            gas = int(fn.estimate_gas({"from": acct.address}) * self.gas_multiplier)
        except Exception:  # noqa: BLE001
            gas = 350_000
        base = w3.eth.gas_price
        tx = fn.build_transaction({
            "from": acct.address,
            "nonce": nonce,
            "gas": gas,
            "maxFeePerGas": int(base * 2),
            "maxPriorityFeePerGas": int(w3.to_wei(30, "gwei")),
            "chainId": self.chain_id,
        })
        signed = acct.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        tx_hash = w3.eth.send_raw_transaction(raw)
        hex_hash = tx_hash.hex()
        if not hex_hash.startswith("0x"):
            hex_hash = "0x" + hex_hash
        LOG.info("[Redeem] sent tx=%s condition=%s side=%s", hex_hash, condition_id, side_u)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=self.receipt_timeout_s)
        if int(receipt.status) != 1:
            raise RuntimeError(f"redeem tx reverted: {hex_hash}")
        return hex_hash

    def usdc_balance(self) -> float | None:
        if not self.available:
            return None
        try:
            abi = [{"constant": True, "inputs": [{"name": "a", "type": "address"}],
                    "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
                    "type": "function"}]
            c = self._w3.eth.contract(address=self._w3.to_checksum_address(self.collateral_usdc), abi=abi)
            return c.functions.balanceOf(self.address).call() / 1e6
        except Exception:  # noqa: BLE001
            return None


# --------------------------------------------------------------------------- engine
import asyncio
from core.state_manager import now_ms


class RedeemEngine:
    def __init__(self, state, *, resolver=None, redeemer=None, interval_s=60, finality_buffer_s=30):
        self.state = state
        self.resolver = resolver or PolymarketResolver()
        self.redeemer = redeemer or PolymarketRedeemer()
        self.interval_s = max(5, int(interval_s))
        self.finality_buffer_ms = int(finality_buffer_s * 1000)

    async def run_once(self):
        positions = await asyncio.to_thread(self.state.active_positions)
        if not positions:
            return
        now = now_ms()
        for p in positions:
            if p.end_time_ms + self.finality_buffer_ms > now:
                continue
            if p.status == "pending_fill":
                LOG.warning("[Redeem] pending_fill past end, executor must confirm: %s", p.market_slug)
                continue
            if p.status == "redeem_pending" and p.last_attempt_ms and now - p.last_attempt_ms < 120_000:
                continue
            try:
                resolved = await asyncio.to_thread(self.resolver.resolve, p.market_slug)
            except Exception as exc:
                LOG.warning("[Redeem] resolve failed %s: %s", p.market_slug, exc)
                await asyncio.to_thread(self.state.record_error, p.trade_id, f"resolve: {type(exc).__name__}: {exc}")
                continue
            if not resolved.closed:
                continue
            if resolved.winner not in ("UP", "DOWN"):
                continue
            changes = {}
            if resolved.condition_id and resolved.condition_id != p.condition_id:
                changes["condition_id"] = resolved.condition_id
            if resolved.neg_risk != p.neg_risk:
                changes["neg_risk"] = resolved.neg_risk
            if changes:
                await asyncio.to_thread(self.state.update_position, p.trade_id, **changes)
                p.condition_id = changes.get("condition_id", p.condition_id)
                p.neg_risk = changes.get("neg_risk", p.neg_risk)
            if resolved.winner != p.side.upper():
                await asyncio.to_thread(self.state.mark_lost, p.trade_id)
                LOG.info("[Redeem] lost market %s side=%s winner=%s; cleared",
                         p.market_slug, p.side, resolved.winner)
                await asyncio.to_thread(self.state.remove_position, p.trade_id)
                continue
            if not p.condition_id:
                LOG.error("[Redeem] missing condition_id: %s", p.market_slug)
                await asyncio.to_thread(self.state.record_error, p.trade_id, "missing condition_id")
                continue
            if p.neg_risk:
                LOG.error("[Redeem] neg-risk unsupported: %s", p.market_slug)
                await asyncio.to_thread(self.state.record_error, p.trade_id, "neg_risk unsupported")
                continue
            await asyncio.to_thread(self.state.mark_redeem_pending, p.trade_id)
            try:
                tx = await asyncio.to_thread(self.redeemer.redeem, p.condition_id, p.side, neg_risk=p.neg_risk)
                amount = float(p.shares)
                await asyncio.to_thread(self.state.mark_redeemed, p.trade_id, tx, amount)
                LOG.info("[Redeem] 成功赎回市场 %s，金额 %.6f USDC。tx=%s", p.market_slug, amount, tx)
                await asyncio.to_thread(self.state.remove_position, p.trade_id)
            except RedeemUnavailable as exc:
                LOG.error("[Redeem] unavailable: %s", exc)
                await asyncio.to_thread(self.state.record_error, p.trade_id, f"unavailable: {exc}")
            except Exception as exc:
                LOG.exception("[Redeem] failed %s: %s", p.market_slug, exc)
                await asyncio.to_thread(self.state.record_error, p.trade_id, f"redeem: {type(exc).__name__}: {exc}")

    async def run_forever(self, stop=None):
        stop = stop or asyncio.Event()
        await asyncio.to_thread(self.state.recover)
        if not self.redeemer.available:
            LOG.error("[Redeem] redeemer unavailable: %s", self.redeemer.reason)
        else:
            LOG.info("[Redeem] signer=%s", self.redeemer.address)
        LOG.info("[Redeem] loop started interval=%ss", self.interval_s)
        while not stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("[Redeem] run_once crashed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass
        LOG.info("[Redeem] loop stopped")


__all__ = [
    "ResolvedMarket",
    "PolymarketResolver",
    "PolymarketRedeemer",
    "RedeemEngine",
    "RedeemUnavailable",
    "normalize_side",
]


# End of file
