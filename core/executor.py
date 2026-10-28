"""Execution adapters: paper fill simulator and live CLOB market orders.

PaperExecutor is deterministic: fill at ask + slippage, pay taker fee, settle 0/1.
LiveExecutor refuses to initialize unless live_trading_confirm.txt contains the
exact confirmation phrase. The private key is read from os.getenv only.
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.feed import Market
from core.strategy import Signal, V6Params

LOG = logging.getLogger("v7.executor")


def _coerce_valid_ask(signal: Signal | None) -> float:
    if signal is None:
        print("[Error] 价格无效 signal=None", flush=True)
        raise ValueError("invalid signal")
    ask = signal.ask
    if ask is None:
        print(f"[Error] 拦截坏单: signal.ask is None signal={signal!r}", flush=True)
        raise ValueError("invalid ask")
    try:
        ask_f = float(ask)
    except Exception:
        print(f"[Error] 价格无效 ask={ask!r}", flush=True)
        raise ValueError("invalid ask") from None
    if not math.isfinite(ask_f) or ask_f <= 0:
        print(f"[Error] 拦截坏单: ask is None or <= 0 ask={ask!r}", flush=True)
        raise ValueError("invalid ask")
    return ask_f


@dataclass
class Fill:
    market_slug: str
    side: str
    token_id: str
    entry_price: float
    shares: float
    stake_usdc: float
    fee_usdc: float
    order_id: str | None = None
    raw: dict[str, Any] | None = None


class BaseExecutor:
    mode = "base"

    def buy(self, signal: Signal, stake_usdc: float) -> Fill:
        raise NotImplementedError

    def settle_paper(self, fill: Fill, winner: str) -> float:
        raise NotImplementedError


class PaperExecutor(BaseExecutor):
    mode = "paper"

    def __init__(self, *, balance: float, slippage_cents: float = 2.0,
                 fee_coefficient: float = 0.07):
        self.balance = float(balance)
        self.slippage = slippage_cents / 100.0
        self.fee_coefficient = fee_coefficient

    def buy(self, signal: Signal, stake_usdc: float) -> Fill:
        ask = _coerce_valid_ask(signal)

        stake = min(float(stake_usdc), self.balance)
        if stake <= 0:
            raise RuntimeError("paper balance exhausted")

        fill_price = min(0.999, ask + self.slippage)
        shares = stake / fill_price

        if not math.isfinite(shares) or shares <= 0:
            print("[Error] 仓位计算为0，放弃下单", flush=True)
            raise RuntimeError("paper order size zero")

        if shares < 1.0:
            if stake >= fill_price * 1.0:
                shares = 1.0
            else:
                print("[Error] 仓位计算为0，放弃下单", flush=True)
                raise RuntimeError("paper order size below minimum trade unit")

        fee = shares * self.fee_coefficient * fill_price * (1 - fill_price)
        cost = shares * fill_price + fee

        if cost > self.balance:
            affordable = self.balance / (fill_price * (1 + self.fee_coefficient * (1 - fill_price)))
            if not math.isfinite(affordable) or affordable <= 0:
                print("[Error] 仓位计算为0，放弃下单", flush=True)
                raise RuntimeError("paper balance cannot cover order")
            shares = affordable
            if shares < 1.0:
                print("[Error] 仓位计算为0，放弃下单", flush=True)
                raise RuntimeError("paper order size below minimum trade unit")
            fee = shares * self.fee_coefficient * fill_price * (1 - fill_price)
            cost = shares * fill_price + fee

        self.balance -= cost
        fill = Fill(
            market_slug=signal.market.slug, side=signal.side,
            token_id=signal.market.up_token if signal.side == "UP" else signal.market.down_token,
            entry_price=fill_price, shares=shares, stake_usdc=stake, fee_usdc=fee,
            order_id=f"paper-{int(__import__('time').time() * 1000)}",
            raw={"paper": True, "ask": ask, "slippage": self.slippage},
        )
        if not math.isfinite(fill.entry_price) or fill.entry_price <= 0:
            raise ValueError(f"invalid fill entry price: {fill.entry_price!r}")
        LOG.info("[Paper] BUY %s %s shares=%.4f entry=%.4f fee=%.4f cost=%.4f",
                 fill.market_slug, fill.side, shares, fill_price, fee, cost)
        return fill

    def settle_paper(self, fill: Fill, winner: str) -> float:
        try:
            won = fill.side == winner
            payout = fill.shares if won else 0.0
            cost = fill.shares * fill.entry_price + fill.fee_usdc
            pnl = payout - cost
            if not math.isfinite(payout) or not math.isfinite(cost) or not math.isfinite(pnl):
                raise ValueError(f"invalid settlement math: payout={payout!r} cost={cost!r} pnl={pnl!r}")
            self.balance += payout
            LOG.info("[Paper] SETTLE %s %s winner=%s won=%s payout=%.4f pnl=%+.4f",
                     fill.market_slug, fill.side, winner, won, payout, pnl)
            return pnl
        except Exception as e:  # 防弹衣，必须清理坏单
            print(f"[Settle Error] 清理坏账: {e}", flush=True)
            return 0.0


class LiveExecutor(BaseExecutor):
    mode = "live"

    def __init__(self, *, cfg: dict, params: V6Params):
        self.cfg = cfg
        self.params = params
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        conf = Path(self.cfg.get("polymarket", {}).get("live_confirm_file", "live_trading_confirm.txt"))
        expected = self.cfg.get("polymarket", {}).get("live_confirm_phrase", "I CONFIRM LIVE TRADING")
        if not conf.exists() or conf.read_text(encoding="utf-8").strip() != expected:
            raise RuntimeError(
                f"LIVE blocked: create {conf} containing exactly: {expected!r}"
            )
        key_env = self.cfg.get("polymarket", {}).get("private_key_env", "POLYMARKET_PRIVATE_KEY")
        key = os.getenv(key_env)
        if not key:
            raise RuntimeError(f"LIVE blocked: environment variable {key_env} is not set")

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
        except Exception:
            try:
                from py_clob_client_v2.client import ClobClient  # type: ignore
                from py_clob_client_v2.clob_types import ApiCreds  # type: ignore
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("LIVE blocked: py-clob-client is not installed") from exc

        host = self.cfg.get("polymarket", {}).get("clob_host", "https://clob.polymarket.com")
        chain_id = int(self.cfg.get("polymarket", {}).get("chain_id", 137))
        sig_type = int(self.cfg.get("polymarket", {}).get("signature_type", 0))
        funder = os.getenv(self.cfg.get("polymarket", {}).get("funder_env", "POLYMARKET_FUNDER")) or None
        client = ClobClient(host, key=key, chain_id=chain_id, signature_type=sig_type, funder=funder)
        try:
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(creds)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("could not derive API creds at startup: %s", exc)
        self._client = client
        LOG.info("LIVE CLOB client initialized (chain_id=%s signature_type=%s)", chain_id, sig_type)

    def buy(self, signal: Signal, stake_usdc: float) -> Fill:
        ask = _coerce_valid_ask(signal)
        if self._client is None:
            raise RuntimeError("LIVE client not initialized")
        token_id = signal.market.up_token if signal.side == "UP" else signal.market.down_token
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
        except Exception:
            from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType  # type: ignore

        args = MarketOrderArgs(token_id=token_id, amount=float(stake_usdc), side="BUY",
                               order_type=OrderType.FOK)
        signed = self._client.create_market_order(args)
        resp = self._client.post_order(signed, OrderType.FOK)
        if not isinstance(resp, dict):
            raise RuntimeError(f"unexpected CLOB response: {resp!r}")
        if str(resp.get("status", "")).lower() not in ("matched", "filled", "success"):
            raise RuntimeError(f"order not filled: {resp}")
        taking = float(resp.get("takingAmount") or 0.0)
        making = float(resp.get("makingAmount") or 0.0)
        shares = taking if taking > 0 else float(stake_usdc) / max(ask, 1e-9)
        entry = (making / shares) if shares > 0 and making > 0 else ask
        fee = shares * 0.07 * entry * (1 - entry)
        fill = Fill(
            market_slug=signal.market.slug, side=signal.side, token_id=token_id,
            entry_price=entry, shares=shares, stake_usdc=float(stake_usdc),
            fee_usdc=fee, order_id=str(resp.get("orderID") or resp.get("orderId") or ""),
            raw=resp,
        )
        if not math.isfinite(fill.entry_price) or fill.entry_price <= 0:
            raise ValueError(f"invalid fill entry price: {fill.entry_price!r}")
        LOG.info("[LIVE] BUY %s %s shares=%.4f entry=%.4f order=%s",
                 fill.market_slug, fill.side, shares, entry, fill.order_id)
        return fill

    def settle_paper(self, fill: Fill, winner: str) -> float:
        raise RuntimeError("live settlements are handled by redeem_engine")


def build_executor(mode: str, cfg: dict, params: V6Params) -> BaseExecutor:
    if mode == "paper":
        return PaperExecutor(
            balance=float(cfg.get("risk", {}).get("starting_balance_usdc", 1000.0)),
            slippage_cents=float(cfg.get("strategy", {}).get("max_slippage_cents", 2.0)),
            fee_coefficient=float(cfg.get("costs", {}).get("fee_coefficient", 0.07)),
        )
    if mode == "live":
        return LiveExecutor(cfg=cfg, params=params)
    raise ValueError(f"unknown mode: {mode}")
