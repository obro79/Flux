from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.market_data.models import MarketTradeMessage, Trade


def normalize_coinbase_message(
    payload: Mapping[str, Any],
) -> MarketTradeMessage | None:
    trades: list[Trade] = []

    for event in payload.get("events", []):
        if not isinstance(event, Mapping):
            continue
        for raw_trade in event.get("trades", []):
            if not isinstance(raw_trade, Mapping):
                continue
            trades.append(
                Trade(
                    exchange="coinbase",
                    trade_id=str(raw_trade["trade_id"]),
                    product_id=str(raw_trade["product_id"]),
                    price=raw_trade["price"],
                    size=raw_trade["size"],
                    side=str(raw_trade["side"]).upper(),
                    time=raw_trade["time"],
                )
            )

    if not trades:
        return None

    return MarketTradeMessage.from_trades(
        exchange="coinbase",
        trades=trades,
        event_type=str(payload.get("type", "update")),
        timestamp=payload.get("timestamp"),
    )


def normalize_kraken_message(
    payload: Mapping[str, Any],
    *,
    symbol_map: Mapping[str, str],
) -> MarketTradeMessage | None:
    if payload.get("channel") != "trade":
        return None

    symbol = payload.get("symbol")
    product_id = symbol_map.get(str(symbol)) if symbol is not None else None
    if not product_id:
        return None

    trades: list[Trade] = []
    for raw_trade in payload.get("data", []):
        if not isinstance(raw_trade, Mapping):
            continue
        trades.append(
            Trade(
                exchange="kraken",
                trade_id=str(raw_trade["trade_id"]),
                product_id=product_id,
                price=raw_trade["price"],
                size=raw_trade["qty"],
                side=str(raw_trade["side"]).upper(),
                time=raw_trade["timestamp"],
            )
        )

    if not trades:
        return None

    return MarketTradeMessage.from_trades(
        exchange="kraken",
        trades=trades,
        timestamp=payload.get("timestamp"),
    )
