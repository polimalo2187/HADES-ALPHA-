from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict

import requests

from app.config import (
    get_bsc_rpc_http_url,
    get_payment_lookback_blocks,
    get_payment_min_confirmations,
    get_payment_receiver_address,
    get_payment_token_contract,
    get_payment_token_decimals,
)

logger = logging.getLogger(__name__)

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_DECIMAL_QUANT = Decimal("0.001")
_DEFAULT_LOG_WINDOW = 250
_MIN_LOG_WINDOW = 1
_APPROX_BSC_BLOCK_SECONDS = 3
_SEARCH_SAFETY_BLOCKS = 40


class VerificationConfigError(RuntimeError):
    pass


def _normalize_hex_address(value: str) -> str:
    value = (value or "").strip().lower()
    if not value:
        return ""
    if value.startswith("0x"):
        value = value[2:]
    return "0x" + value.rjust(40, "0")[-40:]


def _topic_for_address(address: str) -> str:
    normalized = _normalize_hex_address(address)[2:]
    return "0x" + normalized.rjust(64, "0")


def _rpc_call(method: str, params: list[Any]) -> Any:
    url = get_bsc_rpc_http_url()
    if not url:
        raise VerificationConfigError("BSC_RPC_HTTP_URL no está configurado")

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    response = requests.post(url, json=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    if data.get("error"):
        raise RuntimeError(f"RPC error en {method}: {data['error']}")
    return data.get("result")


def _get_latest_block() -> int:
    return int(_rpc_call("eth_blockNumber", []), 16)


def _get_block(block_number: int) -> Dict[str, Any]:
    block = _rpc_call("eth_getBlockByNumber", [hex(block_number), False])
    if not isinstance(block, dict) or not block.get("timestamp"):
        raise RuntimeError(f"No se pudo cargar el bloque {block_number}")
    return block


def _get_block_timestamp(block_number: int) -> datetime:
    block = _get_block(block_number)
    ts = int(block["timestamp"], 16)
    return datetime.utcfromtimestamp(ts)


def _get_block_hash(block_number: int) -> str:
    block = _get_block(block_number)
    block_hash = str(block.get("hash") or "")
    if not block_hash:
        raise RuntimeError(f"No se pudo resolver el hash del bloque {block_number}")
    return block_hash


def _quantize_amount(value: Decimal) -> Decimal:
    return value.quantize(_DECIMAL_QUANT, rounding=ROUND_DOWN)


def _is_limit_exceeded_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "eth_getlogs" in message and (
        "limit exceeded" in message
        or "-32005" in message
        or "query returned more than" in message
        or "block range" in message
        or "response size exceeded" in message
    )


def _query_transfer_logs(base_filter: Dict[str, Any], *, from_block: int | None = None, to_block: int | None = None, block_hash: str | None = None) -> list[Dict[str, Any]]:
    payload = dict(base_filter)
    if block_hash:
        payload["blockHash"] = block_hash
    else:
        if from_block is None or to_block is None:
            raise ValueError("from_block y to_block son obligatorios si no se usa block_hash")
        payload["fromBlock"] = hex(int(from_block))
        payload["toBlock"] = hex(int(to_block))
    return _rpc_call("eth_getLogs", [payload]) or []


def _estimate_from_block(latest_block: int, created_at: datetime | None, expires_at: datetime | None) -> int:
    max_lookback = max(int(get_payment_lookback_blocks()), 1)
    fallback = max(latest_block - max_lookback, 1)
    if not isinstance(created_at, datetime):
        return fallback

    upper_bound = expires_at if isinstance(expires_at, datetime) else datetime.utcnow()
    reference_time = min(datetime.utcnow(), upper_bound)
    age_seconds = max((reference_time - created_at).total_seconds(), 0.0)
    estimated_blocks = int(age_seconds / _APPROX_BSC_BLOCK_SECONDS) + _SEARCH_SAFETY_BLOCKS
    return max(latest_block - min(max_lookback, max(estimated_blocks, _SEARCH_SAFETY_BLOCKS)), 1)


def _get_transfer_logs(token_contract: str, receiver_address: str, from_block: int, to_block: int) -> list[Dict[str, Any]]:
    if from_block > to_block:
        return []

    base_filter = {
        "address": token_contract,
        "topics": [TRANSFER_TOPIC, None, _topic_for_address(receiver_address)],
    }
    logs: list[Dict[str, Any]] = []
    current_from = int(from_block)
    window = min(max(_MIN_LOG_WINDOW, _DEFAULT_LOG_WINDOW), max(to_block - from_block + 1, _MIN_LOG_WINDOW))

    while current_from <= to_block:
        current_to = min(current_from + window - 1, to_block)
        try:
            batch = _query_transfer_logs(base_filter, from_block=current_from, to_block=current_to)
            logs.extend(batch)
            current_from = current_to + 1
            continue
        except RuntimeError as exc:
            if not _is_limit_exceeded_error(exc):
                raise
            if window > _MIN_LOG_WINDOW:
                next_window = max(window // 2, _MIN_LOG_WINDOW)
                if next_window == window:
                    next_window = max(window - 1, _MIN_LOG_WINDOW)
                logger.warning(
                    "RPC limit exceeded consultando logs BEP20; reduciendo ventana de %s a %s bloques",
                    window,
                    next_window,
                )
                window = next_window
                continue

            logger.warning(
                "RPC limit exceeded incluso en un solo bloque (%s); reintentando con blockHash",
                current_from,
            )
            block_hash = _get_block_hash(current_from)
            batch = _query_transfer_logs(base_filter, block_hash=block_hash)
            logs.extend(batch)
            current_from += 1
            window = _MIN_LOG_WINDOW

    return logs


def verify_payment(order: Dict[str, Any]) -> Dict[str, Any]:
    token_contract = (get_payment_token_contract() or order.get("token_contract") or "").lower()
    receiver_address = _normalize_hex_address(get_payment_receiver_address() or order.get("deposit_address") or "")

    if not token_contract or not receiver_address:
        raise VerificationConfigError("PAYMENT_TOKEN_CONTRACT o PAYMENT_RECEIVER_ADDRESS no están configurados")

    min_confirmations = get_payment_min_confirmations()
    token_decimals = get_payment_token_decimals()
    latest_block = _get_latest_block()

    created_at = order.get("created_at") or datetime.utcnow() - timedelta(minutes=60)
    expires_at = order.get("expires_at") or datetime.utcnow() + timedelta(minutes=5)
    expected_amount = _quantize_amount(Decimal(str(order.get("amount_usdt") or "0")))
    from_block = _estimate_from_block(latest_block, created_at, expires_at)

    logs = _get_transfer_logs(token_contract, receiver_address, from_block, latest_block)

    candidates: list[Dict[str, Any]] = []
    for log in logs or []:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue

        block_number = int(log.get("blockNumber"), 16)
        confirmations = latest_block - block_number + 1
        amount_raw = int(log.get("data") or "0x0", 16)
        amount = _quantize_amount(Decimal(amount_raw) / (Decimal(10) ** token_decimals))
        if amount != expected_amount:
            continue

        tx_time = _get_block_timestamp(block_number)
        if tx_time < created_at - timedelta(minutes=2) or tx_time > expires_at + timedelta(minutes=2):
            continue

        from_address = _normalize_hex_address("0x" + topics[1][-40:])
        to_address = _normalize_hex_address("0x" + topics[2][-40:])

        candidates.append(
            {
                "tx_hash": log.get("transactionHash"),
                "from_address": from_address,
                "to_address": to_address,
                "amount_usdt": float(amount),
                "confirmations": confirmations,
                "block_number": block_number,
                "tx_time": tx_time,
                "confirmed": confirmations >= min_confirmations,
            }
        )

    if not candidates:
        return {
            "status": "not_found",
            "reason": "payment_not_found",
            "confirmed": False,
            "confirmations": 0,
        }

    candidates.sort(key=lambda item: (item["confirmed"], item["block_number"]), reverse=True)
    match = candidates[0]
    status = "confirmed" if match["confirmed"] else "unconfirmed"
    reason = "payment_confirmed" if match["confirmed"] else "payment_waiting_confirmations"
    return {
        "status": status,
        "reason": reason,
        **match,
    }
