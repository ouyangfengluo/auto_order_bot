"""
Collect perpetual contract market snapshots across exchanges.

Example:
    python fetch_market_snapshots.py --once
    python fetch_market_snapshots.py --interval-seconds 60 --output logs/contract_market_snapshots.json
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Any

from sdks import ContractSDKFactory


SUPPORTED_EXCHANGES = ["binance", "gateio", "bybit", "okx", "lighter"]
DEFAULT_OUTPUT_PATH = Path(__file__).parent / "logs" / "contract_market_snapshots.json"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("market_snapshot_collector")


def normalize_exchanges(raw_exchanges: str | None) -> list[str]:
    if not raw_exchanges:
        return list(SUPPORTED_EXCHANGES)

    exchanges = []
    for part in raw_exchanges.split(","):
        exchange = str(part).strip().lower()
        if not exchange:
            continue
        if exchange not in SUPPORTED_EXCHANGES:
            raise ValueError(f"Unsupported exchange: {exchange}")
        exchanges.append(exchange)

    if not exchanges:
        return list(SUPPORTED_EXCHANGES)
    return list(dict.fromkeys(exchanges))


def collect_market_snapshots(exchanges: list[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    selected_exchanges = exchanges or list(SUPPORTED_EXCHANGES)
    payload: dict[str, list[dict[str, Any]]] = {}

    for exchange in selected_exchanges:
        try:
            sdk = ContractSDKFactory.get_sdk(exchange)
            payload[exchange] = sdk.list_contract_market_snapshots()
            logger.info("Fetched %s market snapshots for %s", len(payload[exchange]), exchange)
        except Exception as exc:
            logger.exception("Failed to fetch market snapshots for %s", exchange)
            payload[exchange] = []
            logger.warning("Using empty market snapshot list for %s: %s", exchange, exc)

    return payload


def write_market_snapshots(payload: dict[str, list[dict[str, Any]]], output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def run_loop(exchanges: list[str], output_path: Path, interval_seconds: int, once: bool = False):
    while True:
        started_at = time.time()
        payload = collect_market_snapshots(exchanges)
        write_market_snapshots(payload, output_path)
        logger.info("Wrote market snapshots to %s", output_path)

        if once:
            return

        elapsed = time.time() - started_at
        sleep_seconds = max(interval_seconds - elapsed, 0)
        if sleep_seconds:
            time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch perpetual contract price/funding snapshots.")
    parser.add_argument(
        "--exchanges",
        default=",".join(SUPPORTED_EXCHANGES),
        help="Comma-separated exchange list. Supported: binance, gateio, bybit, okx, lighter.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Polling interval in seconds. Default: 60.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output JSON path.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch once and exit instead of polling forever.",
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be greater than 0")

    exchanges = normalize_exchanges(args.exchanges)
    output_path = Path(args.output).expanduser().resolve()
    run_loop(exchanges, output_path, args.interval_seconds, once=args.once)


if __name__ == "__main__":
    main()
