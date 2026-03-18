"""
OKX USDT perpetual contract SDK.
"""
import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from .base_contract_sdk import BaseContractSDK


class OkxContractSDK(BaseContractSDK):
    """OKX USDT perpetual contract SDK based on the V5 REST API."""

    def __init__(self):
        super().__init__()
        self.base_url = self.base_url or "https://www.okx.com"
        self._position_mode_cache: str | None = None
        self._position_mode_checked_at = 0.0
        self._position_mode_ttl_seconds = 60

    def _load_credentials(self):
        self.api_key = os.getenv("OKX_API_KEY")
        self.api_secret = os.getenv("OKX_API_SECRET")
        self.passphrase = os.getenv("OKX_API_PASSPHRASE") or os.getenv("OKX_PASSPHRASE")
        self.base_url = os.getenv("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
        self.trade_mode = self._normalize_trade_mode(os.getenv("OKX_TRADE_MODE"))
        simulated_value = os.getenv("OKX_SIMULATED_TRADING", os.getenv("OKX_DEMO_TRADING", "0"))
        self.simulated_trading = str(simulated_value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _normalize_trade_mode(self, value: Optional[str]) -> str:
        normalized_value = str(value or "cross").strip().lower()
        if normalized_value not in {"cross", "isolated"}:
            return "cross"
        return normalized_value

    def _build_timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _format_number(self, value: float | int | str) -> str:
        formatted = format(Decimal(str(value)), "f")
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted or "0"

    def _sign_request(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        prehash = f"{timestamp}{method.upper()}{request_path}{body}"
        digest = hmac.new(self.api_secret.encode(), prehash.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def _build_auth_headers(self, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        timestamp = self._build_timestamp()
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign_request(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.simulated_trading:
            headers["x-simulated-trading"] = "1"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        auth: bool = False,
    ) -> Dict[str, Any]:
        query_string = urlencode(params or {})
        request_path = f"{path}?{query_string}" if query_string else path
        body = json.dumps(json_body, separators=(",", ":"), ensure_ascii=False) if json_body is not None else ""
        headers = self._build_auth_headers(method, request_path, body) if auth else None

        response = requests.request(
            method=method.upper(),
            url=f"{self.base_url}{request_path}",
            headers=headers,
            data=body if body else None,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def _require_ok(self, payload: Dict[str, Any], default_message: str = "OKX request failed"):
        if str(payload.get("code")) == "0":
            return

        data = payload.get("data")
        if isinstance(data, list) and data:
            detailed_message = data[0].get("sMsg") or data[0].get("msg")
            if detailed_message:
                raise RuntimeError(detailed_message)

        raise RuntimeError(payload.get("msg") or default_message)

    def _to_instrument_id(self, symbol: str) -> str:
        normalized_symbol = str(symbol or "").strip().upper().replace("_", "-").replace("/", "-")
        if not normalized_symbol:
            return "BTC-USDT-SWAP"

        if normalized_symbol.endswith("-SWAP"):
            return normalized_symbol

        parts = [item for item in normalized_symbol.split("-") if item]
        if len(parts) >= 2 and parts[1] == "USDT":
            return f"{parts[0]}-USDT-SWAP"

        compact_symbol = normalized_symbol.replace("-", "")
        if compact_symbol.endswith("USDT"):
            return f"{compact_symbol[:-4]}-USDT-SWAP"

        return f"{compact_symbol}-USDT-SWAP"

    def _to_asset_symbol(self, inst_id: str) -> str:
        normalized_inst_id = self._to_instrument_id(inst_id)
        return normalized_inst_id.split("-")[0]

    def _get_instrument_info(self, inst_id: str) -> Dict[str, Any]:
        payload = self._request(
            "GET",
            "/api/v5/public/instruments",
            params={"instType": "SWAP", "instId": inst_id},
        )
        self._require_ok(payload, default_message=f"Failed to load OKX instrument info for {inst_id}")
        data = payload.get("data", [])
        if not data:
            raise RuntimeError(f"OKX instrument info not found for {inst_id}")
        return data[0]

    def _get_reference_price(self, inst_id: str, price: Optional[float]) -> float:
        if price is not None and price > 0:
            return float(price)

        payload = self._request(
            "GET",
            "/api/v5/public/mark-price",
            params={"instType": "SWAP", "instId": inst_id},
        )
        self._require_ok(payload, default_message=f"Failed to load OKX mark price for {inst_id}")
        data = payload.get("data", [])
        if not data:
            raise RuntimeError(f"OKX mark price not found for {inst_id}")

        reference_price = data[0].get("markPx")
        if not reference_price:
            raise RuntimeError(f"OKX mark price missing for {inst_id}")
        return float(reference_price)

    def _get_funding_snapshot(self, inst_id: str) -> Dict[str, Any]:
        payload = self._request(
            "GET",
            "/api/v5/public/funding-rate",
            params={"instId": inst_id},
        )
        self._require_ok(payload, default_message=f"Failed to load OKX funding rate for {inst_id}")
        data = payload.get("data", [])
        if not data:
            raise RuntimeError(f"OKX funding rate not found for {inst_id}")
        return data[0]

    def _get_funding_snapshot_with_session(self, session: requests.Session, inst_id: str) -> Dict[str, Any]:
        response = session.get(
            f"{self.base_url}/api/v5/public/funding-rate",
            params={"instId": inst_id},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        self._require_ok(payload, default_message=f"Failed to load OKX funding rate for {inst_id}")
        data = payload.get("data", [])
        if not data:
            raise RuntimeError(f"OKX funding rate not found for {inst_id}")
        return data[0]

    def _request_position_mode(self) -> str:
        payload = self._request("GET", "/api/v5/account/config", auth=True)
        self._require_ok(payload, default_message="Failed to query OKX account config")
        data = payload.get("data", [])
        if not data:
            raise RuntimeError("OKX account config is empty")
        return data[0].get("posMode") or "net_mode"

    def get_position_mode(self, refresh: bool = False) -> str:
        if not self.api_key or not self.api_secret or not self.passphrase:
            return "net_mode"

        now = time.time()
        if (
            not refresh
            and self._position_mode_cache
            and now - self._position_mode_checked_at < self._position_mode_ttl_seconds
        ):
            return self._position_mode_cache

        try:
            position_mode = self._request_position_mode()
            self._position_mode_cache = position_mode
            self._position_mode_checked_at = now
            return position_mode
        except Exception as exc:
            if self._position_mode_cache:
                self.logger.warning(
                    "Failed to refresh OKX position mode, using cached value %s: %s",
                    self._position_mode_cache,
                    exc,
                )
                return self._position_mode_cache

            self.logger.warning("Failed to detect OKX position mode, defaulting to net mode: %s", exc)
            return "net_mode"

    def _set_leverage_request(
        self,
        inst_id: str,
        leverage: int,
        pos_side: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "instId": inst_id,
            "lever": str(leverage),
            "mgnMode": self.trade_mode,
        }
        if pos_side:
            payload["posSide"] = pos_side

        response_payload = self._request(
            "POST",
            "/api/v5/account/set-leverage",
            json_body=payload,
            auth=True,
        )
        self._require_ok(response_payload, default_message=f"Failed to set OKX leverage for {inst_id}")

        row = (response_payload.get("data") or [{}])[0]
        if row.get("lever"):
            return {"success": True, "message": f"Leverage set to {row.get('lever')}x"}
        return {"success": True, "message": f"Leverage set to {leverage}x"}

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        if not self.api_key or not self.api_secret or not self.passphrase:
            return {"success": False, "message": "Missing OKX API credentials"}

        inst_id = self._to_instrument_id(symbol)
        position_mode = self.get_position_mode()

        try:
            if position_mode == "long_short_mode" and self.trade_mode == "isolated":
                long_result = self._set_leverage_request(inst_id, leverage, pos_side="long")
                short_result = self._set_leverage_request(inst_id, leverage, pos_side="short")
                if long_result.get("success") and short_result.get("success"):
                    return {"success": True, "message": f"Leverage set to {leverage}x"}
                return {
                    "success": False,
                    "message": long_result.get("message") or short_result.get("message") or "Failed to set leverage",
                }

            return self._set_leverage_request(inst_id, leverage)
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        inst_id = self._to_instrument_id(symbol)
        instrument = self._get_instrument_info(inst_id)
        min_qty = float(instrument.get("minSz") or instrument.get("lotSz") or "0.01")
        step = float(instrument.get("lotSz") or instrument.get("minSz") or "0.01")
        contract_multiplier = float(instrument.get("ctVal") or "1")

        if quantity_mode == "margin":
            reference_price = self._get_reference_price(inst_id, price)
            return self._resolve_margin_quantity(
                margin=quantity,
                leverage=leverage,
                reference_price=reference_price,
                min_qty=min_qty,
                step=step,
                contract_multiplier=contract_multiplier,
                quantity_label="contract size",
            )

        resolved_quantity = self._normalize_contract_quantity(
            quantity=quantity,
            min_qty=min_qty,
            step=step,
            quantity_label="contract size",
        )
        return {
            "quantity": resolved_quantity,
            "input_quantity": quantity,
            "input_mode": "contract",
            "reference_price": float(price) if price else None,
            "contract_multiplier": contract_multiplier,
        }

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: Optional[float] = None,
        order_type: str = "market",
        leverage: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not self.api_key or not self.api_secret or not self.passphrase:
            return {"success": False, "message": "Missing OKX API credentials"}

        inst_id = self._to_instrument_id(symbol)
        order_side = "buy" if side.lower() in ("buy", "long") else "sell"
        pos_side = "long" if side.lower() in ("buy", "long") else "short"
        position_mode = self.get_position_mode()
        normalized_order_type = "limit" if order_type.lower() == "limit" else "market"

        try:
            if leverage:
                leverage_result = self.set_leverage(inst_id, leverage)
                if not leverage_result.get("success"):
                    self.logger.warning("Failed to set OKX leverage before order: %s", leverage_result.get("message"))

            payload = {
                "instId": inst_id,
                "tdMode": self.trade_mode,
                "side": order_side,
                "ordType": normalized_order_type,
                "sz": self._format_number(quantity),
            }

            if position_mode == "long_short_mode":
                payload["posSide"] = pos_side

            if normalized_order_type == "limit":
                if price is None or price <= 0:
                    return {"success": False, "message": "Limit order requires a valid price"}
                payload["px"] = self._format_number(price)

            response_payload = self._request(
                "POST",
                "/api/v5/trade/order",
                json_body=payload,
                auth=True,
            )

            if str(response_payload.get("code")) != "0":
                data = response_payload.get("data") or [{}]
                error_message = data[0].get("sMsg") or response_payload.get("msg") or str(response_payload)
                return {"success": False, "message": error_message}

            row = (response_payload.get("data") or [{}])[0]
            if str(row.get("sCode", "0")) != "0":
                return {"success": False, "message": row.get("sMsg") or "OKX order rejected"}

            order_id = row.get("ordId") or ""
            return {
                "success": True,
                "order_id": str(order_id),
                "message": "Order placed successfully",
            }
        except Exception as exc:
            self.logger.error("OKX contract order failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def list_contract_symbols(self) -> list[str]:
        try:
            payload = self._request(
                "GET",
                "/api/v5/public/instruments",
                params={"instType": "SWAP"},
            )
            self._require_ok(payload, default_message="Failed to load OKX contract list")
            symbols = [
                item["instId"]
                for item in payload.get("data", [])
                if item.get("instId")
                and item.get("state") == "live"
                and item.get("settleCcy") == "USDT"
                and item.get("ctType") == "linear"
            ]
            return sorted(set(symbols))
        except Exception as exc:
            self.logger.error("OKX contract list load failed: %s", exc)
            raise

    def list_contract_market_snapshots(self) -> list[Dict[str, Any]]:
        try:
            with requests.Session() as session:
                instruments_response = session.get(
                    f"{self.base_url}/api/v5/public/instruments",
                    params={"instType": "SWAP"},
                    timeout=15,
                )
                instruments_response.raise_for_status()
                instruments_payload = instruments_response.json()
                self._require_ok(instruments_payload, default_message="Failed to load OKX contract metadata")

                mark_price_response = session.get(
                    f"{self.base_url}/api/v5/public/mark-price",
                    params={"instType": "SWAP"},
                    timeout=15,
                )
                mark_price_response.raise_for_status()
                mark_price_payload = mark_price_response.json()
                self._require_ok(mark_price_payload, default_message="Failed to load OKX mark prices")
                mark_price_map = {
                    item.get("instId"): item.get("markPx")
                    for item in mark_price_payload.get("data", [])
                    if item.get("instId")
                }

                snapshots = []
                for instrument in instruments_payload.get("data", []):
                    contract_code = instrument.get("instId")
                    if (
                        not contract_code
                        or instrument.get("state") != "live"
                        or instrument.get("settleCcy") != "USDT"
                        or instrument.get("ctType") != "linear"
                    ):
                        continue

                    funding_snapshot: Dict[str, Any] = {}
                    for _ in range(3):
                        try:
                            funding_snapshot = self._get_funding_snapshot_with_session(session, contract_code)
                            break
                        except Exception as exc:
                            self.logger.warning("Retrying OKX funding rate for %s: %s", contract_code, exc)
                            time.sleep(0.2)

                    funding_time = self._safe_float(funding_snapshot.get("fundingTime"))
                    prev_funding_time = self._safe_float(funding_snapshot.get("prevFundingTime"))
                    next_funding_time = self._safe_float(funding_snapshot.get("nextFundingTime"))

                    funding_interval_ms = None
                    if funding_time and prev_funding_time and funding_time > prev_funding_time:
                        funding_interval_ms = funding_time - prev_funding_time
                    elif next_funding_time and funding_time and next_funding_time > funding_time:
                        funding_interval_ms = next_funding_time - funding_time

                    snapshots.append(
                        self._build_market_snapshot(
                            symbol=self._to_asset_symbol(contract_code),
                            contract_code=contract_code,
                            price=mark_price_map.get(contract_code),
                            funding_rate=funding_snapshot.get("fundingRate"),
                            funding_interval=funding_interval_ms,
                            funding_interval_unit="milliseconds",
                        )
                    )

            return sorted(snapshots, key=lambda row: row["contract_code"])
        except Exception as exc:
            self.logger.error("OKX market snapshot load failed: %s", exc)
            raise
