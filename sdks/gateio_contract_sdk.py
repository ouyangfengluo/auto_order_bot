"""
Gate.io USDT perpetual contract SDK.
"""
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, Optional

import requests

from .base_contract_sdk import BaseContractSDK


class GateioContractSDK(BaseContractSDK):
    """Gate.io USDT perpetual contract SDK."""

    def __init__(self):
        super().__init__()
        self.host = "https://api.gateio.ws"
        self.prefix = "/api/v4"
        self.base_url = f"{self.host}{self.prefix}"

    def _load_credentials(self):
        self.api_key = os.getenv("GATEIO_API_KEY")
        self.api_secret = os.getenv("GATEIO_API_SECRET")

    def _sign_request(self, method: str, path: str, query: str = "", body: str = "") -> Dict[str, str]:
        timestamp = str(int(time.time()))
        hashed_body = hashlib.sha512(body.encode()).hexdigest()
        request_path = f"{self.prefix}{path}"
        sign_string = f"{method}\n{request_path}\n{query}\n{hashed_body}\n{timestamp}"
        signature = hmac.new(self.api_secret.encode(), sign_string.encode(), hashlib.sha512).hexdigest()
        return {
            "KEY": self.api_key,
            "SIGN": signature,
            "Timestamp": timestamp,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _build_url(self, path: str, query: str = "") -> str:
        if query:
            return f"{self.base_url}{path}?{query}"
        return f"{self.base_url}{path}"

    def _to_order_size(self, quantity: float) -> int:
        return int(round(float(quantity)))

    def _parse_response_payload(self, response: requests.Response) -> Any:
        if not response.text:
            return {}
        try:
            return response.json()
        except ValueError:
            return response.text

    def _format_error_message(self, response: requests.Response, payload: Any) -> str:
        if isinstance(payload, dict):
            label = str(payload.get("label", "") or "").strip()
            message = str(payload.get("message", "") or "").strip()
            details = [item for item in (message, label) if item]
            if details:
                return " | ".join(details)
        if isinstance(payload, list) and payload:
            return str(payload)
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return f"HTTP {response.status_code}"

    def _to_contract_symbol(self, symbol: str) -> str:
        normalized_symbol = symbol.replace("-", "").upper()
        if "_" in normalized_symbol:
            return normalized_symbol
        if normalized_symbol.endswith("USDT"):
            return f"{normalized_symbol[:-4]}_USDT"
        return f"{normalized_symbol}_USDT"

    def _to_asset_symbol(self, contract: str) -> str:
        normalized_contract = str(contract or "").strip().upper()
        if normalized_contract.endswith("_USDT"):
            return normalized_contract[:-5]
        return normalized_contract.replace("_", "")

    def _get_contract_info(self, contract: str) -> Dict[str, Any]:
        response = requests.get(f"{self.base_url}/futures/usdt/contracts/{contract}", timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get("name"):
            raise RuntimeError(f"Gate.io contract info not found for {contract}")
        return data

    def _get_reference_price(self, contract_info: Dict[str, Any], price: Optional[float]) -> float:
        if price is not None and price > 0:
            return float(price)

        reference_price = contract_info.get("mark_price") or contract_info.get("last_price")
        if not reference_price:
            raise RuntimeError(f"Gate.io mark price not found for {contract_info.get('name', 'unknown contract')}")
        return float(reference_price)

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        contract = self._to_contract_symbol(symbol)
        try:
            path = f"/futures/usdt/positions/{contract}/leverage"
            query = f"leverage={int(leverage)}"
            headers = self._sign_request("POST", path, query, "")
            response = requests.post(
                self._build_url(path, query),
                headers=headers,
                timeout=10,
            )
            payload = self._parse_response_payload(response)
            if response.status_code == 200:
                return {"success": True, "message": f"Leverage set to {leverage}x"}
            return {"success": False, "message": self._format_error_message(response, payload)}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: Optional[float] = None,
        order_type: str = "market",
        leverage: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not self.api_key or not self.api_secret:
            return {"success": False, "message": "Missing Gate.io API credentials"}

        contract = self._to_contract_symbol(symbol)
        reduce_only = False
        if side.lower() in ("buy", "long"):
            gate_side = "long"
        else:
            gate_side = "short"

        normalized_order_type = order_type.lower()
        try:
            if leverage:
                leverage_result = self.set_leverage(symbol, leverage)
                if not leverage_result.get("success"):
                    return {"success": False, "message": leverage_result.get("message") or "Failed to set leverage"}

            size = self._to_order_size(quantity)
            if size <= 0:
                return {"success": False, "message": f"Invalid Gate.io contract size: {quantity}"}
            payload = {
                "contract": contract,
                "size": size,
                "reduce_only": reduce_only,
            }
            if gate_side == "short":
                payload["size"] = -abs(size)

            if normalized_order_type == "limit" and price:
                payload["price"] = str(price)
                payload["tif"] = "gtc"
            else:
                payload["price"] = "0"
                payload["tif"] = "ioc"

            body = json.dumps(payload, separators=(",", ":"))
            path = "/futures/usdt/orders"
            headers = self._sign_request("POST", path, "", body)

            response = requests.post(
                self._build_url(path),
                headers=headers,
                data=body,
                timeout=10,
            )
            data = self._parse_response_payload(response)

            if isinstance(data, dict) and "id" in data:
                return {
                    "success": True,
                    "order_id": str(data["id"]),
                    "message": "Order placed successfully",
                }
            return {"success": False, "message": self._format_error_message(response, data)}
        except Exception as exc:
            self.logger.error("Gate.io contract order failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        contract = self._to_contract_symbol(symbol)
        contract_info = self._get_contract_info(contract)
        min_qty = float(contract_info.get("order_size_min", 1))
        step = 1.0
        contract_multiplier = float(contract_info.get("quanto_multiplier", 1))

        if quantity_mode == "margin":
            reference_price = self._get_reference_price(contract_info, price)
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

    def list_contract_symbols(self) -> list[str]:
        try:
            response = requests.get(f"{self.base_url}/futures/usdt/contracts", timeout=10)
            response.raise_for_status()
            data = response.json()
            symbols = [
                item["name"]
                for item in data
                if item.get("name")
                and item.get("in_delisting") is False
                and item.get("status") == "trading"
            ]
            return sorted(set(symbols))
        except Exception as exc:
            self.logger.error("Gate.io contract list load failed: %s", exc)
            raise

    def list_contract_market_snapshots(self) -> list[Dict[str, Any]]:
        try:
            response = requests.get(f"{self.base_url}/futures/usdt/contracts", timeout=15)
            response.raise_for_status()
            data = response.json()

            snapshots = []
            for item in data:
                contract_code = item.get("name")
                if (
                    not contract_code
                    or item.get("in_delisting") is not False
                    or item.get("status") != "trading"
                ):
                    continue

                snapshots.append(
                    self._build_market_snapshot(
                        symbol=self._to_asset_symbol(contract_code),
                        contract_code=contract_code,
                        price=item.get("mark_price") or item.get("last_price"),
                        funding_rate=item.get("funding_rate"),
                        funding_interval=item.get("funding_interval"),
                        funding_interval_unit="seconds",
                    )
                )

            return sorted(snapshots, key=lambda row: row["contract_code"])
        except Exception as exc:
            self.logger.error("Gate.io market snapshot load failed: %s", exc)
            raise
