"""
Binance USDT perpetual contract SDK.
"""
import hashlib
import hmac
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from .base_contract_sdk import BaseContractSDK


class BinanceContractSDK(BaseContractSDK):
    """Binance USDT perpetual contract SDK."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://fapi.binance.com"
        self._position_mode_cache: str | None = None
        self._position_mode_checked_at = 0.0
        self._position_mode_ttl_seconds = 60

    def _load_credentials(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")

    def _sign_request(self, params: Dict[str, Any]) -> str:
        query = urlencode(params)
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return f"{query}&signature={signature}"

    def _get_auth_headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self.api_key}

    def _get_symbol_exchange_info(self, symbol: str) -> Dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/fapi/v1/exchangeInfo",
            params={"symbol": symbol},
            timeout=10,
        )
        data = response.json()
        symbols = data.get("symbols", [])
        if not symbols:
            raise RuntimeError(f"Binance symbol info not found for {symbol}")
        return symbols[0]

    def _get_reference_price(self, symbol: str, price: Optional[float]) -> float:
        if price is not None and price > 0:
            return float(price)

        response = requests.get(
            f"{self.base_url}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=10,
        )
        data = response.json()
        reference_price = data.get("markPrice") or data.get("lastPrice")
        if not reference_price:
            raise RuntimeError(f"Binance mark price not found for {symbol}")
        return float(reference_price)

    def _request_position_mode(self) -> str:
        params = {"timestamp": int(time.time() * 1000)}
        query_string = self._sign_request(params)
        response = requests.get(
            f"{self.base_url}/fapi/v1/positionSide/dual?{query_string}",
            headers=self._get_auth_headers(),
            timeout=10,
        )
        data = response.json()

        if "dualSidePosition" not in data:
            raise RuntimeError(data.get("msg", str(data)))

        dual_side_position = data.get("dualSidePosition")
        if isinstance(dual_side_position, str):
            dual_side_position = dual_side_position.lower() == "true"
        return "hedge" if dual_side_position else "one_way"

    def get_position_mode(self, refresh: bool = False) -> str:
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
                    "Failed to refresh Binance position mode, using cached value %s: %s",
                    self._position_mode_cache,
                    exc,
                )
                return self._position_mode_cache

            self.logger.warning(
                "Failed to detect Binance position mode, defaulting to one-way mode: %s",
                exc,
            )
            return "one_way"

    def _build_order_params(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: Optional[float],
        order_type: str,
        position_mode: str,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT" if order_type == "LIMIT" else "MARKET",
            "quantity": str(quantity),
            "timestamp": int(time.time() * 1000),
        }

        if position_mode == "hedge":
            params["positionSide"] = "LONG" if side == "BUY" else "SHORT"

        if params["type"] == "LIMIT":
            params["timeInForce"] = "GTC"
            params["price"] = str(price)

        return params

    def _submit_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        query_string = self._sign_request(params)
        response = requests.post(
            f"{self.base_url}/fapi/v1/order?{query_string}",
            headers=self._get_auth_headers(),
            timeout=10,
        )
        return response.json()

    def _is_position_side_mismatch(self, data: Dict[str, Any]) -> bool:
        message = str(data.get("msg", data.get("message", data))).lower()
        return "position side does not match user's setting" in message

    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        normalized_symbol = symbol.replace("_", "").replace("-", "").upper()
        if not normalized_symbol.endswith("USDT"):
            normalized_symbol = f"{normalized_symbol}USDT"

        symbol_info = self._get_symbol_exchange_info(normalized_symbol)
        market_lot_filter = next(
            (item for item in symbol_info.get("filters", []) if item.get("filterType") == "MARKET_LOT_SIZE"),
            None,
        )
        lot_filter = next(
            (item for item in symbol_info.get("filters", []) if item.get("filterType") == "LOT_SIZE"),
            None,
        )
        active_lot_filter = market_lot_filter or lot_filter
        if not active_lot_filter:
            raise RuntimeError(f"Binance lot size filter not found for {normalized_symbol}")

        min_qty = float(active_lot_filter.get("minQty", lot_filter.get("minQty", "0.001") if lot_filter else "0.001"))
        step = float(active_lot_filter.get("stepSize", lot_filter.get("stepSize", "0.001") if lot_filter else "0.001"))

        if quantity_mode == "margin":
            reference_price = self._get_reference_price(normalized_symbol, price)
            return self._resolve_margin_quantity(
                margin=quantity,
                leverage=leverage,
                reference_price=reference_price,
                min_qty=min_qty,
                step=step,
                quantity_label="contract quantity",
            )

        resolved_quantity = self._normalize_contract_quantity(
            quantity=quantity,
            min_qty=min_qty,
            step=step,
            quantity_label="contract quantity",
        )
        return {
            "quantity": resolved_quantity,
            "input_quantity": quantity,
            "input_mode": "contract",
            "reference_price": float(price) if price else None,
        }

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        try:
            params = {
                "symbol": symbol,
                "leverage": leverage,
                "timestamp": int(time.time() * 1000),
            }
            query_string = self._sign_request(params)
            response = requests.post(
                f"{self.base_url}/fapi/v1/leverage?{query_string}",
                headers={"X-MBX-APIKEY": self.api_key},
                timeout=10,
            )
            data = response.json()
            if "leverage" in data or data.get("leverage"):
                return {"success": True, "message": f"Leverage set to {leverage}x"}
            return {"success": False, "message": data.get("msg", str(data))}
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
            return {"success": False, "message": "Missing Binance API credentials"}

        normalized_symbol = symbol.replace("_", "").replace("-", "").upper()
        if not normalized_symbol.endswith("USDT"):
            normalized_symbol = f"{normalized_symbol}USDT"

        order_side = "BUY" if side.lower() in ("buy", "long") else "SELL"
        normalized_order_type = order_type.upper()

        try:
            if leverage:
                self.set_leverage(normalized_symbol, leverage)

            position_mode = self.get_position_mode()
            params = self._build_order_params(
                symbol=normalized_symbol,
                side=order_side,
                quantity=quantity,
                price=price,
                order_type=normalized_order_type,
                position_mode=position_mode,
            )
            data = self._submit_order(params)

            if self._is_position_side_mismatch(data):
                self._position_mode_cache = None
                self._position_mode_checked_at = 0.0
                refreshed_position_mode = self.get_position_mode(refresh=True)

                if refreshed_position_mode != position_mode:
                    self.logger.info(
                        "Retrying Binance order with refreshed position mode: %s -> %s",
                        position_mode,
                        refreshed_position_mode,
                    )
                    params = self._build_order_params(
                        symbol=normalized_symbol,
                        side=order_side,
                        quantity=quantity,
                        price=price,
                        order_type=normalized_order_type,
                        position_mode=refreshed_position_mode,
                    )
                    data = self._submit_order(params)

            if "orderId" in data:
                return {
                    "success": True,
                    "order_id": str(data["orderId"]),
                    "message": "Order placed successfully",
                }
            return {"success": False, "message": data.get("msg", str(data))}
        except Exception as exc:
            self.logger.error("Binance contract order failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def list_contract_symbols(self) -> list[str]:
        try:
            response = requests.get(f"{self.base_url}/fapi/v1/exchangeInfo", timeout=10)
            response.raise_for_status()
            data = response.json()
            symbols = [
                item["symbol"]
                for item in data.get("symbols", [])
                if item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == "USDT"
                and item.get("status") == "TRADING"
            ]
            return sorted(set(symbols))
        except Exception as exc:
            self.logger.error("Binance contract list load failed: %s", exc)
            raise
