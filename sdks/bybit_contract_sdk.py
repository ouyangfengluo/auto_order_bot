"""
Bybit USDT perpetual contract SDK.
"""
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any, Dict, Optional

import requests

from .base_contract_sdk import BaseContractSDK


class BybitContractSDK(BaseContractSDK):
    """Bybit USDT perpetual contract SDK."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://api.bybit.com"

    def _load_credentials(self):
        self.api_key = os.getenv("BYBIT_API_KEY")
        self.api_secret = os.getenv("BYBIT_API_SECRET")

    def _sign_request(self, params: Dict[str, Any]) -> Dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        payload = json.dumps(params)
        sign_payload = f"{timestamp}{self.api_key}{recv_window}{payload}"
        signature = hmac.new(self.api_secret.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN-TYPE": "2",
            "Content-Type": "application/json",
        }

    def _to_symbol(self, symbol: str) -> str:
        normalized_symbol = symbol.replace("_", "").replace("-", "").upper()
        if not normalized_symbol.endswith("USDT"):
            normalized_symbol = f"{normalized_symbol}USDT"
        return normalized_symbol

    def _to_asset_symbol(self, symbol: str) -> str:
        normalized_symbol = self._to_symbol(symbol)
        return normalized_symbol[:-4] if normalized_symbol.endswith("USDT") else normalized_symbol

    def _get_instrument_info(self, symbol: str) -> Dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol},
            timeout=10,
        )
        data = response.json()
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg", str(data)))
        items = data.get("result", {}).get("list", [])
        if not items:
            raise RuntimeError(f"Bybit symbol info not found for {symbol}")
        return items[0]

    def _get_reference_price(self, symbol: str, price: Optional[float]) -> float:
        if price is not None and price > 0:
            return float(price)

        response = requests.get(
            f"{self.base_url}/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
            timeout=10,
        )
        data = response.json()
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg", str(data)))
        items = data.get("result", {}).get("list", [])
        if not items:
            raise RuntimeError(f"Bybit ticker not found for {symbol}")

        reference_price = items[0].get("markPrice") or items[0].get("lastPrice")
        if not reference_price:
            raise RuntimeError(f"Bybit mark price not found for {symbol}")
        return float(reference_price)

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        normalized_symbol = self._to_symbol(symbol)
        try:
            params = {
                "category": "linear",
                "symbol": normalized_symbol,
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage),
            }
            response = requests.post(
                f"{self.base_url}/v5/position/set-leverage",
                headers=self._sign_request(params),
                json=params,
                timeout=10,
            )
            data = response.json()
            if data.get("retCode") == 0:
                return {"success": True, "message": f"Leverage set to {leverage}x"}
            return {"success": False, "message": data.get("retMsg", str(data))}
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
            return {"success": False, "message": "Missing Bybit API credentials"}

        normalized_symbol = self._to_symbol(symbol)
        order_side = "Buy" if side.lower() in ("buy", "long") else "Sell"
        normalized_order_type = "Limit" if order_type.lower() == "limit" else "Market"

        try:
            if leverage:
                self.set_leverage(normalized_symbol, leverage)

            params = {
                "category": "linear",
                "symbol": normalized_symbol,
                "side": order_side,
                "orderType": normalized_order_type,
                "qty": str(quantity),
                "orderLinkId": str(uuid.uuid4()),
            }
            if normalized_order_type == "Limit" and price is not None:
                params["price"] = str(price)
                params["timeInForce"] = "GTC"

            response = requests.post(
                f"{self.base_url}/v5/order/create",
                headers=self._sign_request(params),
                json=params,
                timeout=10,
            )
            data = response.json()

            if data.get("retCode") == 0:
                order_id = data.get("result", {}).get("orderId")
                return {
                    "success": True,
                    "order_id": str(order_id) if order_id else "",
                    "message": "Order placed successfully",
                }
            return {"success": False, "message": data.get("retMsg", str(data))}
        except Exception as exc:
            self.logger.error("Bybit contract order failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        normalized_symbol = self._to_symbol(symbol)
        instrument = self._get_instrument_info(normalized_symbol)
        lot_size = instrument.get("lotSizeFilter", {})
        min_qty = float(lot_size.get("minOrderQty", "0.001"))
        step = float(lot_size.get("qtyStep", min_qty))

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

    def list_contract_symbols(self) -> list[str]:
        symbols: list[str] = []
        cursor = ""

        try:
            while True:
                params = {"category": "linear", "limit": 1000}
                if cursor:
                    params["cursor"] = cursor

                response = requests.get(
                    f"{self.base_url}/v5/market/instruments-info",
                    params=params,
                    timeout=10,
                )
                response.raise_for_status()
                data = response.json()

                if data.get("retCode") != 0:
                    raise RuntimeError(data.get("retMsg", str(data)))

                result = data.get("result", {})
                for item in result.get("list", []):
                    if (
                        item.get("contractType") == "LinearPerpetual"
                        and item.get("settleCoin") == "USDT"
                        and item.get("status") == "Trading"
                    ):
                        symbol = item.get("symbol")
                        if symbol:
                            symbols.append(symbol)

                next_cursor = result.get("nextPageCursor") or ""
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor

            return sorted(set(symbols))
        except Exception as exc:
            self.logger.error("Bybit contract list load failed: %s", exc)
            raise

    def list_contract_market_snapshots(self) -> list[Dict[str, Any]]:
        try:
            instruments_response = requests.get(
                f"{self.base_url}/v5/market/instruments-info",
                params={"category": "linear", "limit": 1000},
                timeout=15,
            )
            instruments_response.raise_for_status()
            instruments_payload = instruments_response.json()
            if instruments_payload.get("retCode") != 0:
                raise RuntimeError(instruments_payload.get("retMsg", str(instruments_payload)))

            tickers_response = requests.get(
                f"{self.base_url}/v5/market/tickers",
                params={"category": "linear"},
                timeout=15,
            )
            tickers_response.raise_for_status()
            tickers_payload = tickers_response.json()
            if tickers_payload.get("retCode") != 0:
                raise RuntimeError(tickers_payload.get("retMsg", str(tickers_payload)))

            ticker_map = {
                item.get("symbol"): item
                for item in tickers_payload.get("result", {}).get("list", [])
                if item.get("symbol")
            }

            snapshots = []
            for instrument in instruments_payload.get("result", {}).get("list", []):
                contract_code = instrument.get("symbol")
                if (
                    not contract_code
                    or instrument.get("contractType") != "LinearPerpetual"
                    or instrument.get("settleCoin") != "USDT"
                    or instrument.get("status") != "Trading"
                ):
                    continue

                ticker = ticker_map.get(contract_code, {})
                snapshots.append(
                    self._build_market_snapshot(
                        symbol=self._to_asset_symbol(contract_code),
                        contract_code=contract_code,
                        price=ticker.get("markPrice") or ticker.get("lastPrice"),
                        funding_rate=ticker.get("fundingRate"),
                        funding_interval=instrument.get("fundingInterval") or ticker.get("fundingIntervalHour"),
                        funding_interval_unit="minutes" if instrument.get("fundingInterval") else "hours",
                    )
                )

            return sorted(snapshots, key=lambda row: row["contract_code"])
        except Exception as exc:
            self.logger.error("Bybit market snapshot load failed: %s", exc)
            raise
