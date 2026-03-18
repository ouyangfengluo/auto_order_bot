"""
Binance USDT perpetual contract SDK.
"""
import hashlib
import hmac
import os
import time
from decimal import Decimal, ROUND_DOWN
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
        matched_symbol = next(
            (item for item in symbols if str(item.get("symbol", "")).upper() == str(symbol).upper()),
            None,
        )
        if not matched_symbol:
            raise RuntimeError(f"Binance symbol info not found for {symbol}")
        return matched_symbol

    def _to_asset_symbol(self, symbol: str) -> str:
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol.endswith("USDT") and len(normalized_symbol) > 4:
            return normalized_symbol[:-4]
        return normalized_symbol

    def _to_contract_symbol(self, symbol: str) -> str:
        normalized_symbol = str(symbol or "").replace("_", "").replace("-", "").upper()
        if not normalized_symbol.endswith("USDT"):
            normalized_symbol = f"{normalized_symbol}USDT"
        return normalized_symbol

    def _get_filter(self, symbol_info: Dict[str, Any], filter_type: str) -> Optional[Dict[str, Any]]:
        return next(
            (item for item in symbol_info.get("filters", []) if item.get("filterType") == filter_type),
            None,
        )

    def _format_decimal(self, value: Decimal) -> str:
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _count_decimal_places(self, value: Any) -> int:
        text = self._format_decimal(Decimal(str(value)))
        if "." not in text:
            return 0
        return len(text.split(".", 1)[1])

    def _format_value_to_precision(self, value: Any, precision: Any) -> str:
        precision_int = int(precision)
        value_decimal = Decimal(str(value))
        quantizer = Decimal("1").scaleb(-precision_int)
        normalized_value = value_decimal.quantize(quantizer, rounding=ROUND_DOWN)
        return self._format_decimal(normalized_value)

    def _format_value_to_step(
        self,
        value: float,
        step: Any,
        *,
        minimum: Any = None,
        value_label: str,
    ) -> str:
        step_decimal = Decimal(str(step))
        if step_decimal <= 0:
            raise ValueError(f"{value_label} step must be greater than 0.")

        value_decimal = Decimal(str(value))
        normalized_value = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN) * step_decimal

        if minimum not in (None, ""):
            minimum_decimal = Decimal(str(minimum))
            if normalized_value < minimum_decimal:
                raise ValueError(
                    f"{value_label} is too small after Binance precision normalization: {self._format_decimal(normalized_value)}. "
                    f"Minimum is {self._format_decimal(minimum_decimal)}."
                )

        if normalized_value <= 0:
            raise ValueError(f"{value_label} must be greater than 0 after Binance precision normalization.")

        return self._format_decimal(normalized_value)

    def _format_order_quantity(self, symbol_info: Dict[str, Any], quantity: float, order_type: str) -> str:
        quantity_filter_type = "MARKET_LOT_SIZE" if order_type == "MARKET" else "LOT_SIZE"
        quantity_filter = self._get_filter(symbol_info, quantity_filter_type) or self._get_filter(symbol_info, "LOT_SIZE")
        if not quantity_filter:
            raise RuntimeError("Binance lot size filter not found.")

        return self._format_value_to_step(
            quantity,
            quantity_filter.get("stepSize", "0.001"),
            minimum=quantity_filter.get("minQty", "0.001"),
            value_label="quantity",
        )

    def _format_order_price(self, symbol_info: Dict[str, Any], price: Optional[float]) -> Optional[str]:
        if price is None:
            return None

        price_filter = self._get_filter(symbol_info, "PRICE_FILTER")
        if not price_filter:
            return self._format_decimal(Decimal(str(price)))

        return self._format_value_to_step(
            price,
            price_filter.get("tickSize", "0.01"),
            minimum=price_filter.get("minPrice", "0"),
            value_label="price",
        )

    def _apply_precision_caps(
        self,
        symbol_info: Dict[str, Any],
        *,
        quantity: str,
        price: Optional[str],
    ) -> tuple[str, Optional[str]]:
        capped_quantity = quantity
        quantity_precision = symbol_info.get("quantityPrecision")
        if quantity_precision is not None and self._count_decimal_places(capped_quantity) > int(quantity_precision):
            capped_quantity = self._format_value_to_precision(capped_quantity, quantity_precision)

        capped_price = price
        price_precision = symbol_info.get("pricePrecision")
        if capped_price is not None and price_precision is not None and self._count_decimal_places(capped_price) > int(price_precision):
            capped_price = self._format_value_to_precision(capped_price, price_precision)

        return capped_quantity, capped_price

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
        quantity: str,
        price: Optional[str],
        order_type: str,
        position_mode: str,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT" if order_type == "LIMIT" else "MARKET",
            "quantity": quantity,
            "timestamp": int(time.time() * 1000),
        }

        if position_mode == "hedge":
            params["positionSide"] = "LONG" if side == "BUY" else "SHORT"

        if params["type"] == "LIMIT":
            params["timeInForce"] = "GTC"
            params["price"] = price

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

    def _is_precision_error(self, data: Dict[str, Any]) -> bool:
        message = str(data.get("msg", data.get("message", data))).lower()
        return "precision is over the maximum defined for this asset" in message

    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        normalized_symbol = self._to_contract_symbol(symbol)

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

        normalized_symbol = self._to_contract_symbol(symbol)

        order_side = "BUY" if side.lower() in ("buy", "long") else "SELL"
        normalized_order_type = order_type.upper()

        try:
            if leverage:
                self.set_leverage(normalized_symbol, leverage)

            symbol_info = self._get_symbol_exchange_info(normalized_symbol)
            formatted_quantity = self._format_order_quantity(symbol_info, quantity, normalized_order_type)
            formatted_price = self._format_order_price(symbol_info, price) if normalized_order_type == "LIMIT" else None

            position_mode = self.get_position_mode()
            params = self._build_order_params(
                symbol=normalized_symbol,
                side=order_side,
                quantity=formatted_quantity,
                price=formatted_price,
                order_type=normalized_order_type,
                position_mode=position_mode,
            )
            data = self._submit_order(params)

            if self._is_precision_error(data):
                refreshed_symbol_info = self._get_symbol_exchange_info(normalized_symbol)
                capped_quantity, capped_price = self._apply_precision_caps(
                    refreshed_symbol_info,
                    quantity=formatted_quantity,
                    price=formatted_price,
                )
                if capped_quantity != formatted_quantity or capped_price != formatted_price:
                    self.logger.warning(
                        "Retrying Binance order with refreshed precision caps: symbol=%s quantity=%s->%s price=%s->%s",
                        normalized_symbol,
                        formatted_quantity,
                        capped_quantity,
                        formatted_price,
                        capped_price,
                    )
                    params = self._build_order_params(
                        symbol=normalized_symbol,
                        side=order_side,
                        quantity=capped_quantity,
                        price=capped_price,
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
                        quantity=formatted_quantity,
                        price=formatted_price,
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

    def list_contract_market_snapshots(self) -> list[Dict[str, Any]]:
        try:
            exchange_info_response = requests.get(f"{self.base_url}/fapi/v1/exchangeInfo", timeout=15)
            exchange_info_response.raise_for_status()
            exchange_info = exchange_info_response.json()

            premium_response = requests.get(f"{self.base_url}/fapi/v1/premiumIndex", timeout=15)
            premium_response.raise_for_status()
            premium_items = premium_response.json()

            funding_info_response = requests.get(f"{self.base_url}/fapi/v1/fundingInfo", timeout=15)
            funding_info_response.raise_for_status()
            funding_info_items = funding_info_response.json()

            active_symbols = {
                item["symbol"]
                for item in exchange_info.get("symbols", [])
                if item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == "USDT"
                and item.get("status") == "TRADING"
            }
            funding_interval_map = {
                item.get("symbol"): item.get("fundingIntervalHours")
                for item in funding_info_items
                if item.get("symbol")
            }

            snapshots = []
            for item in premium_items:
                contract_code = item.get("symbol")
                if not contract_code or contract_code not in active_symbols:
                    continue

                snapshots.append(
                    self._build_market_snapshot(
                        symbol=self._to_asset_symbol(contract_code),
                        contract_code=contract_code,
                        price=item.get("markPrice") or item.get("lastPrice"),
                        funding_rate=item.get("lastFundingRate"),
                        funding_interval=funding_interval_map.get(contract_code, 8),
                    )
                )

            return sorted(snapshots, key=lambda row: row["contract_code"])
        except Exception as exc:
            self.logger.error("Binance market snapshot load failed: %s", exc)
            raise
