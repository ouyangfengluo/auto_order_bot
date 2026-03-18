"""
Lighter perpetual contract SDK.
"""
import asyncio
import ctypes.util
import json
import os
import platform
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .base_contract_sdk import BaseContractSDK


def _patch_lighter_windows_runtime():
    """lighter-sdk needs a libc lookup patch on Windows before import."""
    if platform.system() != "Windows":
        return

    def fake_find_msvcrt():
        return "msvcrt"

    ctypes.util.find_msvcrt = fake_find_msvcrt


class LighterContractSDK(BaseContractSDK):
    """Lighter perpetual SDK wrapped into the sync project interface."""

    def __init__(self):
        self.market_meta_path = Path(__file__).resolve().parent.parent / "lighter_market_id.json"
        super().__init__()

    def _load_credentials(self):
        self.api_key = os.getenv("LIGHTER_API_KEY_INDEX")
        self.api_secret = os.getenv("LIGHTER_API_PRIVATE_KEY")
        self.base_url = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai").strip()
        self.account_index = int(os.getenv("LIGHTER_AMOUNT_INDEX", "0") or "0")
        self.api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "-1") or "-1")
        self.private_key = os.getenv("LIGHTER_API_PRIVATE_KEY", "").strip()
        self.fallback_market_index = int(os.getenv("LIGHTER_MARKET_INDEX", "0") or "0")
        self.max_slippage = float(os.getenv("LIGHTER_MAX_SLIPPAGE", "0.02") or "0.02")
        self.margin_mode = self._normalize_margin_mode(os.getenv("LIGHTER_MARGIN_MODE", "isolated"))
        self.nonce_retry_count = max(int(os.getenv("LIGHTER_NONCE_RETRIES", "2") or "2"), 0)
        self.nonce_retry_delay = max(float(os.getenv("LIGHTER_NONCE_RETRY_DELAY", "0.25") or "0.25"), 0.0)

    def _normalize_margin_mode(self, value: str | None) -> str:
        normalized_value = str(value or "isolated").strip().lower()
        if normalized_value not in {"isolated", "cross"}:
            return "isolated"
        return normalized_value

    def _load_lighter_modules(self):
        _patch_lighter_windows_runtime()
        import lighter
        from lighter.configuration import Configuration
        from lighter.signer_client import SignerClient

        return lighter, Configuration, SignerClient

    def _run_async(self, coro):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        result_box: dict[str, Any] = {}
        error_box: dict[str, BaseException] = {}

        def runner():
            try:
                result_box["value"] = asyncio.run(coro)
            except BaseException as exc:  # pragma: no cover - threading pass-through
                error_box["error"] = exc

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        thread.join()

        if "error" in error_box:
            raise error_box["error"]
        return result_box.get("value")

    def _normalize_symbol_key(self, symbol: str | None) -> str:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_symbol = normalized_symbol.replace("_", "").replace("-", "").replace("/", "")
        for quote_suffix in ("USDT", "USDC", "USD"):
            if normalized_symbol.endswith(quote_suffix) and len(normalized_symbol) > len(quote_suffix):
                normalized_symbol = normalized_symbol[: -len(quote_suffix)]
                break
        return normalized_symbol

    def _is_invalid_nonce_message(self, message: str | None) -> bool:
        return "invalid nonce" in str(message or "").strip().lower()

    def _load_market_meta_from_file(self) -> list[dict]:
        if not self.market_meta_path.exists():
            return []

        with open(self.market_meta_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        if not isinstance(data, list):
            raise ValueError("lighter_market_id.json format is invalid, expected a list.")
        return data

    async def _load_market_meta_from_api(self) -> list[dict]:
        lighter, Configuration, _ = self._load_lighter_modules()
        client = lighter.ApiClient(configuration=Configuration(host=self.base_url))
        order_api = lighter.OrderApi(client)

        try:
            payload = await order_api.order_book_details()
            order_book_details = getattr(payload, "order_book_details", []) or []
            return [
                {
                    "symbol": item.symbol,
                    "market_id": item.market_id,
                    "min_base_amount": item.min_base_amount,
                    "min_quote_amount": item.min_quote_amount,
                    "supported_size_decimals": item.supported_size_decimals,
                    "supported_price_decimals": item.supported_price_decimals,
                    "last_trade_price": item.last_trade_price,
                    "status": item.status,
                }
                for item in order_book_details
                if getattr(item, "market_type", "") == "perp"
            ]
        finally:
            await client.close()

    def _load_market_meta(self) -> list[dict]:
        meta = self._load_market_meta_from_file()
        if meta:
            return meta
        return self._run_async(self._load_market_meta_from_api())

    def _get_market_meta(self, symbol: str | None) -> dict:
        normalized_symbol = self._normalize_symbol_key(symbol)
        market_meta = self._load_market_meta()

        for item in market_meta:
            if self._normalize_symbol_key(item.get("symbol")) == normalized_symbol:
                return dict(item)

        if self.fallback_market_index > 0:
            return self._run_async(self._fetch_market_detail_by_market_id(self.fallback_market_index))

        raise ValueError(f"Lighter market metadata not found for symbol: {symbol}")

    async def _fetch_market_detail_by_market_id(self, market_id: int) -> dict:
        lighter, Configuration, _ = self._load_lighter_modules()
        client = lighter.ApiClient(configuration=Configuration(host=self.base_url))
        order_api = lighter.OrderApi(client)

        try:
            payload = await order_api.order_book_details(market_id=market_id)
            order_book_details = getattr(payload, "order_book_details", []) or []
            if not order_book_details:
                raise RuntimeError(f"Lighter market detail not found for market_id={market_id}")

            detail = order_book_details[0]
            return {
                "symbol": detail.symbol,
                "market_id": detail.market_id,
                "min_base_amount": detail.min_base_amount,
                "min_quote_amount": detail.min_quote_amount,
                "supported_size_decimals": detail.supported_size_decimals,
                "supported_price_decimals": detail.supported_price_decimals,
                "last_trade_price": detail.last_trade_price,
                "status": detail.status,
            }
        finally:
            await client.close()

    async def _fetch_market_detail_async(self, market_id: int) -> dict:
        return await self._fetch_market_detail_by_market_id(market_id)

    def _get_market_step(self, meta: dict) -> float:
        decimals = int(meta.get("supported_size_decimals", 0) or 0)
        return 1 / (10 ** decimals) if decimals >= 0 else 1.0

    def _to_base_amount(self, human_size: float, meta: dict) -> int:
        decimals = int(meta.get("supported_size_decimals", 0) or 0)
        scale = 10 ** decimals
        return int(round(human_size * scale))

    def _to_human_size(self, base_amount: int, meta: dict) -> float:
        decimals = int(meta.get("supported_size_decimals", 0) or 0)
        scale = 10 ** decimals
        return base_amount / scale

    def _order_matches(self, order: Any, order_id_raw: str, order_id_int: Optional[int]) -> bool:
        for attr_name in ("order_id", "client_order_id"):
            if str(getattr(order, attr_name, "")) == order_id_raw:
                return True

        if order_id_int is None:
            return False

        for attr_name in ("order_index", "client_order_index"):
            try:
                if int(getattr(order, attr_name, -1)) == order_id_int:
                    return True
            except Exception:
                continue
        return False

    async def _fetch_balance_async(self) -> Dict[str, Any]:
        if self.account_index <= 0:
            raise RuntimeError("LIGHTER_AMOUNT_INDEX is not configured.")

        lighter, Configuration, _ = self._load_lighter_modules()
        client = lighter.ApiClient(configuration=Configuration(host=self.base_url))
        account_api = lighter.AccountApi(client)

        try:
            payload = await account_api.account(by="index", value=str(self.account_index))
            accounts = getattr(payload, "accounts", []) or []
            if not accounts:
                raise RuntimeError(f"Lighter account not found for index={self.account_index}")

            account = accounts[0]
            available = float(getattr(account, "available_balance", 0) or 0)
            total = float(
                getattr(account, "total_asset_value", None)
                or getattr(account, "collateral", 0)
                or 0
            )
            return {
                "success": True,
                "exchange": "lighter",
                "account_index": self.account_index,
                "currency": "USDC",
                "available": available,
                "total": total,
            }
        finally:
            await client.close()

    async def _create_auth_token_async(self) -> str:
        if self.account_index <= 0:
            raise RuntimeError("LIGHTER_AMOUNT_INDEX is not configured.")
        if self.api_key_index < 0:
            raise RuntimeError("LIGHTER_API_KEY_INDEX is not configured.")
        if not self.private_key:
            raise RuntimeError("LIGHTER_API_PRIVATE_KEY is not configured.")

        _, _, SignerClient = self._load_lighter_modules()
        signer = SignerClient(
            url=self.base_url,
            api_private_keys={self.api_key_index: self.private_key},
            account_index=self.account_index,
        )
        try:
            auth_token, error = signer.create_auth_token_with_expiry(api_key_index=self.api_key_index)
            if error is not None or not auth_token:
                raise RuntimeError(f"Failed to create lighter auth token: {error or 'empty auth token'}")
            return auth_token
        finally:
            await signer.close()

    async def _query_order_status_async(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        order_id_raw = str(order_id).strip()
        if not order_id_raw:
            raise ValueError("order_id cannot be empty.")

        order_id_int = None
        try:
            if order_id_raw.isdigit():
                order_id_int = int(order_id_raw)
            else:
                float_value = float(order_id_raw)
                if float_value.is_integer():
                    order_id_int = int(float_value)
        except Exception:
            order_id_int = None

        lighter, Configuration, _ = self._load_lighter_modules()
        client = lighter.ApiClient(configuration=Configuration(host=self.base_url))
        order_api = lighter.OrderApi(client)

        try:
            auth_token = await self._create_auth_token_async()

            market_ids: list[int] = []
            if symbol:
                market_ids.append(int(self._get_market_meta(symbol)["market_id"]))
            else:
                market_ids = [
                    int(item.get("market_id"))
                    for item in self._load_market_meta()
                    if item.get("market_id") is not None
                ]
                market_ids = list(dict.fromkeys(market_ids))[:200]

            inactive_order = None
            cursor = None
            for _ in range(10):
                response = await order_api.account_inactive_orders(
                    account_index=self.account_index,
                    limit=100,
                    market_id=market_ids[0] if len(market_ids) == 1 else None,
                    cursor=cursor,
                    authorization=None,
                    auth=auth_token,
                )
                for order in getattr(response, "orders", []) or []:
                    if self._order_matches(order, order_id_raw, order_id_int):
                        inactive_order = order
                        break
                if inactive_order is not None:
                    break
                cursor = getattr(response, "next_cursor", None)
                if not cursor:
                    break

            if inactive_order is not None:
                order_dict = inactive_order.to_dict() if hasattr(inactive_order, "to_dict") else {}
                return {
                    "success": True,
                    "exchange": "lighter",
                    "source": "inactive_orders",
                    "status": order_dict.get("status"),
                    "filled_qty": float(order_dict.get("filled_base_amount", 0) or 0),
                    "remaining_qty": float(order_dict.get("remaining_base_amount", 0) or 0),
                    "order": order_dict,
                }

            for market_id in market_ids:
                response = await order_api.account_active_orders(
                    account_index=self.account_index,
                    market_id=market_id,
                    authorization=None,
                    auth=auth_token,
                )
                for order in getattr(response, "orders", []) or []:
                    if self._order_matches(order, order_id_raw, order_id_int):
                        order_dict = order.to_dict() if hasattr(order, "to_dict") else {}
                        return {
                            "success": True,
                            "exchange": "lighter",
                            "source": "active_orders",
                            "status": order_dict.get("status"),
                            "filled_qty": float(order_dict.get("filled_base_amount", 0) or 0),
                            "remaining_qty": float(order_dict.get("remaining_base_amount", 0) or 0),
                            "order": order_dict,
                        }

            return {
                "success": False,
                "exchange": "lighter",
                "message": "Order not found in inactive or active orders.",
            }
        finally:
            await client.close()

    async def _update_leverage_async(
        self,
        symbol: str,
        leverage: int,
        *,
        signer: Any = None,
        market_meta: Optional[dict] = None,
    ) -> Dict[str, Any]:
        _, _, SignerClient = self._load_lighter_modules()
        meta = dict(market_meta or self._get_market_meta(symbol))
        owns_signer = signer is None
        if owns_signer:
            signer = SignerClient(
                url=self.base_url,
                api_private_keys={self.api_key_index: self.private_key},
                account_index=self.account_index,
            )
        try:
            margin_mode = (
                SignerClient.ISOLATED_MARGIN_MODE
                if self.margin_mode == "isolated"
                else SignerClient.CROSS_MARGIN_MODE
            )
            last_error = ""
            for attempt in range(self.nonce_retry_count + 1):
                _, _, error = await signer.update_leverage(
                    market_index=int(meta["market_id"]),
                    margin_mode=margin_mode,
                    leverage=int(leverage),
                )
                if error is None:
                    return {"success": True, "message": f"Leverage set to {leverage}x"}

                last_error = str(error)
                if not self._is_invalid_nonce_message(last_error) or attempt >= self.nonce_retry_count:
                    raise RuntimeError(last_error)

                self.logger.warning(
                    "Lighter leverage update hit invalid nonce, retrying %s/%s for %s",
                    attempt + 1,
                    self.nonce_retry_count,
                    meta.get("symbol") or symbol,
                )
                await asyncio.sleep(self.nonce_retry_delay)

            raise RuntimeError(last_error or "Lighter leverage update failed")
        finally:
            if owns_signer:
                await signer.close()

    async def _submit_order_with_retry(
        self,
        *,
        signer: Any,
        meta: dict,
        market_index: int,
        client_order_index: int,
        base_amount: int,
        is_ask: bool,
        normalized_order_type: str,
        price: Optional[float],
    ) -> tuple[Any, Any]:
        _, _, SignerClient = self._load_lighter_modules()
        last_error = ""

        for attempt in range(self.nonce_retry_count + 1):
            if normalized_order_type == "limit":
                if price is None or price <= 0:
                    raise ValueError("Limit order requires a valid price.")
                if meta.get("supported_price_decimals") is None:
                    meta.update(await self._fetch_market_detail_async(market_index))
                price_decimals = int(meta.get("supported_price_decimals", 0) or 0)
                price_int = int(round(float(price) * (10 ** price_decimals)))
                order, response, error = await signer.create_order(
                    market_index=market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    price=price_int,
                    is_ask=is_ask,
                    order_type=SignerClient.ORDER_TYPE_LIMIT,
                    time_in_force=SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    reduce_only=False,
                )
            else:
                order, response, error = await signer.create_market_order_limited_slippage(
                    market_index=market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    max_slippage=self.max_slippage,
                    is_ask=is_ask,
                    reduce_only=False,
                )

            if error is None:
                return order, response

            last_error = str(error)
            if not self._is_invalid_nonce_message(last_error) or attempt >= self.nonce_retry_count:
                raise RuntimeError(last_error)

            self.logger.warning(
                "Lighter order hit invalid nonce, retrying %s/%s for %s",
                attempt + 1,
                self.nonce_retry_count,
                meta.get("symbol") or market_index,
            )
            await asyncio.sleep(self.nonce_retry_delay)

        raise RuntimeError(last_error or "Lighter order placement failed")

    def get_balance(self) -> Dict[str, Any]:
        try:
            return self._run_async(self._fetch_balance_async())
        except Exception as exc:
            self.logger.error("Lighter balance query failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def query_order_status(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        try:
            return self._run_async(self._query_order_status_async(order_id, symbol=symbol))
        except Exception as exc:
            self.logger.error("Lighter order status query failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        if self.account_index <= 0 or self.api_key_index < 0 or not self.private_key:
            return {"success": False, "message": "Missing Lighter account or API credentials"}

        try:
            return self._run_async(self._update_leverage_async(symbol, leverage))
        except Exception as exc:
            self.logger.error("Lighter leverage update failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        meta = self._get_market_meta(symbol)
        min_qty = float(meta.get("min_base_amount", 0) or 0)
        step = self._get_market_step(meta)
        reference_price = None

        if quantity_mode == "margin":
            if price is not None and price > 0:
                reference_price = float(price)
            else:
                market_detail = self._run_async(self._fetch_market_detail_async(int(meta["market_id"])))
                reference_price = float(market_detail.get("last_trade_price") or 0)

            resolved_payload = self._resolve_margin_quantity(
                margin=quantity,
                leverage=leverage,
                reference_price=reference_price,
                min_qty=min_qty,
                step=step,
                quantity_label="base asset amount",
            )
            human_quantity = float(resolved_payload["quantity"])
            native_quantity = self._to_base_amount(human_quantity, meta)
            resolved_payload["quantity"] = native_quantity
            resolved_payload["human_quantity"] = human_quantity
            resolved_payload["market_id"] = int(meta["market_id"])
            return resolved_payload

        human_quantity = self._normalize_contract_quantity(
            quantity=quantity,
            min_qty=min_qty,
            step=step,
            quantity_label="base asset amount",
        )
        native_quantity = self._to_base_amount(human_quantity, meta)
        return {
            "quantity": native_quantity,
            "human_quantity": human_quantity,
            "input_quantity": quantity,
            "input_mode": "contract",
            "reference_price": float(price) if price else None,
            "market_id": int(meta["market_id"]),
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
        if self.account_index <= 0 or self.api_key_index < 0 or not self.private_key:
            return {"success": False, "message": "Missing Lighter account or API credentials"}

        async def _place_order_async():
            _, _, SignerClient = self._load_lighter_modules()
            meta = dict(self._get_market_meta(symbol))
            base_amount = int(round(quantity))
            min_base_amount_native = self._to_base_amount(float(meta.get("min_base_amount", 0) or 0), meta)
            if base_amount < min_base_amount_native:
                raise ValueError(
                    f"Lighter order quantity is too small: {base_amount}. Minimum native size is {min_base_amount_native}."
                )

            signer = SignerClient(
                url=self.base_url,
                api_private_keys={self.api_key_index: self.private_key},
                account_index=self.account_index,
            )
            try:
                check_error = signer.check_client()
                if check_error is not None:
                    raise RuntimeError(f"lighter check_client failed: {check_error}")

                if leverage:
                    leverage_result = await self._update_leverage_async(
                        symbol,
                        leverage,
                        signer=signer,
                        market_meta=meta,
                    )
                    if not leverage_result.get("success"):
                        raise RuntimeError(leverage_result.get("message") or "Failed to set lighter leverage")

                market_index = int(meta["market_id"])
                client_order_index = int(time.time() * 1000)
                is_ask = side.lower() in {"sell", "short"}
                normalized_order_type = order_type.lower()
                order, response = await self._submit_order_with_retry(
                    signer=signer,
                    meta=meta,
                    market_index=market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    is_ask=is_ask,
                    normalized_order_type=normalized_order_type,
                    price=price,
                )

                response_dict = response.to_dict() if hasattr(response, "to_dict") else {}
                order_dict = order.to_dict() if hasattr(order, "to_dict") else {}
                return {
                    "success": True,
                    "order_id": str(client_order_index),
                    "message": "Order placed successfully",
                    "market_id": market_index,
                    "native_quantity": base_amount,
                    "human_quantity": self._to_human_size(base_amount, meta),
                    "tx_hash": response_dict.get("tx_hash") or response_dict.get("txHash"),
                    "order": order_dict,
                    "response": response_dict,
                }
            finally:
                await signer.close()

        try:
            return self._run_async(_place_order_async())
        except Exception as exc:
            self.logger.error("Lighter order placement failed: %s", exc)
            return {"success": False, "message": str(exc)}

    def list_contract_symbols(self) -> list[str]:
        meta = self._load_market_meta()
        symbols = [
            str(item.get("symbol"))
            for item in meta
            if item.get("symbol")
        ]
        return sorted(set(symbols))
