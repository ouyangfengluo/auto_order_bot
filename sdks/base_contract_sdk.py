"""
Base contract SDK definitions.
"""
import logging
from abc import ABC, abstractmethod
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()


class BaseContractSDK(ABC):
    """Base class for USDT perpetual contract SDKs."""

    def __init__(self):
        self.api_key = None
        self.api_secret = None
        self.base_url = None
        self.logger = logging.getLogger(f"contract_sdk.{self.__class__.__name__}")
        self._load_credentials()

    @abstractmethod
    def _load_credentials(self):
        """Load exchange credentials from the environment."""
        raise NotImplementedError

    @abstractmethod
    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Set leverage for the target contract."""
        raise NotImplementedError

    @abstractmethod
    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: Optional[float] = None,
        order_type: str = "market",
        leverage: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Place an order on the exchange."""
        raise NotImplementedError

    @abstractmethod
    def list_contract_symbols(self) -> list[str]:
        """Return the available perpetual contract symbols."""
        raise NotImplementedError

    @abstractmethod
    def list_contract_market_snapshots(self) -> list[Dict[str, Any]]:
        """Return contract snapshots including price, funding rate, and funding interval."""
        raise NotImplementedError

    @abstractmethod
    def resolve_order_quantity(
        self,
        symbol: str,
        quantity: float,
        quantity_mode: str = "contract",
        leverage: Optional[int] = None,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Resolve user input quantity into exchange-native order quantity."""
        raise NotImplementedError

    def get_balance(self) -> Dict[str, Any]:
        """Return account balance when supported by the exchange SDK."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support balance queries.")

    def query_order_status(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Return order status when supported by the exchange SDK."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support order status queries.")

    def _validate_quantity(self, quantity: float, min_qty: float = 0.001) -> float:
        """Clamp quantity to the minimum supported size."""
        if quantity < min_qty:
            return min_qty
        return quantity

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _normalize_funding_rate_pct(self, value: Any) -> Optional[float]:
        rate = self._safe_float(value)
        if rate is None:
            return None
        return round(rate * 100, 8)

    def _normalize_interval_hours(self, value: Any, unit: str = "hours") -> Optional[float | int]:
        interval = self._safe_float(value)
        if interval is None:
            return None

        if unit == "seconds":
            interval /= 3600
        elif unit == "minutes":
            interval /= 60
        elif unit == "milliseconds":
            interval /= 3600000

        rounded_interval = round(interval, 6)
        if float(rounded_interval).is_integer():
            return int(rounded_interval)
        return rounded_interval

    def _build_market_snapshot(
        self,
        *,
        symbol: str,
        contract_code: str,
        price: Any,
        funding_rate: Any,
        funding_interval: Any,
        funding_interval_unit: str = "hours",
    ) -> Dict[str, Any]:
        snapshot = {
            "symbol": symbol,
            "contract_code": contract_code,
            "price": self._safe_float(price),
            "funding_rate": self._normalize_funding_rate_pct(funding_rate),
            "funding_interval": self._normalize_interval_hours(funding_interval, unit=funding_interval_unit),
        }
        return snapshot

    def _floor_to_step(self, value: float, step: float) -> float:
        step_decimal = Decimal(str(step))
        if step_decimal <= 0:
            return float(value)

        value_decimal = Decimal(str(value))
        floored = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN) * step_decimal
        return float(floored)

    def _normalize_contract_quantity(
        self,
        quantity: float,
        min_qty: float,
        step: float,
        quantity_label: str = "quantity",
    ) -> float:
        resolved_quantity = self._floor_to_step(quantity, step)
        if resolved_quantity < min_qty or resolved_quantity <= 0:
            raise ValueError(
                f"{quantity_label} is too small after rounding: {resolved_quantity}. Minimum is {min_qty}."
            )
        return resolved_quantity

    def _resolve_margin_quantity(
        self,
        margin: float,
        leverage: Optional[int],
        reference_price: float,
        min_qty: float,
        step: float,
        contract_multiplier: float = 1.0,
        quantity_label: str = "quantity",
    ) -> Dict[str, Any]:
        if margin <= 0:
            raise ValueError("Initial margin must be greater than 0.")
        if reference_price <= 0:
            raise ValueError("Reference price must be greater than 0.")
        if contract_multiplier <= 0:
            raise ValueError("Contract multiplier must be greater than 0.")

        leverage_used = max(int(leverage or 1), 1)
        notional = margin * leverage_used
        raw_quantity = notional / reference_price / contract_multiplier
        resolved_quantity = self._floor_to_step(raw_quantity, step)

        if resolved_quantity < min_qty or resolved_quantity <= 0:
            raise ValueError(
                f"Initial margin is too small. Computed {quantity_label} {resolved_quantity} is below minimum {min_qty}."
            )

        return {
            "quantity": resolved_quantity,
            "input_quantity": margin,
            "input_mode": "margin",
            "leverage_used": leverage_used,
            "reference_price": reference_price,
            "notional": notional,
            "raw_quantity": raw_quantity,
            "contract_multiplier": contract_multiplier,
        }
