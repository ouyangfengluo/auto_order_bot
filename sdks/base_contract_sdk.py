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
