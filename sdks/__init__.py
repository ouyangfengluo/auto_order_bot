"""
各交易所合约 SDK
"""
from .base_contract_sdk import BaseContractSDK
from .binance_contract_sdk import BinanceContractSDK
from .gateio_contract_sdk import GateioContractSDK
from .bybit_contract_sdk import BybitContractSDK
from .okx_contract_sdk import OkxContractSDK
from .lighter_contract_sdk import LighterContractSDK


class ContractSDKFactory:
    """合约 SDK 工厂"""
    
    _sdks = {}
    
    @classmethod
    def get_sdk(cls, exchange: str) -> BaseContractSDK:
        exchange_map = {
            "binance": BinanceContractSDK,
            "gateio": GateioContractSDK,
            "bybit": BybitContractSDK,
            "okx": OkxContractSDK,
            "lighter": LighterContractSDK,
        }
        if exchange.lower() not in exchange_map:
            raise ValueError(f"不支持的交易所: {exchange}")
        if exchange.lower() not in cls._sdks:
            cls._sdks[exchange.lower()] = exchange_map[exchange.lower()]()
        return cls._sdks[exchange.lower()]


__all__ = [
    "BaseContractSDK",
    "BinanceContractSDK",
    "GateioContractSDK",
    "BybitContractSDK",
    "OkxContractSDK",
    "LighterContractSDK",
    "ContractSDKFactory",
]
