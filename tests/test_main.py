import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import main


class MainResolveQuantityTests(unittest.TestCase):
    def test_load_config_returns_defaults_for_empty_file(self):
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("", encoding="utf-8")

            with patch.object(main, "CONFIG_PATH", config_path):
                payload = main.load_config()

        self.assertEqual({"tasks": [], "enabled": True}, payload)

    def test_load_config_returns_defaults_for_invalid_json(self):
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{invalid", encoding="utf-8")

            with patch.object(main, "CONFIG_PATH", config_path):
                payload = main.load_config()

        self.assertEqual({"tasks": [], "enabled": True}, payload)

    def test_resolve_order_quantity_uses_margin_mode_for_preview_requests(self):
        sdk = Mock()
        sdk.resolve_order_quantity.return_value = {
            "quantity": 0.012,
            "input_quantity": 10.0,
            "input_mode": "margin",
            "reference_price": 84500.5,
            "leverage_used": 10,
            "raw_quantity": 0.01243,
        }

        with patch("main.ContractSDKFactory.get_sdk", return_value=sdk):
            payload = main.resolve_order_quantity_for_task(
                {
                    "exchange": "binance",
                    "symbol": "BTCUSDT",
                    "quantity_mode": "margin",
                    "quantity": 10,
                    "leverage": 10,
                },
                default_quantity_mode="margin",
            )

        sdk.resolve_order_quantity.assert_called_once_with(
            symbol="BTCUSDT",
            quantity=10.0,
            quantity_mode="margin",
            leverage=10,
            price=None,
        )
        self.assertEqual("binance", payload["exchange"])
        self.assertEqual("BTCUSDT", payload["symbol"])
        self.assertEqual("margin", payload["quantity_mode"])
        self.assertEqual(0.012, payload["resolved_quantity"])
        self.assertEqual(0.012, payload["display_quantity"])
        self.assertEqual(84500.5, payload["reference_price"])

    def test_resolve_order_quantity_prefers_human_quantity_for_display(self):
        sdk = Mock()
        sdk.resolve_order_quantity.return_value = {
            "quantity": 123000.0,
            "human_quantity": 0.123,
            "input_quantity": 10.0,
            "input_mode": "margin",
            "reference_price": 81234.5,
            "leverage_used": 5,
            "market_id": 7,
        }

        with patch("main.ContractSDKFactory.get_sdk", return_value=sdk):
            payload = main.resolve_order_quantity_for_task(
                {
                    "exchange": "lighter",
                    "symbol": "BTC",
                    "quantity_mode": "margin",
                    "quantity": 10,
                    "leverage": 5,
                },
                default_quantity_mode="margin",
            )

        self.assertEqual(123000.0, payload["resolved_quantity"])
        self.assertEqual(0.123, payload["display_quantity"])
        self.assertEqual(0.123, payload["human_quantity"])
        self.assertEqual(7, payload["market_id"])


if __name__ == "__main__":
    unittest.main()
