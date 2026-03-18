import unittest
from unittest.mock import Mock, patch

from sdks.binance_contract_sdk import BinanceContractSDK


class BinanceContractSDKTests(unittest.TestCase):
    def setUp(self):
        self.sdk = BinanceContractSDK()
        self.sdk.api_key = "test-key"
        self.sdk.api_secret = "test-secret"

    def test_place_market_order_formats_quantity_to_valid_step(self):
        symbol_info = {
            "filters": [
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "PRICE_FILTER", "minPrice": "0.1", "tickSize": "0.1"},
            ]
        }

        with (
            patch.object(self.sdk, "set_leverage", return_value={"success": True}),
            patch.object(self.sdk, "_get_symbol_exchange_info", return_value=symbol_info),
            patch.object(self.sdk, "get_position_mode", return_value="one_way"),
            patch.object(self.sdk, "_submit_order", return_value={"orderId": 1}) as mock_submit_order,
        ):
            result = self.sdk.place_order(
                symbol="BTCUSDT",
                side="long",
                quantity=0.30000000000000004,
                order_type="market",
                leverage=10,
            )

        self.assertTrue(result["success"])
        submitted_params = mock_submit_order.call_args.args[0]
        self.assertEqual("0.3", submitted_params["quantity"])
        self.assertEqual("MARKET", submitted_params["type"])

    def test_place_limit_order_formats_price_to_valid_tick(self):
        symbol_info = {
            "filters": [
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "PRICE_FILTER", "minPrice": "0.1", "tickSize": "0.01"},
            ]
        }

        with (
            patch.object(self.sdk, "_get_symbol_exchange_info", return_value=symbol_info),
            patch.object(self.sdk, "get_position_mode", return_value="one_way"),
            patch.object(self.sdk, "_submit_order", return_value={"orderId": 2}) as mock_submit_order,
        ):
            result = self.sdk.place_order(
                symbol="BTCUSDT",
                side="long",
                quantity=1.2300000000000002,
                price=123.45000000000002,
                order_type="limit",
            )

        self.assertTrue(result["success"])
        submitted_params = mock_submit_order.call_args.args[0]
        self.assertEqual("1.23", submitted_params["quantity"])
        self.assertEqual("123.45", submitted_params["price"])
        self.assertEqual("LIMIT", submitted_params["type"])

    def test_get_symbol_exchange_info_filters_exact_symbol(self):
        response = Mock()
        response.json.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT", "filters": []},
                {"symbol": "ROBOUSDT", "filters": [{"filterType": "LOT_SIZE", "minQty": "1", "stepSize": "1"}]},
            ]
        }

        with patch("sdks.binance_contract_sdk.requests.get", return_value=response):
            symbol_info = self.sdk._get_symbol_exchange_info("ROBOUSDT")

        self.assertEqual("ROBOUSDT", symbol_info["symbol"])

    def test_place_order_retries_with_precision_caps_after_precision_error(self):
        symbol_info = {
            "quantityPrecision": 0,
            "pricePrecision": 7,
            "filters": [
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "PRICE_FILTER", "minPrice": "0.00001", "tickSize": "0.00001"},
            ],
        }

        with (
            patch.object(self.sdk, "_get_symbol_exchange_info", return_value=symbol_info),
            patch.object(self.sdk, "get_position_mode", return_value="one_way"),
            patch.object(
                self.sdk,
                "_submit_order",
                side_effect=[
                    {"success": False, "msg": "Precision is over the maximum defined for this asset."},
                    {"orderId": 3},
                ],
            ) as mock_submit_order,
        ):
            result = self.sdk.place_order(
                symbol="ROBOUSDT",
                side="short",
                quantity=12698.114,
                order_type="market",
            )

        self.assertTrue(result["success"])
        self.assertEqual(2, mock_submit_order.call_count)
        first_params = mock_submit_order.call_args_list[0].args[0]
        second_params = mock_submit_order.call_args_list[1].args[0]
        self.assertEqual("12698.114", first_params["quantity"])
        self.assertEqual("12698", second_params["quantity"])


if __name__ == "__main__":
    unittest.main()
