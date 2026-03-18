import json
import unittest
from unittest.mock import Mock, patch

from sdks.gateio_contract_sdk import GateioContractSDK


class GateioContractSDKTests(unittest.TestCase):
    def setUp(self):
        self.sdk = GateioContractSDK()
        self.sdk.api_key = "test-key"
        self.sdk.api_secret = "test-secret"

    def test_set_leverage_uses_leverage_subresource(self):
        response = Mock()
        response.status_code = 200
        response.text = "{}"
        response.json.return_value = {}

        with patch("sdks.gateio_contract_sdk.requests.post", return_value=response) as mock_post:
            result = self.sdk.set_leverage("BTC_USDT", 10)

        self.assertEqual({"success": True, "message": "Leverage set to 10x"}, result)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(
            "https://api.gateio.ws/api/v4/futures/usdt/positions/BTC_USDT/leverage?leverage=10",
            args[0],
        )
        self.assertNotIn("data", kwargs)

    def test_place_market_order_uses_integer_size_and_market_fields(self):
        response = Mock()
        response.status_code = 200
        response.text = '{"id":"123"}'
        response.json.return_value = {"id": "123"}

        with (
            patch.object(self.sdk, "set_leverage", return_value={"success": True}) as mock_set_leverage,
            patch("sdks.gateio_contract_sdk.requests.post", return_value=response) as mock_post,
        ):
            result = self.sdk.place_order(
                symbol="BTC_USDT",
                side="long",
                quantity=13.0,
                order_type="market",
                leverage=10,
            )

        self.assertTrue(result["success"])
        mock_set_leverage.assert_called_once_with("BTC_USDT", 10)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual("https://api.gateio.ws/api/v4/futures/usdt/orders", args[0])

        payload = json.loads(kwargs["data"])
        self.assertEqual("BTC_USDT", payload["contract"])
        self.assertEqual(13, payload["size"])
        self.assertEqual("0", payload["price"])
        self.assertEqual("ioc", payload["tif"])
        self.assertFalse(payload["reduce_only"])

    def test_place_order_returns_leverage_error_without_submitting_order(self):
        with (
            patch.object(self.sdk, "set_leverage", return_value={"success": False, "message": "boom"}),
            patch("sdks.gateio_contract_sdk.requests.post") as mock_post,
        ):
            result = self.sdk.place_order(
                symbol="BTC_USDT",
                side="long",
                quantity=13.0,
                order_type="market",
                leverage=10,
            )

        self.assertEqual({"success": False, "message": "boom"}, result)
        mock_post.assert_not_called()

    def test_list_contract_market_snapshots_normalizes_price_rate_and_interval(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = [
            {
                "name": "DOGE_USDT",
                "status": "trading",
                "in_delisting": False,
                "mark_price": "0.1725",
                "last_price": "0.1724",
                "funding_rate": "0.00093",
                "funding_interval": 7200,
            },
            {
                "name": "OLD_USDT",
                "status": "settling",
                "in_delisting": False,
                "mark_price": "1",
                "funding_rate": "0.001",
                "funding_interval": 28800,
            },
        ]

        with patch("sdks.gateio_contract_sdk.requests.get", return_value=response) as mock_get:
            snapshots = self.sdk.list_contract_market_snapshots()

        self.assertEqual(
            [
                {
                    "symbol": "DOGE",
                    "contract_code": "DOGE_USDT",
                    "price": 0.1725,
                    "funding_rate": 0.093,
                    "funding_interval": 2,
                }
            ],
            snapshots,
        )
        mock_get.assert_called_once_with("https://api.gateio.ws/api/v4/futures/usdt/contracts", timeout=15)


if __name__ == "__main__":
    unittest.main()
