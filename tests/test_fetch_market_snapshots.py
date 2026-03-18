import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import fetch_market_snapshots


class FakeSDK:
    def __init__(self, exchange: str):
        self.exchange = exchange

    def list_contract_market_snapshots(self):
        return [
            {
                "symbol": "DOGE",
                "contract_code": f"{self.exchange}-DOGE",
                "price": 0.1725,
                "funding_rate": 0.093,
                "funding_interval": 2,
            }
        ]


class FetchMarketSnapshotsTests(unittest.TestCase):
    def test_collect_market_snapshots_groups_by_exchange(self):
        with patch(
            "fetch_market_snapshots.ContractSDKFactory.get_sdk",
            side_effect=lambda exchange: FakeSDK(exchange),
        ):
            payload = fetch_market_snapshots.collect_market_snapshots(["okx", "gateio"])

        self.assertEqual(["okx", "gateio"], list(payload.keys()))
        self.assertEqual("okx-DOGE", payload["okx"][0]["contract_code"])
        self.assertEqual("gateio-DOGE", payload["gateio"][0]["contract_code"])

    def test_write_market_snapshots_creates_json_file(self):
        payload = {"okx": [{"symbol": "DOGE", "contract_code": "DOGE-USDT-SWAP"}]}

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "snapshots.json"
            fetch_market_snapshots.write_market_snapshots(payload, output_path)

            self.assertTrue(output_path.exists())
            self.assertIn("DOGE-USDT-SWAP", output_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
