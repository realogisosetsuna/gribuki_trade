from __future__ import annotations

import unittest
from urllib.parse import parse_qs, urlsplit

from gribuki_trade.adapters.binance.models import BinanceCredentials
from gribuki_trade.adapters.binance.request_builder import encode_request


class BinanceRequestBuilderTests(unittest.TestCase):
    def test_public_get_places_parameters_in_query_and_keeps_json_header(self) -> None:
        encoded = encode_request(
            method="GET",
            base_url="https://api.binance.com",
            path="/api/v3/ticker/price",
            params=(("symbol", "BTCUSDT"), ("limit", 5)),
        )

        self.assertEqual(
            encoded.url,
            "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT&limit=5",
        )
        self.assertEqual(encoded.headers, {"Accept": "application/json"})
        self.assertIsNone(encoded.body)
        self.assertEqual(encoded.signature, "")

    def test_signed_post_appends_timestamp_before_hmac_and_uses_form_body(self) -> None:
        credentials = BinanceCredentials("public-key", "private-key")
        observed: list[str] = []

        def signer(secret: str, payload: str) -> str:
            observed.append(f"{secret}:{payload}")
            return "signed-value"

        encoded = encode_request(
            method="POST",
            base_url="https://api.binance.com",
            path="/api/v3/order",
            params=(("symbol", "BTCUSDT"), ("quantity", "0.01000000")),
            credentials=credentials,
            recv_window_ms=5_000,
            timestamp_ms=123_456,
            signer=signer,
        )

        self.assertEqual(
            observed,
            ["private-key:symbol=BTCUSDT&quantity=0.01000000&recvWindow=5000&timestamp=123456"],
        )
        self.assertEqual(encoded.signature, "signed-value")
        self.assertEqual(encoded.headers["X-MBX-APIKEY"], "public-key")
        self.assertEqual(encoded.headers["Content-Type"], "application/x-www-form-urlencoded")
        self.assertIsNotNone(encoded.body)
        assert encoded.body is not None
        body = parse_qs(encoded.body.decode("utf-8"))
        self.assertEqual(body["signature"], ["signed-value"])
        self.assertEqual(body["timestamp"], ["123456"])
        self.assertEqual(urlsplit(encoded.url).query, "")

    def test_signed_arguments_must_be_complete(self) -> None:
        credentials = BinanceCredentials("public-key", "private-key")
        with self.assertRaisesRegex(ValueError, "recv_window_ms and timestamp_ms"):
            encode_request(
                method="GET",
                base_url="https://api.binance.com",
                path="/api/v3/account",
                credentials=credentials,
                recv_window_ms=5_000,
            )
        with self.assertRaisesRegex(ValueError, "require credentials"):
            encode_request(
                method="GET",
                base_url="https://api.binance.com",
                path="/api/v3/time",
                timestamp_ms=1,
            )

    def test_unsupported_method_is_rejected_before_encoding(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported Binance HTTP method"):
            encode_request(
                method="PATCH",
                base_url="https://api.binance.com",
                path="/api/v3/order",
            )
