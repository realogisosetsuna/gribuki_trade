import io
import json
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from unittest import TestCase
from unittest.mock import Mock, patch

from gribuki_trade.security import (
    CredentialConfig,
    InteractiveSecretManager,
    KeyringSecretProvider,
    MemorySecretProvider,
    SecretProvider,
    SecretProviderError,
    SecretProviderUnavailable,
    SecretValue,
)

SENSITIVE_VALUE = "never-print-this-token"


class MemorySecretProviderTests(TestCase):
    def test_satisfies_protocol_and_crud_is_idempotent(self) -> None:
        provider = MemorySecretProvider()

        self.assertIsInstance(provider, SecretProvider)
        self.assertIsNone(provider.get_secret("api-token"))
        provider.set_secret("api-token", SENSITIVE_VALUE)
        self.assertEqual(provider.get_secret("api-token"), SENSITIVE_VALUE)
        self.assertTrue(provider.delete_secret("api-token"))
        self.assertFalse(provider.delete_secret("api-token"))

    def test_repr_contains_neither_secret_names_nor_values(self) -> None:
        provider = MemorySecretProvider({"sensitive-name": SENSITIVE_VALUE})

        rendered = repr(provider)

        self.assertNotIn("sensitive-name", rendered)
        self.assertNotIn(SENSITIVE_VALUE, rendered)

    def test_rejects_empty_names_and_values(self) -> None:
        provider = MemorySecretProvider()

        with self.assertRaisesRegex(ValueError, "name"):
            provider.get_secret("  ")
        with self.assertRaisesRegex(ValueError, "value"):
            provider.set_secret("token", "")


class KeyringSecretProviderTests(TestCase):
    def test_keyring_import_is_delayed_until_first_operation(self) -> None:
        with patch(
            "gribuki_trade.security.secrets.import_module",
            side_effect=ImportError("not installed"),
        ) as import_keyring:
            provider = KeyringSecretProvider("test-service")
            import_keyring.assert_not_called()
            with self.assertRaises(SecretProviderUnavailable):
                provider.get_secret("api-token")

        import_keyring.assert_called_once_with("keyring")

    def test_backend_round_trip_uses_service_and_name(self) -> None:
        backend = Mock()
        backend.get_password.return_value = SENSITIVE_VALUE
        provider = KeyringSecretProvider("test-service")

        with (
            tempfile.TemporaryDirectory() as directory, patch.dict(
                "os.environ", {"GRIBUKI_TRADE_SECRET_FILE": f"{directory}/secrets.json"}
            ),
            patch("gribuki_trade.security.secrets.import_module", return_value=backend),
            patch("gribuki_trade.security.secrets._dpapi_protect", return_value=b"cipher"),
        ):
            provider.set_secret("api-token", SENSITIVE_VALUE)
            result = provider.get_secret("api-token")
            deleted = provider.delete_secret("api-token")

        backend.set_password.assert_called_once_with(
            "test-service", "api-token", SENSITIVE_VALUE
        )
        backend.get_password.assert_called_once_with("test-service", "api-token")
        backend.delete_password.assert_called_once_with("test-service", "api-token")
        self.assertEqual(result, SENSITIVE_VALUE)
        self.assertTrue(deleted)

    def test_backend_error_does_not_leak_secret_in_exception_or_context(self) -> None:
        class UnsafeBackend:
            def set_password(self, service: str, name: str, value: str) -> None:
                raise RuntimeError(f"backend rejected {value}")

        provider = KeyringSecretProvider()
        with patch(
            "gribuki_trade.security.secrets.import_module",
            return_value=UnsafeBackend(),
        ):
            try:
                provider.set_secret("api-token", SENSITIVE_VALUE)
            except SecretProviderError as error:
                rendered = repr(error) + str(error) + repr(error.__context__)
            else:
                self.fail("expected a sanitized provider error")

        self.assertNotIn(SENSITIVE_VALUE, rendered)

    def test_successful_write_keeps_encrypted_user_bound_fallback(self) -> None:
        backend = Mock()
        provider = KeyringSecretProvider("test-service")
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/secrets.json"
            with (
                patch.dict("os.environ", {"GRIBUKI_TRADE_SECRET_FILE": path}),
                patch("gribuki_trade.security.secrets.import_module", return_value=backend),
                patch(
                    "gribuki_trade.security.secrets._dpapi_protect",
                    return_value=b"ciphertext",
                ),
            ):
                provider.set_secret("api-token", SENSITIVE_VALUE)
            with open(path, encoding="utf-8") as stored:
                payload = json.load(stored)
            self.assertNotIn(SENSITIVE_VALUE, json.dumps(payload))
            self.assertEqual(payload["api-token"], "Y2lwaGVydGV4dA==")

    def test_missing_keyring_value_reads_encrypted_fallback(self) -> None:
        backend = Mock()
        backend.get_password.return_value = None
        provider = KeyringSecretProvider("test-service")
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/secrets.json"
            with open(path, "w", encoding="utf-8") as stored:
                json.dump({"api-token": "Y2lwaGVydGV4dA=="}, stored)
            with (
                patch.dict("os.environ", {"GRIBUKI_TRADE_SECRET_FILE": path}),
                patch("gribuki_trade.security.secrets.import_module", return_value=backend),
                patch(
                    "gribuki_trade.security.secrets._dpapi_unprotect",
                    return_value=SENSITIVE_VALUE,
                ),
            ):
                self.assertEqual(provider.get_secret("api-token"), SENSITIVE_VALUE)


class SecretRepresentationTests(TestCase):
    def test_secret_value_and_config_repr_are_redacted(self) -> None:
        value = SecretValue(SENSITIVE_VALUE)
        config = CredentialConfig(
            username="private-account",
            password=SENSITIVE_VALUE,
            api_key="key-value",
            api_secret=value,
        )

        rendered = repr(value) + str(value) + repr(config)

        self.assertEqual(value.reveal(), SENSITIVE_VALUE)
        self.assertNotIn(SENSITIVE_VALUE, rendered)
        self.assertNotIn("private-account", rendered)
        self.assertNotIn("key-value", rendered)

    def test_interactive_manager_uses_hidden_reader_without_output(self) -> None:
        provider = MemorySecretProvider()
        hidden_reader = Mock(return_value=SENSITIVE_VALUE)
        manager = InteractiveSecretManager(provider, getpass_fn=hidden_reader)
        stdout = io.StringIO()
        stderr = io.StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            manager.set_secret("api-token")
            value = manager.get_secret("api-token")
            deleted = manager.delete_secret("api-token")

        hidden_reader.assert_called_once()
        self.assertEqual(value, SENSITIVE_VALUE)
        self.assertTrue(deleted)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")
