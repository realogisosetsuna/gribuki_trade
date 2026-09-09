# Security agent map

Read `ARCHITECTURE.md` and
`docs/architecture/execution-boundaries.md#secrets-and-generated-state`.
This narrows the root secret-handling rule for the credential boundary.

- `secrets.py` owns OS keyring access and the Windows DPAPI user-bound
  encrypted fallback; it must never write plaintext credentials.
- `interactive.py` owns hidden input; `config.py` owns redacted secret values.
- CLI/GUI callers use the provider interface and never read the fallback JSON
  directly. Preserve atomic writes, same-user decryption and sanitized errors.
- The fallback location is outside the repository by default; never add its
  contents to fixtures, logs, settings or documentation.

Tests: `tests/unit/runtime/test_security_secrets.py` and
`tests/unit/runtime/test_integration_settings.py`. Use Git Bash and run these
contracts before repository quality gates when changing persistence behavior.
