"""Symmetric encryption for credentials stored at rest.

Per-user repository credentials (ENA WEBIN passwords, Zenodo tokens, ...) are
kept in Postgres but must never sit there in plaintext: a database dump alone
should not reveal a usable secret. This module wraps Fernet (AES-128-CBC +
HMAC) with a key sourced from the environment, so the decryption key lives
outside the database it protects.

The key is a url-safe base64-encoded 32-byte value. Generate one with:

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

and set it as CREDENTIAL_ENCRYPTION_KEY in the environment.
"""

from cryptography.fernet import Fernet, InvalidToken

from common.utils.helpers import get_env


class CredentialEncryptionError(Exception):
    """Raised when credentials cannot be encrypted or decrypted."""


def _get_fernet():
    key = get_env("CREDENTIAL_ENCRYPTION_KEY")
    if not key:
        raise CredentialEncryptionError(
            "CREDENTIAL_ENCRYPTION_KEY is not set; cannot encrypt/decrypt "
            "stored credentials."
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except (ValueError, TypeError) as exc:
        raise CredentialEncryptionError(
            "CREDENTIAL_ENCRYPTION_KEY is not a valid Fernet key."
        ) from exc


def encrypt(plaintext: str) -> str:
    """Encrypt a plaintext string, returning a url-safe token for storage."""
    if plaintext is None:
        raise CredentialEncryptionError("Cannot encrypt None.")
    return _get_fernet().encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt(token: str) -> str:
    """Decrypt a token produced by encrypt(); raises on a bad key or tampering."""
    if not token:
        raise CredentialEncryptionError("Cannot decrypt an empty token.")
    try:
        return _get_fernet().decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise CredentialEncryptionError(
            "Stored credential could not be decrypted (wrong key or corrupted "
            "data)."
        ) from exc
