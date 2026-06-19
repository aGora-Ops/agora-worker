"""Mirrors api-service/app/core/security.py — Fernet token encryption only.

Both services must use the same SECRET_KEY (or TOKEN_ENCRYPTION_KEY) so tokens
encrypted by the api-service can be decrypted here. Keep the derivation logic
identical: same KDF label, same algorithm.
"""
import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from app.core.config import settings

_FERNET_KDF_LABEL = b"pipelineiq-token-encryption-v1:"


def _get_fernet() -> Fernet:
    if settings.TOKEN_ENCRYPTION_KEY:
        return Fernet(settings.TOKEN_ENCRYPTION_KEY.encode())
    key_bytes = hashlib.sha256(_FERNET_KDF_LABEL + settings.SECRET_KEY.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key_bytes))


def decrypt_token(encrypted: str) -> str:
    """Decrypt a GitHub token previously encrypted by the api-service.

    Raises ``cryptography.fernet.InvalidToken`` if the key does not match —
    callers should treat this as a hard failure, not silently fall back.
    """
    return _get_fernet().decrypt(encrypted.encode()).decode()
