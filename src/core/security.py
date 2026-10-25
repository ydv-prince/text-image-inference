"""
Security & Cryptographic Hashing Utilities
Location: src/core/security.py

Guarantees:
1. API keys are generated with 256 bits of cryptographically secure random entropy.
2. Plaintext API keys are never persisted or logged.
3. Hashing uses salted SHA-256 digests.
4. Key verification uses constant-time string comparison (hmac.compare_digest) to prevent timing attacks.
"""

import hashlib
import hmac
import secrets
from typing import Tuple


def generate_api_key(prefix: str = "aarav_live") -> Tuple[str, str, str]:
    """
    Generates a cryptographically secure API key.
    
    Returns:
        Tuple of (raw_api_key, key_prefix, key_entropy)
        Example: ('aarav_live_4f8a...3b', 'aarav_live', '4f8a...')
    """
    token = secrets.token_urlsafe(32)
    raw_key = f"{prefix}_{token}"
    return raw_key, prefix, token


def hash_api_key(raw_key: str, salt: str) -> str:
    """
    Produces a salted SHA-256 hex digest for an API key.
    Formula: SHA-256(salt + ":" + raw_key)
    """
    if not raw_key or not salt:
        raise ValueError("Both raw_key and salt are required for hashing.")
    payload = f"{salt}:{raw_key}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def verify_api_key(raw_key: str, expected_hash: str, salt: str) -> bool:
    """
    Verifies a raw API key against a stored salted hash using constant-time comparison.
    Protects against timing side-channel attacks.
    """
    if not raw_key or not expected_hash or not salt:
        return False
    computed_hash = hash_api_key(raw_key, salt)
    return hmac.compare_digest(computed_hash, expected_hash)
