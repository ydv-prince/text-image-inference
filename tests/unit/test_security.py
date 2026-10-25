"""
Unit Tests for Cryptographic Security & API Key Hashing
Location: tests/unit/test_security.py
"""

import pytest
from src.core.security import generate_api_key, hash_api_key, verify_api_key


def test_generate_api_key():
    """Verifies format, entropy, and uniqueness of generated keys."""
    raw_key1, prefix1, token1 = generate_api_key(prefix="aarav_live")
    raw_key2, prefix2, token2 = generate_api_key(prefix="aarav_live")

    assert raw_key1.startswith("aarav_live_")
    assert prefix1 == "aarav_live"
    assert len(token1) >= 40  # 32 bytes base64url encoded is ~43 chars
    assert raw_key1 != raw_key2  # Uniqueness guarantee


def test_hash_api_key_deterministic():
    """Verifies that identical raw keys and salts produce identical SHA-256 digests."""
    raw_key = "aarav_live_testsecretkey123"
    salt = "fixed_test_salt"

    digest1 = hash_api_key(raw_key, salt)
    digest2 = hash_api_key(raw_key, salt)

    assert digest1 == digest2
    assert len(digest1) == 64  # SHA-256 hex string length


def test_hash_api_key_different_salts():
    """Verifies that different salts produce completely different digests (rainbow table immunity)."""
    raw_key = "aarav_live_testsecretkey123"
    digest1 = hash_api_key(raw_key, salt="salt_alpha")
    digest2 = hash_api_key(raw_key, salt="salt_beta")

    assert digest1 != digest2


def test_verify_api_key_success():
    """Verifies constant-time comparison succeeds for matching keys."""
    raw_key = "aarav_live_validkey"
    salt = "secret_salt"
    expected_hash = hash_api_key(raw_key, salt)

    assert verify_api_key(raw_key, expected_hash, salt) is True


def test_verify_api_key_failure():
    """Verifies constant-time comparison rejects invalid or tampered keys."""
    raw_key = "aarav_live_validkey"
    wrong_key = "aarav_live_wrongkey"
    salt = "secret_salt"
    expected_hash = hash_api_key(raw_key, salt)

    assert verify_api_key(wrong_key, expected_hash, salt) is False
    assert verify_api_key(raw_key, expected_hash, salt="wrong_salt") is False
    assert verify_api_key("", expected_hash, salt) is False


def test_hash_empty_key_raises_error():
    """Verifies that hashing empty keys or salts raises ValueError."""
    with pytest.raises(ValueError):
        hash_api_key("", "salt")
    with pytest.raises(ValueError):
        hash_api_key("key", "")
