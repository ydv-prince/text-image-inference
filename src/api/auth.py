"""
API Key Authentication Dependency
Location: src/api/auth.py
"""

from datetime import datetime, timezone
from fastapi import Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.errors import AuthenticationException
from src.core.security import hash_api_key
from src.infrastructure.database import get_db_session
from src.infrastructure.models import ApiKeyModel


async def get_authenticated_api_key(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> ApiKeyModel:
    """
    Extracts, hashes, and verifies client API key against the database.
    Rejects missing, invalid, or revoked keys with HTTP 401.
    """
    raw_key = request.headers.get("X-API-Key")
    if not raw_key:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            raw_key = auth_header[7:].strip()

    if not raw_key:
        raise AuthenticationException("Missing API key. Provide via 'X-API-Key' or 'Authorization: Bearer <key>'.")

    # Salted cryptographic hash
    key_hash = hash_api_key(raw_key, settings.API_KEY_SALT.get_secret_value())

    # Query key from DB
    stmt = select(ApiKeyModel).where(ApiKeyModel.key_hash == key_hash)
    result = await db.execute(stmt)
    api_key = result.scalar_one_or_none()

    if not api_key:
        raise AuthenticationException("Invalid API key.")

    if not api_key.is_active:
        raise AuthenticationException("API key has been revoked.")

    # Update last_used_at telemetry
    api_key.last_used_at = datetime.now(timezone.utc)
    await db.commit()

    return api_key
