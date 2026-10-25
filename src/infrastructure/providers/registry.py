"""
Inference Provider Registry
Location: src/infrastructure/providers/registry.py

Architectural Guarantees:
1. Explicit selection: No silent or automatic fallback to external cloud services.
2. Default selection:
   - 'mock' in testing environments (ENVIRONMENT == 'testing')
   - 'local-gpu' in local development and production environments
3. Unknown or unconfigured providers produce an explicit error.
"""

from typing import Dict, List, Optional

from src.core.config import settings
from src.core.errors import EntityNotFoundException, ValidationException
from src.core.logging import logger
from src.domain.interfaces import ImageProviderAdapter
from src.infrastructure.providers.local_diffusers import LocalDiffusersAdapter
from src.infrastructure.providers.mock import MockProviderAdapter


class ProviderRegistry:
    """
    Central registry managing available inference provider adapters.
    """

    def __init__(self):
        self._providers: Dict[str, ImageProviderAdapter] = {}
        self._initialize_default_providers()

    def _initialize_default_providers(self) -> None:
        """Registers the baseline local and mock adapters."""
        # 1. Register Mock Provider (Always available for testing & offline validation)
        mock_adapter = MockProviderAdapter()
        self.register(mock_adapter)

        # 2. Register Local GPU Provider
        local_adapter = LocalDiffusersAdapter(
            max_dimension=settings.MAX_LOCAL_WIDTH,
        )
        self.register(local_adapter)

    def register(self, adapter: ImageProviderAdapter) -> None:
        """Registers an inference provider adapter."""
        self._providers[adapter.name] = adapter
        logger.debug(f"Registered inference provider: '{adapter.name}'")

    def get_default_provider_name(self) -> str:
        """Determines the default provider based on current environment settings."""
        if settings.ENVIRONMENT == "testing":
            return "mock"
        return "local-gpu"

    def get_provider(self, name: Optional[str] = None) -> ImageProviderAdapter:
        """
        Retrieves a provider adapter by explicit name.
        
        Args:
            name: Provider name ('local-gpu', 'mock') or None/'auto' for environment default.
            
        Raises:
            EntityNotFoundException: If the requested provider is not registered.
        """
        if not name or name == "auto":
            target_name = self.get_default_provider_name()
        else:
            target_name = name.lower().strip()

        if target_name not in self._providers:
            available = ", ".join(self._providers.keys())
            raise EntityNotFoundException("Provider", f"{target_name} (available: {available})")

        return self._providers[target_name]

    def list_providers(self) -> List[str]:
        """Returns list of registered provider names."""
        return list(self._providers.keys())


# Singleton registry instance
provider_registry = ProviderRegistry()
