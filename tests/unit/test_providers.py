"""
Unit Tests for Inference Provider Adapters & Registry
Location: tests/unit/test_providers.py
"""

import io
import pytest
from PIL import Image

from src.core.config import settings
from src.core.errors import EntityNotFoundException, ValidationException
from src.domain.interfaces import ProviderHealth, ProviderRequest, ProviderResponse
from src.infrastructure.providers.local_diffusers import LocalDiffusersAdapter
from src.infrastructure.providers.mock import MockProviderAdapter
from src.infrastructure.providers.registry import ProviderRegistry


# -------------------------------------------------------------------------
# 1. Provider Request Domain Contract & Validation Tests
# -------------------------------------------------------------------------

def test_provider_request_valid():
    """Verifies that a well-formed request is created successfully."""
    req = ProviderRequest(
        prompt="A misty bamboo forest at sunrise",
        negative_prompt="blurry",
        width=512,
        height=512,
        steps=4,
        seed=42,
    )
    assert req.prompt == "A misty bamboo forest at sunrise"
    assert req.width == 512
    assert req.height == 512
    assert req.steps == 4
    assert req.seed == 42


def test_provider_request_empty_prompt():
    """Verifies that empty or whitespace-only prompt raises ValueError."""
    with pytest.raises(ValueError, match="Prompt must not be empty"):
        ProviderRequest(prompt="")
    with pytest.raises(ValueError, match="Prompt must not be empty"):
        ProviderRequest(prompt="   ")


def test_provider_request_prompt_length_exceeded():
    """Verifies that prompt exceeding 2000 characters raises ValueError."""
    with pytest.raises(ValueError, match="exceeds maximum length"):
        ProviderRequest(prompt="a" * 2001)


def test_provider_request_non_multiple_of_64():
    """Verifies that dimensions not divisible by 64 raise ValueError."""
    with pytest.raises(ValueError, match="must be multiples of 64"):
        ProviderRequest(prompt="Valid prompt", width=500, height=512)
    with pytest.raises(ValueError, match="must be multiples of 64"):
        ProviderRequest(prompt="Valid prompt", width=512, height=550)


def test_provider_request_steps_bounds():
    """Verifies that step counts outside [1, 50] raise ValueError."""
    with pytest.raises(ValueError, match="must be between 1 and 50"):
        ProviderRequest(prompt="Valid prompt", steps=0)
    with pytest.raises(ValueError, match="must be between 1 and 50"):
        ProviderRequest(prompt="Valid prompt", steps=51)


# -------------------------------------------------------------------------
# 2. Mock Provider Determinism & Synthetic Watermark Tests
# -------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mock_provider_generation():
    """Verifies that MockProviderAdapter produces valid, synthetic-labeled PNG bytes."""
    adapter = MockProviderAdapter()
    req = ProviderRequest(prompt="Test prompt", width=512, height=512, steps=4)

    response = await adapter.generate_image(req)

    # 1. Verify contract fields
    assert isinstance(response, ProviderResponse)
    assert response.provider_name == "mock"
    assert response.model_id == "mock-v1"
    assert response.width == 512
    assert response.height == 512
    assert response.mime_type == "image/png"
    assert response.duration_ms >= 0
    assert response.is_synthetic is True  # Golden rule: clearly labeled synthetic

    # 2. Verify image decodability and dimensions
    pil_img = Image.open(io.BytesIO(response.image_bytes))
    assert pil_img.format == "PNG"
    assert pil_img.size == (512, 512)


@pytest.mark.asyncio
async def test_mock_provider_failure_injection():
    """Verifies that MockProviderAdapter failure injection raises expected error."""
    adapter = MockProviderAdapter(fail_next=True)
    req = ProviderRequest(prompt="Test prompt")

    with pytest.raises(RuntimeError, match="Injected mock provider failure"):
        await adapter.generate_image(req)

    # Subsequent call should succeed (one-time flag)
    resp = await adapter.generate_image(req)
    assert resp.is_synthetic is True


@pytest.mark.asyncio
async def test_mock_provider_health():
    """Verifies that MockProviderAdapter reports healthy status."""
    adapter = MockProviderAdapter()
    health = await adapter.health_check()

    assert isinstance(health, ProviderHealth)
    assert health.provider_name == "mock"
    assert health.is_healthy is True
    assert health.is_loaded is True
    assert health.device == "in-memory"


# -------------------------------------------------------------------------
# 3. Provider Registry Selection & Configuration Tests
# -------------------------------------------------------------------------

def test_registry_default_selection():
    """Verifies registry selects mock in testing and local-gpu otherwise."""
    registry = ProviderRegistry()

    # In testing environment (set by conftest.py)
    default_adapter = registry.get_provider()
    assert default_adapter.name == "mock"

    # Explicit 'auto' requests default
    auto_adapter = registry.get_provider("auto")
    assert auto_adapter.name == "mock"


def test_registry_explicit_provider_lookup():
    """Verifies explicit lookup returns correct adapter instances."""
    registry = ProviderRegistry()

    mock = registry.get_provider("mock")
    assert mock.name == "mock"
    assert isinstance(mock, MockProviderAdapter)

    local = registry.get_provider("local-gpu")
    assert local.name == "local-gpu"
    assert isinstance(local, LocalDiffusersAdapter)


def test_registry_unknown_provider_raises_error():
    """Verifies that requesting an unregistered provider raises EntityNotFoundException."""
    registry = ProviderRegistry()

    with pytest.raises(EntityNotFoundException, match="Provider with identifier 'cloud-super-gpu"):
        registry.get_provider("cloud-super-gpu")


def test_registry_list_providers():
    """Verifies listing all registered providers."""
    registry = ProviderRegistry()
    providers = registry.list_providers()

    assert "mock" in providers
    assert "local-gpu" in providers


# -------------------------------------------------------------------------
# 4. Local GPU Adapter Configuration & Safety Validation Tests
# -------------------------------------------------------------------------

def test_local_diffusers_request_validation():
    """Verifies that LocalDiffusersAdapter strictly rejects requests exceeding 512x512."""
    adapter = LocalDiffusersAdapter(max_dimension=512)

    # Valid 512x512 request
    valid_req = ProviderRequest(prompt="Valid", width=512, height=512)
    adapter.validate_request(valid_req)  # Should not raise

    # Dangerous 768x768 request that would trigger OOM on 4GB VRAM
    invalid_req = ProviderRequest(prompt="Dangerous", width=768, height=768)
    with pytest.raises(ValidationException, match="strictly limited to maximum 512x512"):
        adapter.validate_request(invalid_req)


def test_local_diffusers_adapter_initial_state():
    """Verifies adapter initial configuration before any generation."""
    adapter = LocalDiffusersAdapter(
        base_model_id="runwayml/stable-diffusion-v1-5",
        adapter_id="latent-consistency/lcm-lora-sdv1-5",
        max_dimension=512,
    )
    assert adapter.name == "local-gpu"
    assert adapter.is_loaded is False
    assert adapter.max_dimension == 512


@pytest.mark.asyncio
async def test_local_diffusers_health_check_safe():
    """Verifies health check executes safely without loading weights."""
    adapter = LocalDiffusersAdapter()
    health = await adapter.health_check()

    assert isinstance(health, ProviderHealth)
    assert health.provider_name == "local-gpu"
    assert health.is_loaded is False
    if health.is_healthy:
        assert health.device is not None
        assert health.vram_total_mb is not None
        assert health.vram_total_mb > 1000
