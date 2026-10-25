# ADR-002: Pluggable Inference Provider Abstraction with Local GPU Priority

**Status:** REVISED (v2.0.0)  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer & ML Infrastructure Architect  
**Deciders:** Core Engineering Team  

---

## 1. Context & Problem Statement
The user has an NVIDIA GeForce RTX 2050 Mobile GPU with 4 GB VRAM on Windows. The project aims to run inference locally at zero cost during development, while preserving the ability to scale to cloud providers (Pollinations, fal.ai) or use synthetic mocks during automated testing.

Hardcoding direct PyTorch calls into API route handlers would violate Clean Architecture, make testing impossible on non-GPU CI machines, and prevent future cloud deployment.

## 2. Decision Drivers
- **Zero Cost Inference:** Default to physical local GPU hardware.
- **Test Isolation:** Unit and integration tests must run instantly (<5ms) without requiring GPU initialization or external network calls.
- **Provider Agility:** Switch effortlessly between Local GPU, Mock, Pollinations, and Fal via configuration.
- **Separation of Concerns:** Business logic interacts solely with domain request/response contracts.

## 3. Decision Outcome
**Chosen Option: Pluggable Adapter Architecture with Local GPU Primary Engine.**

1. `LocalDiffusersAdapter`: Primary engine. Executes `stabilityai/sd-turbo` in FP16 using PyTorch CUDA with attention slicing on the RTX 2050 Mobile GPU.
2. `MockProviderAdapter`: Deterministic synthetic adapter for automated tests and CI/CD pipelines.
3. `PollinationsAdapter`: Free community cloud fallback when local GPU is busy or running in cloud-only environments.
4. `FalAdapter`: High-performance commercial cloud fallback (opt-in only with API key).

### Positive Consequences:
- Free, offline development using host GPU compute.
- Automated tests run in seconds on any machine (even GitHub Actions with no GPU) using `MockProviderAdapter`.
- Zero refactoring required to swap or upgrade models later.
