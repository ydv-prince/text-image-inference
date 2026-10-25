# ADR-001: Adoption of Modular Monolith Architecture

**Status:** ACCEPTED  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer  
**Deciders:** Core Engineering Team  

---

## 1. Context & Problem Statement
When designing an inference management gateway like Aarav Image Generation API, teams frequently face the architectural dilemma between:
- A distributed microservices architecture (separate API gateway service, message broker, worker service, image transcoding service, and auth service).
- A monolithic application.

Microservices offer fine-grained horizontal scaling but impose significant operational complexity: network serialization overhead, distributed tracing requirements, complex local developer setup (requiring Kubernetes or 6 Docker containers), and failure modes like split-brain or network partitions. Conversely, an unstructured monolith often degenerates into a tangled "big ball of mud" where business logic, database queries, and HTTP routing are tightly coupled.

## 2. Decision Drivers
- **Simplicity of Local Development:** The project must be runnable with single-command setup on developer laptops without requiring heavy background infrastructure.
- **Strict Domain Boundaries:** Code must remain clean, modular, and maintainable with clear dependency direction (Hexagonal / Clean Architecture).
- **Future Extensibility:** Components (such as the background worker) should be separable into independent container deployments without refactoring domain logic.
- **Low Overhead:** Low latency P95 response times (<30ms) for API ingestion.

## 3. Considered Options
1. **Option 1: Distributed Microservices with RabbitMQ / Kafka**
2. **Option 2: Unstructured Monolith (Single script / flat modules)**
3. **Option 3: Modular Monolith with Clean Architecture (Recommended)**

## 4. Decision Outcome
**Chosen Option: Option 3 (Modular Monolith with Clean Architecture).**

The codebase is organized into strictly bounded modules:
- `src/domain`: Pure business rules, state machines, and interfaces (no external framework imports).
- `src/core`: Logging, config, error structures.
- `src/services`: Application use cases, queue worker, and lifecycle orchestration.
- `src/infrastructure`: Database repositories, storage adapters, and external provider clients.
- `src/api`: FastAPI web routes and HTTP DTOs.

### Positive Consequences:
- **Zero Distributed Latency:** In-process function calls between services and repositories without JSON network serialization overhead.
- **Trivial Local Testing:** Entire test suite can execute in-memory in seconds without running dockerized broker daemons.
- **Operational Simplicity:** Can run as a single process for development, or split into two containers (`api` and `worker`) in production from the exact same codebase.

### Negative Consequences / Trade-offs:
- Requires strict code review discipline to prevent developers from bypassing interfaces and directly coupling modules. (Enforced via linting and module structure).
