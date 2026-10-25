# ADR-007: Free-Tier Infrastructure Stack & Cost Model (Zero-Budget First)

**Status:** ACCEPTED  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer & Technical Lead  
**Deciders:** Core Engineering Team  

---

## 1. Context & Financial Constraints
The project owner is learning system design and has set a strict **initial budget of ₹0**. The platform must be fully functional, testable locally, and deployable publicly without incurring recurring server bills or paid API subscriptions.

Later, the owner may consider purchasing an inexpensive domain name and a server if the project demands it.

## 2. Analysis of Hosting Components & Realities

### 2.1 The Difference Between a Domain, a CPU Server, and a GPU Server
A critical system design mistake made by beginners is conflating domains, generic servers, and GPU compute:
1. **A Domain Name is Not a Server:**
   - A domain (e.g. `aarav-api.com` or `.in`, costing ₹800–₹1,200/year) is merely a pointer in the Domain Name System (DNS).
   - A domain provides **zero compute, zero memory, and zero storage**. Purchasing a domain does not give you anywhere to host code.
   - For an initial prototype, a custom domain is completely unnecessary. Free hosting providers supply robust subdomains (e.g., `aarav-ai.web.app` or `aarav-api.onrender.com`) with automated SSL certificates.
2. **A Low-Cost CPU VPS is Not a GPU:**
   - Low-cost VPS instances (e.g., $4/mo on Hetzner or DigitalOcean) provide 1 shared vCPU and 1 GB of RAM.
   - They **lack a physical graphics processing unit (GPU)**.
   - Running image diffusion on a CPU takes 2 to 5 minutes per image, causing HTTP timeouts and consuming 100% of the host CPU.
3. **Dedicated GPU Cloud Hosting is Expensive:**
   - Cloud GPU instances (like AWS EC2 `g4dn.xlarge` with NVIDIA T4) cost ~$0.526/hour ($380/month if left running 24/7).
   - Serverless GPU platforms (RunPod, Modal, Fal.ai) charge per-second or per-image, requiring prepaid balances or credit cards.

## 3. Decision Outcome: The ₹0 Free-Tier Hybrid Stack
We select an entirely free, decoupled cloud architecture coupled with the user's local RTX 2050 Mobile GPU:

| Component | Selected Platform | Free Tier Quota | Role & Limitations |
| :--- | :--- | :--- | :--- |
| **Frontend UI** | **Firebase Hosting** | 10 GB storage, 360 MB/day transfer | Static HTML/CSS/JS delivery. Cannot run Python or GPU inference. |
| **Durable DB Queue** | **Supabase PostgreSQL** | 500 MB DB storage, 50,000 active users | ACID relational job queue and API key storage. |
| **Control Plane API** | **Render / Fly.io** | 512 MB RAM free web service | Handles HTTP ingestion and passes jobs to DB. Sleeps after 15m idle. |
| **Asset CDN & Storage**| **Cloudinary** | 25 monthly transformation credits (~25 GB) | Stores generated PNGs and provides fast global CDN delivery. |
| **GPU Inference** | **Local RTX 2050 Mobile** | Unlimited (Uses local home power) | Executes SD-Turbo diffusion inference in 1-2s. |

### Long-Term Purchasing Evaluation:
If the user later decides to transition away from home PC hosting to a 24/7 cloud server:
- **Phase 1 (Now):** ₹0 cost using local GPU + free cloud tiers.
- **Phase 2 (Optional Domain):** Purchase a `.in` or `.com` domain (~₹800/yr) purely for custom branding.
- **Phase 3 (Cloud Serverless GPU):** Rather than renting an expensive $300/mo dedicated GPU VPS, attach a pay-as-you-go serverless adapter (like `fal.ai` at $0.003/image) so costs only scale when users generate images.
