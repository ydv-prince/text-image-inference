# ADR-006: Hybrid Hosted Architecture & Outbound Worker Polling

**Status:** ACCEPTED  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer & Security Architect  
**Deciders:** Core Engineering Team  

---

## 1. Context & Problem Statement
In Mode B (Hybrid Hosted Deployment), public web clients need to access the Aarav Image Generation platform over the internet, but the actual GPU hardware (RTX 2050 Mobile) resides on the developer's private home Windows PC.

Connecting a private home computer to a public cloud application presents significant security and networking challenges:
1. **Network Address Translation (NAT) & CGNAT:** Most residential ISPs place home connections behind Carrier-Grade NAT (CGNAT) or dynamic IP addresses, preventing external cloud servers from directly opening inbound connections to the PC.
2. **Security Vulnerability of Inbound Ports:** Opening router ports (port forwarding / UPnP) or running tunneling tools (like public Ngrok endpoints) directly exposes the home computer's private IP and operating system to automated internet port scans and DDoS attacks.
3. **Availability & Power Cycles:** Home computers are powered off at night, put to sleep, or restart for Windows updates.

## 2. Considered Options
- **Option 1: Inbound Port Forwarding / Dynamic DNS:** Open port 8000 on home router to public internet. *(REJECTED: Severe security vulnerability; fails behind CGNAT).*
- **Option 2: Continuous Tunneling (Ngrok / Cloudflare Tunnel):** Tunnel public HTTP traffic directly to the local FastAPI process. *(REJECTED: Free tunnels have bandwidth limits, ephemeral URLs, and the entire app goes down when the PC sleeps).*
- **Option 3: Outbound HTTPS Worker Polling with Decoupled Cloud Queue (Recommended):** Cloud control plane hosts the API and database; local PC acts purely as an outbound HTTP client that polls for jobs.

## 3. Decision Outcome
**Chosen Option: Option 3 (Outbound HTTPS Worker Polling with Decoupled Cloud Queue).**

### Mechanism:
1. Public users interact with the hosted control plane (Firebase Frontend + Hosted FastAPI API + Supabase PostgreSQL).
2. The local GPU worker initiates an **outbound HTTPS request** (`POST /api/v1/worker/jobs/lease`) using a 256-bit secret token (`WORKER_SHARED_SECRET`).
3. Outbound HTTPS traffic effortlessly traverses home routers, NAT, and firewalls without requiring any network configuration.
4. When an image is rendered, the worker uploads the asset to Cloudinary or Supabase Storage and notifies the control plane via another outbound HTTPS call.
5. **Offline Queueing:** If the home PC is turned off, jobs accumulate safely in Supabase PostgreSQL (`PENDING`). As soon as the PC boots and the worker starts, it claims and processes all pending jobs in order.

### Positive Consequences:
- **Zero Inbound Security Risk:** Zero open ports on the home network.
- **Zero ISP Constraints:** Works seamlessly on any Wi-Fi, Ethernet, or mobile hotspot connection.
- **High Web Availability:** The public website remains online and accessible via Firebase Hosting 24/7, even when the home PC is powered down.
