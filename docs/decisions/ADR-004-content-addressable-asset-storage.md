# ADR-004: Content-Addressable Asset Storage & Directory Fan-Out

**Status:** ACCEPTED  
**Date:** 2026-10-21  
**Author:** Principal System Design Engineer & Storage Architect  
**Deciders:** Core Engineering Team  

---

## 1. Context & Problem Statement
Image generation systems produce binary blobs (PNG, JPEG, WebP) ranging from 500 KB to 5 MB per file. A typical naive implementation stores files using random UUIDs in a single flat directory:
`storage/images/f47ac10b-58cc-4372-a567-0e02b2c3d479.png`

This naive approach creates three major production bottlenecks:
1. **Filesystem Directory Inode Degradation:** Operating systems (ext4, NTFS) suffer exponential performance degradation when a single directory exceeds 10,000 to 50,000 file entries, causing disk directory lookups and listing commands to crawl.
2. **Redundant Storage of Identical Images:** If identical seeds or deterministic prompts generate identical image binaries, redundant disk space is consumed.
3. **Security Vulnerabilities:** Naive filename handling exposes applications to path traversal attacks (`../../etc/passwd`) or arbitrary extension execution.

## 2. Decision Drivers
- **Filesystem Scalability:** System must scale to millions of images without directory lookup slowdown.
- **Deduplication:** Identical binary images must be stored only once.
- **Security & Integrity:** File paths must be mathematically derived from verified content, preventing tampering and path traversal.

## 3. Considered Options
1. **Option 1: Flat UUID Naming:** Store as `/storage/images/{uuid}.png`.
2. **Option 2: Direct Database BLOB Storage:** Store raw bytes directly inside SQLite / PostgreSQL tables.
3. **Option 3: Content-Addressable Storage (CAS) with 2-Tier Directory Fan-Out (Recommended):** Derive path from SHA-256 digest: `/storage/images/{hash[0:2]}/{hash[2:4]}/{hash}.{ext}`.

## 4. Decision Outcome
**Chosen Option: Option 3 (Content-Addressable Storage with 2-Tier Directory Fan-Out).**

### Mechanism:
1. When raw bytes are received, calculate the SHA-256 digest:
   $$\text{digest} = \text{SHA256}(\text{bytes})$$
   *(Example: `a39fbc481d6501f28b7e283b54d63a8a9bc6810c92bf76de6f827a513515ea21`)*
2. Construct fan-out path using the first 4 hex characters:
   `storage/images/a3/9f/a39fbc481d6501f28b7e283b54d63a8a9bc6810c92bf76de6f827a513515ea21.png`
3. Write file via temporary atomic rename to prevent partial writes during worker crashes.
4. Record reference in `generated_assets` table. If the file already exists on disk, skip the physical write and simply increment database references.

### Positive Consequences:
- **Scales to Millions of Files:** $256 \times 256 = 65,536$ subdirectories ensure that even with 1,000,000 assets, each folder holds fewer than 16 files on average.
- **Automatic Deduplication:** Redundant generations consume zero additional disk space.
- **Tamper Evident:** If any file is modified on disk, its hash no longer matches, allowing instant bit-rot detection.
- **Path Traversal Proof:** The filename consists strictly of 64 hexadecimal characters, eliminating user-controlled directory escape sequences.

### Negative Consequences / Trade-offs:
- Minor CPU overhead to compute SHA-256 over image buffer (typically ~2ms for a 1.5 MB image, negligible compared to 2000ms GPU generation).
