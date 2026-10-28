#!/usr/bin/env bash
# =============================================================================
# Inference-1 — API Dependency Lockfile Generator (CI / Approved Build Runner)
# =============================================================================
# Purpose: Resolves direct dependencies from requirements-api.txt and generates
#          a deterministic, hash-pinned lockfile for CPython 3.10 on Linux x86_64.
# Safety:  Must only be executed in an approved CI/CD runner or container environment
#          possessing sanctioned PyPI egress. Never bypass local firewalls.
# Target:  Preserves requirements-api.lock until the candidate is validated and
#          explicitly approved by the operator.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_SPEC="${REPO_ROOT}/requirements-api.txt"
CANDIDATE_LOCK="${REPO_ROOT}/requirements-api.candidate.lock"
ACTIVE_LOCK="${REPO_ROOT}/requirements-api.lock"

# -----------------------------------------------------------------------------
# 0. CLI Argument Parsing & Safety Pre-flight
# -----------------------------------------------------------------------------
FORCE=0

for arg in "$@"; do
    case "${arg}" in
        --force|-f)
            FORCE=1
            ;;
        --help|-h)
            echo "Usage: $0 [--force|-f]"
            echo "Options:"
            echo "  --force, -f  Explicitly authorize replacement of an existing candidate lockfile."
            echo "  --help, -h   Display this help message."
            exit 0
            ;;
        *)
            echo "ERROR: Unrecognized argument '${arg}'." >&2
            echo "Usage: $0 [--force|-f]" >&2
            exit 1
            ;;
    esac
done

echo "=== Inference-1: API Dependency Resolution ==="
echo "Target Platform: Linux x86_64 (x86_64-unknown-linux-gnu)"
echo "Target Runtime:  CPython 3.10"
echo "Input Spec:      ${INPUT_SPEC}"
echo "Output Lock:     ${CANDIDATE_LOCK}"
echo "Active Lock:     ${ACTIVE_LOCK}"

# Enforce active lockfile protection
if [ "${CANDIDATE_LOCK}" = "${ACTIVE_LOCK}" ]; then
    echo "FATAL: Candidate lockfile destination path cannot match active lockfile." >&2
    exit 1
fi

if [ ! -f "${INPUT_SPEC}" ]; then
    echo "ERROR: Input specification file '${INPUT_SPEC}' not found." >&2
    exit 1
fi

# Track whether a candidate lockfile was initiated/created during THIS invocation.
# Preexisting candidate files awaiting operator review must NEVER be cleaned up on failure.
CREATED_CANDIDATE_THIS_RUN=0

# Trap: On any failure, remove candidate lockfile ONLY if it was created during this run
cleanup_on_error() {
    local exit_code=$?
    if [ ${exit_code} -ne 0 ]; then
        if [ ${CREATED_CANDIDATE_THIS_RUN} -eq 1 ] && [ -f "${CANDIDATE_LOCK}" ]; then
            echo "Resolution or validation failed with code ${exit_code}." >&2
            echo "Removing incomplete candidate lockfile '${CANDIDATE_LOCK}' created during this run..." >&2
            rm -f "${CANDIDATE_LOCK}"
        fi
    fi
}
trap cleanup_on_error EXIT

# Check if candidate lockfile already exists
if [ -f "${CANDIDATE_LOCK}" ]; then
    if [ ${FORCE} -eq 0 ]; then
        echo "ERROR: An unpromoted candidate lockfile already exists at:" >&2
        echo "       ${CANDIDATE_LOCK}" >&2
        echo "" >&2
        echo "Safety preservation triggered:" >&2
        echo "  An existing candidate may be awaiting operator review or promotion." >&2
        echo "  To preserve prior work, the generator will not overwrite it by default." >&2
        echo "" >&2
        echo "To resolve:" >&2
        echo "  1. Review existing candidate:  diff -u ${ACTIVE_LOCK} ${CANDIDATE_LOCK}" >&2
        echo "  2. Manually move it aside:    mv ${CANDIDATE_LOCK} ${CANDIDATE_LOCK}.bak" >&2
        echo "  3. Or explicitly overwrite:   $0 --force" >&2
        exit 1
    else
        echo "NOTE: Explicit --force provided. Removing existing candidate lockfile '${CANDIDATE_LOCK}'."
        rm -f "${CANDIDATE_LOCK}"
    fi
fi

# -----------------------------------------------------------------------------
# 1. Resolver Tooling Selection & Platform Compatibility
# -----------------------------------------------------------------------------
if command -v uv &>/dev/null; then
    RESOLVER="uv"
    echo "Using resolver: uv ($(uv --version))"
elif python3 -m pip list 2>/dev/null | grep -q "pip-tools"; then
    RESOLVER="pip-compile"
    echo "Using resolver: pip-compile ($(python3 -m piptools --version 2>/dev/null || echo 'pip-tools'))"
elif command -v pip-compile &>/dev/null; then
    RESOLVER="pip-compile"
    echo "Using resolver: pip-compile ($(pip-compile --version))"
else
    echo "ERROR: Neither 'uv' nor 'pip-tools' is installed in this environment." >&2
    echo "To install in an approved build environment with sanctioned PyPI access:" >&2
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh   OR   pip install pip-tools==7.4.1" >&2
    exit 1
fi

# -----------------------------------------------------------------------------
# 2. Platform Architecture Verification
# -----------------------------------------------------------------------------
if [ "${RESOLVER}" = "pip-compile" ]; then
    echo "Verifying host architecture compatibility for pip-compile..."
    HOST_OS="$(uname -s)"
    HOST_ARCH="$(uname -m)"
    if [ "${HOST_OS}" != "Linux" ] || [ "${HOST_ARCH}" != "x86_64" ]; then
        echo "ERROR: 'pip-compile' evaluates dependencies based on host environment markers." >&2
        echo "       Host is '${HOST_OS} ${HOST_ARCH}', which cannot reliably compile for Linux x86_64." >&2
        echo "       Please use 'uv' (which supports cross-platform compilation) or run inside an x86_64 Linux runner." >&2
        exit 1
    fi

    # Verify host Python interpreter is CPython 3.10
    python3 -c "
import platform, sys
if sys.version_info[:2] != (3, 10):
    sys.exit(f'ERROR: pip-compile requires Python 3.10, found {sys.version.split()[0]}')
if platform.python_implementation() != 'CPython':
    sys.exit(f'ERROR: pip-compile requires CPython, found {platform.python_implementation()}')
"
fi

# -----------------------------------------------------------------------------
# 3. Execute Resolution with SHA-256 Hashes
# -----------------------------------------------------------------------------
echo "Resolving dependencies with SHA-256 hashes..."
CREATED_CANDIDATE_THIS_RUN=1

if [ "${RESOLVER}" = "uv" ]; then
    uv pip compile \
        --python-version 3.10 \
        --python-platform x86_64-unknown-linux-gnu \
        --generate-hashes \
        --output-file "${CANDIDATE_LOCK}" \
        "${INPUT_SPEC}"
else
    pip-compile \
        --generate-hashes \
        --resolver=backtracking \
        --allow-unsafe \
        --output-file="${CANDIDATE_LOCK}" \
        "${INPUT_SPEC}"
fi

if [ ! -f "${CANDIDATE_LOCK}" ]; then
    echo "ERROR: Candidate lockfile was not generated." >&2
    exit 1
fi

echo "Candidate lockfile successfully written to: ${CANDIDATE_LOCK}"

# -----------------------------------------------------------------------------
# 4. Automated Candidate Invariant & Integrity Validation
# -----------------------------------------------------------------------------
echo "Validating candidate lockfile integrity and invariants..."

python3 - "${CANDIDATE_LOCK}" "${INPUT_SPEC}" << 'EOF'
import sys
from pathlib import Path
from packaging.requirements import Requirement, InvalidRequirement
from packaging.utils import canonicalize_name

candidate_path = Path(sys.argv[1])
direct_path = Path(sys.argv[2])

if not candidate_path.exists():
    print(f"ERROR: Candidate lockfile '{candidate_path}' does not exist.", file=sys.stderr)
    sys.exit(1)

lines = [l.strip() for l in candidate_path.read_text(encoding="utf-8").splitlines() if l.strip() and not l.startswith("#")]

lock_map = {}
package_hashes = {}
current_pkg = None

for line in lines:
    tokens = [t.strip() for t in line.split() if t.strip()]
    if not tokens:
        continue

    # Case A: Line continuation hash entry
    if tokens[0].startswith("--hash="):
        if not current_pkg:
            print(f"ERROR: Orphan hash entry with no preceding package: {line}", file=sys.stderr)
            sys.exit(1)
        for t in tokens:
            if t.startswith("--hash="):
                h = t.split("=", 1)[1]
                if not h.startswith("sha256:"):
                    print(f"ERROR: Non-SHA256 hash in candidate: {h}", file=sys.stderr)
                    sys.exit(1)
                digest = h.split(":", 1)[1]
                if len(digest) != 64 or not all(c in "0123456789abcdefABCDEF" for c in digest):
                    print(f"ERROR: Invalid SHA-256 digest format: {h}", file=sys.stderr)
                    sys.exit(1)
                package_hashes[current_pkg].append(h)
        continue

    # Case B: Package requirement entry
    req_token = tokens[0]
    try:
        r = Requirement(req_token)
    except InvalidRequirement as e:
        print(f"ERROR: Malformed requirement syntax '{req_token}': {e}", file=sys.stderr)
        sys.exit(1)

    norm = canonicalize_name(r.name)
    if norm in lock_map:
        print(f"ERROR: Duplicate normalized package in candidate lockfile: '{norm}'", file=sys.stderr)
        sys.exit(1)

    if len(r.specifier) != 1:
        print(f"ERROR: Package '{norm}' must have exactly one pinned specifier, got {r.specifier}", file=sys.stderr)
        sys.exit(1)

    spec = next(iter(r.specifier))
    if spec.operator != "==":
        print(f"ERROR: Package '{norm}' must use exact '==' pinning, got '{spec.operator}'", file=sys.stderr)
        sys.exit(1)

    lock_map[norm] = spec.version
    current_pkg = norm
    package_hashes[norm] = []

    # Check for inline hashes on the same line
    for t in tokens[1:]:
        if t.startswith("--hash="):
            h = t.split("=", 1)[1]
            if not h.startswith("sha256:"):
                print(f"ERROR: Non-SHA256 hash in candidate: {h}", file=sys.stderr)
                sys.exit(1)
            digest = h.split(":", 1)[1]
            if len(digest) != 64 or not all(c in "0123456789abcdefABCDEF" for c in digest):
                print(f"ERROR: Invalid SHA-256 digest format: {h}", file=sys.stderr)
                sys.exit(1)
            package_hashes[norm].append(h)

# 1. Assert each package has valid hashes
for pkg, hashes in package_hashes.items():
    if not hashes:
        print(f"ERROR: Package '{pkg}' has no SHA-256 hashes generated in candidate lockfile.", file=sys.stderr)
        sys.exit(1)

print(f"Validated {len(lock_map)} unique locked packages with verified SHA-256 hashes.")

# 2. Assert all direct dependencies are present and constraints satisfied
direct_lines = [l.strip() for l in direct_path.read_text(encoding="utf-8").splitlines() if l.strip() and not l.startswith("#")]
for dline in direct_lines:
    dr = Requirement(dline)
    norm = canonicalize_name(dr.name)
    if norm not in lock_map:
        print(f"ERROR: Direct dependency '{norm}' missing from candidate lockfile.", file=sys.stderr)
        sys.exit(1)
    locked_ver = lock_map[norm]
    if not dr.specifier.contains(locked_ver):
        print(f"ERROR: Locked version {locked_ver} for '{norm}' violates constraint {dr.specifier}", file=sys.stderr)
        sys.exit(1)
    print(f"  [OK] Direct dependency: {norm}=={locked_ver} satisfies {dr.specifier}")

# 3. Assert GPU and Dev exclusions
forbidden = [
    "torch", "torchvision", "diffusers", "transformers", "accelerate", "safetensors", "peft",
    "pytest", "pytest-asyncio"
]
for f in forbidden:
    norm_f = canonicalize_name(f)
    if norm_f in lock_map:
        print(f"ERROR: Forbidden package '{norm_f}' detected in candidate lockfile!", file=sys.stderr)
        sys.exit(1)

print("All candidate lockfile invariant and security checks passed successfully.")
EOF

# Disarm trap on successful completion
trap - EXIT

# -----------------------------------------------------------------------------
# 5. Explicit Operator Review & Promotion Guidance
# -----------------------------------------------------------------------------
echo ""
echo "========================================================================"
echo "Candidate Lockfile Generated and Validated Successfully"
echo "========================================================================"
echo "Candidate: ${CANDIDATE_LOCK}"
echo "Active:    ${ACTIVE_LOCK}"
echo ""
echo "To review differences against active lockfile:"
echo "  diff -u ${ACTIVE_LOCK} ${CANDIDATE_LOCK} || true"
echo ""
echo "To promote candidate to active lockfile after manual operator approval:"
echo "  cp ${CANDIDATE_LOCK} ${ACTIVE_LOCK} && rm ${CANDIDATE_LOCK}"
echo "========================================================================"
