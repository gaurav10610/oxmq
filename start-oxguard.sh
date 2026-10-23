#!/usr/bin/env bash
# ==============================================================================
# OxGuard 365 -> OxDoc 365 :: Root Launch Wrapper (Backward Compatibility)
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/start-oxdoc.sh" "$@"
