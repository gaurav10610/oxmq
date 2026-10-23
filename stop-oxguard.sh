#!/usr/bin/env bash
# ==============================================================================
# OxGuard 365 -> OxDoc 365 :: Root Stop Wrapper (Backward Compatibility)
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/stop-oxdoc.sh" "$@"
