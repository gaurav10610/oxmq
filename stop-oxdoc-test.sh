#!/usr/bin/env bash
# ==============================================================================
# OxDoc 365 :: Root Test Stack Teardown & Infra Destroy Wrapper
# Gracefully stops Java applications, tears down all Docker containers,
# cleans volumes, and verifies all stack ports are freed.
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/oxmq-examples/oxdoc-365/stop-test.sh" "$@"
