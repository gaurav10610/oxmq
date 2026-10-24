#!/usr/bin/env bash
# ==============================================================================
# OxDoc 365 :: Root Test Stack Launch & Multi-Tenant Runner
# Builds local OxMQ dependencies, launches Docker stack & services, opens
# browser dashboards, and executes live multi-tenant indexing tests.
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/oxmq-examples/oxdoc-365/start-test.sh" "$@"
