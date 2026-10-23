#!/usr/bin/env bash
# ==============================================================================
# OxDoc 365 :: Root Launch Wrapper
# Starts the entire local stack (PostgreSQL, Redis, Bull-Board, Spring Boot Engine).
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/oxmq-examples/oxdoc-365/start.sh" "$@"
