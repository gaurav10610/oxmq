#!/usr/bin/env bash
# ==============================================================================
# OxDoc 365 :: Root Stop Wrapper
# Stops the entire local stack (Spring Boot Engine, PostgreSQL, Redis, Bull-Board).
# Pass --down or --clean to also remove Docker containers and volumes.
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/oxmq-examples/oxdoc-365/stop.sh" "$@"
