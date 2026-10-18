#!/usr/bin/env bash
# ==============================================================================
# OxGuard 365 :: Local Stack Shutdown Script
# Gracefully stops the OxGuard 365 Java application and Docker containers.
# Pass --down or --clean to tear down Docker containers and remove volumes.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PID_FILE="${SCRIPT_DIR}/.oxguard.pid"

# ANSI Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

CLEAN_MODE=false
for arg in "$@"; do
    case "$arg" in
        --down|--clean|-v)
            CLEAN_MODE=true
            ;;
    esac
done

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${YELLOW}  🛑  OXGUARD 365 :: STOPPING LOCAL DEVELOPMENT STACK${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"

# 1. Stop OxGuard Java Application Process
STOPPED_APP=false

# Check PID file first
if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
    if ps -p "${PID}" >/dev/null 2>&1; then
        echo -n -e "${BLUE}▶ Gracefully stopping OxGuard 365 process (PID: ${PID})...${NC}"
        kill -15 "${PID}" 2>/dev/null || true
        
        # Wait up to 10 seconds for graceful termination
        WAIT_SECONDS=0
        while ps -p "${PID}" >/dev/null 2>&1 && [ ${WAIT_SECONDS} -lt 10 ]; do
            echo -n "."
            sleep 1
            WAIT_SECONDS=$((WAIT_SECONDS + 1))
        done
        
        if ps -p "${PID}" >/dev/null 2>&1; then
            echo -e " ${YELLOW}[FORCE KILL]${NC}"
            kill -9 "${PID}" 2>/dev/null || true
        else
            echo -e " ${GREEN}[STOPPED]${NC}"
        fi
        STOPPED_APP=true
    fi
    rm -f "${PID_FILE}"
fi

# Fallback: check ports 8080 and 8081 for any stray instances
for port in 8080 8081; do
    PORT_PIDS=$(lsof -ti :${port} 2>/dev/null || true)
    if [ -n "${PORT_PIDS}" ]; then
        echo -e "${BLUE}▶ Freeing port ${port} (PID: ${PORT_PIDS})...${NC}"
        kill -15 ${PORT_PIDS} 2>/dev/null || true
        sleep 1
        kill -9 ${PORT_PIDS} 2>/dev/null || true
        STOPPED_APP=true
    fi
done

if [ "${STOPPED_APP}" = true ]; then
    echo -e "${GREEN}  ✔ Application processes stopped and ports 8080 & 8081 freed.${NC}"
else
    echo -e "${CYAN}  ℹ No active OxGuard 365 Java application found on ports 8080/8081.${NC}"
fi

# 2. Stop Docker Containers
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    if [ "${CLEAN_MODE}" = true ]; then
        echo -e "${BLUE}▶ Removing Docker containers and volumes (Clean mode)...${NC}"
        docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
        for container in oxguard-bullboard oxguard-postgres oxguard-redis oxmq-redis; do
            if docker ps -a -q -f name="^/${container}$" | grep -q .; then
                docker rm -f "${container}" 2>/dev/null || true
            fi
        done
        echo -e "${GREEN}  ✔ Containers and data volumes removed.${NC}"
    else
        echo -e "${BLUE}▶ Stopping Docker containers (PostgreSQL, Redis, Bull-Board)...${NC}"
        docker compose -f "${COMPOSE_FILE}" stop 2>/dev/null || true
        for container in oxguard-bullboard oxguard-postgres oxguard-redis oxmq-redis; do
            if docker ps -q -f name="^/${container}$" | grep -q .; then
                docker stop "${container}" 2>/dev/null || true
            fi
        done
        echo -e "${GREEN}  ✔ Containers stopped (PostgreSQL data preserved).${NC}"
    fi
else
    echo -e "${YELLOW}  ⚠️ Docker daemon not accessible; skipped container shutdown.${NC}"
fi

echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN}  ✔ ALL OXGUARD 365 SERVICES STOPPED CLEANLY${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}"
echo -e "  To start again: ${CYAN}./start.sh${NC}\n"
