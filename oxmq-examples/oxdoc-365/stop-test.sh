#!/usr/bin/env bash
# ==============================================================================
# OxDoc 365 :: Stop & Tear Down Test Infrastructure
# 1. Gracefully terminates OxDoc 365 and Mock Office 365 Server processes
# 2. Rams down the complete Docker stack (PostgreSQL, Redis, Bull-Board, Prometheus, Grafana)
# 3. Removes all Docker volumes, networks, and orphaned containers
# 4. Cleans PID files and verifies all ports are fully liberated
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PID_FILE="${SCRIPT_DIR}/.oxdoc.pid"
MOCK_PID_FILE="${SCRIPT_DIR}/.mockserver.pid"

# ANSI Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${YELLOW}  🛑  OXDOC 365 :: TEARING DOWN TEST STACK & INFRASTRUCTURE${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"

# 1. Stop Java Application Processes (OxDoc 365 & Mock Office 365 Server)
STOPPED_APP=false

for pid_info in "OxDoc 365:${PID_FILE}" "Mock Office 365 Server:${MOCK_PID_FILE}"; do
    NAME="${pid_info%%:*}"
    PFILE="${pid_info#*:}"
    if [ -f "${PFILE}" ]; then
        PID=$(cat "${PFILE}")
        if ps -p "${PID}" >/dev/null 2>&1; then
            echo -n -e "${BLUE}▶ Gracefully stopping ${NAME} (PID: ${PID})...${NC}"
            kill -15 "${PID}" 2>/dev/null || true
            
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
        rm -f "${PFILE}"
    fi
done

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
    echo -e "${CYAN}  ℹ No active Java application processes found on ports 8080/8081.${NC}"
fi

# 2. Ram down Docker Containers, Networks, and Volumes
CONTAINER_LIST="oxdoc-bullboard oxdoc-postgres oxdoc-redis oxdoc-redis-exporter oxdoc-prometheus oxdoc-grafana"

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo -e "\n${BLUE}▶ Tearing down Docker infrastructure (volumes & orphan containers)...${NC}"
    docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
    for container in ${CONTAINER_LIST}; do
        if docker ps -a -q -f name="^/${container}$" | grep -q .; then
            docker rm -f "${container}" >/dev/null 2>&1 || true
        fi
    done
    docker rm -f oxguard-grafana oxguard-bullboard oxguard-prometheus oxguard-redis oxguard-postgres 2>/dev/null || true
    docker volume rm oxguard-365_oxguard_pgdata oxguard_pgdata 2>/dev/null || true
    echo -e "${GREEN}  ✔ All Docker containers, networks, and data volumes destroyed.${NC}"
else
    echo -e "${YELLOW}  ⚠️ Docker daemon not accessible; skipped container teardown.${NC}"
fi

# 3. Verify all stack ports are liberated
echo -e "\n${BLUE}▶ Verifying all ports are free...${NC}"
ALL_PORTS="8080 8081 3000 3001 5432 6379 9090 9121"
STRAY_FOUND=false
for p in ${ALL_PORTS}; do
    PIDS=$(lsof -ti :${p} 2>/dev/null || true)
    if [ -n "${PIDS}" ]; then
        echo -e "${YELLOW}  ⚠️ Port ${p} is occupied by PID ${PIDS}. Terminating...${NC}"
        kill -9 ${PIDS} 2>/dev/null || true
        STRAY_FOUND=true
    fi
done

if [ "${STRAY_FOUND}" = false ]; then
    echo -e "${GREEN}  ✔ All ports (8080, 8081, 3000, 3001, 5432, 6379, 9090) are completely clean.${NC}"
fi

echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN}  ✔ COMPLETE OXDOC 365 TEST INFRASTRUCTURE RAMMED DOWN SUCCESSFULLY${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}\n"
