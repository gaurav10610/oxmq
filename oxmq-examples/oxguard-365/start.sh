#!/usr/bin/env bash
# ==============================================================================
# OxGuard 365 :: Local Stack Startup Script
# Starts PostgreSQL, Redis, Bull-Board, and the OxGuard 365 Spring Boot Engine.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PID_FILE="${SCRIPT_DIR}/.oxguard.pid"
LOG_FILE="${SCRIPT_DIR}/oxguard.log"
JAR_FILE="${SCRIPT_DIR}/target/oxguard-365-1.5.0.jar"

# ANSI Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${CYAN}  🛡️  OXGUARD 365 :: STARTING LOCAL DEVELOPMENT STACK${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"

# 1. Pre-flight Checks
if ! command -v docker >/dev/null 2>&1; then
    echo -e "${RED}✖ Error: 'docker' command not found. Please install Docker.${NC}"
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}✖ Error: Docker daemon is not running. Please start Docker Desktop or the Docker service.${NC}"
    exit 1
fi

if ! command -v java >/dev/null 2>&1; then
    echo -e "${RED}✖ Error: 'java' command not found. Please install Java 17 LTS or higher.${NC}"
    exit 1
fi

# 2. Check if already running
if [ -f "${PID_FILE}" ]; then
    EXISTING_PID=$(cat "${PID_FILE}")
    if ps -p "${EXISTING_PID}" >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  OxGuard 365 is already running (PID: ${EXISTING_PID}).${NC}"
        echo -e "${GREEN}   Dashboard UI:             http://localhost:8080${NC}"
        echo -e "${GREEN}   Bull-Board UI:            http://localhost:3000${NC}"
        echo -e "${GREEN}   Mock Office 365 Server:   http://localhost:8081${NC}"
        echo -e "${CYAN}   To restart, run: ./stop.sh && ./start.sh${NC}"
        exit 0
    else
        rm -f "${PID_FILE}"
    fi
fi

# Also check if ports 8080 or 8081 are occupied by another process
if lsof -ti :8080 >/dev/null 2>&1 || lsof -ti :8081 >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Port 8080 or 8081 is already occupied by a running process.${NC}"
    echo -e "   Run ${BOLD}./stop.sh${NC} to stop any previous instances before starting."
    exit 1
fi

# 3. Start Docker Infrastructure (Postgres, Redis, Bull-Board)
echo -e "${BLUE}▶ Starting Docker containers (PostgreSQL 16, Redis 7, Bull-Board 9.9.0)...${NC}"
if ! docker compose -f "${COMPOSE_FILE}" up -d 2>/dev/null; then
    # In case containers exist from previous run or manual setup, start them directly
    for container in oxguard-postgres oxguard-redis oxmq-redis oxguard-bullboard; do
        if docker ps -a -q -f name="^/${container}$" | grep -q .; then
            docker start "${container}" >/dev/null 2>&1 || true
        fi
    done
    docker compose -f "${COMPOSE_FILE}" up -d >/dev/null 2>&1 || true
fi

echo -n "  Waiting for PostgreSQL (5432) to accept connections..."
MAX_PG_RETRIES=30
PG_RETRY=0
until docker exec oxguard-postgres pg_isready -U oxguard >/dev/null 2>&1 || [ ${PG_RETRY} -eq ${MAX_PG_RETRIES} ]; do
    echo -n "."
    sleep 1
    PG_RETRY=$((PG_RETRY + 1))
done
if [ ${PG_RETRY} -eq ${MAX_PG_RETRIES} ]; then
    echo -e " ${RED}[FAILED]${NC}"
    echo -e "${RED}PostgreSQL failed to become ready in time.${NC}"
    exit 1
fi
echo -e " ${GREEN}[READY]${NC}"

echo -n "  Waiting for Redis (6379) to respond to PING..."
MAX_REDIS_RETRIES=20
REDIS_RETRY=0
until [ "$(docker exec oxguard-redis redis-cli ping 2>/dev/null || docker exec oxmq-redis redis-cli ping 2>/dev/null)" = "PONG" ] || [ ${REDIS_RETRY} -eq ${MAX_REDIS_RETRIES} ]; do
    echo -n "."
    sleep 1
    REDIS_RETRY=$((REDIS_RETRY + 1))
done
if [ ${REDIS_RETRY} -eq ${MAX_REDIS_RETRIES} ]; then
    echo -e " ${RED}[FAILED]${NC}"
    echo -e "${RED}Redis failed to respond in time.${NC}"
    exit 1
fi
echo -e " ${GREEN}[READY]${NC}"

echo -n "  Waiting for Bull-Board (3000) to respond..."
MAX_BB_RETRIES=30
BB_RETRY=0
until curl -s http://localhost:3000/ >/dev/null 2>&1 || [ ${BB_RETRY} -eq ${MAX_BB_RETRIES} ]; do
    echo -n "."
    sleep 1
    BB_RETRY=$((BB_RETRY + 1))
done
if [ ${BB_RETRY} -eq ${MAX_BB_RETRIES} ]; then
    echo -e " ${YELLOW}[WARNING] (Bull-board still starting in background)${NC}"
else
    echo -e " ${GREEN}[READY]${NC}"
fi

echo -n "  Waiting for Prometheus (9090) to respond..."
PROM_RETRY=0
until curl -s http://localhost:9090/-/healthy >/dev/null 2>&1 || [ ${PROM_RETRY} -eq 20 ]; do
    echo -n "."
    sleep 1
    PROM_RETRY=$((PROM_RETRY + 1))
done
if [ ${PROM_RETRY} -eq 20 ]; then
    echo -e " ${YELLOW}[WARNING] (Prometheus starting in background)${NC}"
else
    echo -e " ${GREEN}[READY]${NC}"
fi

echo -n "  Waiting for Grafana (3001) to respond..."
GRAF_RETRY=0
until curl -s http://localhost:3001/api/health >/dev/null 2>&1 || [ ${GRAF_RETRY} -eq 20 ]; do
    echo -n "."
    sleep 1
    GRAF_RETRY=$((GRAF_RETRY + 1))
done
if [ ${GRAF_RETRY} -eq 20 ]; then
    echo -e " ${YELLOW}[WARNING] (Grafana starting in background)${NC}"
else
    echo -e " ${GREEN}[READY]${NC}"
fi

# 4. Ensure Application Jar is Built
echo -e "${BLUE}▶ Packaging OxGuard 365 fat JAR via Maven Wrapper...${NC}"
(cd "${REPO_ROOT}" && ./mvnw package -pl oxmq-examples/oxguard-365 -DskipTests -q)
echo -e "${GREEN}  JAR successfully created: ${JAR_FILE}${NC}"

# 5. Start OxGuard 365 Spring Boot Application
echo -e "${BLUE}▶ Launching OxGuard 365 (Spring Boot + Mock Office 365 Server)...${NC}"
nohup java -jar "${JAR_FILE}" > "${LOG_FILE}" 2>&1 &
APP_PID=$!
echo "${APP_PID}" > "${PID_FILE}"
echo -e "${GREEN}  Process started with PID: ${APP_PID} (Logs: ${LOG_FILE})${NC}"

# 6. Wait for HTTP readiness
echo -n "  Waiting for OxGuard Dashboard (8080) to become healthy..."
MAX_APP_RETRIES=40
APP_RETRY=0
until curl -s http://localhost:8080/api/v1/dashboard/metrics >/dev/null 2>&1 || [ ${APP_RETRY} -eq ${MAX_APP_RETRIES} ]; do
    # Check if process died prematurely
    if ! ps -p "${APP_PID}" >/dev/null 2>&1; then
        echo -e " ${RED}[CRASHED]${NC}"
        echo -e "${RED}Application process terminated unexpectedly. Check ${LOG_FILE}:${NC}"
        tail -n 25 "${LOG_FILE}"
        rm -f "${PID_FILE}"
        exit 1
    fi
    echo -n "."
    sleep 1
    APP_RETRY=$((APP_RETRY + 1))
done

if [ ${APP_RETRY} -eq ${MAX_APP_RETRIES} ]; then
    echo -e " ${RED}[TIMEOUT]${NC}"
    echo -e "${RED}Application took too long to respond. Inspect logs: tail -f ${LOG_FILE}${NC}"
    exit 1
fi
echo -e " ${GREEN}[READY]${NC}"

# 7. Print Success Banner & URLs
echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN}  🚀 OXGUARD 365 STACK IS LIVE AND OPERATIONAL!${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}"
echo -e "  🌐 ${BOLD}OxGuard DSPM Dashboard:${NC}    ${CYAN}http://localhost:8080${NC}"
echo -e "  📈 ${BOLD}Grafana Metrics Telemetry:${NC} ${CYAN}http://localhost:3001/d/oxmq-overview${NC}"
echo -e "  🔥 ${BOLD}Prometheus Metrics:${NC}        ${CYAN}http://localhost:9090${NC}"
echo -e "  📊 ${BOLD}Bull-Board Queue Monitor:${NC}  ${CYAN}http://localhost:3000${NC}"
echo -e "  🏢 ${BOLD}Mock Office 365 Server:${NC}    ${CYAN}http://localhost:8081${NC}"
echo -e "  🐘 ${BOLD}PostgreSQL Database:${NC}       ${CYAN}localhost:5432${NC} (db: oxguard, user: oxguard)"
echo -e "  ⚡ ${BOLD}Redis 7 Queue Backend:${NC}     ${CYAN}localhost:6379${NC}"
echo -e "  📄 ${BOLD}Application Logs:${NC}          ${CYAN}tail -f ${LOG_FILE}${NC}"
echo -e "  🛑 ${BOLD}Stop Stack Command:${NC}        ${YELLOW}./stop.sh${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}\n"
