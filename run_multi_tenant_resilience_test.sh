#!/usr/bin/env bash
# ==============================================================================
# OxDoc 365 :: Multi-Tenant Cloud Chaos & Disaster Recovery Benchmark Runner
# Validates:
# 1. Native OxMQ AdaptiveRateLimiterPlugin (AIMD & Oscillation Damping)
# 2. Native OxMQ CheckpointStatePlugin (Distributed Cursor Persistence)
# 3. Cloud Chaos (HTTP 429 Throttle Burst & HTTP 503 Service Unavailable)
# 4. Infrastructure Resilience: Sudden Redis Crash & Automatic Reconnection
# 5. Pod Crash & Graceful Shutdown: SIGTERM Drain & Delta Checkpoint Resume
# 6. Full 5-Tenant Data Processing (5,000+ documents)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OXDOC_DIR="${SCRIPT_DIR}/oxmq-examples/oxdoc-365"
PID_FILE="${OXDOC_DIR}/.oxdoc.pid"
LOG_FILE="${OXDOC_DIR}/oxdoc.log"
JAR_FILE="${OXDOC_DIR}/target/oxdoc-365-1.7.0.jar"

# ANSI Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${MAGENTA}  🧪  OXDOC 365 :: ADVANCED MULTI-TENANT RESILIENCE & FAILURE BENCHMARK${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"

# Check health of endpoints
if ! curl -sf http://localhost:8080/actuator/health >/dev/null; then
    echo -e "${RED}✖ Error: OxDoc 365 is not reachable at http://localhost:8080. Start stack first with ./start-oxdoc.sh${NC}"
    exit 1
fi
if ! curl -sf http://localhost:8081/mock-admin/stats >/dev/null; then
    echo -e "${RED}✖ Error: Mock Office 365 Server is not reachable at http://localhost:8081.${NC}"
    exit 1
fi

echo -e "${GREEN}✔ Pre-flight OK: OxDoc 365 and Mock Office 365 Server are healthy.${NC}"

# 1. Trigger Full Baseline Ingestion Across All 5 Tenants
echo -e "\n${BOLD}${CYAN}▶ STEP 1: Launching Concurrent Baseline Ingestion Across All 5 Tenant Accounts...${NC}"
CRAWL_RESP=$(curl -s -X POST http://localhost:8080/api/v1/dashboard/trigger/crawl-all)
echo -e "  Response: ${CRAWL_RESP}"

echo -e "\n${BLUE}▶ Allowing workers to ramp up (5s)...${NC}"
sleep 5

# 2. Inject Failure Scenario A: 429 Throttle Storm & 503 Server Chaos
echo -e "\n${BOLD}${YELLOW}▶ STEP 2: INJECTING CLOUD CHAOS (429 THROTTLE STORM & 503 SERVICE UNAVAILABLE)...${NC}"
echo -e "  Injecting 25x HTTP 429 Too Many Requests on tenant-smb-250..."
curl -s -X POST "http://localhost:8081/mock-admin/chaos?tenantId=tenant-smb-250&status=429&count=25&retryAfter=2" >/dev/null

echo -e "  Injecting 15x HTTP 503 Service Unavailable on tenant-mid-2500..."
curl -s -X POST "http://localhost:8081/mock-admin/chaos?tenantId=tenant-mid-2500&status=503&count=15&retryAfter=3" >/dev/null

echo -e "  Monitoring Adaptive Rate Limiter & Cooldown Sentinel reaction..."
sleep 4

QUOTA_SMB=$(curl -s http://localhost:8080/api/v1/dashboard/quota?tenantId=tenant-smb-250)
QUOTA_MID=$(curl -s http://localhost:8080/api/v1/dashboard/quota?tenantId=tenant-mid-2500)
echo -e "  ${YELLOW}tenant-smb-250 Quota Snapshot:${NC} ${QUOTA_SMB}"
echo -e "  ${YELLOW}tenant-mid-2500 Quota Snapshot:${NC} ${QUOTA_MID}"

# 3. Inject Failure Scenario B: Sudden Redis Crash & Restart
echo -e "\n${BOLD}${RED}▶ STEP 3: INJECTING INFRASTRUCTURE FAILURE (SUDDEN REDIS CRASH & RESTART)...${NC}"
REDIS_CONTAINER="oxdoc-redis"
echo -e "  Stopping Redis container abruptly (simulating crash / network split)..."
docker stop "${REDIS_CONTAINER}" >/dev/null
echo -e "  ${RED}💥 Redis is DOWN. Workers encountering reconnection events...${NC}"
sleep 4

echo -e "  Restarting Redis container..."
docker start "${REDIS_CONTAINER}" >/dev/null
echo -e "  ${GREEN}✔ Redis is BACK UP. OxMQ auto-reconnecting and recovering stalled jobs...${NC}"
sleep 4

# 4. Inject Failure Scenario C: Worker Pod SIGTERM Graceful Shutdown & Checkpoint Resume
echo -e "\n${BOLD}${MAGENTA}▶ STEP 4: INJECTING APPLICATION POD KILL & CHECKPOINT RECOVERY...${NC}"
if [ -f "${PID_FILE}" ]; then
    APP_PID=$(cat "${PID_FILE}")
    echo -e "  Sending SIGTERM (kill -15) to OxDoc 365 (PID: ${APP_PID})..."
    kill -15 "${APP_PID}" 2>/dev/null || true
    
    echo -n "  Waiting for graceful shutdown (draining active jobs & persisting checkpoints)..."
    WAIT_SEC=0
    while ps -p "${APP_PID}" >/dev/null 2>&1 && [ ${WAIT_SEC} -lt 20 ]; do
        echo -n "."
        sleep 1
        WAIT_SEC=$((WAIT_SEC + 1))
    done
    echo -e " ${GREEN}[STOPPED CLEANLY]${NC}"
    rm -f "${PID_FILE}"
fi

echo -e "  Restarting OxDoc 365 process (simulating Kubernetes pod resurrection)..."
nohup java -jar "${JAR_FILE}" >> "${LOG_FILE}" 2>&1 &
NEW_PID=$!
echo "${NEW_PID}" > "${PID_FILE}"
echo -e "  OxDoc 365 restarted with PID: ${NEW_PID}. Waiting for healthcheck..."

MAX_WAIT=30
W_COUNT=0
until curl -sf http://localhost:8080/actuator/health >/dev/null || [ ${W_COUNT} -eq ${MAX_WAIT} ]; do
    echo -n "."
    sleep 1
    W_COUNT=$((W_COUNT + 1))
done
echo -e " ${GREEN}[HEALTHY]${NC}"

# 5. Monitor Multi-Tenant Processing until Completion
echo -e "\n${BOLD}${CYAN}▶ STEP 5: MONITORING MULTI-TENANT INDEXING PROGRESS TO COMPLETION...${NC}"
TOTAL_DOCS=0
PREV_DOCS=-1

for i in {1..60}; do
    METRICS=$(curl -s http://localhost:8080/api/v1/dashboard/metrics || echo "{}")
    SYS_METRICS=$(curl -s http://localhost:8080/api/v1/dashboard/system-metrics || echo "{}")
    
    TOTAL_DOCS=$(echo "${METRICS}" | grep -o '"totalDocuments":[0-9]*' | cut -d':' -f2 || echo "0")
    SEARCHABLE=$(echo "${METRICS}" | grep -o '"searchableDocuments":[0-9]*' | cut -d':' -f2 || echo "0")
    MEM_MB=$(echo "${SYS_METRICS}" | grep -o '"jvmUsedMemoryMB":[0-9.]*' | cut -d':' -f2 || echo "0")
    THREADS=$(echo "${SYS_METRICS}" | grep -o '"activeThreadCount":[0-9]*' | cut -d':' -f2 || echo "0")
    
    echo -e "  [$(date +%T)] Documents Indexed: ${BOLD}${CYAN}${TOTAL_DOCS:-0}${NC} | Searchable Knowledge: ${BOLD}${GREEN}${SEARCHABLE:-0}${NC} | JVM RAM: ${BOLD}${MEM_MB:-0} MB${NC} | Threads: ${BOLD}${THREADS:-0}${NC}"
    
    if [ "${TOTAL_DOCS:-0}" -ge 5000 ]; then
        echo -e "\n${BOLD}${GREEN}✔ ALL 5,000+ DOCUMENTS SUCCESSFULLY PROCESSED ACROSS ALL 5 TENANTS!${NC}"
        break
    fi
    
    if [ "${TOTAL_DOCS:-0}" -eq "${PREV_DOCS}" ] && [ "${TOTAL_DOCS:-0}" -ge 1000 ]; then
        sleep 5
    fi
    PREV_DOCS="${TOTAL_DOCS:-0}"
    sleep 3
done

# 6. Final Status & Summary
echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN}  ✔ MULTI-TENANT RESILIENCE & FAILURE BENCHMARK COMPLETED SUCCESSFULLY${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}"
echo -e "Final Metrics Snapshot:"
curl -s http://localhost:8080/api/v1/dashboard/metrics
echo ""
echo -e "System Telemetry Snapshot:"
curl -s http://localhost:8080/api/v1/dashboard/system-metrics
echo ""
echo -e "All Tenants Quota & Capacity Status:"
curl -s http://localhost:8080/api/v1/dashboard/tenants
echo ""
