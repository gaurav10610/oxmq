-- BullMQ-compatible moveToActiveBatch script for OxMQ
-- KEYS:
-- 1: bull:<q>:wait
-- 2: bull:<q>:active
-- 3: bull:<q>:delayed
-- 4: bull:<q>:stalled
-- 5: bull:<q>:meta
--
-- ARGV:
-- 1: queue prefix
-- 2: worker lock token
-- 3: lock duration (ms)
-- 4: current timestamp (ms)
-- 5: batch size (e.g. 50, 100, 500)

local prefix = ARGV[1]
local token = ARGV[2]
local lockDuration = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local batchSize = tonumber(ARGV[5]) or 100

-- Check if queue is paused
local isPaused = redis.call("HGET", KEYS[5], "paused")
if isPaused == "1" or isPaused == "true" then
    return {}
end

-- 1. Move matured delayed jobs to wait list
local delayedJobs = redis.call("ZRANGEBYSCORE", KEYS[3], 0, now, "LIMIT", 0, batchSize)
if delayedJobs and #delayedJobs > 0 then
    for _, delayedJobId in ipairs(delayedJobs) do
        redis.call("ZREM", KEYS[3], delayedJobId)
        redis.call("LPUSH", KEYS[1], delayedJobId)
    end
end

-- 2. Fetch up to batchSize jobs from wait list
local results = {}
for i = 1, batchSize do
    local jobId = redis.call("RPOP", KEYS[1])
    if not jobId then
        break
    end

    local jobKey = prefix .. ":" .. tostring(jobId)
    local lockKey = jobKey .. ":lock"

    -- Set lock
    redis.call("SET", lockKey, token, "PX", lockDuration)

    -- Move to active list
    redis.call("LPUSH", KEYS[2], tostring(jobId))

    -- Update job metadata
    redis.call("HSET", jobKey, "processedOn", now)
    redis.call("HINCRBY", jobKey, "attemptsMade", 1)

    local jobData = redis.call("HGETALL", jobKey)
    table.insert(results, {tostring(jobId), jobData})
end

return results
