-- BullMQ-compatible moveToActive script for OxMQ
-- KEYS:
-- 1: bull:<q>:wait
-- 2: bull:<q>:active
-- 3: bull:<q>:delayed
-- 4: bull:<q>:stalled
-- 5: bull:<q>:meta
-- 6: bull:<q>:limiter
--
-- ARGV:
-- 1: queue prefix (e.g. "bull:myqueue")
-- 2: lock token / worker id
-- 3: lock duration (ms)
-- 4: current timestamp (ms)
-- 5: max concurrency / rate limit flag (optional)

local prefix = ARGV[1]
local token = ARGV[2]
local lockDuration = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Check if queue is paused
local isPaused = redis.call("HGET", KEYS[5], "paused")
if isPaused == "1" or isPaused == "true" then
    return nil
end

-- 1. Move matured delayed jobs to wait list
local delayedJobs = redis.call("ZRANGEBYSCORE", KEYS[3], 0, now, "LIMIT", 0, 100)
if delayedJobs and #delayedJobs > 0 then
    for _, delayedJobId in ipairs(delayedJobs) do
        redis.call("ZREM", KEYS[3], delayedJobId)
        redis.call("LPUSH", KEYS[1], delayedJobId)
    end
end

-- 2. Fetch next job from wait list
local jobId = redis.call("RPOP", KEYS[1])
if not jobId then
    return nil
end

local jobKey = prefix .. ":" .. tostring(jobId)
local lockKey = jobKey .. ":lock"

-- 3. Set worker lock with TTL
redis.call("SET", lockKey, token, "PX", lockDuration)

-- 4. Move to active list
redis.call("LPUSH", KEYS[2], tostring(jobId))

-- 5. Update job metadata
redis.call("HSET", jobKey, "processedOn", now)
redis.call("HINCRBY", jobKey, "attemptsMade", 1)

-- 6. Return jobId and job fields
local jobData = redis.call("HGETALL", jobKey)
return {tostring(jobId), jobData}
