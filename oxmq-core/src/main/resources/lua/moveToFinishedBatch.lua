-- BullMQ-compatible moveToFinishedBatch script for OxMQ
-- KEYS:
-- 1: bull:<q>:active
-- 2: bull:<q>:completed
-- 3: bull:<q>:failed
-- 4: bull:<q>:events
--
-- ARGV:
-- 1: queue prefix
-- 2: status ("completed" or "failed")
-- 3: token
-- 4: timestamp (ms)
-- 5: result or failed reason
-- 6+: jobIds...

local prefix = ARGV[1]
local status = ARGV[2]
local token = ARGV[3]
local now = tonumber(ARGV[4])
local resultOrReason = ARGV[5]

local completedCount = 0

for i = 6, #ARGV do
    local jobId = ARGV[i]
    local jobKey = prefix .. ":" .. tostring(jobId)
    local lockKey = jobKey .. ":lock"

    -- Remove lock
    local currentLock = redis.call("GET", lockKey)
    if currentLock == token then
        redis.call("DEL", lockKey)
    end

    -- Remove from active list
    redis.call("LREM", KEYS[1], 0, tostring(jobId))

    if status == "completed" then
        redis.call("HMSET", jobKey,
            "finishedOn", now,
            "returnvalue", resultOrReason
        )
        redis.call("ZADD", KEYS[2], now, tostring(jobId))
        completedCount = completedCount + 1
    else
        redis.call("HMSET", jobKey,
            "finishedOn", now,
            "failedReason", resultOrReason
        )
        redis.call("ZADD", KEYS[3], now, tostring(jobId))
    end
end

redis.call("PUBLISH", KEYS[4], '{"event":"batchFinished","count":' .. completedCount .. '}')
return completedCount
