-- BullMQ-compatible moveToFinished script for OxMQ
-- KEYS:
-- 1: bull:<q>:active
-- 2: bull:<q>:completed
-- 3: bull:<q>:failed
-- 4: bull:<q>:delayed
-- 5: bull:<q>:events
--
-- ARGV:
-- 1: queue prefix
-- 2: jobId
-- 3: returnvalue or failedReason
-- 4: status ("completed" or "failed")
-- 5: lock token
-- 6: timestamp (ms)
-- 7: maxAttempts
-- 8: retryDelay (ms)
-- 9: removeOnComplete (1 or 0)
-- 10: removeOnFail (1 or 0)

local prefix = ARGV[1]
local jobId = ARGV[2]
local resultOrReason = ARGV[3]
local status = ARGV[4]
local token = ARGV[5]
local now = tonumber(ARGV[6])
local maxAttempts = tonumber(ARGV[7]) or 1
local retryDelay = tonumber(ARGV[8]) or 0
local removeOnComplete = tonumber(ARGV[9]) or 0
local removeOnFail = tonumber(ARGV[10]) or 0

local jobKey = prefix .. ":" .. tostring(jobId)
local lockKey = jobKey .. ":lock"

-- Verify and remove lock
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
    if removeOnComplete == 1 then
        redis.call("DEL", jobKey)
    else
        redis.call("ZADD", KEYS[2], now, tostring(jobId))
    end

    -- Check if parent exists (DAG flow resolution)
    local parentKey = redis.call("HGET", jobKey, "parentKey")
    if parentKey and parentKey ~= "" then
        redis.call("HSET", parentKey .. ":childrenValues", tostring(jobId), resultOrReason)
        local remaining = redis.call("HINCRBY", parentKey, "unresolvedChildrenCount", -1)
        if remaining <= 0 then
            -- Find parent queue prefix and move parent to wait
            local pWaitKey = redis.call("HGET", parentKey, "queueWaitKey")
            local pJobId = redis.call("HGET", parentKey, "id")
            if pWaitKey and pJobId then
                redis.call("LPUSH", pWaitKey, pJobId)
            end
        end
    end

    redis.call("PUBLISH", KEYS[5], '{"event":"completed","jobId":"' .. tostring(jobId) .. '"}')
    return 1
else
    -- Failed status: check if we should retry
    local attemptsMade = tonumber(redis.call("HGET", jobKey, "attemptsMade") or "1")
    if attemptsMade < maxAttempts and retryDelay >= 0 then
        local retryScore = now + retryDelay
        redis.call("HMSET", jobKey,
            "failedReason", resultOrReason
        )
        redis.call("ZADD", KEYS[4], retryScore, tostring(jobId))
        redis.call("PUBLISH", KEYS[5], '{"event":"retried","jobId":"' .. tostring(jobId) .. '"}')
        return 0 -- retried
    else
        -- Permanent failure
        redis.call("HMSET", jobKey,
            "finishedOn", now,
            "failedReason", resultOrReason
        )
        if removeOnFail == 1 then
            redis.call("DEL", jobKey)
        else
            redis.call("ZADD", KEYS[3], now, tostring(jobId))
        end
        redis.call("PUBLISH", KEYS[5], '{"event":"failed","jobId":"' .. tostring(jobId) .. '"}')
        return -1 -- permanent failure
    end
end
