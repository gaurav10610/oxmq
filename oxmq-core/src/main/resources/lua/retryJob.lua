-- retryJob script for OxMQ
-- KEYS:
-- 1: bull:<q>:failed
-- 2: bull:<q>:wait
-- 3: bull:<q>:events
--
-- ARGV:
-- 1: queue prefix
-- 2: jobId

local prefix = ARGV[1]
local jobId = ARGV[2]

local removed = redis.call("ZREM", KEYS[1], tostring(jobId))
if removed == 1 then
    local jobKey = prefix .. ":" .. tostring(jobId)
    redis.call("HDEL", jobKey, "finishedOn", "failedReason")
    redis.call("LPUSH", KEYS[2], tostring(jobId))
    redis.call("PUBLISH", KEYS[3], '{"event":"retried","jobId":"' .. tostring(jobId) .. '"}')
    return 1
end

return 0
