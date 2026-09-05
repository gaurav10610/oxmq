-- cleanQueue script for OxMQ
-- KEYS:
-- 1: target set (bull:<q>:completed or bull:<q>:failed)
--
-- ARGV:
-- 1: queue prefix
-- 2: timestamp grace cutoff (ms)
-- 3: limit

local prefix = ARGV[1]
local graceTimestamp = tonumber(ARGV[2])
local limit = tonumber(ARGV[3]) or 1000

local jobs = redis.call("ZRANGEBYSCORE", KEYS[1], 0, graceTimestamp, "LIMIT", 0, limit)
local count = 0

if jobs and #jobs > 0 then
    for _, jobId in ipairs(jobs) do
        redis.call("ZREM", KEYS[1], jobId)
        redis.call("DEL", prefix .. ":" .. tostring(jobId))
        redis.call("DEL", prefix .. ":" .. tostring(jobId) .. ":logs")
        count = count + 1
    end
end

return count
