-- Sliding window rate limiter for OxMQ
-- KEYS:
-- 1: bull:<q>:limiter
--
-- ARGV:
-- 1: max jobs allowed
-- 2: duration window in ms
-- 3: current timestamp in ms

local max = tonumber(ARGV[1])
local duration = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local clearBefore = now - duration

-- Remove entries older than sliding window
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, clearBefore)

-- Check current count in window
local currentCount = redis.call("ZCARD", KEYS[1])

if currentCount < max then
    -- Record this execution
    redis.call("ZADD", KEYS[1], now, now .. ":" .. math.random(100000, 999999))
    redis.call("PEXPIRE", KEYS[1], duration * 2)
    return 1 -- Allowed
else
    return 0 -- Rate limited
end
