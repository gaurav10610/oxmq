-- extendLock script for OxMQ
-- KEYS:
-- 1: bull:<q>:<jobId>:lock
--
-- ARGV:
-- 1: lock token
-- 2: duration (ms)

local token = ARGV[1]
local duration = tonumber(ARGV[2])

local currentLock = redis.call("GET", KEYS[1])
if currentLock == token then
    redis.call("PEXPIRE", KEYS[1], duration)
    return 1
end

return 0
