-- pauseQueue script for OxMQ
-- KEYS:
-- 1: bull:<q>:meta
-- 2: bull:<q>:events
--
-- ARGV:
-- 1: "pause" or "resume"

local action = ARGV[1]

if action == "pause" then
    redis.call("HSET", KEYS[1], "paused", "1")
    redis.call("PUBLISH", KEYS[2], '{"event":"paused"}')
    return 1
else
    redis.call("HDEL", KEYS[1], "paused")
    redis.call("PUBLISH", KEYS[2], '{"event":"resumed"}')
    return 0
end
