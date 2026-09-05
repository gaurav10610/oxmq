-- obliterate queue script for OxMQ
-- KEYS:
-- 1: bull:<q>:wait
-- 2: bull:<q>:active
-- 3: bull:<q>:delayed
-- 4: bull:<q>:completed
-- 5: bull:<q>:failed
-- 6: bull:<q>:stalled
-- 7: bull:<q>:meta
-- 8: bull:<q>:limiter
--
-- ARGV:
-- 1: queue prefix

local prefix = ARGV[1]

-- Helper to purge a list or set of job ids
local function purgeJobs(key, isZSet)
    local jobIds
    if isZSet then
        jobIds = redis.call("ZRANGE", key, 0, -1)
    else
        jobIds = redis.call("LRANGE", key, 0, -1)
    end
    if jobIds and #jobIds > 0 then
        for _, jobId in ipairs(jobIds) do
            redis.call("DEL", prefix .. ":" .. tostring(jobId))
            redis.call("DEL", prefix .. ":" .. tostring(jobId) .. ":lock")
            redis.call("DEL", prefix .. ":" .. tostring(jobId) .. ":logs")
            redis.call("DEL", prefix .. ":" .. tostring(jobId) .. ":childrenValues")
        end
    end
    redis.call("DEL", key)
end

purgeJobs(KEYS[1], false)
purgeJobs(KEYS[2], false)
purgeJobs(KEYS[3], true)
purgeJobs(KEYS[4], true)
purgeJobs(KEYS[5], true)
purgeJobs(KEYS[6], true)
redis.call("DEL", KEYS[7])
redis.call("DEL", KEYS[8])

return 1
