-- BullMQ-compatible addJob script for OxMQ
-- KEYS:
-- 1: bull:<q>:wait
-- 2: bull:<q>:delayed
-- 3: bull:<q>:id
-- 4: bull:<q>:events
-- 5: bull:<q>:meta
--
-- ARGV:
-- 1: queue prefix (e.g. "bull:myqueue")
-- 2: custom jobId (or empty string)
-- 3: name
-- 4: data (JSON string)
-- 5: opts (JSON string)
-- 6: timestamp (ms)
-- 7: delay (ms)
-- 8: parentKey (or empty string)

local prefix = ARGV[1]
local customJobId = ARGV[2]
local name = ARGV[3]
local data = ARGV[4]
local opts = ARGV[5]
local timestamp = tonumber(ARGV[6])
local delay = tonumber(ARGV[7]) or 0
local parentKey = ARGV[8]

local jobId = customJobId
if jobId == nil or jobId == "" then
    jobId = redis.call("INCR", KEYS[3])
else
    local existing = redis.call("EXISTS", prefix .. ":" .. jobId)
    if existing == 1 then
        return tostring(jobId) -- Deduplication: already exists
    end
end

local jobKey = prefix .. ":" .. tostring(jobId)

-- Store job hash fields
redis.call("HMSET", jobKey,
    "name", name,
    "data", data,
    "opts", opts,
    "timestamp", timestamp,
    "delay", delay,
    "attemptsMade", 0
)

if parentKey ~= nil and parentKey ~= "" then
    redis.call("HSET", jobKey, "parentKey", parentKey)
end

if delay > 0 then
    local score = timestamp + delay
    redis.call("ZADD", KEYS[2], score, tostring(jobId))
else
    redis.call("LPUSH", KEYS[1], tostring(jobId))
    redis.call("PUBLISH", KEYS[4], '{"event":"waiting","jobId":"' .. tostring(jobId) .. '"}')
end

return tostring(jobId)
