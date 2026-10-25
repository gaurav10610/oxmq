--[[
  Adds a job belonging to a virtual group.
  Enforces optional group backlog bounds (maxSize) and indexes the group for fair round-robin scheduling.

  KEYS:
    1: groupsWaitingKey     (bull:<queue>:groups:waiting)
    2: groupWaitKey         (bull:<queue>:groups:<groupId>)
    3: groupPrioritizedKey  (bull:<queue>:groups:<groupId>:prioritized)
    4: groupsPausedKey      (bull:<queue>:groups:paused)
    5: groupsMaxedKey       (bull:<queue>:groups:maxed)
    6: groupsLimitedKey     (bull:<queue>:groups:limited)
    7: idKey                (bull:<queue>:id)
    8: eventsKey            (bull:<queue>:events)

  ARGV:
    1: prefixKey            (e.g. "bull:<queue>:")
    2: customJobId          (string or "")
    3: name                 (job name)
    4: timestamp            (enqueue timestamp)
    5: dataJson             (payload JSON string)
    6: optsJson             (options JSON string)
    7: groupId              (virtual group identifier)
    8: maxSize              (max group backlog limit, 0 = unlimited)
    9: priority             (intra-group priority, 0 = FIFO)
    10: deduplicationId     (optional deduplication id or "")
]]

local rcall = redis.call

local groupsWaitingKey    = KEYS[1]
local groupWaitKey        = KEYS[2]
local groupPrioritizedKey = KEYS[3]
local groupsPausedKey     = KEYS[4]
local groupsMaxedKey      = KEYS[5]
local groupsLimitedKey    = KEYS[6]
local idKey               = KEYS[7]
local eventsKey           = KEYS[8]

local prefixKey           = ARGV[1]
local customJobId         = ARGV[2]
local jobName             = ARGV[3]
local timestamp           = ARGV[4]
local dataJson            = ARGV[5]
local optsJson            = ARGV[6]
local groupId             = ARGV[7]
local maxSize             = tonumber(ARGV[8]) or 0
local priority            = tonumber(ARGV[9]) or 0
local deduplicationId     = ARGV[10]

-- 1. Deduplication check if specified
if deduplicationId and deduplicationId ~= "" then
    local dedupKey = prefixKey .. "de:" .. deduplicationId
    local existingJobId = rcall("GET", dedupKey)
    if existingJobId then
        return existingJobId
    end
end

-- 2. Max size check for virtual group backlog
if maxSize > 0 then
    local currentCount = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioritizedKey)
    if currentCount >= maxSize then
        return redis.error_reply("GROUP_MAX_SIZE_EXCEEDED")
    end
end

-- 3. Determine job ID
local jobId
if customJobId and customJobId ~= "" then
    jobId = customJobId
else
    jobId = tostring(rcall("INCR", idKey))
end

local jobKey = prefixKey .. jobId

-- 4. Store deduplication key if needed
if deduplicationId and deduplicationId ~= "" then
    local dedupKey = prefixKey .. "de:" .. deduplicationId
    rcall("SET", dedupKey, jobId)
end

-- 5. Create job hash in Redis
rcall("HSET", jobKey,
    "name", jobName,
    "data", dataJson,
    "opts", optsJson,
    "timestamp", timestamp,
    "groupKey", groupId,
    "attemptsMade", 0,
    "attemptsStarted", 0
)

-- 6. Add job to group sub-queue
if priority > 0 then
    rcall("ZADD", groupPrioritizedKey, priority, jobId)
else
    rcall("LPUSH", groupWaitKey, jobId)
end

-- 7. Register group in groupsWaiting if not paused, maxed, or limited
local isPaused = rcall("SISMEMBER", groupsPausedKey, groupId)
local isMaxed = rcall("SISMEMBER", groupsMaxedKey, groupId)
local isLimited = rcall("ZSCORE", groupsLimitedKey, groupId)

if isPaused == 0 and isMaxed == 0 and not isLimited then
    -- Check if groupId already in groupsWaiting list
    local inList = false
    local waitingGroups = rcall("LRANGE", groupsWaitingKey, 0, -1)
    for _, g in ipairs(waitingGroups) do
        if g == groupId then
            inList = true
            break
        end
    end
    if not inList then
        rcall("LPUSH", groupsWaitingKey, groupId)
    end
end

-- 8. Emit waiting event
if eventsKey and eventsKey ~= "" then
    rcall("XADD", eventsKey, "*", "event", "waiting", "jobId", jobId, "group", groupId)
end

return jobId
