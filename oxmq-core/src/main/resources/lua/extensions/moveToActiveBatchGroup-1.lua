--[[
  Pops a batch of jobs with strict Group Affinity (homogeneous tenant batch).
  All jobs in the returned batch belong to the exact same virtual group.

  KEYS:
    1: groupsWaitingKey      (bull:<queue>:groups:waiting)
    2: groupsActiveCountKey  (bull:<queue>:groups:active:count)
    3: groupsPausedKey       (bull:<queue>:groups:paused)
    4: groupsLimitedKey      (bull:<queue>:groups:limited)
    5: groupsMaxedKey        (bull:<queue>:groups:maxed)
    6: activeKey             (bull:<queue>:active)
    7: eventsKey             (bull:<queue>:events)

  ARGV:
    1: prefixKey             (bull:<queue>:)
    2: workerId              (worker lock token)
    3: lockDurationMs        (lock TTL)
    4: maxBatchSize          (max jobs to include in the batch)
    5: maxGroupConcurrency   (0 = unlimited, > 0 = cap per group)
    6: nowTimestamp          (current time in ms)
]]

local rcall = redis.call

local groupsWaitingKey     = KEYS[1]
local groupsActiveCountKey = KEYS[2]
local groupsPausedKey      = KEYS[3]
local groupsLimitedKey     = KEYS[4]
local groupsMaxedKey       = KEYS[5]
local activeKey            = KEYS[6]
local eventsKey            = KEYS[7]

local prefixKey            = ARGV[1]
local workerId             = ARGV[2]
local lockDurationMs       = tonumber(ARGV[3]) or 30000
local maxBatchSize         = tonumber(ARGV[4]) or 10
local maxGroupConcurrency  = tonumber(ARGV[5]) or 0
local nowTimestamp         = tonumber(ARGV[6]) or 0

-- Promote expired limited groups back to waiting
local expiredGroups = rcall("ZRANGEBYSCORE", groupsLimitedKey, 0, nowTimestamp)
if expiredGroups and #expiredGroups > 0 then
    for _, gid in ipairs(expiredGroups) do
        rcall("ZREM", groupsLimitedKey, gid)
        local remaining = rcall("LLEN", prefixKey .. "groups:" .. gid) + rcall("ZCARD", prefixKey .. "groups:" .. gid .. ":prioritized")
        if remaining > 0 and rcall("SISMEMBER", groupsPausedKey, gid) == 0 and rcall("SISMEMBER", groupsMaxedKey, gid) == 0 then
            rcall("LREM", groupsWaitingKey, 0, gid)
            rcall("LPUSH", groupsWaitingKey, gid)
        end
    end
end

local numWaitingGroups = rcall("LLEN", groupsWaitingKey)
if numWaitingGroups == 0 then
    return nil
end

local iterations = math.min(numWaitingGroups, 100)

for i = 1, iterations do
    local groupId = rcall("RPOPLPUSH", groupsWaitingKey, groupsWaitingKey)
    if not groupId then
        break
    end

    if rcall("SISMEMBER", groupsPausedKey, groupId) == 1 then
        rcall("LREM", groupsWaitingKey, 1, groupId)
    else
        local expireTs = rcall("ZSCORE", groupsLimitedKey, groupId)
        local isLimited = false
        if expireTs then
            if tonumber(expireTs) > nowTimestamp then
                isLimited = true
                rcall("LREM", groupsWaitingKey, 1, groupId)
            else
                rcall("ZREM", groupsLimitedKey, groupId)
            end
        end

        if not isLimited then
            local groupConcurrencyKey = prefixKey .. "groups:concurrency"
            local groupConcurrencyOverride = tonumber(rcall("HGET", groupConcurrencyKey, groupId))
            local effectiveConcurrency = groupConcurrencyOverride or maxGroupConcurrency

            local activeCount = tonumber(rcall("HGET", groupsActiveCountKey, groupId) or 0)
            if effectiveConcurrency > 0 and activeCount >= effectiveConcurrency then
                rcall("SADD", groupsMaxedKey, groupId)
                rcall("LREM", groupsWaitingKey, 1, groupId)
            else
                local groupPrioritizedKey = prefixKey .. "groups:" .. groupId .. ":prioritized"
                local groupWaitKey = prefixKey .. "groups:" .. groupId

                local jobs = {}
                local allowedSlots = maxBatchSize
                if effectiveConcurrency > 0 then
                    allowedSlots = math.min(maxBatchSize, effectiveConcurrency - activeCount)
                end

                while #jobs < allowedSlots do
                    local pJobs = rcall("ZPOPMIN", groupPrioritizedKey, 1)
                    local jobId = nil
                    if pJobs and #pJobs > 0 then
                        jobId = pJobs[1]
                    else
                        jobId = rcall("RPOP", groupWaitKey)
                    end

                    if not jobId then
                        break
                    end

                    local lockKey = prefixKey .. jobId .. ":lock"
                    rcall("SET", lockKey, workerId, "PX", lockDurationMs)
                    rcall("LPUSH", activeKey, jobId)

                    local jobKey = prefixKey .. jobId
                    rcall("HINCRBY", jobKey, "attemptsStarted", 1)
                    rcall("HSET", jobKey, "processedOn", nowTimestamp)

                    table.insert(jobs, { rcall("HGETALL", jobKey), jobId })
                end

                if #jobs > 0 then
                    rcall("HINCRBY", groupsActiveCountKey, groupId, #jobs)

                    local remaining = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioritizedKey)
                    if remaining == 0 then
                        rcall("LREM", groupsWaitingKey, 1, groupId)
                    elseif effectiveConcurrency > 0 and (activeCount + #jobs) >= effectiveConcurrency then
                        rcall("SADD", groupsMaxedKey, groupId)
                        rcall("LREM", groupsWaitingKey, 1, groupId)
                    end

                    return jobs
                else
                    rcall("LREM", groupsWaitingKey, 1, groupId)
                end
            end
        end
    end
end

return nil
