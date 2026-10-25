--[[
  Pops the next available job across virtual groups using fair round-robin scheduling.
  Enforces per-group concurrency caps, group rate limits, and pause states.

  KEYS:
    1: groupsWaitingKey      (bull:<queue>:groups:waiting)
    2: groupsActiveCountKey  (bull:<queue>:groups:active:count)
    3: groupsPausedKey       (bull:<queue>:groups:paused)
    4: groupsLimitedKey      (bull:<queue>:groups:limited)
    5: groupsMaxedKey        (bull:<queue>:groups:maxed)
    6: activeKey             (bull:<queue>:active)
    7: eventsKey             (bull:<queue>:events)

  ARGV:
    1: prefixKey             (e.g. "bull:<queue>:")
    2: workerId              (worker lock owner)
    3: lockDurationMs        (lock TTL in ms)
    4: maxGroupConcurrency   (0 = unlimited, > 0 = cap per group)
    5: nowTimestamp          (current timestamp in ms)
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
local maxGroupConcurrency  = tonumber(ARGV[4]) or 0
local nowTimestamp         = tonumber(ARGV[5]) or 0

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
    local nextLimited = rcall("ZRANGE", groupsLimitedKey, 0, 0, "WITHSCORES")
    if nextLimited and #nextLimited >= 2 then
        local nextTs = tonumber(nextLimited[2])
        if nextTs and nextTs > nowTimestamp then
            return {0, 0, nextTs - nowTimestamp}
        end
    end
    return nil
end

local iterations = math.min(numWaitingGroups, 100)

for i = 1, iterations do
    -- Fair circular rotation: Pop right and push left
    local groupId = rcall("RPOPLPUSH", groupsWaitingKey, groupsWaitingKey)
    if not groupId then
        break
    end

    -- 1. Check if group is paused
    if rcall("SISMEMBER", groupsPausedKey, groupId) == 1 then
        rcall("LREM", groupsWaitingKey, 1, groupId)
    else
        -- 2. Check if group is rate-limited
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
            -- 3. Check group concurrency cap (with per-group dynamic override support)
            local groupConcurrencyKey = prefixKey .. "groups:concurrency"
            local groupConcurrencyOverride = tonumber(rcall("HGET", groupConcurrencyKey, groupId))
            local effectiveConcurrency = groupConcurrencyOverride or maxGroupConcurrency

            local activeCount = tonumber(rcall("HGET", groupsActiveCountKey, groupId) or 0)
            if effectiveConcurrency > 0 and activeCount >= effectiveConcurrency then
                rcall("SADD", groupsMaxedKey, groupId)
                rcall("LREM", groupsWaitingKey, 1, groupId)
            else
                -- 4. Group is eligible! Try to fetch next job (prioritized first, then FIFO)
                local groupPrioritizedKey = prefixKey .. "groups:" .. groupId .. ":prioritized"
                local groupWaitKey = prefixKey .. "groups:" .. groupId

                local jobId = nil
                local pJobs = rcall("ZPOPMIN", groupPrioritizedKey, 1)
                if pJobs and #pJobs > 0 then
                    jobId = pJobs[1]
                else
                    jobId = rcall("RPOP", groupWaitKey)
                end

                if not jobId then
                    -- Group has no more jobs waiting, remove from waiting list
                    rcall("LREM", groupsWaitingKey, 1, groupId)
                else
                    -- 5. Mark job as active & set lock
                    rcall("HINCRBY", groupsActiveCountKey, groupId, 1)

                    local lockKey = prefixKey .. jobId .. ":lock"
                    rcall("SET", lockKey, workerId, "PX", lockDurationMs)
                    rcall("LPUSH", activeKey, jobId)

                    local jobKey = prefixKey .. jobId
                    rcall("HINCRBY", jobKey, "attemptsStarted", 1)
                    rcall("HSET", jobKey, "processedOn", nowTimestamp)

                    -- 6. Check if group is now drained or maxed
                    local remainingJobs = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioritizedKey)
                    if remainingJobs == 0 then
                        rcall("LREM", groupsWaitingKey, 1, groupId)
                    elseif effectiveConcurrency > 0 and (activeCount + 1) >= effectiveConcurrency then
                        rcall("SADD", groupsMaxedKey, groupId)
                        rcall("LREM", groupsWaitingKey, 1, groupId)
                    end

                    -- 7. Emit active event
                    if eventsKey and eventsKey ~= "" then
                        rcall("XADD", eventsKey, "*", "event", "active", "jobId", jobId, "group", groupId)
                    end

                    return { rcall("HGETALL", jobKey), jobId }
                end
            end
        end
    end
end

return nil
