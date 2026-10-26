--[[
  Self-healing group repair script.
  Audits virtual groups in groups:maxed, checks actual active jobs currently executing
  in the active list, reconciles groups:active:count, and if the group is no longer
  at or above its concurrency cap, removes it from groups:maxed and restores it to
  groups:waiting so workers can resume processing it.

  KEYS:
    1: groupsWaitingKey     (bull:<queue>:groups:waiting)
    2: groupsMaxedKey       (bull:<queue>:groups:maxed)
    3: groupsActiveCountKey (bull:<queue>:groups:active:count)
    4: groupsPausedKey      (bull:<queue>:groups:paused)
    5: groupsLimitedKey     (bull:<queue>:groups:limited)
    6: activeKey            (bull:<queue>:active)

  ARGV:
    1: prefixKey            (bull:<queue>:)
    2: targetGroupId        (specific group ID to repair, or "" for all maxed groups)
    3: maxGroupConcurrency  (default concurrency cap per group)
]]

local rcall = redis.call

local groupsWaitingKey     = KEYS[1]
local groupsMaxedKey       = KEYS[2]
local groupsActiveCountKey = KEYS[3]
local groupsPausedKey      = KEYS[4]
local groupsLimitedKey     = KEYS[5]
local activeKey            = KEYS[6]

local prefixKey            = ARGV[1]
local targetGroupId        = ARGV[2]
local maxGroupConcurrency  = tonumber(ARGV[3]) or 1

-- 1. Determine which groups to evaluate
local groupsToRepair = {}
if targetGroupId and targetGroupId ~= "" then
    table.insert(groupsToRepair, targetGroupId)
else
    local allMaxed = rcall("SMEMBERS", groupsMaxedKey)
    if allMaxed then
        for _, g in ipairs(allMaxed) do
            table.insert(groupsToRepair, g)
        end
    end
end

if #groupsToRepair == 0 then
    return 0
end

-- 2. Audit real active jobs across the active list
local activeJobIds = rcall("LRANGE", activeKey, 0, -1)
local realActiveCounts = {}

if activeJobIds and #activeJobIds > 0 then
    for _, jid in ipairs(activeJobIds) do
        local gid = rcall("HGET", prefixKey .. jid, "groupKey")
        if gid and gid ~= "" then
            realActiveCounts[gid] = (realActiveCounts[gid] or 0) + 1
        end
    end
end

local repairedCount = 0
local groupConcurrencyKey = prefixKey .. "groups:concurrency"

-- 3. Reconcile each group
for _, gid in ipairs(groupsToRepair) do
    local actualCount = realActiveCounts[gid] or 0
    if actualCount == 0 then
        rcall("HDEL", groupsActiveCountKey, gid)
    else
        rcall("HSET", groupsActiveCountKey, gid, actualCount)
    end

    local groupConcurrencyOverride = tonumber(rcall("HGET", groupConcurrencyKey, gid))
    local effectiveConcurrency = groupConcurrencyOverride or maxGroupConcurrency

    -- If actual active is below concurrency limit, group is no longer maxed!
    if effectiveConcurrency == 0 or actualCount < effectiveConcurrency then
        rcall("SREM", groupsMaxedKey, gid)
        repairedCount = repairedCount + 1

        local isPaused = rcall("SISMEMBER", groupsPausedKey, gid)
        local isLimited = rcall("ZSCORE", groupsLimitedKey, gid)

        if isPaused == 0 and not isLimited then
            local groupWaitKey = prefixKey .. "groups:" .. gid
            local groupPrioritizedKey = prefixKey .. "groups:" .. gid .. ":prioritized"
            local remaining = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioritizedKey)

            if remaining > 0 then
                -- Add back to groupsWaiting if not already present
                local inList = false
                local waitingGroups = rcall("LRANGE", groupsWaitingKey, 0, -1)
                for _, w in ipairs(waitingGroups) do
                    if w == gid then
                        inList = true
                        break
                    end
                end
                if not inList then
                    rcall("LPUSH", groupsWaitingKey, gid)
                end
            end
        end
    end
end

return repairedCount
