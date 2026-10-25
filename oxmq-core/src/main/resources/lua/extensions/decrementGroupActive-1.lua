--[[
  Decrements active count for a virtual group when a job finishes or fails.
  If the group was previously maxed out and has waiting jobs, moves it back to groups:waiting.

  KEYS:
    1: groupsWaitingKey      (bull:<queue>:groups:waiting)
    2: groupsActiveCountKey  (bull:<queue>:groups:active:count)
    3: groupsMaxedKey        (bull:<queue>:groups:maxed)
    4: groupsPausedKey       (bull:<queue>:groups:paused)

  ARGV:
    1: prefixKey             (bull:<queue>:)
    2: groupId               (group identifier)
    3: maxGroupConcurrency   (concurrency cap per group)
]]

local rcall = redis.call

local groupsWaitingKey     = KEYS[1]
local groupsActiveCountKey = KEYS[2]
local groupsMaxedKey       = KEYS[3]
local groupsPausedKey      = KEYS[4]

local prefixKey            = ARGV[1]
local groupId              = ARGV[2]
local maxGroupConcurrency  = tonumber(ARGV[3]) or 0

if not groupId or groupId == "" then
    return 0
end

local active = tonumber(rcall("HINCRBY", groupsActiveCountKey, groupId, -1))
if active <= 0 then
    rcall("HDEL", groupsActiveCountKey, groupId)
    active = 0
end

-- If group was maxed, check if it can now resume waiting
local isMaxed = rcall("SISMEMBER", groupsMaxedKey, groupId)
local isPaused = rcall("SISMEMBER", groupsPausedKey, groupId)

if isMaxed == 1 and isPaused == 0 and (maxGroupConcurrency == 0 or active < maxGroupConcurrency) then
    local groupWaitKey = prefixKey .. "groups:" .. groupId
    local groupPrioritizedKey = prefixKey .. "groups:" .. groupId .. ":prioritized"
    local remaining = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioritizedKey)

    if remaining > 0 then
        rcall("SREM", groupsMaxedKey, groupId)
        -- Add back to groupsWaiting if not already present
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
    else
        rcall("SREM", groupsMaxedKey, groupId)
    end
end

return active
