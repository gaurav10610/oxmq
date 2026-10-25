--[[
  Deletes all pending jobs in a virtual group and cleans up group indices.

  KEYS:
    1: groupsWaitingKey     (bull:<queue>:groups:waiting)
    2: groupsPausedKey      (bull:<queue>:groups:paused)
    3: groupsMaxedKey       (bull:<queue>:groups:maxed)
    4: groupsLimitedKey     (bull:<queue>:groups:limited)
    5: groupsActiveCountKey (bull:<queue>:groups:active:count)
    6: eventsKey            (bull:<queue>:events)

  ARGV:
    1: prefixKey            (bull:<queue>:)
    2: groupId              (group identifier)
]]

local rcall = redis.call

local groupsWaitingKey     = KEYS[1]
local groupsPausedKey      = KEYS[2]
local groupsMaxedKey       = KEYS[3]
local groupsLimitedKey     = KEYS[4]
local groupsActiveCountKey = KEYS[5]
local eventsKey            = KEYS[6]

local prefixKey            = ARGV[1]
local groupId              = ARGV[2]

if not groupId or groupId == "" then
    return 0
end

local groupWaitKey = prefixKey .. "groups:" .. groupId
local groupPrioritizedKey = prefixKey .. "groups:" .. groupId .. ":prioritized"

-- 1. Remove all waiting jobs from group
local jobIds = rcall("LRANGE", groupWaitKey, 0, -1)
for _, jobId in ipairs(jobIds) do
    rcall("DEL", prefixKey .. jobId)
    rcall("DEL", prefixKey .. jobId .. ":lock")
end
rcall("DEL", groupWaitKey)

-- 2. Remove all prioritized jobs from group
local prioJobIds = rcall("ZRANGE", groupPrioritizedKey, 0, -1)
for _, jobId in ipairs(prioJobIds) do
    rcall("DEL", prefixKey .. jobId)
    rcall("DEL", prefixKey .. jobId .. ":lock")
end
rcall("DEL", groupPrioritizedKey)

-- 3. Cleanup group registries
rcall("LREM", groupsWaitingKey, 0, groupId)
rcall("SREM", groupsPausedKey, groupId)
rcall("SREM", groupsMaxedKey, groupId)
rcall("ZREM", groupsLimitedKey, groupId)
rcall("HDEL", groupsActiveCountKey, groupId)

if eventsKey and eventsKey ~= "" then
    rcall("XADD", eventsKey, "*", "event", "deleted:group", "group", groupId)
end

return #jobIds + #prioJobIds
