--[[
  Resumes processing for a paused virtual group.

  KEYS:
    1: groupsWaitingKey (bull:<queue>:groups:waiting)
    2: groupsPausedKey  (bull:<queue>:groups:paused)
    3: groupsMaxedKey   (bull:<queue>:groups:maxed)
    4: eventsKey        (bull:<queue>:events)

  ARGV:
    1: prefixKey        (bull:<queue>:)
    2: groupId          (group identifier)
]]

local rcall = redis.call

local groupsWaitingKey = KEYS[1]
local groupsPausedKey  = KEYS[2]
local groupsMaxedKey   = KEYS[3]
local eventsKey        = KEYS[4]

local prefixKey        = ARGV[1]
local groupId          = ARGV[2]

if not groupId or groupId == "" then
    return 0
end

local removed = rcall("SREM", groupsPausedKey, groupId)
if removed == 1 then
    local isMaxed = rcall("SISMEMBER", groupsMaxedKey, groupId)
    if isMaxed == 0 then
        local groupWaitKey = prefixKey .. "groups:" .. groupId
        local groupPrioritizedKey = prefixKey .. "groups:" .. groupId .. ":prioritized"
        local remaining = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioritizedKey)

        if remaining > 0 then
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
    end

    if eventsKey and eventsKey ~= "" then
        rcall("XADD", eventsKey, "*", "event", "resumed:group", "group", groupId)
    end
    return 1
end

return 0
