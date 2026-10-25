--[[
  Pauses processing for a virtual group.
  Running jobs are allowed to complete, but workers will not pick up new jobs from this group.

  KEYS:
    1: groupsWaitingKey (bull:<queue>:groups:waiting)
    2: groupsPausedKey  (bull:<queue>:groups:paused)
    3: eventsKey        (bull:<queue>:events)

  ARGV:
    1: groupId          (group identifier)
]]

local rcall = redis.call

local groupsWaitingKey = KEYS[1]
local groupsPausedKey  = KEYS[2]
local eventsKey        = KEYS[3]
local groupId          = ARGV[1]

if not groupId or groupId == "" then
    return 0
end

local added = rcall("SADD", groupsPausedKey, groupId)
if added == 1 then
    rcall("LREM", groupsWaitingKey, 0, groupId)
    if eventsKey and eventsKey ~= "" then
        rcall("XADD", eventsKey, "*", "event", "paused:group", "group", groupId)
    end
    return 1
end

return 0
