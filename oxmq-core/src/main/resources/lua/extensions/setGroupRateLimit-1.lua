--[[
  Applies a dynamic rate limit / throttling window to a virtual group.

  KEYS:
    1: groupsWaitingKey (bull:<queue>:groups:waiting)
    2: groupsLimitedKey (bull:<queue>:groups:limited)
    3: eventsKey        (bull:<queue>:events)
    4: groupLimitKey    (bull:<queue>:groups:<groupId>:limit) [optional]

  ARGV:
    1: groupId          (group identifier)
    2: expireTimestamp  (absolute millisecond timestamp when rate limit lifts)
    3: durationMs       (duration of rate limit in ms)
    4: maxJobs          (maximum jobs allowed in duration window, default 1)
]]

local rcall = redis.call

local groupsWaitingKey = KEYS[1]
local groupsLimitedKey = KEYS[2]
local eventsKey        = KEYS[3]
local groupLimitKey    = KEYS[4]

local groupId          = ARGV[1]
local expireTimestamp  = tonumber(ARGV[2]) or 0
local durationMs       = tonumber(ARGV[3]) or 0
local maxJobs          = tonumber(ARGV[4]) or 1

if not groupId or groupId == "" then
    return 0
end

rcall("ZADD", groupsLimitedKey, expireTimestamp, groupId)
rcall("LREM", groupsWaitingKey, 0, groupId)

if groupLimitKey and groupLimitKey ~= "" then
    rcall("SET", groupLimitKey, maxJobs, "PX", durationMs)
end

if eventsKey and eventsKey ~= "" then
    rcall("XADD", eventsKey, "*", "event", "ratelimit:group", "group", groupId, "duration", durationMs, "max", maxJobs)
end

return 1
