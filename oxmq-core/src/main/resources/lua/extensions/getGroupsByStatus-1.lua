--[[
  Retrieves virtual groups for a specified status ('waiting', 'limited', 'maxed', 'paused')
  along with their pending job counts.

  KEYS:
    1: targetKey   (bull:<queue>:groups:<status>)

  ARGV:
    1: prefixKey   (bull:<queue>:)
    2: status      ('waiting', 'limited', 'maxed', 'paused')
    3: start       (pagination start offset, 0-based)
    4: stop        (pagination end offset, -1 for all)
]]

local rcall = redis.call

local targetKey = KEYS[1]
local prefixKey = ARGV[1]
local status    = ARGV[2]
local start     = tonumber(ARGV[3]) or 0
local stop      = tonumber(ARGV[4]) or -1

local groupIds = {}

if status == "waiting" then
    groupIds = rcall("LRANGE", targetKey, start, stop)
elseif status == "limited" then
    groupIds = rcall("ZRANGE", targetKey, start, stop)
elseif status == "maxed" or status == "paused" then
    local all = rcall("SMEMBERS", targetKey)
    local len = #all
    if stop == -1 or stop >= len then
        stop = len - 1
    end
    for i = start + 1, stop + 1 do
        if all[i] then
            table.insert(groupIds, all[i])
        end
    end
end

local result = {}
for _, gid in ipairs(groupIds) do
    local groupWaitKey = prefixKey .. "groups:" .. gid
    local groupPrioKey = prefixKey .. "groups:" .. gid .. ":prioritized"
    local jobCount = rcall("LLEN", groupWaitKey) + rcall("ZCARD", groupPrioKey)
    table.insert(result, { gid, tostring(jobCount) })
end

return result
