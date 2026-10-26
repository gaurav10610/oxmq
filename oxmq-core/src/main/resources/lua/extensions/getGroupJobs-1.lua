--[[
  Retrieves a paginated list of job IDs belonging to a virtual group.

  KEYS:
    1: groupWaitKey (bull:<queue>:groups:<groupId>)
    2: groupPrioKey (bull:<queue>:groups:<groupId>:prioritized)

  ARGV:
    1: prefixKey    (bull:<queue>:)
    2: start        (start index, 0-based)
    3: stop         (stop index, -1 for all)
]]

local rcall = redis.call

local groupWaitKey = KEYS[1]
local groupPrioKey = KEYS[2]

local start = tonumber(ARGV[2]) or 0
local stop  = tonumber(ARGV[3]) or -1

-- Fetch prioritized jobs first, then standard FIFO jobs
local prioJobs = rcall("ZRANGE", groupPrioKey, 0, -1)
local standardJobs = rcall("LRANGE", groupWaitKey, 0, -1)

local allJobs = {}
for _, id in ipairs(prioJobs) do
    table.insert(allJobs, id)
end
for _, id in ipairs(standardJobs) do
    table.insert(allJobs, id)
end

local total = #allJobs
if stop == -1 or stop >= total then
    stop = total - 1
end

local result = {}
for i = start + 1, stop + 1 do
    if allJobs[i] then
        table.insert(result, allJobs[i])
    end
end

return result
