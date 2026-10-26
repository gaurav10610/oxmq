--[[
  Atomically pops a batch of standard jobs (prioritized first, then FIFO wait list)
  and marks them active with worker lock tokens in a single round-trip.

  KEYS:
    1: waitKey            (bull:<queue>:wait)
    2: activeKey          (bull:<queue>:active)
    3: prioritizedKey     (bull:<queue>:prioritized)
    4: eventsKey          (bull:<queue>:events)
    5: markerKey          (bull:<queue>:marker)
    6: pausedKey          (bull:<queue>:paused)

  ARGV:
    1: prefixKey          (bull:<queue>:)
    2: workerId           (worker lock token)
    3: lockDurationMs     (lock TTL in ms)
    4: maxBatchSize       (max jobs to pop)
    5: nowTimestamp       (current timestamp in ms)
]]

local rcall = redis.call

local waitKey        = KEYS[1]
local activeKey      = KEYS[2]
local prioritizedKey = KEYS[3]
local eventsKey      = KEYS[4]
local markerKey      = KEYS[5]
local pausedKey      = KEYS[6]

local prefixKey      = ARGV[1]
local workerId       = ARGV[2]
local lockDurationMs = tonumber(ARGV[3]) or 30000
local maxBatchSize   = tonumber(ARGV[4]) or 10
local nowTimestamp   = tonumber(ARGV[5]) or 0

-- Check if queue is paused
if pausedKey and pausedKey ~= "" and rcall("EXISTS", pausedKey) == 1 then
    return nil
end

local jobs = {}
local poppedCount = 0

-- 1. Pop from prioritized jobs first if present
if prioritizedKey and prioritizedKey ~= "" and rcall("EXISTS", prioritizedKey) == 1 then
    local needed = maxBatchSize - poppedCount
    if needed > 0 then
        local pJobs = rcall("ZPOPMIN", prioritizedKey, needed)
        if pJobs and #pJobs > 0 then
            for i = 1, #pJobs, 2 do
                local jobId = pJobs[i]
                if jobId and jobId ~= "" then
                    poppedCount = poppedCount + 1
                    local jobKey = prefixKey .. jobId
                    local lockKey = jobKey .. ":lock"
                    rcall("SET", lockKey, workerId, "PX", lockDurationMs)
                    rcall("LPUSH", activeKey, jobId)
                    rcall("HINCRBY", jobKey, "attemptsStarted", 1)
                    rcall("HSET", jobKey, "processedOn", nowTimestamp)
                    if eventsKey and eventsKey ~= "" then
                        rcall("XADD", eventsKey, "*", "event", "active", "jobId", jobId)
                    end
                    table.insert(jobs, { rcall("HGETALL", jobKey), jobId })
                end
            end
        end
    end
end

-- 2. Pop remaining from FIFO wait list
local remainingNeeded = maxBatchSize - poppedCount
if remainingNeeded > 0 and waitKey and waitKey ~= "" then
    for _ = 1, remainingNeeded do
        local jobId = rcall("RPOP", waitKey)
        if not jobId then
            break
        end
        local jobKey = prefixKey .. jobId
        local lockKey = jobKey .. ":lock"
        rcall("SET", lockKey, workerId, "PX", lockDurationMs)
        rcall("LPUSH", activeKey, jobId)
        rcall("HINCRBY", jobKey, "attemptsStarted", 1)
        rcall("HSET", jobKey, "processedOn", nowTimestamp)
        if eventsKey and eventsKey ~= "" then
            rcall("XADD", eventsKey, "*", "event", "active", "jobId", jobId)
        end
        table.insert(jobs, { rcall("HGETALL", jobKey), jobId })
    end
end

if #jobs == 0 then
    return nil
end

return jobs
