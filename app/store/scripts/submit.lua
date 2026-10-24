-- Atomically create a canonical task with its queue, metric, and submitted event.
-- KEYS: 1 task HASH, 2 ready ZSET, 3 delayed ZSET, 4 doorbell LIST,
--       5 metrics HASH, 6 task events LIST.
-- ARGV: 1 task-hash JSON, 2 event JSON, 3 execute-at Unix seconds (0 for ready),
--       4 maximum retained wake-up tokens.
-- Returns 1 when created, 0 when EXISTS rejects a duplicate ID. Retrying after
-- a lost response cannot overwrite a task or duplicate its queue/event/metric.

-- Validate key types before writing so a later WRONGTYPE error cannot strand a task.
local function require_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    if actual ~= 'none' and actual ~= expected then
        error('unexpected Redis type for ' .. key)
    end
end

require_type(KEYS[1], 'hash')
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end

-- Preflight before writing: Lua errors do not roll back earlier mutations.
require_type(KEYS[2], 'zset')
require_type(KEYS[3], 'zset')
require_type(KEYS[4], 'list')
require_type(KEYS[5], 'hash')
require_type(KEYS[6], 'list')
local task = cjson.decode(ARGV[1])
cjson.decode(ARGV[2])
local execute_at = tonumber(ARGV[3])
local cap = tonumber(ARGV[4])
local priority = tonumber(task.priority)
if not execute_at or not cap or cap < 1 or not priority or not task.id then
    error('invalid submission arguments')
end
local submitted = redis.call('HGET', KEYS[5], 'submitted')
-- Fail before HSET if HINCRBY would reject the counter or exceed safe Lua integer precision.
if submitted and (not string.match(submitted, '^%-?%d+$')
    or math.abs(tonumber(submitted)) >= 9007199254740991) then
    error('invalid submitted counter')
end
local fields = {}
for key, value in pairs(task) do
    if type(value) ~= 'string' and type(value) ~= 'number' then
        error('invalid task field')
    end
    fields[#fields + 1] = key
    fields[#fields + 1] = value
end

-- All checks have passed; the record, queue, and monitoring now commit together.
redis.call('HSET', KEYS[1], unpack(fields))
if execute_at > 0 then
    redis.call('ZADD', KEYS[3], execute_at, task.id)
else
    -- Ready scores are negative priorities; the capped token wakes a blocked worker.
    redis.call('ZADD', KEYS[2], -priority, task.id)
    if redis.call('LLEN', KEYS[4]) < cap then redis.call('RPUSH', KEYS[4], '1') end
end
redis.call('HINCRBY', KEYS[5], 'submitted', 1)
redis.call('LPUSH', KEYS[6], ARGV[2])
redis.call('LTRIM', KEYS[6], 0, 199)
return 1
