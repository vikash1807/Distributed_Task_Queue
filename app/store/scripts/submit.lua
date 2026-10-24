-- Atomically create a task and queue membership; optional cron cursor commit.
-- KEYS: 1 task HASH, 2 ready ZSET, 3 delayed ZSET, 4 doorbell LIST,
--       5 metrics HASH, 6 task events LIST, 7 leader STRING, 8 cron HASH.
-- ARGV: 1 task hash JSON, 2 submitted-event JSON, 3 execute_at Unix seconds
--       (0 means ready), 4 signal cap, 5 expected lease ('' for ordinary submit),
--       6 cron revision ('' for ordinary submit), 7 expected cursor ('' if none),
--       8 slot Unix seconds (0 for ordinary submit).
-- Returns 1 created, 0 already exists, -1 stale leader, -2 changed/disabled cron.
-- EXISTS guards duplicate creation. Ownership/revision/cursor checks guard stale
-- materializers. Task, index, metric, event and cursor commit together; a retry
-- after a lost response never overwrites or re-enqueues an existing task.

-- Validate key types before writing so a later WRONGTYPE error cannot strand a task.
local function require_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    if actual ~= 'none' and actual ~= expected then
        error('unexpected Redis type for ' .. key)
    end
end

-- Recheck ownership at commit time: Python's earlier lease check alone can race takeover.
if ARGV[5] ~= '' and redis.call('GET', KEYS[7]) ~= ARGV[5] then return -1 end
local cron = ARGV[6] ~= ''
if cron and ARGV[5] == '' then return -1 end
local slot = tonumber(ARGV[8])
if cron then
    -- Reject pause/delete/recreate and cursor changes since this job snapshot was read.
    require_type(KEYS[8], 'hash')
    if redis.call('HGET', KEYS[8], 'revision') ~= ARGV[6]
        or redis.call('HGET', KEYS[8], 'enabled') ~= '1'
        or (redis.call('HGET', KEYS[8], 'last_materialized_slot_unix') or '') ~= ARGV[7]
    then return -2 end
    local baseline = tonumber(redis.call('HGET', KEYS[8], 'eligible_after_unix'))
    if not slot or not baseline or slot <= baseline
        or (ARGV[7] ~= '' and slot <= tonumber(ARGV[7])) then return -2 end
end

require_type(KEYS[1], 'hash')
if redis.call('EXISTS', KEYS[1]) == 1 then
    -- An existing instance counts as success without resetting its status or queue position.
    if cron then redis.call('HSET', KEYS[8], 'last_materialized_slot_unix', slot) end
    return 0
end

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

-- All checks have passed; the record, queue, monitoring, and optional cursor now commit together.
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
if cron then redis.call('HSET', KEYS[8], 'last_materialized_slot_unix', slot) end
return 1
