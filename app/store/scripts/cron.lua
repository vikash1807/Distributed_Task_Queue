-- KEYS[1]: cron HASH, KEYS[2]: cron ID SET.
-- ARGV[1]: create/delete/enabled; ARGV[2]: ID; ARGV[3]: definition hash JSON
-- for create, '1'/'0' for enabled, unused for delete; ARGV[4]: fresh revision.
-- Returns JSON record on create/update, '' for absent update, 0/1 on delete,
-- or '' for duplicate create. Definition/index updates are atomic. Enabled
-- changes invalidate stale materializer snapshots; create has a fresh revision
-- to prevent ABA after delete/recreate. Repeated enabled updates are idempotent.
-- Reject incompatible key types before any write; Lua errors do not roll back prior writes.
local function require_type(key, expected)
    local actual = redis.call('TYPE', key).ok
    if actual ~= 'none' and actual ~= expected then error('unexpected Redis type') end
end
require_type(KEYS[1], 'hash')
require_type(KEYS[2], 'set')

-- Convert HGETALL's alternating fields/values into the JSON record returned to Python.
local function record()
    local flat = redis.call('HGETALL', KEYS[1])
    local data = {}
    for i = 1, #flat, 2 do data[flat[i]] = flat[i + 1] end
    return cjson.encode(data)
end

if ARGV[1] == 'delete' then
    -- Keep the definition and its listing index consistent; existing task instances survive.
    local removed = redis.call('DEL', KEYS[1])
    redis.call('SREM', KEYS[2], ARGV[2])
    return removed
end
if ARGV[1] == 'create' then
    if redis.call('EXISTS', KEYS[1]) == 1 then return '' end
    local data = cjson.decode(ARGV[3])
    local fields = {}
    for key, value in pairs(data) do
        if type(value) ~= 'string' then error('invalid definition field') end
        fields[#fields + 1] = key
        fields[#fields + 1] = value
    end
    -- Creation starts at server time so a new job cannot backfill earlier schedule slots.
    local now = redis.call('TIME')[1]
    redis.call('HSET', KEYS[1], unpack(fields))
    redis.call('HSET', KEYS[1], 'created_unix', now, 'eligible_after_unix', now,
        'last_materialized_slot_unix', '', 'revision', ARGV[4])
    redis.call('SADD', KEYS[2], ARGV[2])
    return record()
end
if ARGV[1] ~= 'enabled' or (ARGV[3] ~= '1' and ARGV[3] ~= '0') then
    error('invalid cron operation')
end
if redis.call('EXISTS', KEYS[1]) == 0 then return '' end
if redis.call('HGET', KEYS[1], 'enabled') ~= ARGV[3] then
    if ARGV[3] == '1' then
        -- Resume from now while preserving the separate last-successful-slot cursor.
        redis.call('HSET', KEYS[1], 'eligible_after_unix', redis.call('TIME')[1])
    end
    -- The new revision makes any already-read materializer snapshot stale.
    redis.call('HSET', KEYS[1], 'enabled', ARGV[3], 'revision', ARGV[4])
end
return record()
