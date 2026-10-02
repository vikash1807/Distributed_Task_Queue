-- Atomically release an owned lease and route a failed attempt. The ZREM is
-- the race guard against ACK, the reaper, and another failure transition.
--
-- KEYS[1] = processing ZSET
-- KEYS[2] = task HASH
-- KEYS[3] = owner's in-flight task SET
-- KEYS[4] = delayed ZSET
-- KEYS[5] = dead-letter LIST
-- KEYS[6] = metrics HASH
--
-- ARGV[1] = task ID
-- ARGV[2] = expected owner node ID
-- ARGV[3] = handler error
-- ARGV[4] = retry execute-at time in Unix seconds
-- ARGV[5] = FailedTask JSON for the DLQ
--
-- Returns: 0 if the lease was lost, 1 if scheduled for retry, 2 if dead-lettered.

local task_id = ARGV[1]
local owner = ARGV[2]

if redis.call('HGET', KEYS[2], 'owner') ~= owner then
    return 0
end

if redis.call('ZREM', KEYS[1], task_id) ~= 1 then
    return 0
end

redis.call('SREM', KEYS[3], task_id)
local retries = tonumber(redis.call('HGET', KEYS[2], 'retries')) or 0
local max_retries = tonumber(redis.call('HGET', KEYS[2], 'max_retries')) or 0

if retries < max_retries then
    retries = retries + 1
    redis.call('HSET', KEYS[2],
        'retries', retries, 'status', 'pending', 'owner', '', 'error', ARGV[3])
    redis.call('ZADD', KEYS[4], tonumber(ARGV[4]), task_id)
    redis.call('HINCRBY', KEYS[6], 'retries', 1)
    redis.call('HINCRBY', KEYS[6], 'failed', 1)
    return 1
end

redis.call('HSET', KEYS[2], 'status', 'failed', 'owner', '', 'error', ARGV[3])
redis.call('LPUSH', KEYS[5], ARGV[5])
redis.call('HINCRBY', KEYS[6], 'failed', 1)
return 2
