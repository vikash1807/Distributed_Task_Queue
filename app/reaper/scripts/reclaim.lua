-- reclaim.lua
-- Atomically recovers one task from the processing ZSET. An expired-lease
-- scan reclaims only leases due by ARGV[2]. A dead-node scan reclaims tasks
-- owned by ARGV[3] immediately, but only while that node's heartbeat is absent.
-- The processing ZREM is the race guard against ACK, NACK, and other reapers.
--
-- KEYS[1] = processing/lease ZSET (taskqueue:processing)
-- KEYS[2] = ready task ZSET (taskqueue:ready)
-- KEYS[3] = task record HASH (taskqueue:task:{id})
-- KEYS[4] = ready doorbell LIST (taskqueue:ready:signal)
-- KEYS[5] = dead-letter LIST (taskqueue:deadletter)
-- KEYS[6] = metrics HASH (taskqueue:metrics)
-- KEYS[7] = dead node heartbeat key (taskqueue:node:{id}:hb);
--           ignored for expired-lease scans, which pass KEYS[1] here
--
-- ARGV[1] = task ID
-- ARGV[2] = current time (Unix milliseconds)
-- ARGV[3] = expected dead-node owner, or '' for an expired-lease scan
-- ARGV[4] = maximum number of ready doorbell tokens
-- ARGV[5] = failure time (ISO 8601 timestamp) for a DLQ entry
--
-- Returns: 0 if this call did not reclaim the lease,
--          1 if the task was returned to ready with retries incremented,
--          2 if retries were exhausted and the task was sent to the DLQ,
--          3 if an orphaned lease was removed (task record missing).

local processing_key = KEYS[1]
local ready_key = KEYS[2]
local task_key = KEYS[3]
local signal_key = KEYS[4]
local deadletter_key = KEYS[5]
local metrics_key = KEYS[6]
local heartbeat_key = KEYS[7]

local task_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
local expected_owner = ARGV[3]
local signal_cap = tonumber(ARGV[4])
local failed_at = ARGV[5]

-- The node task SET is an index. If its task ID no longer has a lease,
-- remove stale membership without touching the task record or metrics.
local lease_deadline_ms = tonumber(redis.call('ZSCORE', processing_key, task_id))
if not lease_deadline_ms then
    if expected_owner ~= '' then
        redis.call('SREM', 'taskqueue:node:' .. expected_owner .. ':tasks', task_id)
    end
    return 0
end

if expected_owner ~= '' then
    -- A node that refreshed its heartbeat must keep its work. The owner field
    -- is authoritative; an old node task SET entry cannot reclaim a new lease.
    if redis.call('EXISTS', heartbeat_key) == 1 then
        return 0
    end

    -- An indexed task with no record cannot be executed. Remove its lease
    -- immediately rather than waiting for its visibility timeout.
    if redis.call('EXISTS', task_key) == 0 then
        if redis.call('ZREM', processing_key, task_id) == 1 then
            redis.call('SREM', 'taskqueue:node:' .. expected_owner .. ':tasks', task_id)
            return 3
        end
        return 0
    end

    if redis.call('HGET', task_key, 'owner') ~= expected_owner then
        redis.call('SREM', 'taskqueue:node:' .. expected_owner .. ':tasks', task_id)
        return 0
    end
elseif lease_deadline_ms > now_ms then
    -- The lease was extended or has not expired yet.
    return 0
end

-- Only the caller that removes the lease may alter its task state. Redis
-- serializes this script with ACK/NACK and other reclaim attempts.
if redis.call('ZREM', processing_key, task_id) ~= 1 then
    return 0
end

local owner = redis.call('HGET', task_key, 'owner')
if owner and owner ~= '' then
    redis.call('SREM', 'taskqueue:node:' .. owner .. ':tasks', task_id)
end

-- For an expired lease, the owner may be unknown because the task HASH has
-- disappeared. The lease is now gone; do not create a new task or DLQ entry.
if redis.call('EXISTS', task_key) == 0 then
    return 3
end

local retries = tonumber(redis.call('HGET', task_key, 'retries')) or 0
local max_retries = tonumber(redis.call('HGET', task_key, 'max_retries')) or 0
local reason = expected_owner ~= '' and 'worker node died' or 'task lease expired'

if retries < max_retries then
    -- Match normal retry semantics: increment only when another attempt is
    -- allowed. Ready scores are negative priorities, so larger priorities pop
    -- first. Ring the capped doorbell to wake a worker waiting on ready work.
    retries = retries + 1
    local priority = tonumber(redis.call('HGET', task_key, 'priority')) or 0

    redis.call('HSET', task_key, 'retries', retries, 'status', 'pending', 'owner', '')
    redis.call('ZADD', ready_key, -priority, task_id)
    if redis.call('LLEN', signal_key) < signal_cap then
        redis.call('RPUSH', signal_key, '1')
    end

    redis.call('HINCRBY', metrics_key, 'retries', 1)
    redis.call('HINCRBY', metrics_key, 'failed', 1)
    redis.call('HINCRBY', metrics_key, 'reaper_reclaims', 1)
    return 1
end

-- Build the same FailedTask-shaped JSON used by the DLQ reader. A malformed
-- or absent payload becomes JSON null; an absent created_at becomes null.
local payload = cjson.null
local raw_payload = redis.call('HGET', task_key, 'payload')
if raw_payload and raw_payload ~= '' then
    local ok, decoded = pcall(cjson.decode, raw_payload)
    if ok and type(decoded) == 'table' then
        payload = decoded
    end
end

local created_at = redis.call('HGET', task_key, 'created_at')
if not created_at or created_at == '' then
    created_at = cjson.null
end

local failed_task = {
    id = redis.call('HGET', task_key, 'id') or task_id,
    type = redis.call('HGET', task_key, 'type') or '',
    payload = payload,
    priority = tonumber(redis.call('HGET', task_key, 'priority')) or 0,
    delay = tonumber(redis.call('HGET', task_key, 'delay')) or 0,
    max_retries = max_retries,
    retries = retries,
    status = 'failed',
    created_at = created_at,
    error = reason,
    owner = '',
    failed_at = failed_at,
    reason = reason,
}

redis.call('HSET', task_key, 'status', 'failed', 'owner', '', 'error', reason)
redis.call('LPUSH', deadletter_key, cjson.encode(failed_task))
redis.call('HINCRBY', metrics_key, 'failed', 1)
redis.call('HINCRBY', metrics_key, 'reaper_reclaims', 1)

return 2
