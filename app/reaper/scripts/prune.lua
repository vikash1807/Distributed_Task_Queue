-- prune.lua
-- Atomically removes a dead node after its visibility grace period. Keep the
-- node in the registry while its heartbeat is absent so /api/nodes can show it
-- as alive=false. Do not prune if the node has returned or still has indexed
-- tasks that need reclamation.
--
-- KEYS[1] = registered node IDs SET (taskqueue:nodes)
-- KEYS[2] = node heartbeat key (taskqueue:node:{id}:hb)
-- KEYS[3] = node metadata key (taskqueue:node:{id}:meta)
-- KEYS[4] = node's owned task IDs SET (taskqueue:node:{id}:tasks)
-- KEYS[5] = node dead tombstone key (taskqueue:node:{id}:dead)
--
-- ARGV[1] = node ID
-- ARGV[2] = cutoff time (current Unix milliseconds - DEAD_NODE_GRACE_MS)
--
-- Returns: 1 if the node was pruned, 0 if it must remain visible.

local nodes_key = KEYS[1]
local heartbeat_key = KEYS[2]
local metadata_key = KEYS[3]
local tasks_key = KEYS[4]
local tombstone_key = KEYS[5]

local node_id = ARGV[1]
local cutoff_ms = tonumber(ARGV[2])

-- No tombstone means the node has not been declared dead. A timestamp newer
-- than the cutoff means its grace period has not elapsed yet.
local dead_since_ms = tonumber(redis.call('GET', tombstone_key))
if not dead_since_ms or dead_since_ms > cutoff_ms then
    return 0
end

-- A heartbeat can return after detection. Indexed tasks must be reclaimed
-- before removing the node's metadata and registry entry.
if redis.call('EXISTS', heartbeat_key) == 1 then
    return 0
end

if redis.call('SCARD', tasks_key) > 0 then
    return 0
end

redis.call('SREM', nodes_key, node_id)
redis.call('DEL', metadata_key, tasks_key, tombstone_key)

return 1
