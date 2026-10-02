-- mark_dead.lua
-- Atomically records the first observation of a dead worker node. The node
-- must still be registered and its heartbeat must be absent. Checking both
-- here prevents a concurrent graceful deregistration or heartbeat refresh
-- from being mistaken for a node failure.
--
-- KEYS[1] = registered node IDs SET (taskqueue:nodes)
-- KEYS[2] = node heartbeat key (taskqueue:node:{id}:hb)
-- KEYS[3] = node dead tombstone key (taskqueue:node:{id}:dead)
--
-- ARGV[1] = node ID
-- ARGV[2] = detection time (Unix milliseconds)
--
-- Returns: 0 if the node is alive or no longer registered,
--          1 if this call created the tombstone (emit node_dead),
--          2 if a tombstone already exists (do not emit node_dead again).

local nodes_key = KEYS[1]
local heartbeat_key = KEYS[2]
local tombstone_key = KEYS[3]

local node_id = ARGV[1]
local detected_at_ms = ARGV[2]

-- A graceful shutdown removes the node from the registry. A refreshed
-- heartbeat means the node is still alive, even if a prior scan saw it dead.
if redis.call('SISMEMBER', nodes_key, node_id) == 0 then
    return 0
end

if redis.call('EXISTS', heartbeat_key) == 1 then
    return 0
end

-- Keep the first detection timestamp so prune.lua can apply the grace period.
-- SET NX also lets only one reaper emit the node_dead event.
if redis.call('SET', tombstone_key, detected_at_ms, 'NX') then
    return 1
end

return 2
