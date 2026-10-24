-- KEYS[1]: leader STRING; ARGV[1]: complete owner value.
-- Returns 1 when released, 0 for absent/different owner. Safe to retry after a
-- lost response: a successor's lease can never be removed by this owner.
-- A delayed release from an expired tenure must leave a successor's key untouched.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
