-- KEYS[1]: leader STRING; ARGV[1]: complete owner value; ARGV[2]: TTL milliseconds.
-- Returns 1 when renewed, 0 for absent/different owner. Compare before mutation
-- prevents an expired tenure renewing a successor, including the same API process.
-- Compare and extend within one script so a successor cannot appear between the two operations.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
