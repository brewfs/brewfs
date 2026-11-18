"""Directory grants for a Redis upper and an immutable lower, in a private test database."""

import time


GRANT = """
local phase = redis.call('HGET', KEYS[1], 'phase') or 'sealed'
if phase == 'sealed' then redis.call('SADD', KEYS[2], ARGV[1]) end
return phase
"""

BEGIN = """
local phase = redis.call('HGET', KEYS[1], 'phase') or 'sealed'
local owner = redis.call('HGET', KEYS[1], 'operation')
if phase == 'draining' and owner ~= ARGV[1] then return 'busy' end
if owner == ARGV[1] then
    if redis.call('HGET', KEYS[1], 'name') ~= ARGV[2] or
       redis.call('HGET', KEYS[1], 'payload') ~= ARGV[3] then return 'conflict' end
    return phase
end
if redis.call('HEXISTS', KEYS[2], ARGV[2]) == 1 then return 'exists' end
if phase == 'writable' then
    redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
    redis.call('HSET', KEYS[1], 'modified_ns', ARGV[4])
else
    phase = 'draining'
end
redis.call('HSET', KEYS[1], 'phase', phase, 'operation', ARGV[1], 'name', ARGV[2], 'payload', ARGV[3])
return phase
"""

PUBLISH = """
if redis.call('HGET', KEYS[1], 'operation') ~= ARGV[1] then return 'busy' end
if redis.call('HGET', KEYS[1], 'phase') == 'writable' then return 'writable' end
if redis.call('SCARD', KEYS[2]) ~= 0 then return 'draining' end
local name = redis.call('HGET', KEYS[1], 'name')
local payload = redis.call('HGET', KEYS[1], 'payload')
redis.call('HSET', KEYS[3], name, payload)
redis.call('HSET', KEYS[1], 'phase', 'writable', 'modified_ns', ARGV[2])
return 'writable'
"""


class Directory:
    def __init__(self, redis, name):
        self.redis = redis
        self.state = f"dir:{name}:state"
        self.readers = f"dir:{name}:readers"
        self.files = f"dir:{name}:files"

    def grant(self, reader):
        return self.redis.eval(GRANT, 2, self.state, self.readers, reader).decode()

    def begin_create(self, operation, name, payload):
        return self.redis.eval(
            BEGIN, 2, self.state, self.files, operation, name, payload, str(time.time_ns())
        ).decode()

    def publish(self, operation):
        return self.redis.eval(
            PUBLISH, 3, self.state, self.readers, self.files, operation, str(time.time_ns())
        ).decode()
