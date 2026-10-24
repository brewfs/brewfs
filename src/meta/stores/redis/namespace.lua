-- Redis has no script rollback. Prepare/validate every write before executing
-- any of them, including the trash xattr. No namespace or live-inode marker
-- can escape if path resolution, encoding, key types, or ACL checks fail.
if not redis.acl_check_cmd then
    error('atomic namespace operations require Redis 7 or newer (Lua ACL preflight)')
end
local namespace_writes = {}
local namespace_types = {}
local write_types = {HSET='hash', HDEL='hash', SADD='set', SREM='set', ZADD='zset', ZREM='zset'}

local function stage(command, ...)
    local args = {...}
    local expected = write_types[command]
    if expected and namespace_types[args[1]] and namespace_types[args[1]] ~= expected then
        error('conflicting namespace key types: ' .. args[1])
    end
    if expected and not namespace_types[args[1]] then
        local reply = redis.call('TYPE', args[1])
        local actual = type(reply) == 'table' and reply.ok or reply
        if actual ~= 'none' and actual ~= expected then
            error('invalid namespace key type: ' .. args[1])
        end
        namespace_types[args[1]] = expected
    end
    -- Check every command before committing, including commands late in the
    -- write sequence, so a denied command cannot leave partial data.
    if not redis.acl_check_cmd(command, unpack(args)) then
        error('namespace command denied: ' .. command)
    end
    table.insert(namespace_writes, {command, unpack(args)})
end

local function commit_staged()
    for _, command in ipairs(namespace_writes) do
        redis.call(unpack(command))
    end
end

local function stage_trash(ino, parent, name, node_prefix, xattr_prefix, xattr_name, deleted_at)
    local parts = {name}
    local seen = {}
    local depth = 0
    while parent ~= 1 do
        if seen[parent] or depth >= 4096 then
            error('cyclic or excessive trash ancestry')
        end
        seen[parent] = true
        depth = depth + 1
        local raw = redis.call('GET', node_prefix .. parent)
        local node = raw and cjson.decode(raw)
        if type(node) ~= 'table' or node.kind ~= 'Dir' or node.deleted
            or type(node.name) ~= 'string' or node.name == ''
            or type(node.parent) ~= 'number' or node.parent <= 0 then
            error('invalid trash ancestry')
        end
        table.insert(parts, 1, node.name)
        parent = node.parent
    end
    stage('HSET', xattr_prefix .. ino, xattr_name, cjson.encode({
        original_path='/' .. table.concat(parts, '/'), deleted_at=deleted_at
    }))
end
