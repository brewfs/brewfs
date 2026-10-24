-- KEYS: inode, parent inode, parent dentries, deleted set, inode xattrs.
-- ARGV: inode, parent inode, destination name, timestamp, trash xattr name.
local raw = redis.call('GET', KEYS[1])
if not raw then
    return cjson.encode({ok=false, error='not_found'})
end
local node = cjson.decode(raw)
if node.deleted ~= true or node.attr.nlink ~= 0
    or redis.call('HEXISTS', KEYS[4], ARGV[1]) ~= 1 then
    return cjson.encode({ok=false, error='not_deleted'})
end
local parent_raw = redis.call('GET', KEYS[2])
if not parent_raw then
    return cjson.encode({ok=false, error='parent_not_found'})
end
local parent = cjson.decode(parent_raw)
if parent.kind ~= 'Dir' or parent.deleted then
    return cjson.encode({ok=false, error='parent_not_directory'})
end
if redis.call('HEXISTS', KEYS[3], ARGV[3]) == 1 then
    return cjson.encode({ok=false, error='already_exists'})
end

node.deleted = false
node.attr.nlink = 1
node.parent = tonumber(ARGV[2])
node.name = ARGV[3]
node.attr.ctime = tonumber(ARGV[4])
parent.attr.mtime = tonumber(ARGV[4])
parent.attr.ctime = tonumber(ARGV[4])

stage('HDEL', KEYS[5], ARGV[5])
stage('SET', KEYS[1], cjson.encode(node))
stage('HDEL', KEYS[4], ARGV[1])
stage('HSET', KEYS[3], ARGV[3], ARGV[1])
if redis.call('ZSCORE', KEYS[3] .. ':names', '') then
    stage('ZADD', KEYS[3] .. ':names', 0, ARGV[3])
elseif redis.call('HLEN', KEYS[3]) == 0 then
    stage('DEL', KEYS[3] .. ':names')
    stage('ZADD', KEYS[3] .. ':names', 0, '', 0, ARGV[3])
end
stage('SET', KEYS[2], cjson.encode(parent))
local response = cjson.encode({ok=true})
commit_staged()
return response
