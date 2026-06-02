//! Redis-based metadata store implementation.
//!
//! This store focuses on the core interfaces needed by the VFS layer so that
//! the filesystem can persist metadata in Redis. It purposely keeps the key
//! layout simple (one key per inode plus a hash per directory) and uses JSON
//! serialization for file attributes. Advanced features (sessions, quota, etc.)
//! can be layered on later by extending the schema.

use super::{apply_truncate_plan, build_paths_from_names, trim_slices_in_place};
use crate::chunk::SliceDesc;
use crate::meta::client::session::{Session, SessionInfo};
use crate::meta::config::{Config, DatabaseType};
use crate::meta::file_lock::{
    FileLockInfo, FileLockQuery, FileLockRange, FileLockType, PlockRecord,
};
use crate::meta::store::{
    DirEntry, FileAttr, FileType, LockName, MetaError, MetaStore, RetryReason, SetAttrFlags,
    SetAttrRequest, StatFsSnapshot,
};
use crate::meta::{INODE_ID_KEY, SLICE_ID_KEY};
use async_trait::async_trait;
use chrono::Utc;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info};
use uuid::Uuid;

const ROOT_INODE: i64 = 1;
const COUNTER_INODE_KEY: &str = "nextinode";
const COUNTER_SLICE_KEY: &str = "nextchunk";
const NODE_KEY_PREFIX: &str = "i";
const DIR_KEY_PREFIX: &str = "d";
const CHUNK_KEY_PREFIX: &str = "c";
const DELETED_SET_KEY: &str = "delslices";
const ALL_SESSIONS_KEY: &str = "allsessions";
const SESSION_INFOS_KEY: &str = "sessioninfos";
const PLOCK_PREFIX: &str = "plock";
const PLOCK_EPOCH_KEY: &str = "plock_epoch";
const LOCKS_KEY: &str = "locks";
const LOCKED_KEY: &str = "locked";
const LINK_PARENT_KEY_PREFIX: &str = "lp:";
const TRUNCATE_REWRITE_MAX_RETRIES: usize = 64;

// Lua script for atomically replacing a chunk's slice list when the version
// matches the caller's expectation.  Returns 1 on success, 0 on version mismatch.
// KEYS[1] = chunk list key
// KEYS[2] = chunk version key
// ARGV[1] = expected version
// ARGV[2] = new version
// ARGV[3..N] = serialized slice data (may be empty to delete the chunk)
const CHUNK_CAS_LUA: &str = r#"
    local expected = tonumber(ARGV[1])
    local new_ver  = tonumber(ARGV[2])

    local current = redis.call('GET', KEYS[2])
    local current_ver = 0
    if current then
        current_ver = tonumber(current)
    end

    if current_ver ~= expected then
        return 0
    end

    redis.call('DEL', KEYS[1])
    for i = 3, #ARGV do
        redis.call('RPUSH', KEYS[1], ARGV[i])
    end

    if new_ver > 0 then
        redis.call('SET', KEYS[2], new_ver)
    else
        redis.call('DEL', KEYS[2])
    end

    return 1
"#;

// Lua script for atomically releasing all locks held by a dead session.
// Constructs plock keys dynamically from the locked set so the entire
// cleanup is atomic — no TOCTOU window between reading locked_files and
// deleting the lock fields.
const CLEANUP_SESSION_LUA: &str = r#"
    local cjson = cjson

    local locked_key  = KEYS[1]
    local sessions_key = KEYS[2]
    local infos_key   = KEYS[3]
    local sid_str     = ARGV[1]
    local plock_prefix = ARGV[2]

    -- Collect every inode this session holds locks on
    local locked_files = redis.call('SMEMBERS', locked_key)

    for _, inode in ipairs(locked_files) do
        local plock_key = plock_prefix .. inode
        local fields = redis.call('HKEYS', plock_key)
        local to_delete = {}
        for _, field in ipairs(fields) do
            -- field format: sid:owner; match the sid prefix
            if string.sub(field, 1, #sid_str + 1) == sid_str .. ":" then
                table.insert(to_delete, field)
            end
        end
        if #to_delete > 0 then
            redis.call('HDEL', plock_key, unpack(to_delete))
        end
    end

    -- Remove session bookkeeping
    redis.call('DEL',  locked_key)
    redis.call('ZREM', sessions_key, sid_str)
    redis.call('HDEL', infos_key,   sid_str)

    return cjson.encode({ok=true})
"#;

// Lua script for atomic BSD flock (whole-file advisory lock).
// Each lock value is a simple string: "R" (shared/read) or "W" (exclusive/write).
// Follows the same fencing pattern as SET_PLOCK_LUA.
macro_rules! flock_lua {
    () => {
        r#"
    local cjson = cjson

    local flock_key = KEYS[1]
    local locked_key = KEYS[2]
    local field = ARGV[1]
    local lock_type = tonumber(ARGV[2])  -- 0=Read, 1=Write, 2=UnLock
    local inode_str = ARGV[3]
    local epoch = tonumber(ARGV[4])

    -- helpers: extract {epoch, value} from stored raw
    local function parse_val(raw)
        if raw == false or raw == nil then return {epoch=0, val=nil} end
        local ok, parsed = pcall(cjson.decode, raw)
        if not ok or type(parsed) ~= "table" then return {epoch=0, val=nil} end
        return {epoch = tonumber(parsed.epoch) or 0, val = parsed.val}
    end

    if lock_type == 2 then  -- UnLock
        local current_raw = redis.call('HGET', flock_key, field)
        if current_raw == false then
            return cjson.encode({ok=true})
        end
        local cur = parse_val(current_raw)
        if cur.epoch > epoch then
            return cjson.encode({ok=true})  -- stale unlock, ignore
        end
        redis.call('HDEL', flock_key, field)
        -- Remove from locked set if field was the last one
        if redis.call('HLEN', flock_key) == 0 then
            redis.call('SREM', locked_key, inode_str)
        end
        return cjson.encode({ok=true})
    end

    -- ReadLock or WriteLock
    local all = redis.call('HGETALL', flock_key)
    for i = 1, #all, 2 do
        local other_field = all[i]
        local other_raw = all[i + 1]
        if other_field ~= field then
            local other = parse_val(other_raw)
            if other.val == "W" then
                return cjson.encode({ok=false, error="lock_conflict"})
            end
            if lock_type == 1 and other.val == "R" then
                -- Write lock conflicts with any Read lock
                return cjson.encode({ok=false, error="lock_conflict"})
            end
        end
    end

    -- Acquire: store as {epoch, val}
    local val = (lock_type == 1) and "W" or "R"
    redis.call('HSET', flock_key, field,
                cjson.encode({epoch = epoch, val = val}))
    redis.call('SADD', locked_key, inode_str)
    return cjson.encode({ok=true})
"#
    };
}

// Lua script for atomically setting or releasing a POSIX advisory lock.
// Each lock value is a JSON object {epoch, records} where epoch is the
// monotonic fencing token of the session that wrote it.  A stale session
// (whose locks were cleaned up by another node) cannot release locks
// written by a newer incarnation because its epoch will be lower.
// This performs a read-check-write cycle inside Redis so that concurrent
// lock attempts cannot both pass the conflict check — the same pattern
// used by CREATE_ENTRY_LUA, LINK_LUA, etc.
const SET_PLOCK_LUA: &str = r#"
    local cjson = cjson

    local plock_key = KEYS[1]
    local locked_key = KEYS[2]
    local field = ARGV[1]
    local lock_type = tonumber(ARGV[2])  -- 0=Read, 1=Write, 2=UnLock
    local pid = tonumber(ARGV[3])
    local range_start = tonumber(ARGV[4])
    local range_end   = tonumber(ARGV[5])
    local inode_str   = ARGV[6]
    local epoch       = tonumber(ARGV[7])

    -- ------------------------------------------------------------------
    -- helpers
    -- ------------------------------------------------------------------
    local function overlaps(a_s, a_e, b_s, b_e)
        return a_e > b_s and a_s < b_e
    end

    -- Extract the records array from a stored lock value.
    -- Value is {epoch:N, records:[...]}.  Accepts legacy bare-array
    -- format (epoch=0) for transparent upgrade.
    local function get_records(raw)
        if raw == false or raw == nil then return {} end
        local ok, parsed = pcall(cjson.decode, raw)
        if not ok then return {} end
        -- object with records key
        if type(parsed) == "table" and parsed.records then
            return parsed.records
        end
        -- object with epoch but no records (corner case)
        if type(parsed) == "table" and parsed.epoch then
            return {}
        end
        -- legacy bare array
        if type(parsed) == "table" and #parsed > 0 then
            return parsed
        end
        return {}
    end

    -- Extract epoch from a stored lock value.
    local function get_epoch(raw)
        if raw == false or raw == nil then return 0 end
        local ok, parsed = pcall(cjson.decode, raw)
        if not ok then return 0 end
        if type(parsed) == "table" and parsed.epoch then
            return tonumber(parsed.epoch) or 0
        end
        return 0
    end

    local function check_conflict(new_type, new_start, new_end, other_raw)
        local records = get_records(other_raw)
        for _, r in ipairs(records) do
            -- 1 = Write lock
            if (new_type == 1 or r.lock_type == 1)
               and overlaps(new_start, new_end,
                            r.lock_range.start, r.lock_range["end"]) then
                return true
            end
        end
        return false
    end

    -- Merge a new lock record into an existing (same-owner) list.
    -- Returns the new list with UnLock ranges removed and adjacent
    -- same-type-same-pid records merged.
    local function update_locks(existing, new_lock)
        local result = {}
        local inserted = false

        for _, r in ipairs(existing) do
            if r.lock_range["end"] <= new_lock.lock_range.start then
                table.insert(result, r)
            elseif r.lock_range.start >= new_lock.lock_range["end"] then
                if not inserted then
                    table.insert(result, new_lock)
                    inserted = true
                end
                table.insert(result, r)
            else
                if r.lock_range.start < new_lock.lock_range.start then
                    table.insert(result, {
                        lock_type = r.lock_type, pid = r.pid,
                        lock_range = {start = r.lock_range.start,
                                      ["end"] = new_lock.lock_range.start}
                    })
                end
                if not inserted then
                    table.insert(result, new_lock)
                    inserted = true
                end
                if r.lock_range["end"] > new_lock.lock_range["end"] then
                    table.insert(result, {
                        lock_type = r.lock_type, pid = r.pid,
                        lock_range = {start = new_lock.lock_range["end"],
                                      ["end"] = r.lock_range["end"]}
                    })
                end
            end
        end

        if not inserted then
            table.insert(result, new_lock)
        end

        -- Remove UnLock records and empty ranges
        local filtered = {}
        for _, r in ipairs(result) do
            if r.lock_type ~= 2 and r.lock_range.start < r.lock_range["end"] then  -- 2 = UnLock
                table.insert(filtered, r)
            end
        end

        -- Merge adjacent same-type same-pid records
        local merged = {}
        for _, r in ipairs(filtered) do
            local last = merged[#merged]
            if last ~= nil
               and last.lock_type == r.lock_type
               and last.pid == r.pid
               and last.lock_range["end"] == r.lock_range.start then
                last.lock_range["end"] = r.lock_range["end"]
            else
                table.insert(merged, r)
            end
        end

        return merged
    end

    -- ------------------------------------------------------------------
    -- main
    -- ------------------------------------------------------------------
    if lock_type == 2 then  -- UnLock
        local current_raw = redis.call('HGET', plock_key, field)
        if current_raw == false then
            return cjson.encode({ok=true})
        end

        local stored_epoch = get_epoch(current_raw)
        -- Fencing: if another (newer) session already wrote here, our
        -- unlock is stale — silently ignore so we don't corrupt the
        -- active session's locks.
        if stored_epoch > epoch then
            return cjson.encode({ok=true})
        end

        local existing = get_records(current_raw)
        local new_lock = {
            lock_type = 2, pid = pid,  -- 2 = UnLock
            lock_range = {start = range_start, ["end"] = range_end}
        }
        local new_records = update_locks(existing, new_lock)

        if #new_records == 0 then
            redis.call('HDEL', plock_key, field)
            redis.call('SREM', locked_key, inode_str)
        else
            redis.call('HSET', plock_key, field,
                        cjson.encode({epoch = epoch, records = new_records}))
        end
        return cjson.encode({ok=true})
    else
        -- ReadLock or WriteLock — check conflicts with all OTHER owners
        local all = redis.call('HGETALL', plock_key)
        for i = 1, #all, 2 do
            local other_field = all[i]
            local other_raw   = all[i + 1]
            if other_field ~= field then
                if check_conflict(lock_type, range_start, range_end, other_raw) then
                    return cjson.encode({ok=false, error="lock_conflict"})
                end
            end
        end

        -- Merge into current owner's list
        local current_raw = redis.call('HGET', plock_key, field)
        local existing = get_records(current_raw)

        local new_lock = {
            lock_type = lock_type, pid = pid,
            lock_range = {start = range_start, ["end"] = range_end}
        }
        local new_records = update_locks(existing, new_lock)

        redis.call('HSET', plock_key, field,
                    cjson.encode({epoch = epoch, records = new_records}))
        redis.call('SADD', locked_key, inode_str)
        return cjson.encode({ok=true})
    end
"#;

const CHUNK_ID_BASE: u64 = 1_000_000_000u64;
pub(super) const REDIS_TXN_LOCK_STRIPES: usize = 1024;
static REDIS_TXN_LOCKS: OnceLock<Vec<tokio::sync::Mutex<()>>> = OnceLock::new();

const DELAYED_COUNTER_KEY: &str = "ds_counter";
const DELAYED_KEY_PREFIX: &str = "ds";
const DELAYED_INDEX_KEY: &str = "ds_idx";
const UNCOMMITTED_KEY_PREFIX: &str = "uc";
const UNCOMMITTED_PENDING_INDEX_KEY: &str = "uc_pending_idx";
const UNCOMMITTED_ORPHAN_INDEX_KEY: &str = "uc_orphan_idx";
const COMPACT_RETRY_LIMIT: usize = 64;

// Lua script for atomically appending a slice AND extending file size in one RTT.
// KEYS[1] = chunk_key, KEYS[2] = version_key, KEYS[3] = node_key
// ARGV[1] = serialized slice data, ARGV[2] = new_size, ARGV[3] = timestamp
const WRITE_SLICE_LUA: &str = r#"
    redis.call('RPUSH', KEYS[1], ARGV[1])
    redis.call('INCR', KEYS[2])
    local node_json = redis.call('GET', KEYS[3])
    if not node_json then
        return cjson.encode({ok=false, error="node_not_found"})
    end
    local ok, node = pcall(cjson.decode, node_json)
    if not ok or not node or not node.attr or not node.attr.size then
        return cjson.encode({ok=false, error="corrupt_node"})
    end
    local new_size = tonumber(ARGV[2])
    local timestamp = tonumber(ARGV[3])
    if new_size <= node.attr.size then
        return cjson.encode({ok=true, updated=false})
    end
    node.attr.size = new_size
    node.attr.mtime = timestamp
    node.attr.ctime = timestamp
    if node.attr.mode then
        node.attr.mode = bit.band(node.attr.mode, bit.bnot(6144))
    end
    redis.call('SET', KEYS[3], cjson.encode(node))
    return cjson.encode({ok=true, updated=true})
"#;

// Lua script for atomically extending file size
const EXTEND_FILE_SIZE_LUA: &str = r#"
    local node_json = redis.call('GET', KEYS[1])
    if not node_json then
        return cjson.encode({ok=false, error="node_not_found"})
    end
    local ok, node = pcall(cjson.decode, node_json)
    if not ok or not node or not node.attr or not node.attr.size then
        return cjson.encode({ok=false, error="corrupt_node"})
    end
    local new_size = tonumber(ARGV[1])
    local timestamp = tonumber(ARGV[2])
    if new_size <= node.attr.size then
        return cjson.encode({ok=true, updated=false})
    end
    node.attr.size = new_size
    node.attr.mtime = timestamp
    node.attr.ctime = timestamp
    -- POSIX: clear setuid/setgid bits on write (security: prevent privilege escalation)
    if node.attr.mode then
        node.attr.mode = bit.band(node.attr.mode, bit.bnot(6144))  -- Clear 06000 (setuid+setgid)
    end
    redis.call('SET', KEYS[1], cjson.encode(node))
    return cjson.encode({ok=true, updated=true})
"#;

// Lua script for atomically incrementing nlink and updating link_parents
const LINK_LUA: &str = r#"
    local node_key = KEYS[1]
    local lp_key = KEYS[2]
    local dir_key = KEYS[3]
    local parent_ino = ARGV[1]
    local name = ARGV[2]
    local timestamp = tonumber(ARGV[3])

    local node_json = redis.call('GET', node_key)
    if not node_json then
        return cjson.encode({ok=false, error="node_not_found"})
    end
    local ok, node = pcall(cjson.decode, node_json)
    if not ok or not node or not node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end

    -- Validate node state (defense against races)
    if node.deleted or node.attr.nlink == 0 then
        return cjson.encode({ok=false, error="node_not_found"})
    end
    if node.kind == "Dir" then
        return cjson.encode({ok=false, error="is_directory"})
    end
    if node.kind == "Symlink" then
        return cjson.encode({ok=false, error="is_symlink"})
    end

    -- Check if link name already exists in directory
    local existing = redis.call('HEXISTS', dir_key, name)
    if existing == 1 then
        return cjson.encode({ok=false, error="already_exists"})
    end

    -- If transitioning from nlink=1 to nlink=2, save original parent/name to link_parents
    if node.attr.nlink == 1 then
        local original_member = node.parent .. ":" .. node.name
        redis.call('SADD', lp_key, original_member)
        -- Transition to hardlink state: parent=0, name=""
        node.parent = 0
        node.name = ""
    end

    -- Increment nlink
    node.attr.nlink = node.attr.nlink + 1
    node.attr.ctime = timestamp

    -- Add new link to link_parents set
    local member = parent_ino .. ":" .. name
    redis.call('SADD', lp_key, member)

    -- Add to directory
    redis.call('HSET', dir_key, name, node.ino)

    -- Save node
    redis.call('SET', node_key, cjson.encode(node))

    return cjson.encode({ok=true, attr=node.attr})
"#;

// Lua script for atomically decrementing nlink and updating link_parents
const UNLINK_LUA: &str = r#"
    local dir_key = KEYS[1]
    local parent_node_key = KEYS[2]
    local deleted_set_key = KEYS[3]
    local parent_ino = ARGV[1]
    local name = ARGV[2]
    local timestamp = tonumber(ARGV[3])
    local node_prefix = ARGV[4]
    local link_parent_prefix = ARGV[5]

    local dentry_ino = redis.call('HGET', dir_key, name)
    if not dentry_ino then
        return cjson.encode({ok=false, error="not_found", ino=parent_ino})
    end
    local child_ino = tonumber(dentry_ino)
    local node_key = node_prefix .. dentry_ino
    local lp_key = link_parent_prefix .. dentry_ino

    -- Validate node exists before making any mutations
    local node_json = redis.call('GET', node_key)
    if not node_json then
        return cjson.encode({ok=false, error="node_not_found", ino=child_ino})
    end
    local ok, node = pcall(cjson.decode, node_json)
    if not ok or not node or not node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end
    if node.kind == "Dir" then
        return cjson.encode({ok=false, error="is_directory", ino=child_ino})
    end

    -- Remove from directory
    redis.call('HDEL', dir_key, name)

    -- Remove from link_parents (idempotent)
    local member = parent_ino .. ":" .. name
    redis.call('SREM', lp_key, member)

    -- Decrement nlink
    if node.attr.nlink > 0 then
        node.attr.nlink = node.attr.nlink - 1
    end
    node.attr.ctime = timestamp

    -- If transitioning from nlink=2 to nlink=1, restore parent/name from remaining link_parent
    if node.attr.nlink == 1 then
        local remaining_members = redis.call('SMEMBERS', lp_key)
        if #remaining_members == 1 then
            local parts = {}
            for part in string.gmatch(remaining_members[1], "[^:]+") do
                table.insert(parts, part)
            end
            if #parts >= 2 then
                node.parent = tonumber(parts[1])
                node.name = table.concat(parts, ":", 2)
            end
            -- Clear link_parents set
            redis.call('DEL', lp_key)
        end
    end

    local deleted = node.attr.nlink == 0
    if deleted then
        node.deleted = true
        redis.call('HSET', deleted_set_key, tostring(node.ino), 1)
    end

    -- Save node
    redis.call('SET', node_key, cjson.encode(node))

    local parent_json = redis.call('GET', parent_node_key)
    if parent_json then
        local parent_ok, parent_node = pcall(cjson.decode, parent_json)
        if parent_ok and parent_node and parent_node.attr and parent_node.kind == "Dir" then
            parent_node.attr.mtime = timestamp
            parent_node.attr.ctime = timestamp
            redis.call('SET', parent_node_key, cjson.encode(parent_node))
        end
    end

    return cjson.encode({ok=true, ino=child_ino})
"#;

// Lua script for atomically removing directory entry and updating parent nlink
const RMDIR_LUA: &str = r#"
    local cjson = cjson

    local parent_dir_key = KEYS[1]
    local child_node_key = KEYS[2]
    local parent_node_key = KEYS[3]
    local child_dir_key = KEYS[4]
    local name = ARGV[1]
    local child_ino = tonumber(ARGV[2])
    local parent_ino = tonumber(ARGV[3])
    local timestamp = tonumber(ARGV[4])

    -- Check dentry exists and matches expected inode
    local dentry_ino = redis.call('HGET', parent_dir_key, name)
    if not dentry_ino then
        return cjson.encode({ok=false, error="not_found", ino=parent_ino})
    end
    if tonumber(dentry_ino) ~= child_ino then
        return cjson.encode({ok=false, error="not_found", ino=parent_ino})
    end

    -- Get child node
    local child_json = redis.call('GET', child_node_key)
    if not child_json then
        return cjson.encode({ok=false, error="node_not_found", ino=child_ino})
    end

    -- Decode child node with pcall
    local ok, child_node = pcall(cjson.decode, child_json)
    if not ok or not child_node or not child_node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end

    -- Check is directory
    if child_node.kind ~= "Dir" then
        return cjson.encode({ok=false, error="not_directory", ino=child_ino})
    end

    -- Check empty
    local child_len = redis.call('HLEN', child_dir_key)
    if child_len > 0 then
        return cjson.encode({ok=false, error="dir_not_empty", ino=child_ino})
    end

    -- Get parent node and update
    local parent_json = redis.call('GET', parent_node_key)
    if parent_json then
        local ok_p, parent_node = pcall(cjson.decode, parent_json)
        if ok_p and parent_node and parent_node.attr then
            parent_node.attr.nlink = parent_node.attr.nlink - 1
            parent_node.attr.mtime = timestamp
            parent_node.attr.ctime = timestamp
            redis.call('SET', parent_node_key, cjson.encode(parent_node))
        end
    end

    -- Atomic delete
    redis.call('HDEL', parent_dir_key, name)
    redis.call('DEL', child_node_key)
    redis.call('DEL', child_dir_key)

    return cjson.encode({ok=true})
"#;

// Lua script for atomically creating directory entry with inode allocation
const CREATE_ENTRY_LUA: &str = r#"
    local cjson = cjson

    local parent_dir_key = KEYS[1]
    local parent_node_key = KEYS[2]
    local counter_key = KEYS[3]
    local name = ARGV[1]
    local kind = ARGV[2]
    local timestamp = tonumber(ARGV[3])
    local parent_ino = tonumber(ARGV[4])
    local default_mode = tonumber(ARGV[5])
    local uid = tonumber(ARGV[6])
    local gid = tonumber(ARGV[7])

    -- Get parent node
    local parent_json = redis.call('GET', parent_node_key)
    if not parent_json then
        return cjson.encode({ok=false, error="parent_not_found"})
    end

    -- Decode parent node with pcall
    local ok, parent_node = pcall(cjson.decode, parent_json)
    if not ok or not parent_node or not parent_node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end

    -- Check parent is directory
    if parent_node.kind ~= "Dir" then
        return cjson.encode({ok=false, error="parent_not_directory"})
    end

    -- Check entry doesn't already exist
    local existing = redis.call('HEXISTS', parent_dir_key, name)
    if existing == 1 then
        return cjson.encode({ok=false, error="already_exists"})
    end

    -- Allocate new inode atomically
    local new_ino = redis.call('INCR', counter_key)

    local final_gid = gid
    local final_mode = default_mode
    if bit.band(parent_node.attr.mode, 1024) ~= 0 then
        final_gid = parent_node.attr.gid
    end

    -- Determine nlink based on kind
    local nlink = 1
    if kind == "Dir" then
        nlink = 2
    end

    -- Create new node
    local new_node = {
        ino = new_ino,
        parent = parent_ino,
        name = name,
        kind = kind,
        attr = {
            size = 0,
            mode = final_mode,
            uid = uid,
            gid = final_gid,
            atime = timestamp,
            mtime = timestamp,
            ctime = timestamp,
            nlink = nlink
        },
        deleted = false
    }

    -- Save new node
    redis.call('SET', 'i' .. new_ino, cjson.encode(new_node))

    -- Add directory entry
    redis.call('HSET', parent_dir_key, name, new_ino)

    -- Update parent if creating directory (nlink++)
    if kind == "Dir" then
        parent_node.attr.nlink = parent_node.attr.nlink + 1
    end

    -- Update parent timestamps
    parent_node.attr.mtime = timestamp
    parent_node.attr.ctime = timestamp
    redis.call('SET', parent_node_key, cjson.encode(parent_node))

    return cjson.encode({ok=true, ino=new_ino, gid=final_gid})
"#;

// Lua script for atomically renaming file or directory with POSIX overwrite semantics.
const RENAME_LUA: &str = r#"
    local cjson = cjson

    local old_parent_dir_key = KEYS[1]
    local new_parent_dir_key = KEYS[2]
    local old_parent_node_key = KEYS[3]
    local new_parent_node_key = KEYS[4]
    local deleted_set_key = KEYS[5]
    local old_name = ARGV[1]
    local new_name = ARGV[2]
    local old_parent_ino = tonumber(ARGV[3])
    local new_parent_ino = tonumber(ARGV[4])
    local timestamp = tonumber(ARGV[5])
    local node_prefix = ARGV[6]
    local link_parent_prefix = ARGV[7]

    -- Check source dentry exists.
    local dentry_ino = redis.call('HGET', old_parent_dir_key, old_name)
    if not dentry_ino then
        return cjson.encode({ok=false, error="not_found", ino=old_parent_ino})
    end
    local child_ino = tonumber(dentry_ino)
    local child_node_key = node_prefix .. dentry_ino
    local link_parents_key = link_parent_prefix .. dentry_ino

    -- Check new_parent exists and is directory
    local new_parent_json = redis.call('GET', new_parent_node_key)
    if not new_parent_json then
        return cjson.encode({ok=false, error="parent_not_found", ino=new_parent_ino})
    end
    local ok_np, new_parent_node = pcall(cjson.decode, new_parent_json)
    if not ok_np or not new_parent_node or not new_parent_node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end
    if new_parent_node.kind ~= "Dir" then
        return cjson.encode({ok=false, error="parent_not_directory", ino=new_parent_ino})
    end

    -- Get child (source) node early; needed for type-checking against destination
    local child_json = redis.call('GET', child_node_key)
    if not child_json then
        return cjson.encode({ok=false, error="node_not_found", ino=tonumber(dentry_ino)})
    end
    local ok_child, child_node = pcall(cjson.decode, child_json)
    if not ok_child or not child_node or not child_node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end

    -- Atomically handle existing destination (POSIX rename semantics: destination is
    -- replaced atomically; no window for concurrent renames to observe a partial state).
    local new_parent_nlink_adj = 0
    local replaced_ino = nil
    local dest_ino_str = redis.call('HGET', new_parent_dir_key, new_name)
    if dest_ino_str then
        if dest_ino_str == dentry_ino then
            return cjson.encode({ok=true, ino=child_ino})
        end
        replaced_ino = tonumber(dest_ino_str)
        local dest_node_key = node_prefix .. dest_ino_str
        local dest_node_json = redis.call('GET', dest_node_key)
        if not dest_node_json then
            return cjson.encode({ok=false, error="corrupt_node"})
        end
        local ok_dest, dest_node = pcall(cjson.decode, dest_node_json)
        if not ok_dest or not dest_node or not dest_node.attr then
            return cjson.encode({ok=false, error="corrupt_node"})
        end

        local src_kind = child_node.kind
        local dest_kind = dest_node.kind

        if src_kind == "Dir" and dest_kind == "Dir" then
            -- Destination directory must be empty
            local dest_dir_key = 'd' .. dest_ino_str
            local dest_entry_count = redis.call('HLEN', dest_dir_key)
            if dest_entry_count > 0 then
                return cjson.encode({ok=false, error="target_dir_not_empty", ino=tonumber(dest_ino_str)})
            end
            -- Remove destination directory atomically
            redis.call('HDEL', new_parent_dir_key, new_name)
            redis.call('DEL', dest_node_key)
            redis.call('DEL', dest_dir_key)
            -- Destination dir had a ".." entry pointing to new_parent; account for its removal
            new_parent_nlink_adj = new_parent_nlink_adj - 1
        elseif src_kind == "Dir" then
            -- Directory cannot replace a non-directory
            return cjson.encode({ok=false, error="target_not_directory", ino=tonumber(dest_ino_str)})
        elseif dest_kind == "Dir" then
            -- Non-directory cannot replace a directory
            return cjson.encode({ok=false, error="target_is_directory", ino=tonumber(dest_ino_str)})
        else
            -- file/symlink replacing file/symlink: decrement nlink, delete node if no links remain
            dest_node.attr.nlink = dest_node.attr.nlink - 1
            redis.call('HDEL', new_parent_dir_key, new_name)
            if dest_node.attr.nlink <= 0 then
                dest_node.attr.nlink = 0
                dest_node.attr.ctime = timestamp
                dest_node.deleted = true
                redis.call('SET', dest_node_key, cjson.encode(dest_node))
                redis.call('HSET', deleted_set_key, dest_ino_str, 1)
                redis.call('DEL', link_parent_prefix .. dest_ino_str)
            else
                dest_node.attr.ctime = timestamp
                redis.call('SET', dest_node_key, cjson.encode(dest_node))
            end
        end
    end

    -- Update node parent/name OR link_parents based on node kind/nlink
    -- Directories always track parent/name inline even though their nlink is >= 2.
    if child_node.kind == "Dir" or child_node.attr.nlink <= 1 then
        -- Single parent: update node directly
        child_node.parent = new_parent_ino
        child_node.name = new_name
    else
        -- Hardlink: update link_parents set
        local members = redis.call('SMEMBERS', link_parents_key)
        local new_members = {}
        local found = false

        for _, member in ipairs(members) do
            -- Find first colon only to handle filenames with colons
            local sep_pos = string.find(member, ":", 1, true)
            if sep_pos and sep_pos > 1 and sep_pos < #member then
                local parent_str = string.sub(member, 1, sep_pos - 1)
                local name_str = string.sub(member, sep_pos + 1)
                local parent_num = tonumber(parent_str)
                if parent_num == old_parent_ino and name_str == old_name then
                    table.insert(new_members, new_parent_ino .. ":" .. new_name)
                    found = true
                else
                    table.insert(new_members, member)
                end
            else
                table.insert(new_members, member)
            end
        end

        if not found then
            return cjson.encode({ok=false, error="link_parent_not_found", ino=child_ino})
        end

        -- Replace link_parents set atomically
        redis.call('DEL', link_parents_key)
        for _, member in ipairs(new_members) do
            redis.call('SADD', link_parents_key, member)
        end

        -- Hardlinked files have parent=0, name=""
        child_node.parent = 0
        child_node.name = ""
    end

    -- Update child timestamps
    child_node.attr.mtime = timestamp
    child_node.attr.ctime = timestamp

    -- Remove old dentry and add new dentry
    redis.call('HDEL', old_parent_dir_key, old_name)
    redis.call('HSET', new_parent_dir_key, new_name, dentry_ino)

    -- Save updated child node
    redis.call('SET', child_node_key, cjson.encode(child_node))

    -- Update parent directory timestamps and directory link counts.
    -- For same-directory rename, new_parent_node already represents the only
    -- parent that needs touching, so avoid a redundant GET/SET of the same key.
    if old_parent_ino ~= new_parent_ino then
        local old_parent_json = redis.call('GET', old_parent_node_key)
        if old_parent_json then
            local ok_op, old_parent_node = pcall(cjson.decode, old_parent_json)
            if ok_op and old_parent_node and old_parent_node.attr then
                if child_node.kind == "Dir" then
                    old_parent_node.attr.nlink = old_parent_node.attr.nlink - 1
                end
                old_parent_node.attr.mtime = timestamp
                old_parent_node.attr.ctime = timestamp
                redis.call('SET', old_parent_node_key, cjson.encode(old_parent_node))
            end
        end
    end

    if child_node.kind == "Dir" and old_parent_ino ~= new_parent_ino then
        new_parent_node.attr.nlink = new_parent_node.attr.nlink + 1
    end
    -- Apply nlink adjustment from atomically-removed destination directory
    new_parent_node.attr.nlink = new_parent_node.attr.nlink + new_parent_nlink_adj
    new_parent_node.attr.mtime = timestamp
    new_parent_node.attr.ctime = timestamp
    redis.call('SET', new_parent_node_key, cjson.encode(new_parent_node))

    return cjson.encode({ok=true, ino=child_ino, replaced_ino=replaced_ino})
"#;

const RENAME_EXCHANGE_LUA: &str = r#"
    local cjson = cjson

    local old_parent_dir_key = KEYS[1]
    local new_parent_dir_key = KEYS[2]
    local old_node_key = KEYS[3]
    local new_node_key = KEYS[4]
    local old_parent_node_key = KEYS[5]
    local new_parent_node_key = KEYS[6]
    local old_link_parents_key = KEYS[7]
    local new_link_parents_key = KEYS[8]
    local old_name = ARGV[1]
    local new_name = ARGV[2]
    local old_parent_ino = tonumber(ARGV[3])
    local new_parent_ino = tonumber(ARGV[4])
    local timestamp = tonumber(ARGV[5])
    local expected_old_ino = tonumber(ARGV[6])
    local expected_new_ino = tonumber(ARGV[7])

    -- Check both entries exist and match expected inodes
    local old_dentry_ino = redis.call('HGET', old_parent_dir_key, old_name)
    if not old_dentry_ino then
        return cjson.encode({ok=false, error="not_found", ino=old_parent_ino})
    end
    if tonumber(old_dentry_ino) ~= expected_old_ino then
        return cjson.encode({ok=false, error="stale_conflict"})
    end

    local new_dentry_ino = redis.call('HGET', new_parent_dir_key, new_name)
    if not new_dentry_ino then
        return cjson.encode({ok=false, error="not_found", ino=new_parent_ino})
    end
    if tonumber(new_dentry_ino) ~= expected_new_ino then
        return cjson.encode({ok=false, error="stale_conflict"})
    end

    -- GET both nodes
    local old_node_json = redis.call('GET', old_node_key)
    if not old_node_json then
        return cjson.encode({ok=false, error="corrupt_node"})
    end
    local ok_old, old_node = pcall(cjson.decode, old_node_json)
    if not ok_old or not old_node or not old_node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end

    local new_node_json = redis.call('GET', new_node_key)
    if not new_node_json then
        return cjson.encode({ok=false, error="corrupt_node"})
    end
    local ok_new, new_node = pcall(cjson.decode, new_node_json)
    if not ok_new or not new_node or not new_node.attr then
        return cjson.encode({ok=false, error="corrupt_node"})
    end

    -- Pre-check link_parents for hardlinked nodes before swapping dentries
    if old_node.kind ~= "Dir" and old_node.attr.nlink > 1 then
        local old_member = old_parent_ino .. ":" .. old_name
        if redis.call('SISMEMBER', old_link_parents_key, old_member) == 0 then
            return cjson.encode({ok=false, error="link_parent_not_found"})
        end
    end
    if new_node.kind ~= "Dir" and new_node.attr.nlink > 1 then
        local new_member = new_parent_ino .. ":" .. new_name
        if redis.call('SISMEMBER', new_link_parents_key, new_member) == 0 then
            return cjson.encode({ok=false, error="link_parent_not_found"})
        end
    end

    -- Swap directory entries atomically
    redis.call('HSET', old_parent_dir_key, old_name, new_dentry_ino)
    redis.call('HSET', new_parent_dir_key, new_name, old_dentry_ino)

    -- Update old_node (hardlinked files use link_parents; directories keep parent/name)
    if old_node.kind ~= "Dir" and old_node.attr.nlink > 1 then
        local old_members = redis.call('SMEMBERS', old_link_parents_key)
        local new_old_members = {}
        local found = false

        for _, member in ipairs(old_members) do
            -- Find first colon only to handle filenames with colons
            local sep_pos = string.find(member, ":", 1, true)
            if sep_pos and sep_pos > 1 and sep_pos < #member then
                local parent_str = string.sub(member, 1, sep_pos - 1)
                local name_str = string.sub(member, sep_pos + 1)
                local parent_num = tonumber(parent_str)
                if parent_num == old_parent_ino and name_str == old_name then
                    table.insert(new_old_members, new_parent_ino .. ":" .. new_name)
                    found = true
                else
                    table.insert(new_old_members, member)
                end
            else
                table.insert(new_old_members, member)
            end
        end

        if not found then
            return cjson.encode({ok=false, error="link_parent_not_found"})
        end

        redis.call('DEL', old_link_parents_key)
        for _, member in ipairs(new_old_members) do
            redis.call('SADD', old_link_parents_key, member)
        end

        old_node.parent = 0
        old_node.name = ""
    else
        old_node.parent = new_parent_ino
        old_node.name = new_name
    end

    -- Update new_node (hardlinked files use link_parents; directories keep parent/name)
    if new_node.kind ~= "Dir" and new_node.attr.nlink > 1 then
        local new_members = redis.call('SMEMBERS', new_link_parents_key)
        local new_new_members = {}
        local found = false

        for _, member in ipairs(new_members) do
            -- Find first colon only to handle filenames with colons
            local sep_pos = string.find(member, ":", 1, true)
            if sep_pos and sep_pos > 1 and sep_pos < #member then
                local parent_str = string.sub(member, 1, sep_pos - 1)
                local name_str = string.sub(member, sep_pos + 1)
                local parent_num = tonumber(parent_str)
                if parent_num == new_parent_ino and name_str == new_name then
                    table.insert(new_new_members, old_parent_ino .. ":" .. old_name)
                    found = true
                else
                    table.insert(new_new_members, member)
                end
            else
                table.insert(new_new_members, member)
            end
        end

        if not found then
            return cjson.encode({ok=false, error="link_parent_not_found"})
        end

        redis.call('DEL', new_link_parents_key)
        for _, member in ipairs(new_new_members) do
            redis.call('SADD', new_link_parents_key, member)
        end

        new_node.parent = 0
        new_node.name = ""
    else
        new_node.parent = old_parent_ino
        new_node.name = old_name
    end

    -- Update timestamps for both nodes
    old_node.attr.mtime = timestamp
    old_node.attr.ctime = timestamp
    new_node.attr.mtime = timestamp
    new_node.attr.ctime = timestamp

    -- SET both nodes
    redis.call('SET', old_node_key, cjson.encode(old_node))
    redis.call('SET', new_node_key, cjson.encode(new_node))

    -- Update parent directory timestamps
    local old_parent_json = redis.call('GET', old_parent_node_key)
    if old_parent_json then
        local ok_op, old_parent_node = pcall(cjson.decode, old_parent_json)
        if ok_op and old_parent_node and old_parent_node.attr then
            old_parent_node.attr.mtime = timestamp
            old_parent_node.attr.ctime = timestamp
            redis.call('SET', old_parent_node_key, cjson.encode(old_parent_node))
        end
    end

    local new_parent_json = redis.call('GET', new_parent_node_key)
    if new_parent_json then
        local ok_np, new_parent_node = pcall(cjson.decode, new_parent_json)
        if ok_np and new_parent_node and new_parent_node.attr then
            new_parent_node.attr.mtime = timestamp
            new_parent_node.attr.ctime = timestamp
            redis.call('SET', new_parent_node_key, cjson.encode(new_parent_node))
        end
    end

    return cjson.encode({ok=true})
"#;

/// Wrapper for deserializing plock values stored by the Lua script.
/// Format: `{"epoch": N, "records": [{lock_type, pid, lock_range}]}`
/// Also handles legacy bare-array format for transparent upgrade.
#[derive(Debug, Deserialize)]
struct PlockValue {
    #[serde(default)]
    #[allow(dead_code)]
    epoch: Option<i64>,
    #[serde(default)]
    records: Vec<PlockRecord>,
}

/// Response structure for Lua script results
#[derive(Debug, Deserialize)]
struct LuaResponse {
    ok: bool,
    #[serde(default)]
    ino: Option<i64>,
    #[allow(dead_code)]
    #[serde(default)]
    updated: Option<bool>,
    #[allow(dead_code)]
    #[serde(default)]
    count: Option<usize>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    gid: Option<u32>,
    #[serde(default)]
    attr: Option<serde_json::Value>,
    #[serde(default)]
    replaced_ino: Option<i64>,
    #[serde(default)]
    msg: Option<String>, // For Internal error details
}

/// Minimal Redis-backed meta store.
pub struct RedisMetaStore {
    conn: ConnectionManager,
    _config: Config,
    node_cache: moka::future::Cache<i64, Option<StoredNode>>,
    /// Current session id.  Wrapped in a Mutex so it can be updated on
    /// session restart (OnceLock would permanently fail on second start).
    sid: std::sync::Mutex<Option<Uuid>>,
    /// Monotonic fencing token, incremented at session creation.
    /// Prevents a stale session from releasing locks after cleanup.
    /// Same Mutex treatment as `sid` — sessions can be restarted.
    epoch: std::sync::Mutex<Option<i64>>,
    chunk_scan_cursor: std::sync::Mutex<Option<String>>,
    chunk_scan_buffer: std::sync::Mutex<Vec<u64>>,
    chunk_scan_next_cursor: std::sync::Mutex<Option<String>>,
    global_lock_tokens: std::sync::Mutex<HashMap<String, String>>,
}

impl RedisMetaStore {
    async fn resolve_redis_url(url: &str) -> Result<String, MetaError> {
        let Some((scheme, rest)) = url.split_once("://") else {
            return Ok(url.to_string());
        };
        if scheme != "redis" && scheme != "rediss" {
            return Ok(url.to_string());
        }

        let (authority, suffix) = match rest.split_once('/') {
            Some((authority, suffix)) => (authority, format!("/{suffix}")),
            None => (rest, String::new()),
        };
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((userinfo, hostport)) => (Some(userinfo), hostport),
            None => (None, authority),
        };

        if hostport.is_empty() {
            return Ok(url.to_string());
        }

        let (host, port, had_explicit_port) = if let Some(stripped) = hostport.strip_prefix('[') {
            let Some(end_bracket) = stripped.find(']') else {
                return Ok(url.to_string());
            };
            let host = &stripped[..end_bracket];
            let remainder = &stripped[end_bracket + 1..];
            let port = remainder.strip_prefix(':').unwrap_or("6379");
            (host, port, remainder.starts_with(':'))
        } else if hostport.matches(':').count() <= 1 {
            match hostport.split_once(':') {
                Some((host, port)) => (host, port, true),
                None => (hostport, "6379", false),
            }
        } else {
            return Ok(url.to_string());
        };

        if host.is_empty()
            || host.parse::<IpAddr>().is_ok()
            || host.eq_ignore_ascii_case("localhost")
        {
            return Ok(url.to_string());
        }

        let port_num = port.parse::<u16>().map_err(|e| {
            MetaError::Config(format!("Failed to parse Redis URL port in {url}: {e}"))
        })?;

        let resolved_ip = lookup_host((host, port_num))
            .await
            .map_err(|e| MetaError::Config(format!("Failed to resolve Redis host '{host}': {e}")))?
            .next()
            .ok_or_else(|| {
                MetaError::Config(format!("Redis host '{host}' resolved to no addresses"))
            })?
            .ip();

        let resolved_host = match resolved_ip {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => format!("[{ip}]"),
        };

        let mut resolved_authority = String::new();
        if let Some(userinfo) = userinfo {
            resolved_authority.push_str(userinfo);
            resolved_authority.push('@');
        }
        resolved_authority.push_str(&resolved_host);
        if had_explicit_port || port_num != 6379 {
            resolved_authority.push(':');
            resolved_authority.push_str(&port_num.to_string());
        }

        Ok(format!("{scheme}://{resolved_authority}{suffix}"))
    }

    async fn from_config_inner(config: Config) -> Result<Self, MetaError> {
        let conn = Self::create_connection(&config).await?;
        let store = Self {
            conn,
            _config: config,
            node_cache: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(30))
                .build(),
            sid: std::sync::Mutex::new(None),
            epoch: std::sync::Mutex::new(None),
            chunk_scan_cursor: std::sync::Mutex::new(None),
            chunk_scan_buffer: std::sync::Mutex::new(Vec::new()),
            chunk_scan_next_cursor: std::sync::Mutex::new(None),
            global_lock_tokens: std::sync::Mutex::new(HashMap::new()),
        };
        store.init_root_directory().await?;
        Ok(store)
    }

    /// Create or open the store from a backend path. The path is expected to
    /// contain a `brewfs.yml` that specifies the Redis URL.
    #[allow(dead_code)]
    pub async fn new(backend_path: &Path) -> Result<Self, MetaError> {
        let config =
            Config::from_path(backend_path).map_err(|e| MetaError::Config(e.to_string()))?;
        Self::from_config_inner(config).await
    }

    /// Build a store from the given configuration.
    #[allow(dead_code)]
    pub async fn from_config(config: Config) -> Result<Self, MetaError> {
        Self::from_config_inner(config).await
    }

    async fn create_connection(config: &Config) -> Result<ConnectionManager, MetaError> {
        match &config.database.db_config {
            DatabaseType::Redis { url } => {
                info!("connecting to redis: {url}");
                let resolved_url = Self::resolve_redis_url(url).await?;
                if *url != resolved_url {
                    info!("redis host resolved: {url} -> {resolved_url}");
                }
                let client = redis::Client::open(resolved_url.as_str()).map_err(|e| {
                    MetaError::Config(format!(
                        "Failed to parse Redis URL {resolved_url} (from {url}): {e}"
                    ))
                })?;
                let cm = ConnectionManager::new(client).await.map_err(|e| {
                    MetaError::Config(format!(
                        "Failed to connect to Redis backend using {resolved_url}: {e}"
                    ))
                })?;
                info!("redis connection established");
                Ok(cm)
            }
            _ => Err(MetaError::Config(
                "RedisMetaStore requires database.type = redis".to_string(),
            )),
        }
    }
    fn node_key(&self, ino: i64) -> String {
        format!("{NODE_KEY_PREFIX}{ino}")
    }

    fn dir_key(&self, ino: i64) -> String {
        format!("{DIR_KEY_PREFIX}{ino}")
    }

    fn chunk_key(&self, chunk_id: u64) -> String {
        let inode = chunk_id / CHUNK_ID_BASE;
        let chunk_index = chunk_id % CHUNK_ID_BASE;
        format!("{CHUNK_KEY_PREFIX}{inode}_{chunk_index}")
    }

    fn chunk_version_key(&self, chunk_id: u64) -> String {
        format!("{}:v", self.chunk_key(chunk_id))
    }

    fn chunk_id(&self, ino: i64, chunk_index: u64) -> u64 {
        let ino_u64 = u64::try_from(ino).expect("inode must be non-negative");
        ino_u64
            .checked_mul(CHUNK_ID_BASE)
            .and_then(|v| v.checked_add(chunk_index))
            .unwrap_or_else(|| {
                panic!(
                    "chunk_id overflow for inode {} chunk_index {}",
                    ino, chunk_index
                )
            })
    }

    fn local_locks() -> &'static [tokio::sync::Mutex<()>] {
        REDIS_TXN_LOCKS
            .get_or_init(|| {
                (0..REDIS_TXN_LOCK_STRIPES)
                    .map(|_| tokio::sync::Mutex::new(()))
                    .collect()
            })
            .as_slice()
    }

    pub(crate) fn local_lock_slot_for_key(key: &str) -> usize {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        (hash as usize) % REDIS_TXN_LOCK_STRIPES
    }

    fn local_lock_for_key(key: &str) -> &'static tokio::sync::Mutex<()> {
        &Self::local_locks()[Self::local_lock_slot_for_key(key)]
    }

    #[allow(dead_code)]
    pub(crate) async fn with_local_lock_for_key<Fut>(key: &str, fut: Fut) -> Fut::Output
    where
        Fut: std::future::Future,
    {
        let _guard = Self::local_lock_for_key(key).lock().await;
        fut.await
    }

    fn parse_chunk_id_from_chunk_key(key: &str) -> Option<u64> {
        let rest = key.strip_prefix(CHUNK_KEY_PREFIX)?;
        let (ino_str, idx_str) = rest.split_once('_')?;
        let ino: u64 = ino_str.parse().ok()?;
        let idx: u64 = idx_str.parse().ok()?;
        ino.checked_mul(CHUNK_ID_BASE)?.checked_add(idx)
    }

    fn deleted_set_key(&self) -> &'static str {
        DELETED_SET_KEY
    }

    fn counter_key(key: &str) -> Result<&'static str, MetaError> {
        let suffix = match key {
            INODE_ID_KEY => COUNTER_INODE_KEY,
            SLICE_ID_KEY => COUNTER_SLICE_KEY,
            other => {
                return Err(MetaError::NotSupported(format!(
                    "counter {other} not supported by RedisMetaStore"
                )));
            }
        };
        Ok(suffix)
    }

    fn delayed_key(&self, delayed_id: i64) -> String {
        format!("{DELAYED_KEY_PREFIX}{delayed_id}")
    }

    fn uncommitted_key(&self, slice_id: u64) -> String {
        format!("{UNCOMMITTED_KEY_PREFIX}{slice_id}")
    }

    fn locked_key(sid: Uuid) -> String {
        format!("{}{}", LOCKED_KEY, sid)
    }

    fn link_parent_key(ino: i64) -> String {
        format!("{LINK_PARENT_KEY_PREFIX}{ino}")
    }

    async fn init_root_directory(&self) -> Result<(), MetaError> {
        let mut conn = self.conn.clone();
        let root_key = self.node_key(ROOT_INODE);

        // Use SETNX so concurrent initializations can't both pass an exists
        // check and then both write — the first SETNX wins and initialises,
        // the second is a no-op.  This eliminates the TOCTOU race between
        // the old EXISTS + SET pair.
        let now = current_time();
        let attr = StoredAttr {
            size: 0,
            mode: 0o040755,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: 2,
        };
        let root = StoredNode {
            ino: ROOT_INODE,
            parent: ROOT_INODE,
            name: "/".to_string(),
            kind: NodeKind::Dir,
            attr,
            symlink_target: None,
            deleted: false,
        };

        let data = serde_json::to_vec(&root).map_err(|e| MetaError::Internal(e.to_string()))?;
        // SETNX returns 1 if the key was created, 0 if it already existed.
        let created: bool = redis::cmd("SETNX")
            .arg(&root_key)
            .arg(&data)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if !created {
            return Ok(()); // another caller initialised first
        }

        // Ensure the root directory hash exists for emptiness checks.
        // (HSET + HDEL is idempotent — harmless if another init races here.)
        let _: () = redis::cmd("HSET")
            .arg(self.dir_key(ROOT_INODE))
            .arg("__root__")
            .arg(ROOT_INODE)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let _: () = redis::cmd("HDEL")
            .arg(self.dir_key(ROOT_INODE))
            .arg("__root__")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        // Initialize counters so new inodes/slices start after the root.
        let inode_counter = Self::counter_key(INODE_ID_KEY)?;
        let _: () = redis::cmd("SETNX")
            .arg(inode_counter)
            .arg(ROOT_INODE + 1)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let slice_counter = Self::counter_key(SLICE_ID_KEY)?;
        let _: () = redis::cmd("SETNX")
            .arg(slice_counter)
            .arg(1)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn get_node(&self, ino: i64) -> Result<Option<StoredNode>, MetaError> {
        if let Some(cached) = self.node_cache.get(&ino).await {
            return Ok(cached);
        }

        let mut conn = self.conn.clone();
        let data: Option<Vec<u8>> = conn.get(self.node_key(ino)).await.map_err(redis_err)?;
        let result = if let Some(bytes) = data {
            Some(serde_json::from_slice(&bytes).map_err(|e| MetaError::Internal(e.to_string()))?)
        } else {
            None
        };
        self.node_cache.insert(ino, result.clone()).await;
        Ok(result)
    }

    async fn get_nodes(&self, inodes: &[i64]) -> Result<Vec<Option<StoredNode>>, MetaError> {
        if inodes.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![None; inodes.len()];
        let mut missing = HashMap::<i64, Vec<usize>>::new();
        for (idx, ino) in inodes.iter().copied().enumerate() {
            if let Some(cached) = self.node_cache.get(&ino).await {
                results[idx] = cached;
            } else {
                missing.entry(ino).or_default().push(idx);
            }
        }

        if missing.is_empty() {
            return Ok(results);
        }

        let missing_inodes: Vec<i64> = missing.keys().copied().collect();
        let keys: Vec<String> = missing_inodes
            .iter()
            .map(|&ino| self.node_key(ino))
            .collect();
        let mut conn = self.conn.clone();
        let values: Vec<Option<Vec<u8>>> = conn.get(&keys).await.map_err(redis_err)?;

        for (ino, value) in missing_inodes.into_iter().zip(values.into_iter()) {
            let node = match value {
                Some(bytes) => Some(
                    serde_json::from_slice(&bytes)
                        .map_err(|e| MetaError::Internal(e.to_string()))?,
                ),
                None => None,
            };
            self.node_cache.insert(ino, node.clone()).await;
            if let Some(indexes) = missing.remove(&ino) {
                for idx in indexes {
                    results[idx] = node.clone();
                }
            }
        }

        Ok(results)
    }

    async fn invalidate_nodes(&self, inodes: &[i64]) {
        for &ino in inodes {
            self.node_cache.invalidate(&ino).await;
        }
    }

    async fn save_node(&self, node: &StoredNode) -> Result<(), MetaError> {
        let mut conn = self.conn.clone();
        let data = serde_json::to_vec(node).map_err(|e| MetaError::Internal(e.to_string()))?;
        let _: () = conn
            .set(self.node_key(node.ino), data)
            .await
            .map_err(redis_err)?;
        self.node_cache.insert(node.ino, Some(node.clone())).await;
        Ok(())
    }

    async fn delete_node(&self, ino: i64) -> Result<(), MetaError> {
        let mut conn = self.conn.clone();
        let result = conn.del(self.node_key(ino)).await.map_err(redis_err);
        if result.is_ok() {
            self.node_cache.invalidate(&ino).await;
        }
        result
    }

    async fn load_link_parents(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        let mut conn = self.conn.clone();
        let members: Vec<String> = conn
            .smembers(Self::link_parent_key(ino))
            .await
            .map_err(redis_err)?;

        let mut out = Vec::with_capacity(members.len());
        for m in members {
            let Some((p, name)) = m.split_once(':') else {
                continue;
            };
            if let Ok(parent) = p.parse::<i64>() {
                out.push((parent, name.to_string()));
            }
        }

        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn bump_dir_times(&self, ino: i64, now: i64) -> Result<(), MetaError> {
        if let Some(mut node) = self.get_node(ino).await?
            && node.kind == NodeKind::Dir
        {
            node.attr.mtime = now;
            node.attr.ctime = now;
            self.save_node(&node).await?;
        }
        Ok(())
    }

    async fn directory_child(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let mut conn = self.conn.clone();
        let value: Option<i64> = conn
            .hget(self.dir_key(parent), name)
            .await
            .map_err(redis_err)?;
        Ok(value)
    }

    async fn ensure_parent_dir(&self, parent: i64) -> Result<StoredNode, MetaError> {
        let parent_node = self
            .get_node(parent)
            .await?
            .ok_or(MetaError::ParentNotFound(parent))?;
        if parent_node.kind != NodeKind::Dir {
            return Err(MetaError::NotDirectory(parent));
        }
        Ok(parent_node)
    }

    async fn create_entry(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
    ) -> Result<i64, MetaError> {
        let parent_dir_key = self.dir_key(parent);
        let parent_node_key = self.node_key(parent);
        let counter_key = COUNTER_INODE_KEY;

        let kind_str = match kind {
            FileType::File => "File",
            FileType::Dir => "Dir",
            FileType::Symlink => "Symlink",
        };
        let default_mode = if kind == FileType::Dir {
            0o040755
        } else if kind == FileType::Symlink {
            0o120777
        } else {
            0o100644
        };
        let now = current_time();

        let script = redis::Script::new(CREATE_ENTRY_LUA);
        let result: String = script
            .key(&parent_dir_key) // KEYS[1]
            .key(&parent_node_key) // KEYS[2]
            .key(counter_key) // KEYS[3]
            .arg(&name) // ARGV[1]
            .arg(kind_str) // ARGV[2]
            .arg(now) // ARGV[3]
            .arg(parent) // ARGV[4]
            .arg(default_mode) // ARGV[5]
            .arg(0u32) // ARGV[6] - uid (default 0)
            .arg(0u32) // ARGV[7] - gid (default 0)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("parent_not_found") => Err(MetaError::ParentNotFound(parent)),
            Some("parent_not_directory") => Err(MetaError::NotDirectory(parent)),
            Some("already_exists") => Err(MetaError::AlreadyExists { parent, name }),
            Some("corrupt_node") => Err(MetaError::Internal("corrupt parent node".into())),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                let new_ino = response
                    .ino
                    .ok_or_else(|| MetaError::Internal("missing ino in response".into()))?;
                let final_gid = response
                    .gid
                    .ok_or_else(|| MetaError::Internal("missing gid in create response".into()))?;

                let nlink = if kind == FileType::Dir { 2 } else { 1 };
                let node_kind = NodeKind::from(kind);
                let new_node = StoredNode {
                    ino: new_ino,
                    parent,
                    name,
                    kind: node_kind,
                    attr: StoredAttr {
                        size: 0,
                        mode: default_mode,
                        uid: 0,
                        gid: final_gid,
                        atime: now,
                        mtime: now,
                        ctime: now,
                        nlink,
                    },
                    symlink_target: None,
                    deleted: false,
                };

                if let Some(Some(mut parent_node)) = self.node_cache.get(&parent).await {
                    if kind == FileType::Dir {
                        parent_node.attr.nlink = parent_node.attr.nlink.saturating_add(1);
                    }
                    parent_node.attr.mtime = now;
                    parent_node.attr.ctime = now;
                    self.node_cache.insert(parent, Some(parent_node)).await;
                }
                self.node_cache.insert(new_ino, Some(new_node)).await;
                Ok(new_ino)
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }
    async fn alloc_id(&self, key: &str) -> Result<i64, MetaError> {
        let mut conn = self.conn.clone();
        let redis_key = Self::counter_key(key)?;
        conn.incr(redis_key, 1).await.map_err(redis_err)
    }

    fn plock_key(&self, inode: i64) -> String {
        format!("{}:{}", PLOCK_PREFIX, inode)
    }

    fn plock_field(&self, sid: &Uuid, owner: i64) -> String {
        format!("{}:{}", sid, owner)
    }

    /// Atomically set or release a POSIX advisory lock.
    ///
    /// Uses a Redis Lua script to perform the read-check-write cycle
    /// atomically, following the same pattern as the other metadata
    /// mutations (CREATE_ENTRY_LUA, RENAME_LUA, etc.).  This avoids the
    /// TOCTOU race that a plain HGETALL + HSET would have.
    async fn try_set_plock(
        &self,
        inode: i64,
        owner: i64,
        new_lock: PlockRecord,
        lock_type: FileLockType,
        range: FileLockRange,
    ) -> Result<(), MetaError> {
        let sid = self.get_sid()?;
        let epoch = self.get_epoch()?;
        let plock_key = self.plock_key(inode);
        let locked_key = Self::locked_key(sid);
        let field = self.plock_field(&sid, owner);

        let lock_type_num = lock_type.as_u32();

        let script = redis::Script::new(SET_PLOCK_LUA);
        let result: String = script
            .key(&plock_key)
            .key(&locked_key)
            .arg(&field)
            .arg(lock_type_num)
            .arg(new_lock.pid)
            .arg(new_lock.lock_range.start)
            .arg(new_lock.lock_range.end)
            .arg(inode)
            .arg(epoch)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("plock Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("lock_conflict") => Err(MetaError::LockConflict {
                inode,
                owner,
                range,
            }),
            Some(other) => Err(MetaError::Internal(format!("plock Lua error: {other}"))),
            None if response.ok => Ok(()),
            None => Err(MetaError::Internal("unexpected plock Lua response".into())),
        }
    }

    async fn rewrite_trimmed_slices(
        &self,
        chunk_id: u64,
        cutoff_offset: u64,
    ) -> Result<(), MetaError> {
        let chunk_key = self.chunk_key(chunk_id);
        let version_key = self.chunk_version_key(chunk_id);
        let script = redis::Script::new(CHUNK_CAS_LUA);

        for _ in 0..TRUNCATE_REWRITE_MAX_RETRIES {
            let mut conn = self.conn.clone();

            // Read version and current slices in one round-trip.
            let (version, raw): (Option<i64>, Vec<Vec<u8>>) = redis::pipe()
                .cmd("GET")
                .arg(&version_key)
                .cmd("LRANGE")
                .arg(&chunk_key)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            let current_version = version.unwrap_or(0);

            let mut slices = Vec::with_capacity(raw.len());
            for entry in &raw {
                let desc: SliceDesc = crate::meta::serialization::deserialize_meta(entry)?;
                slices.push(desc);
            }
            trim_slices_in_place(&mut slices, cutoff_offset);

            let new_version = current_version + 1;

            // Atomic CAS via Lua: replace list iff version still matches.
            let ok: i32 = script
                .key(&chunk_key)
                .key(&version_key)
                .arg(current_version)
                .arg(new_version)
                .arg(
                    slices
                        .iter()
                        .map(|s| crate::meta::serialization::serialize_meta(s))
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;

            if ok == 1 {
                return Ok(());
            }
        }

        Err(MetaError::Internal(format!(
            "truncate rewrite retried too many times for chunk {chunk_id}"
        )))
    }

    async fn shutdown_session_by_id(&self, session_id: Uuid) -> Result<(), MetaError> {
        let locked_key = Self::locked_key(session_id);
        let sid_str = session_id.to_string();
        let plock_prefix = format!("{PLOCK_PREFIX}:");

        let script = redis::Script::new(CLEANUP_SESSION_LUA);
        let result: String = script
            .key(&locked_key)
            .key(ALL_SESSIONS_KEY)
            .key(SESSION_INFOS_KEY)
            .arg(&sid_str)
            .arg(&plock_prefix)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("cleanup_session Lua parse error: {e}")))?;

        if response.ok {
            Ok(())
        } else {
            Err(MetaError::Internal(response.error.unwrap_or_else(|| {
                "cleanup_session Lua script failed".into()
            })))
        }
    }

    async fn prune_slices_for_truncate(
        &self,
        ino: i64,
        new_size: u64,
        old_size: u64,
        chunk_size: u64,
    ) -> Result<(), MetaError> {
        apply_truncate_plan(
            new_size,
            old_size,
            chunk_size,
            |cutoff_chunk, cutoff_offset| async move {
                let chunk_id = self.chunk_id(ino, cutoff_chunk);
                self.rewrite_trimmed_slices(chunk_id, cutoff_offset).await?;
                Ok(())
            },
            |start, end| async move {
                for idx in start..end {
                    let chunk_id = self.chunk_id(ino, idx);
                    let key = self.chunk_key(chunk_id);
                    let version_key = self.chunk_version_key(chunk_id);
                    let mut conn = self.conn.clone();
                    redis::pipe()
                        .atomic()
                        .cmd("DEL")
                        .arg(&key)
                        .ignore()
                        .cmd("DEL")
                        .arg(&version_key)
                        .ignore()
                        .query_async::<()>(&mut conn)
                        .await
                        .map_err(redis_err)?;
                }
                Ok(())
            },
        )
        .await
    }

    fn set_sid(&self, session_id: Uuid) {
        *self.sid.lock().unwrap() = Some(session_id);
    }
    fn get_sid(&self) -> Result<Uuid, MetaError> {
        self.sid
            .lock()
            .map_err(|_| MetaError::Internal("sid lock poisoned".to_string()))?
            .ok_or_else(|| MetaError::Internal("sid has not been set".to_string()))
    }

    fn set_epoch(&self, epoch: i64) {
        *self.epoch.lock().unwrap() = Some(epoch);
    }
    fn get_epoch(&self) -> Result<i64, MetaError> {
        self.epoch
            .lock()
            .map_err(|_| MetaError::Internal("epoch lock poisoned".to_string()))?
            .ok_or_else(|| MetaError::Internal("epoch has not been set".to_string()))
    }

    async fn refresh_session(
        session_id: Uuid,
        mut conn: ConnectionManager,
    ) -> Result<(), MetaError> {
        let session_id_string = session_id.to_string();
        let expire = (Utc::now() + chrono::Duration::minutes(5)).timestamp_millis();
        redis::Cmd::zadd(ALL_SESSIONS_KEY, session_id_string, expire)
            .exec_async(&mut conn)
            .await
            .map_err(|err| MetaError::Internal(err.to_string()))?;
        Ok(())
    }

    async fn life_cycle(token: CancellationToken, session_id: Uuid, conn: ConnectionManager) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            select! {
                _ = interval.tick() => {
                    // refresh session
                    match Self::refresh_session(session_id, conn.clone()).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Failed to refresh session: {}", err);
                        }
                    }

                }
                _ = token.cancelled() => {
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl MetaStore for RedisMetaStore {
    fn name(&self) -> &'static str {
        "redis-meta-store"
    }

    fn capabilities(&self) -> crate::meta::store::MetaStoreCapabilities {
        crate::meta::store::MetaStoreCapabilities {
            namespace: true,
            file_data: true,
            batch_stat: true,
            hardlinks: true,
            symlinks: true,
            rename_exchange: true,
            open_close_tracking: false,
            stat_fs: true,
            sessions: true,
            global_locks: true,
            plocks: true,
            flocks: true,
            xattr: false,
            acl: false,
            quota: false,
            dump_load: false,
            compaction: true,
            watch_invalidation: false,
        }
    }

    async fn from_config(config: Config) -> Result<Self, MetaError> {
        Self::from_config_inner(config).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        Ok(self.get_node(ino).await?.map(|n| n.as_file_attr()))
    }

    /// Batch stat implementation using Redis MGET for optimal performance
    #[tracing::instrument(
        level = "trace",
        skip(self, inodes),
        fields(inode_count = inodes.len())
    )]
    async fn batch_stat(&self, inodes: &[i64]) -> Result<Vec<Option<FileAttr>>, MetaError> {
        let nodes = self.get_nodes(inodes).await?;
        Ok(nodes
            .into_iter()
            .map(|node| node.map(|node| node.as_file_attr()))
            .collect())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        self.directory_child(parent, name).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        if path.is_empty() {
            return Ok(None);
        }
        if path == "/" {
            return Ok(Some((ROOT_INODE, FileType::Dir)));
        }
        let mut current = ROOT_INODE;
        for segment in path.split('/').filter(|s| !s.is_empty()) {
            let Some(next) = self.lookup(current, segment).await? else {
                return Ok(None);
            };
            current = next;
        }
        if let Some(node) = self.get_node(current).await? {
            Ok(Some((node.ino, node.kind.into())))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if node.kind != NodeKind::Dir {
            return Err(MetaError::NotDirectory(ino));
        }
        let mut conn = self.conn.clone();
        let entries: Vec<(String, i64)> =
            conn.hgetall(self.dir_key(ino)).await.map_err(redis_err)?;
        let child_inodes: Vec<i64> = entries.iter().map(|(_, child)| *child).collect();
        let nodes = self.get_nodes(&child_inodes).await?;

        let mut result = Vec::with_capacity(entries.len());
        for ((name, child), node) in entries.into_iter().zip(nodes.into_iter()) {
            if let Some(node) = node {
                result.push(DirEntry {
                    name,
                    ino: child,
                    kind: node.kind.into(),
                });
            }
        }
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_entry(parent, name, FileType::Dir).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        // Step 1: Lookup child ino first (preserves NotFound(parent) behavior)
        let Some(child) = self.lookup(parent, name).await? else {
            return Err(MetaError::NotFound(parent));
        };

        // Step 2: Construct Redis keys
        let parent_dir_key = self.dir_key(parent);
        let child_node_key = self.node_key(child);
        let parent_node_key = self.node_key(parent);
        let child_dir_key = self.dir_key(child);
        let now = current_time();

        // Step 3: Invoke Lua script atomically
        let script = redis::Script::new(RMDIR_LUA);
        let result: String = script
            .key(&parent_dir_key)
            .key(&child_node_key)
            .key(&parent_node_key)
            .key(&child_dir_key)
            .arg(name)
            .arg(child)
            .arg(parent)
            .arg(now)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        // Step 4: Parse response and map errors
        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("not_found") => Err(MetaError::NotFound(response.ino.unwrap_or(parent))),
            Some("node_not_found") => Err(MetaError::NotFound(response.ino.unwrap_or(child))),
            Some("not_directory") => Err(MetaError::NotDirectory(response.ino.unwrap_or(child))),
            Some("dir_not_empty") => {
                Err(MetaError::DirectoryNotEmpty(response.ino.unwrap_or(child)))
            }
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                self.invalidate_nodes(&[parent, child]).await;
                Ok(())
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_entry(parent, name, FileType::File).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, parent, name))]
    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        if ino == ROOT_INODE {
            return Err(MetaError::NotSupported(
                "cannot create hard links to the root inode".into(),
            ));
        }
        self.ensure_parent_dir(parent).await?;

        let node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if node.kind == NodeKind::Dir {
            return Err(MetaError::NotSupported(
                "cannot create hard links to directories".into(),
            ));
        }
        if node.kind == NodeKind::Symlink {
            return Err(MetaError::NotSupported(
                "cannot create hard links to symbolic links".into(),
            ));
        }
        if node.deleted || node.attr.nlink == 0 {
            return Err(MetaError::NotFound(ino));
        }

        let node_key = self.node_key(ino);
        let lp_key = Self::link_parent_key(ino);
        let dir_key = self.dir_key(parent);
        let now = current_time();

        let script = redis::Script::new(LINK_LUA);
        let result: String = script
            .key(&node_key)
            .key(&lp_key)
            .key(&dir_key)
            .arg(parent)
            .arg(name)
            .arg(now)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("node_not_found") => Err(MetaError::NotFound(ino)),
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some("already_exists") => Err(MetaError::AlreadyExists {
                parent,
                name: name.to_string(),
            }),
            Some("is_directory") => Err(MetaError::NotSupported(
                "cannot create hard links to directories".into(),
            )),
            Some("is_symlink") => Err(MetaError::NotSupported(
                "cannot create hard links to symbolic links".into(),
            )),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                let attr_json = response
                    .attr
                    .ok_or_else(|| MetaError::Internal("missing attr in link response".into()))?;
                let stored_attr: StoredAttr = serde_json::from_value(attr_json)
                    .map_err(|e| MetaError::Internal(format!("attr parse error: {e}")))?;

                self.invalidate_nodes(&[ino, parent]).await;
                self.bump_dir_times(parent, now).await?;
                Ok(stored_attr.to_file_attr(ino, node.kind.into()))
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name, target))]
    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        let ino = self
            .create_entry(parent, name.to_string(), FileType::Symlink)
            .await?;
        let now = current_time();

        let mut node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;
        node.attr.size = target.len() as u64;
        node.attr.atime = now;
        node.attr.mtime = now;
        node.attr.ctime = now;
        node.symlink_target = Some(target.to_string());

        self.save_node(&node).await?;

        Ok((ino, node.as_file_attr()))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        let node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;

        if node.kind != NodeKind::Symlink {
            return Err(MetaError::NotSupported(format!(
                "inode {ino} is not a symbolic link"
            )));
        }

        node.symlink_target
            .ok_or_else(|| MetaError::Internal(format!("symlink target missing for inode {ino}")))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        let dir_key = self.dir_key(parent);
        let parent_node_key = self.node_key(parent);
        let deleted_set_key = self.deleted_set_key();
        let now = current_time();

        let script = redis::Script::new(UNLINK_LUA);
        let result: String = script
            .key(&dir_key)
            .key(&parent_node_key)
            .key(deleted_set_key)
            .arg(parent)
            .arg(name)
            .arg(now)
            .arg(NODE_KEY_PREFIX)
            .arg(LINK_PARENT_KEY_PREFIX)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("not_found") => Err(MetaError::NotFound(parent)),
            Some("node_not_found") => Err(MetaError::NotFound(response.ino.unwrap_or(parent))),
            Some("is_directory") => Err(MetaError::NotSupported(format!(
                "{} is not unlinkable",
                response.ino.unwrap_or(parent)
            ))),
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                let child = response
                    .ino
                    .ok_or_else(|| MetaError::Internal("missing ino in unlink response".into()))?;
                self.invalidate_nodes(&[parent, child]).await;
                Ok(())
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip(self),
        fields(old_parent, old_name, new_parent, new_name)
    )]
    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        // Self-rename optimization: no-op if same location
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let old_parent_dir_key = self.dir_key(old_parent);
        let new_parent_dir_key = self.dir_key(new_parent);
        let old_parent_node_key = self.node_key(old_parent);
        let new_parent_node_key = self.node_key(new_parent);
        let deleted_set_key = self.deleted_set_key();
        let now = current_time();

        let script = redis::Script::new(RENAME_LUA);
        let result: String = script
            .key(&old_parent_dir_key) // KEYS[1]
            .key(&new_parent_dir_key) // KEYS[2]
            .key(&old_parent_node_key) // KEYS[3]
            .key(&new_parent_node_key) // KEYS[4]
            .key(deleted_set_key) // KEYS[5]
            .arg(old_name) // ARGV[1]
            .arg(&new_name) // ARGV[2]
            .arg(old_parent) // ARGV[3]
            .arg(new_parent) // ARGV[4]
            .arg(now) // ARGV[5]
            .arg(NODE_KEY_PREFIX) // ARGV[6]
            .arg(LINK_PARENT_KEY_PREFIX) // ARGV[7]
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("not_found") => Err(MetaError::NotFound(response.ino.unwrap_or(old_parent))),
            Some("parent_not_found") => Err(MetaError::ParentNotFound(new_parent)),
            Some("parent_not_directory") => Err(MetaError::NotDirectory(new_parent)),
            Some("target_dir_not_empty") => Err(MetaError::DirectoryNotEmpty(
                response.ino.unwrap_or(new_parent),
            )),
            Some("target_is_directory") => Err(MetaError::Io(std::io::Error::from(
                std::io::ErrorKind::IsADirectory,
            ))),
            Some("target_not_directory") => Err(MetaError::Io(std::io::Error::from(
                std::io::ErrorKind::NotADirectory,
            ))),
            Some("node_not_found") => Err(MetaError::NotFound(response.ino.unwrap_or(old_parent))),
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some("link_parent_not_found") => Err(MetaError::Internal(format!(
                "expected link parent binding {old_parent}/{old_name} for inode {}",
                response.ino.unwrap_or(old_parent)
            ))),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                let child = response
                    .ino
                    .ok_or_else(|| MetaError::Internal("missing ino in rename response".into()))?;
                let mut invalidated = vec![old_parent, new_parent, child];
                if let Some(replaced_ino) = response.replaced_ino {
                    invalidated.push(replaced_ino);
                }
                self.invalidate_nodes(&invalidated).await;
                Ok(())
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let Some(old_ino) = self.lookup(old_parent, old_name).await? else {
            return Err(MetaError::Internal(format!(
                "Entry '{}' not found in parent {} for exchange",
                old_name, old_parent
            )));
        };

        let Some(new_ino) = self.lookup(new_parent, new_name).await? else {
            return Err(MetaError::Internal(format!(
                "Entry '{}' not found in parent {} for exchange",
                new_name, new_parent
            )));
        };

        let old_parent_dir_key = self.dir_key(old_parent);
        let new_parent_dir_key = self.dir_key(new_parent);
        let old_node_key = self.node_key(old_ino);
        let new_node_key = self.node_key(new_ino);
        let old_parent_node_key = self.node_key(old_parent);
        let new_parent_node_key = self.node_key(new_parent);
        let old_link_parents_key = Self::link_parent_key(old_ino);
        let new_link_parents_key = Self::link_parent_key(new_ino);
        let now = current_time();

        let script = redis::Script::new(RENAME_EXCHANGE_LUA);
        let result: String = script
            .key(&old_parent_dir_key)
            .key(&new_parent_dir_key)
            .key(&old_node_key)
            .key(&new_node_key)
            .key(&old_parent_node_key)
            .key(&new_parent_node_key)
            .key(&old_link_parents_key)
            .key(&new_link_parents_key)
            .arg(old_name)
            .arg(new_name)
            .arg(old_parent)
            .arg(new_parent)
            .arg(now)
            .arg(old_ino)
            .arg(new_ino)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Failed to parse Lua response: {e}")))?;
        match response.error.as_deref() {
            Some("stale_conflict") => Err(MetaError::ContinueRetry(RetryReason::VersionConflict)),
            Some("not_found") => Err(MetaError::NotFound(response.ino.unwrap_or(old_parent))),
            Some("internal") => {
                let msg = response.msg.unwrap_or_else(|| "unknown error".to_string());
                Err(MetaError::Internal(msg))
            }
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some("link_parent_not_found") => Err(MetaError::Internal(
                "expected link parent binding not found during exchange".into(),
            )),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                self.invalidate_nodes(&[old_parent, new_parent, old_ino, new_ino])
                    .await;
                Ok(())
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }
    #[tracing::instrument(
        level = "trace",
        skip(self, req),
        fields(ino, size = req.size, flags = ?flags)
    )]
    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        let mut node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;
        let mut ctime_update = false;
        let now = current_time();

        if let Some(mode) = req.mode {
            let kind_bits = node.attr.mode & 0o170000;
            node.attr.mode = kind_bits | (mode & 0o777);
            ctime_update = true;
        }

        if let Some(uid) = req.uid {
            node.attr.uid = uid;
            ctime_update = true;
        }
        if let Some(gid) = req.gid {
            node.attr.gid = gid;
            ctime_update = true;
        }

        if flags.contains(SetAttrFlags::CLEAR_SUID) {
            node.attr.mode &= !0o4000;
            ctime_update = true;
        }
        if flags.contains(SetAttrFlags::CLEAR_SGID) {
            node.attr.mode &= !0o2000;
            ctime_update = true;
        }

        if let Some(size) = req.size {
            if node.kind != NodeKind::File {
                return Err(MetaError::NotSupported(
                    "truncate flag only supported for regular files".into(),
                ));
            }
            if node.attr.size != size {
                node.attr.size = size;
                node.attr.mtime = now;
            }
            ctime_update = true;
        }

        if flags.contains(SetAttrFlags::SET_ATIME_NOW) {
            node.attr.atime = now;
            ctime_update = true;
        } else if let Some(atime) = req.atime {
            node.attr.atime = atime;
            ctime_update = true;
        }

        if flags.contains(SetAttrFlags::SET_MTIME_NOW) {
            node.attr.mtime = now;
            ctime_update = true;
        } else if let Some(mtime) = req.mtime {
            node.attr.mtime = mtime;
            ctime_update = true;
        }

        if let Some(ctime) = req.ctime {
            node.attr.ctime = ctime;
        } else if ctime_update {
            node.attr.ctime = now;
        }

        self.node_cache.invalidate(&ino).await;
        self.save_node(&node).await?;
        Ok(node.attr.to_file_attr(node.ino, node.kind.into()))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, size))]
    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let mut node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;
        let now = current_time();
        node.attr.size = size;
        node.attr.mtime = now;
        node.attr.ctime = now;
        self.save_node(&node).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, size))]
    async fn extend_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let script = redis::Script::new(EXTEND_FILE_SIZE_LUA);
        let node_key = self.node_key(ino);
        let now = current_time();

        let result: String = script
            .key(&node_key)
            .arg(size)
            .arg(now)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        let response: LuaResponse = serde_json::from_str(&result)
            .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?;

        match response.error.as_deref() {
            Some("node_not_found") => Err(MetaError::NotFound(ino)),
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                self.node_cache.invalidate(&ino).await;
                Ok(())
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, size, chunk_size))]
    async fn truncate(&self, ino: i64, size: u64, chunk_size: u64) -> Result<(), MetaError> {
        let mut node = self.get_node(ino).await?.ok_or(MetaError::NotFound(ino))?;
        let old_size = node.attr.size;
        let now = current_time();
        self.prune_slices_for_truncate(ino, size, old_size, chunk_size)
            .await?;
        node.attr.size = size;
        node.attr.mtime = now;
        node.attr.ctime = now;
        self.save_node(&node).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        let Some(node) = self.get_node(ino).await? else {
            return Ok(vec![]);
        };

        if node.ino == ROOT_INODE {
            return Ok(vec![(None, "/".to_string())]);
        }

        if node.deleted || node.attr.nlink == 0 {
            return Ok(vec![]);
        }

        if node.kind == NodeKind::Dir || node.attr.nlink <= 1 {
            return Ok(vec![(Some(node.parent), node.name)]);
        }

        let link_parents = self.load_link_parents(ino).await?;
        let mut out = Vec::with_capacity(link_parents.len());
        for (p, n) in link_parents {
            out.push((Some(p), n));
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        if ino == ROOT_INODE {
            return Ok(vec!["/".to_string()]);
        }

        let names = self.get_names(ino).await?;

        build_paths_from_names(ROOT_INODE, names, |current_ino| async move {
            let node = self.get_node(current_ino).await?;

            Ok(node.map(|node| (node.parent, node.name)))
        })
        .await
    }

    fn root_ino(&self) -> i64 {
        ROOT_INODE
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize(&self) -> Result<(), MetaError> {
        self.init_root_directory().await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        let mut conn = self.conn.clone();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{NODE_KEY_PREFIX}*"))
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        if keys.is_empty() {
            return Ok(StatFsSnapshot {
                total_space: 0,
                available_space: 0,
                used_inodes: 0,
                available_inodes: 0,
            });
        }

        let nodes: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut used_space = 0u64;
        let mut used_inodes = 0u64;

        for (key, data) in keys.iter().zip(nodes.into_iter()) {
            let Some(bytes) = data else {
                continue;
            };
            let node: StoredNode = serde_json::from_slice(&bytes)
                .map_err(|e| MetaError::Internal(format!("Failed to parse node {key}: {e}")))?;

            if node.deleted || node.attr.nlink == 0 {
                continue;
            }

            used_space = used_space.saturating_add(node.attr.size);
            used_inodes = used_inodes.saturating_add(1);
        }

        Ok(StatFsSnapshot {
            total_space: used_space,
            available_space: 0,
            used_inodes,
            available_inodes: 0,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        let mut conn = self.conn.clone();
        let raw: Vec<String> = conn
            .hkeys(self.deleted_set_key())
            .await
            .map_err(redis_err)?;
        let mut inodes = Vec::with_capacity(raw.len());
        for key in raw {
            match key.parse::<i64>() {
                Ok(id) => inodes.push(id),
                Err(e) => {
                    tracing::warn!("invalid inode id in delSlices: {key}, err={e}");
                }
            }
        }
        Ok(inodes)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(batch_size, max_age_secs))]
    // GC Phase 1: find aged delayed slices, delete their chunk meta, and mark them for block deletion.
    async fn process_delayed_slices(
        &self,
        batch_size: usize,
        max_age_secs: i64,
    ) -> Result<Vec<(u64, u64, u64, i64)>, MetaError> {
        if batch_size == 0 {
            return Ok(vec![]);
        }

        let mut conn = self.conn.clone();
        let cutoff = Utc::now().timestamp() - max_age_secs;

        let delayed_ids: Vec<i64> = redis::cmd("ZRANGEBYSCORE")
            .arg(DELAYED_INDEX_KEY)
            .arg("-inf")
            .arg(cutoff)
            .arg("LIMIT")
            .arg(0)
            .arg(batch_size)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        if delayed_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();

        for delayed_id in delayed_ids {
            let ds_key = self.delayed_key(delayed_id);
            let fields: std::collections::HashMap<String, String> =
                conn.hgetall(&ds_key).await.map_err(redis_err)?;

            if fields.is_empty() {
                tracing::warn!(
                    delayed_id = delayed_id,
                    "delayed slice hash missing, cleaning up stale index"
                );
                let _: () = redis::pipe()
                    .atomic()
                    .cmd("DEL")
                    .arg(&ds_key)
                    .ignore()
                    .cmd("ZREM")
                    .arg(DELAYED_INDEX_KEY)
                    .arg(delayed_id)
                    .ignore()
                    .query_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
                continue;
            }

            let status = fields.get("st").cloned().unwrap_or_default();
            let slice_id = match fields.get("sid").and_then(|v| v.parse::<u64>().ok()) {
                Some(v) => v,
                None => {
                    tracing::warn!(
                        delayed_id = delayed_id,
                        "failed to parse sid from delayed slice hash, cleaning up"
                    );
                    let _: () = redis::pipe()
                        .atomic()
                        .cmd("DEL")
                        .arg(&ds_key)
                        .ignore()
                        .cmd("ZREM")
                        .arg(DELAYED_INDEX_KEY)
                        .arg(delayed_id)
                        .ignore()
                        .query_async(&mut conn)
                        .await
                        .map_err(redis_err)?;
                    continue;
                }
            };
            let offset = fields
                .get("off")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let size = fields
                .get("sz")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            if status == "meta_deleted" {
                result.push((slice_id, offset, size, delayed_id));
                continue;
            }

            if status != "pending" {
                continue;
            }

            let chunk_id = fields
                .get("cid")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let chunk_key = self.chunk_key(chunk_id);
            let version_key = self.chunk_version_key(chunk_id);

            let raw: Vec<Vec<u8>> = redis::cmd("LRANGE")
                .arg(&chunk_key)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            let mut target_entry = None;
            for entry in raw {
                let desc: SliceDesc = crate::meta::serialization::deserialize_meta(&entry)?;
                if desc.slice_id == slice_id {
                    target_entry = Some(entry);
                    break;
                }
            }

            let mut pipe = redis::pipe();
            pipe.atomic();

            if let Some(entry_bytes) = target_entry {
                pipe.cmd("LREM")
                    .arg(&chunk_key)
                    .arg(0)
                    .arg(&entry_bytes)
                    .ignore();
                pipe.cmd("INCR").arg(&version_key).ignore();
            }

            pipe.hset(&ds_key, "st", "meta_deleted").ignore();

            let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;
            result.push((slice_id, offset, size, delayed_id));
        }

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(delayed_count = delayed_ids.len()))]
    // GC Phase 2: permanently remove delayed slice records after their blocks have been deleted.
    async fn confirm_delayed_deleted(&self, delayed_ids: &[i64]) -> Result<(), MetaError> {
        if delayed_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        for delayed_id in delayed_ids {
            let ds_key = self.delayed_key(*delayed_id);
            pipe.cmd("DEL").arg(&ds_key).ignore();
            pipe.cmd("ZREM")
                .arg(DELAYED_INDEX_KEY)
                .arg(delayed_id)
                .ignore();
        }

        let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn remove_file_metadata(&self, ino: i64) -> Result<(), MetaError> {
        let mut conn = self.conn.clone();
        let _: () = conn
            .hdel(self.deleted_set_key(), ino.to_string())
            .await
            .map_err(redis_err)?;
        self.delete_node(ino).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(self),
        fields(chunk_id, slice_count = tracing::field::Empty)
    )]
    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        let mut conn = self.conn.clone();
        let raw: Vec<Vec<u8>> = redis::cmd("LRANGE")
            .arg(self.chunk_key(chunk_id))
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .instrument(tracing::trace_span!("get_slices.redis_lrange", chunk_id))
            .await
            .map_err(redis_err)?;
        let mut slices = Vec::new();
        for entry in raw {
            let desc: SliceDesc = crate::meta::serialization::deserialize_meta(&entry)?;
            slices.push(desc);
        }
        tracing::Span::current().record("slice_count", slices.len());
        Ok(slices)
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, slice),
        fields(chunk_id, slice_id = slice.slice_id, offset = slice.offset, len = slice.length)
    )]
    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError> {
        let chunk_key = self.chunk_key(chunk_id);
        let version_key = self.chunk_version_key(chunk_id);
        let data = crate::meta::serialization::serialize_meta(&slice)?;
        let _txn_guard = Self::local_lock_for_key(&chunk_key).lock().await;
        let mut conn = self.conn.clone();
        redis::pipe()
            .atomic()
            .cmd("RPUSH")
            .arg(&chunk_key)
            .arg(data)
            .ignore()
            .cmd("INCR")
            .arg(&version_key)
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, slice),
        fields(ino, chunk_id, slice_id = slice.slice_id, offset = slice.offset, len = slice.length, new_size)
    )]
    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError> {
        // Combined Lua script: append slice + extend file size in a single RTT.
        let chunk_key = self.chunk_key(chunk_id);
        let version_key = self.chunk_version_key(chunk_id);
        let node_key = self.node_key(ino);
        let data = crate::meta::serialization::serialize_meta(&slice)?;
        let now = current_time();

        let response: LuaResponse = {
            let _txn_guard = Self::local_lock_for_key(&chunk_key).lock().await;
            let script = redis::Script::new(WRITE_SLICE_LUA);
            let result: String = script
                .key(&chunk_key)
                .key(&version_key)
                .key(&node_key)
                .arg(data)
                .arg(new_size)
                .arg(now)
                .invoke_async(&mut self.conn.clone())
                .await
                .map_err(redis_err)?;

            serde_json::from_str(&result)
                .map_err(|e| MetaError::Internal(format!("Lua response parse error: {e}")))?
        };

        match response.error.as_deref() {
            Some("node_not_found") => Err(MetaError::NotFound(ino)),
            Some("corrupt_node") => Err(MetaError::Internal("corrupt node data".into())),
            Some(other) => Err(MetaError::Internal(format!("Lua error: {other}"))),
            None if response.ok => {
                // The Lua script may have updated the node's size; invalidate the
                // local cache so subsequent stat() calls see the new value.
                self.node_cache.invalidate(&ino).await;
                Ok(())
            }
            None => Err(MetaError::Internal("unexpected Lua response".into())),
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(limit))]
    async fn list_chunk_ids(&self, limit: usize) -> Result<Vec<u64>, MetaError> {
        if limit == 0 {
            return Ok(vec![]);
        }

        // Drain any buffered chunk IDs from a previous mid-page limit hit first.
        {
            let mut buffer = self.chunk_scan_buffer.lock().unwrap();
            if !buffer.is_empty() {
                let take = limit.min(buffer.len());
                let result: Vec<u64> = buffer.drain(..take).collect();
                if buffer.is_empty() {
                    let mut cursor = self.chunk_scan_cursor.lock().unwrap();
                    let mut next_cursor = self.chunk_scan_next_cursor.lock().unwrap();
                    *cursor = next_cursor.take();
                }
                return Ok(result);
            }
        }

        let page_size = limit.clamp(64, 256);
        let mut start_key = self.chunk_scan_cursor.lock().unwrap().clone();
        let started_from_cursor = start_key.is_some();
        let mut chunk_ids = Vec::new();
        let mut wrapped = false;
        let mut conn = self.conn.clone();

        loop {
            let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
                .arg(start_key.as_deref().unwrap_or("0"))
                .arg("MATCH")
                .arg(format!("{CHUNK_KEY_PREFIX}*"))
                .arg("COUNT")
                .arg(page_size)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            let mut chunk_idx_in_page = 0;
            for key in &keys {
                if let Some(chunk_id) = Self::parse_chunk_id_from_chunk_key(key) {
                    chunk_ids.push(chunk_id);
                    chunk_idx_in_page += 1;
                    if chunk_ids.len() == limit {
                        // Any remaining chunk IDs in this SCAN page that we haven't
                        // consumed need to be buffered so they aren't skipped.
                        let remaining: Vec<u64> = keys
                            .iter()
                            .filter_map(|k| Self::parse_chunk_id_from_chunk_key(k))
                            .skip(chunk_idx_in_page)
                            .collect();
                        if !remaining.is_empty() {
                            let mut buffer = self.chunk_scan_buffer.lock().unwrap();
                            buffer.extend(remaining);
                            // Save the next cursor so we can resume from the following
                            // page once the buffer is fully drained.
                            let mut next_cursor_guard = self.chunk_scan_next_cursor.lock().unwrap();
                            *next_cursor_guard = Some(next_cursor);
                        } else {
                            let mut cursor = self.chunk_scan_cursor.lock().unwrap();
                            *cursor = Some(next_cursor);
                            let mut next_cursor_guard = self.chunk_scan_next_cursor.lock().unwrap();
                            *next_cursor_guard = None;
                        }
                        return Ok(chunk_ids);
                    }
                }
            }

            if next_cursor == "0" {
                if wrapped || !started_from_cursor {
                    let mut cursor = self.chunk_scan_cursor.lock().unwrap();
                    *cursor = None;
                    let mut next_cursor_guard = self.chunk_scan_next_cursor.lock().unwrap();
                    *next_cursor_guard = None;
                    break;
                }
                start_key = Some("0".to_string());
                wrapped = true;
                continue;
            }

            start_key = Some(next_cursor);
        }

        Ok(chunk_ids)
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, new_slices, old_slices_to_delay),
        fields(chunk_id)
    )]
    // Replace old chunk slices with new ones and create delayed records for the removed slices.
    async fn replace_slices_for_compact(
        &self,
        chunk_id: u64,
        new_slices: &[SliceDesc],
        old_slices_to_delay: &[u8],
    ) -> Result<(), MetaError> {
        if !old_slices_to_delay.is_empty() && !old_slices_to_delay.len().is_multiple_of(20) {
            tracing::warn!(
                chunk_id = chunk_id,
                delayed_len = old_slices_to_delay.len(),
                "replace_slices_for_compact: invalid delayed data length"
            );
            return Err(MetaError::Internal(
                "Invalid delayed data length".to_string(),
            ));
        }

        let delayed_slices = SliceDesc::decode_delayed_data(old_slices_to_delay)
            .ok_or_else(|| MetaError::Internal("Invalid delayed data length".to_string()))?;
        let delayed_ids: std::collections::HashSet<u64> =
            delayed_slices.iter().map(|(id, _, _)| *id).collect();

        let chunk_key = self.chunk_key(chunk_id);
        let version_key = self.chunk_version_key(chunk_id);
        let script = redis::Script::new(CHUNK_CAS_LUA);
        let _txn_guard = Self::local_lock_for_key(&chunk_key).lock().await;

        for _ in 0..COMPACT_RETRY_LIMIT {
            let mut conn = self.conn.clone();

            // Read version and current slices in one round-trip.
            let (version, raw): (Option<i64>, Vec<Vec<u8>>) = redis::pipe()
                .cmd("GET")
                .arg(&version_key)
                .cmd("LRANGE")
                .arg(&chunk_key)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            let current_version = version.unwrap_or(0);

            let mut kept = Vec::new();
            for entry in &raw {
                let desc: SliceDesc = crate::meta::serialization::deserialize_meta(entry)?;
                if !delayed_ids.contains(&desc.slice_id) {
                    kept.push(entry.clone());
                }
            }

            // Build new list: kept existing + new slices.
            let mut final_data = kept;
            for slice in new_slices {
                final_data.push(crate::meta::serialization::serialize_meta(slice)?);
            }

            let new_version = current_version + 1;

            // Atomic CAS via Lua: replace list iff version still matches.
            let ok: i32 = script
                .key(&chunk_key)
                .key(&version_key)
                .arg(current_version)
                .arg(new_version)
                .arg(&final_data)
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;

            if ok == 0 {
                continue;
            }

            // CAS succeeded — create delayed records for removed slices.
            if !delayed_slices.is_empty() {
                let n = delayed_slices.len() as i64;
                let last_id: i64 = redis::cmd("INCRBY")
                    .arg(DELAYED_COUNTER_KEY)
                    .arg(n)
                    .query_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
                let first_id = last_id - n + 1;
                let now = Utc::now().timestamp();

                let mut pipe = redis::pipe();
                pipe.atomic();
                for (i, (slice_id, offset, size)) in delayed_slices.iter().enumerate() {
                    let delayed_id = first_id + i as i64;
                    let ds_key = self.delayed_key(delayed_id);
                    pipe.hset(&ds_key, "sid", slice_id.to_string());
                    pipe.hset(&ds_key, "off", offset.to_string());
                    pipe.hset(&ds_key, "sz", u64::from(*size).to_string());
                    pipe.hset(&ds_key, "st", "pending");
                    pipe.hset(&ds_key, "ca", now.to_string());
                    pipe.hset(&ds_key, "cid", chunk_id.to_string());
                    pipe.cmd("ZADD")
                        .arg(DELAYED_INDEX_KEY)
                        .arg(now)
                        .arg(delayed_id)
                        .ignore();
                }
                pipe.query_async::<()>(&mut conn).await.map_err(redis_err)?;
            }

            return Ok(());
        }

        Err(MetaError::ContinueRetry(RetryReason::CompactConflict))
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, new_slices, old_slices_to_delay, expected_slices),
        fields(chunk_id)
    )]
    // Versioned slice replacement: verify chunk state matches expectations before swapping slices.
    async fn replace_slices_for_compact_with_version(
        &self,
        chunk_id: u64,
        new_slices: &[SliceDesc],
        old_slices_to_delay: &[u8],
        expected_slices: &[SliceDesc],
    ) -> Result<(), MetaError> {
        if !old_slices_to_delay.is_empty() && !old_slices_to_delay.len().is_multiple_of(20) {
            tracing::warn!(
                chunk_id = chunk_id,
                delayed_len = old_slices_to_delay.len(),
                "replace_slices_for_compact_with_version: invalid delayed data length"
            );
            return Err(MetaError::Internal(
                "Invalid delayed data length".to_string(),
            ));
        }

        let delayed_slices = SliceDesc::decode_delayed_data(old_slices_to_delay)
            .ok_or_else(|| MetaError::Internal("Invalid delayed data length".to_string()))?;

        let chunk_key = self.chunk_key(chunk_id);
        let version_key = self.chunk_version_key(chunk_id);
        let script = redis::Script::new(CHUNK_CAS_LUA);
        let _txn_guard = Self::local_lock_for_key(&chunk_key).lock().await;

        for _ in 0..COMPACT_RETRY_LIMIT {
            let mut conn = self.conn.clone();

            // Read version and current slices in one round-trip.
            let (version, raw): (Option<i64>, Vec<Vec<u8>>) = redis::pipe()
                .cmd("GET")
                .arg(&version_key)
                .cmd("LRANGE")
                .arg(&chunk_key)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            let current_version = version.unwrap_or(0);
            let new_version = current_version + 1;

            let mut current_slices = Vec::with_capacity(raw.len());
            for entry in &raw {
                current_slices.push(crate::meta::serialization::deserialize_meta::<SliceDesc>(
                    entry,
                )?);
            }

            if current_slices.len() != expected_slices.len() {
                tracing::debug!(
                    chunk_id = chunk_id,
                    expected_count = expected_slices.len(),
                    actual_count = current_slices.len(),
                    "Concurrent modification detected: slice count mismatch"
                );
                return Err(MetaError::ContinueRetry(RetryReason::CompactConflict));
            }

            let current_map: HashMap<u64, (u64, u64)> = current_slices
                .iter()
                .map(|s| (s.slice_id, (s.offset, s.length)))
                .collect();

            for expected in expected_slices {
                match current_map.get(&expected.slice_id) {
                    Some((offset, length)) => {
                        if *offset != expected.offset || *length != expected.length {
                            tracing::debug!(
                                chunk_id = chunk_id,
                                slice_id = expected.slice_id,
                                expected_offset = expected.offset,
                                expected_length = expected.length,
                                actual_offset = offset,
                                actual_length = length,
                                "Concurrent modification detected: slice content changed"
                            );
                            return Err(MetaError::ContinueRetry(RetryReason::CompactConflict));
                        }
                    }
                    None => {
                        tracing::debug!(
                            chunk_id = chunk_id,
                            slice_id = expected.slice_id,
                            "Concurrent modification detected: slice missing"
                        );
                        return Err(MetaError::ContinueRetry(RetryReason::CompactConflict));
                    }
                }
            }

            // Serialize new slices for the CAS.
            let mut final_data: Vec<Vec<u8>> = Vec::with_capacity(new_slices.len());
            for slice in new_slices {
                final_data.push(crate::meta::serialization::serialize_meta(slice)?);
            }

            // Atomic CAS via Lua: replace list iff version still matches.
            let ok: i32 = script
                .key(&chunk_key)
                .key(&version_key)
                .arg(current_version)
                .arg(new_version)
                .arg(&final_data)
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;

            if ok == 0 {
                continue;
            }

            // CAS succeeded — create delayed records and clean up uncommitted entries.
            if !delayed_slices.is_empty() {
                let n = delayed_slices.len() as i64;
                let last_id: i64 = redis::cmd("INCRBY")
                    .arg(DELAYED_COUNTER_KEY)
                    .arg(n)
                    .query_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
                let first_id = last_id - n + 1;
                let now = Utc::now().timestamp();

                let mut pipe = redis::pipe();
                pipe.atomic();
                for (i, (slice_id, offset, size)) in delayed_slices.iter().enumerate() {
                    let delayed_id = first_id + i as i64;
                    let ds_key = self.delayed_key(delayed_id);
                    pipe.hset(&ds_key, "sid", slice_id.to_string());
                    pipe.hset(&ds_key, "off", offset.to_string());
                    pipe.hset(&ds_key, "sz", u64::from(*size).to_string());
                    pipe.hset(&ds_key, "st", "pending");
                    pipe.hset(&ds_key, "ca", now.to_string());
                    pipe.hset(&ds_key, "cid", chunk_id.to_string());
                    pipe.cmd("ZADD")
                        .arg(DELAYED_INDEX_KEY)
                        .arg(now)
                        .arg(delayed_id)
                        .ignore();
                }
                // Clean up uncommitted records for new slices
                for slice in new_slices {
                    let uc_key = self.uncommitted_key(slice.slice_id);
                    pipe.cmd("DEL").arg(&uc_key).ignore();
                    pipe.cmd("ZREM")
                        .arg(UNCOMMITTED_PENDING_INDEX_KEY)
                        .arg(slice.slice_id.to_string())
                        .ignore();
                    pipe.cmd("ZREM")
                        .arg(UNCOMMITTED_ORPHAN_INDEX_KEY)
                        .arg(slice.slice_id.to_string())
                        .ignore();
                }
                pipe.query_async::<()>(&mut conn).await.map_err(redis_err)?;
            } else {
                // No delayed slices, still clean up uncommitted records.
                for slice in new_slices {
                    let uc_key = self.uncommitted_key(slice.slice_id);
                    redis::pipe()
                        .atomic()
                        .cmd("DEL")
                        .arg(&uc_key)
                        .ignore()
                        .cmd("ZREM")
                        .arg(UNCOMMITTED_PENDING_INDEX_KEY)
                        .arg(slice.slice_id.to_string())
                        .ignore()
                        .cmd("ZREM")
                        .arg(UNCOMMITTED_ORPHAN_INDEX_KEY)
                        .arg(slice.slice_id.to_string())
                        .ignore()
                        .query_async::<()>(&mut conn)
                        .await
                        .map_err(redis_err)?;
                }
            }

            return Ok(());
        }

        Err(MetaError::ContinueRetry(RetryReason::CompactConflict))
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, operation),
        fields(slice_id, chunk_id, size)
    )]
    // Track a newly written slice as uncommitted so GC can clean it up if the commit fails.
    async fn record_uncommitted_slice(
        &self,
        slice_id: u64,
        chunk_id: u64,
        size: u64,
        operation: &str,
    ) -> Result<i64, MetaError> {
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp();
        let uc_key = self.uncommitted_key(slice_id);

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hset(&uc_key, "cid", chunk_id.to_string());
        pipe.hset(&uc_key, "sz", size.to_string());
        pipe.hset(&uc_key, "ca", now.to_string());
        pipe.hset(&uc_key, "op", operation);
        pipe.hset(&uc_key, "st", "pending");
        pipe.cmd("ZADD")
            .arg(UNCOMMITTED_PENDING_INDEX_KEY)
            .arg(now)
            .arg(slice_id.to_string())
            .ignore();

        let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;
        Ok(slice_id as i64)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(slice_id))]
    // Mark an uncommitted slice as committed by removing its tracking record.
    async fn confirm_slice_committed(&self, slice_id: u64) -> Result<(), MetaError> {
        let mut conn = self.conn.clone();
        let uc_key = self.uncommitted_key(slice_id);

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.cmd("DEL").arg(&uc_key).ignore();
        pipe.cmd("ZREM")
            .arg(UNCOMMITTED_PENDING_INDEX_KEY)
            .arg(slice_id.to_string())
            .ignore();
        pipe.cmd("ZREM")
            .arg(UNCOMMITTED_ORPHAN_INDEX_KEY)
            .arg(slice_id.to_string())
            .ignore();

        let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(max_age_secs, batch_size))]
    // Scan for stale uncommitted slices: mark orphans for block cleanup and remove committed leftovers.
    async fn cleanup_orphan_uncommitted_slices(
        &self,
        max_age_secs: i64,
        batch_size: usize,
    ) -> Result<Vec<(u64, u64)>, MetaError> {
        if batch_size == 0 {
            return Ok(vec![]);
        }

        let mut conn = self.conn.clone();
        let cutoff = Utc::now().timestamp() - max_age_secs;

        // Scan pending index
        let pending_ids: Vec<u64> = redis::cmd("ZRANGEBYSCORE")
            .arg(UNCOMMITTED_PENDING_INDEX_KEY)
            .arg("-inf")
            .arg(cutoff)
            .arg("LIMIT")
            .arg(0)
            .arg(batch_size)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        // Scan orphan index
        let orphan_ids: Vec<u64> = redis::cmd("ZRANGE")
            .arg(UNCOMMITTED_ORPHAN_INDEX_KEY)
            .arg(0)
            .arg(batch_size - 1)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        if pending_ids.is_empty() && orphan_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut cleaned = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for slice_id in pending_ids {
            if !seen.insert(slice_id) {
                continue;
            }

            let uc_key = self.uncommitted_key(slice_id);
            let fields: std::collections::HashMap<String, String> =
                conn.hgetall(&uc_key).await.map_err(redis_err)?;

            if fields.is_empty() {
                // Stale index entry, clean up
                let _: () = redis::pipe()
                    .atomic()
                    .cmd("ZREM")
                    .arg(UNCOMMITTED_PENDING_INDEX_KEY)
                    .arg(slice_id.to_string())
                    .ignore()
                    .query_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
                continue;
            }

            let chunk_id = fields
                .get("cid")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let size = fields
                .get("sz")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let chunk_key = self.chunk_key(chunk_id);

            let raw: Vec<Vec<u8>> = redis::cmd("LRANGE")
                .arg(&chunk_key)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            let mut exists = false;
            for entry in raw {
                let desc: SliceDesc = crate::meta::serialization::deserialize_meta(&entry)?;
                if desc.slice_id == slice_id {
                    exists = true;
                    break;
                }
            }

            if exists {
                // Committed, clean up
                let _: () = redis::pipe()
                    .atomic()
                    .cmd("DEL")
                    .arg(&uc_key)
                    .ignore()
                    .cmd("ZREM")
                    .arg(UNCOMMITTED_PENDING_INDEX_KEY)
                    .arg(slice_id.to_string())
                    .ignore()
                    .query_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
            } else {
                // Orphan
                cleaned.push((slice_id, size));
                let mut pipe = redis::pipe();
                pipe.atomic();
                pipe.hset(&uc_key, "st", "orphan");
                pipe.cmd("ZREM")
                    .arg(UNCOMMITTED_PENDING_INDEX_KEY)
                    .arg(slice_id.to_string())
                    .ignore();
                pipe.cmd("ZADD")
                    .arg(UNCOMMITTED_ORPHAN_INDEX_KEY)
                    .arg(0)
                    .arg(slice_id.to_string())
                    .ignore();
                let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;
            }
        }

        for slice_id in orphan_ids {
            if seen.insert(slice_id) {
                let uc_key = self.uncommitted_key(slice_id);
                let size: Option<String> = conn.hget(&uc_key, "sz").await.map_err(redis_err)?;
                let size_val = size.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
                cleaned.push((slice_id, size_val));
            }
        }

        Ok(cleaned)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(slice_count = slice_ids.len()))]
    // Delete uncommitted slice tracking records after their orphan blocks have been cleaned up.
    async fn delete_uncommitted_slices(&self, slice_ids: &[u64]) -> Result<(), MetaError> {
        if slice_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        for slice_id in slice_ids {
            let uc_key = self.uncommitted_key(*slice_id);
            pipe.cmd("DEL").arg(&uc_key).ignore();
            pipe.cmd("ZREM")
                .arg(UNCOMMITTED_PENDING_INDEX_KEY)
                .arg(slice_id.to_string())
                .ignore();
            pipe.cmd("ZREM")
                .arg(UNCOMMITTED_ORPHAN_INDEX_KEY)
                .arg(slice_id.to_string())
                .ignore();
        }

        let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(key))]
    async fn next_id(&self, key: &str) -> Result<i64, MetaError> {
        self.alloc_id(key).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(name))]
    async fn get_counter(&self, name: &str) -> Result<i64, MetaError> {
        let mut conn = self.conn.clone();
        let value: Option<i64> = conn.get(name).await.map_err(redis_err)?;
        Ok(value.unwrap_or(0))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(name, delta))]
    async fn incr_counter(&self, name: &str, delta: i64) -> Result<i64, MetaError> {
        let mut conn = self.conn.clone();
        conn.incr(name, delta).await.map_err(redis_err)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(name, value, diff))]
    async fn set_counter_if_small(
        &self,
        name: &str,
        value: i64,
        diff: i64,
    ) -> Result<bool, MetaError> {
        let script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            local curr_val = tonumber(current) or 0
            local threshold = tonumber(ARGV[1]) - tonumber(ARGV[2])
            if curr_val < threshold then
                redis.call('SET', KEYS[1], tonumber(ARGV[1]))
                return true
            else
                return false
            end
            "#,
        );
        let mut conn = self.conn.clone();
        let result: bool = script
            .key(name)
            .arg(value)
            .arg(diff)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(pid = session_info.process_id))]
    async fn start_session(
        &self,
        session_info: SessionInfo,
        token: CancellationToken,
    ) -> Result<Session, MetaError> {
        let mut conn = self.conn.clone();

        // Increment the global fencing-token epoch so that locks written by
        // this session carry a monotonic version.  Stale sessions whose
        // locks were cleaned up carry a lower epoch and are silently ignored.
        let epoch: i64 = redis::cmd("INCR")
            .arg(PLOCK_EPOCH_KEY)
            .query_async(&mut conn)
            .await
            .map_err(|err| MetaError::Internal(format!("Failed to incr plock epoch: {err}")))?;
        self.set_epoch(epoch);

        let session_id = Uuid::now_v7();
        let expire = (Utc::now() + chrono::Duration::minutes(5)).timestamp_millis();
        let session = Session {
            session_id,
            session_info: session_info.clone(),
            expire,
        };

        let session_info_json = serde_json::to_string(&session_info)
            .map_err(|err| MetaError::Internal(err.to_string()))?;

        let session_id_string = session_id.to_string();

        redis::pipe()
            .atomic()
            .zadd(ALL_SESSIONS_KEY, &session_id_string, expire)
            .hset(SESSION_INFOS_KEY, &session_id_string, session_info_json)
            .exec_async(&mut conn)
            .await
            .map_err(|err| MetaError::Internal(err.to_string()))?;
        self.set_sid(session_id);

        tokio::spawn(Self::life_cycle(
            token.clone(),
            session_id,
            self.conn.clone(),
        ));

        Ok(session)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn shutdown_session(&self) -> Result<(), MetaError> {
        let session_id = self.get_sid()?;
        self.shutdown_session_by_id(session_id).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn cleanup_sessions(&self) -> Result<(), MetaError> {
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp_millis();
        let sessions: Vec<String> = redis::Cmd::zrangebyscore(ALL_SESSIONS_KEY, "-inf", now)
            .query_async(&mut conn)
            .await
            .map_err(|err| MetaError::Internal(err.to_string()))?;
        for session in sessions {
            let session_id =
                Uuid::from_str(&session).map_err(|err| MetaError::Internal(err.to_string()))?;
            self.shutdown_session_by_id(session_id).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(lock_name = ?lock_name, ttl_secs))]
    async fn get_global_lock(&self, lock_name: LockName, ttl_secs: u64) -> bool {
        let lock_name = lock_name.to_string();
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp_millis();
        // Generate a unique nonce to prevent token collision between nodes
        // that acquire the lock within the same millisecond after TTL expiry.
        let nonce: u64 = rand::random();
        let token_value = format!("{}:{}", now, nonce);

        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local field = ARGV[1]
            local now_time = tonumber(ARGV[2])
            local diff = tonumber(ARGV[3])
            local new_token = ARGV[4]

            local current = redis.call("HGET", key, field)

            if current == false then
                redis.call("HSET", key, field, new_token)
                return new_token
            else
                -- Parse timestamp from "timestamp:nonce" format
                local colon_pos = string.find(current, ":", 1, true)
                local locked_at
                if colon_pos then
                    locked_at = tonumber(string.sub(current, 1, colon_pos - 1))
                else
                    locked_at = tonumber(current)
                end
                if locked_at == nil then
                    -- Corrupted value, overwrite
                    redis.call("HSET", key, field, new_token)
                    return new_token
                end
                if now_time < locked_at + diff then
                    return false
                else
                    redis.call("HSET", key, field, new_token)
                    return new_token
                end
            end
            "#,
        );

        let diff = chrono::Duration::seconds(ttl_secs as i64).num_milliseconds();

        let resp: Result<redis::Value, _> = script
            .key(LOCKS_KEY)
            .arg(&lock_name)
            .arg(now)
            .arg(diff)
            .arg(&token_value)
            .invoke_async(&mut conn)
            .await;

        match resp {
            Ok(redis::Value::BulkString(bytes)) => {
                if let Ok(returned_token) = std::str::from_utf8(&bytes) {
                    if returned_token == "false" || returned_token == "0" {
                        return false;
                    }
                    if let Ok(mut tokens) = self.global_lock_tokens.lock() {
                        tokens.insert(lock_name, returned_token.to_string());
                    }
                    return true;
                }
                false
            }
            Ok(redis::Value::SimpleString(s)) => {
                if s == "false" || s == "0" {
                    return false;
                }
                if let Ok(mut tokens) = self.global_lock_tokens.lock() {
                    tokens.insert(lock_name, s);
                }
                true
            }
            Ok(redis::Value::Nil) => false,
            Ok(other) => {
                tracing::warn!("Unexpected response from get_global_lock Lua: {:?}", other);
                false
            }
            Err(err) => {
                error!("{}", err.to_string());
                false
            }
        }
    }

    async fn is_global_lock_held(&self, lock_name: LockName, ttl_secs: u64) -> bool {
        let lock_name = lock_name.to_string();
        let mut conn = self.conn.clone();
        let now = Utc::now().timestamp_millis();
        let ttl_millis = chrono::Duration::seconds(ttl_secs as i64).num_milliseconds();

        let stored: Option<String> = conn
            .hget(LOCKS_KEY, &lock_name)
            .await
            .map_err(redis_err)
            .ok()
            .flatten();

        match stored {
            Some(value) => {
                // Parse timestamp from "timestamp:nonce" or legacy plain integer format
                let locked_at = if let Some(colon_pos) = value.find(':') {
                    value[..colon_pos].parse::<i64>().unwrap_or(0)
                } else {
                    value.parse::<i64>().unwrap_or(0)
                };
                now <= locked_at + ttl_millis
            }
            None => false,
        }
    }

    async fn release_global_lock(&self, lock_name: LockName) -> bool {
        let lock_name = lock_name.to_string();
        let expected_token = match self.global_lock_tokens.lock() {
            Ok(tokens) => tokens.get(&lock_name).cloned(),
            Err(err) => {
                error!("Error reading local lock token {}: {}", lock_name, err);
                None
            }
        };
        let Some(expected_token) = expected_token else {
            return false;
        };

        let mut conn = self.conn.clone();

        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local field = ARGV[1]
            local expected = ARGV[2]

            local current = redis.call("HGET", key, field)
            if current == false then
                return false
            end

            if current == expected then
                redis.call("HDEL", key, field)
                return true
            else
                return false
            end
            "#,
        );

        let resp: Result<bool, _> = script
            .key(LOCKS_KEY)
            .arg(&lock_name)
            .arg(&expected_token)
            .invoke_async(&mut conn)
            .await;

        match resp {
            Ok(released) => {
                if released && let Ok(mut tokens) = self.global_lock_tokens.lock() {
                    tokens.remove(&lock_name);
                }
                released
            }
            Err(err) => {
                error!("Error releasing lock {}: {}", lock_name, err);
                false
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    // returns the current lock owner for a range on a file.
    #[tracing::instrument(level = "trace", skip(self, query), fields(inode, owner = query.owner))]
    async fn get_plock(
        &self,
        inode: i64,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        let mut conn = self.conn.clone();
        let plock_key = self.plock_key(inode);
        let sid = self.get_sid()?;
        let current_field = format!("{}:{}", sid, query.owner);
        // Single HGETALL fetches all lock entries — iterate the result directly
        // instead of making redundant HKEYS + per-field HGET calls.
        let plock_entries: std::collections::HashMap<String, String> =
            conn.hgetall(&plock_key).await.map_err(redis_err)?;

        // Helper to parse plock values: new {epoch, records} format first,
        // then fall back to legacy bare-array format for transparent upgrade.
        let parse_records = |raw: &str| -> Vec<PlockRecord> {
            serde_json::from_str::<PlockValue>(raw)
                .map(|v| v.records)
                .or_else(|_| serde_json::from_str::<Vec<PlockRecord>>(raw))
                .unwrap_or_default()
        };

        // First, try to get locks from current session's field
        if let Some(records_json) = plock_entries.get(&current_field) {
            let records = parse_records(records_json);
            if let Some(v) = PlockRecord::get_plock(&records, query, &sid, &sid) {
                return Ok(v);
            }
        }

        // Check other sessions' locks from the already-fetched HGETALL result
        for (field, records_json) in &plock_entries {
            if *field == current_field {
                continue;
            }

            let parts: Vec<&str> = field.split(':').collect();
            if parts.len() != 2 {
                continue;
            }

            let lock_sid = Uuid::parse_str(parts[0])
                .map_err(|_| MetaError::Internal("Invalid sid in plock field".to_string()))?;

            let records = parse_records(records_json);

            if let Some(v) = PlockRecord::get_plock(&records, query, &sid, &lock_sid) {
                return Ok(v);
            }
        }

        Ok(FileLockInfo {
            lock_type: FileLockType::UnLock,
            range: FileLockRange { start: 0, end: 0 },
            pid: 0,
        })
    }

    // sets a file range lock on given file.
    #[tracing::instrument(
        level = "trace",
        skip(self),
        fields(inode, owner, block, lock_type = ?lock_type, pid)
    )]
    async fn set_plock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), MetaError> {
        let new_lock = PlockRecord::new(lock_type, pid, range.start, range.end);

        // If blocking: add a small random delay before the first attempt so
        // concurrent waiters don't burst in sync.  This gives each waiter a
        // different cadence and statistical fairness without needing a
        // server-side queue.
        if block {
            let initial_jitter_ms = (rand::random::<f64>() * 2.0) as u64;
            tokio::time::sleep(tokio::time::Duration::from_millis(initial_jitter_ms)).await;
        }

        let mut attempt: u32 = 0;
        loop {
            let result = self
                .try_set_plock(inode, owner, new_lock, lock_type, range)
                .await;

            match result {
                Ok(()) => return Ok(()),
                Err(MetaError::LockConflict { .. }) if block => {
                    // Exponential backoff with jitter to prevent thundering-herd.
                    // Base 2 ms, capped at 50 ms so long-waiting clients aren't
                    // excessively penalized relative to new arrivals.
                    let base_ms: u64 = 2;
                    let max_ms: u64 = 50;
                    let exp = attempt.min(8);
                    let delay_ms = (base_ms * (1u64 << exp)).min(max_ms);
                    // Jitter: 0.5–1.5 × delay
                    let jitter: f64 = 0.5 + rand::random::<f64>();
                    let jittered_ms = ((delay_ms as f64) * jitter) as u64;
                    tokio::time::sleep(tokio::time::Duration::from_millis(jittered_ms)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Set or release a BSD flock (whole-file advisory lock).
    ///
    /// When `block` is true and a conflict is detected the call will poll
    /// with short sleeps (1 ms for write locks, 10 ms for read locks)
    /// mirroring the JuiceFS strategy for fairness.
    async fn set_flock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        if block {
            let initial_jitter_ms = (rand::random::<f64>() * 2.0) as u64;
            tokio::time::sleep(tokio::time::Duration::from_millis(initial_jitter_ms)).await;
        }

        loop {
            let sid = self.get_sid()?;
            let epoch = self.get_epoch()?;
            let flock_key = format!("flock:{inode}");
            let locked_key = Self::locked_key(sid);
            let field = self.plock_field(&sid, owner);

            let script = redis::Script::new(flock_lua!());
            let result: String = script
                .key(&flock_key)
                .key(&locked_key)
                .arg(&field)
                .arg(lock_type.as_u32())
                .arg(inode)
                .arg(epoch)
                .invoke_async(&mut self.conn.clone())
                .await
                .map_err(redis_err)?;

            let response: LuaResponse = serde_json::from_str(&result)
                .map_err(|e| MetaError::Internal(format!("flock Lua response parse error: {e}")))?;

            let result = match response.error.as_deref() {
                Some("lock_conflict") => Err(MetaError::LockConflict {
                    inode,
                    owner,
                    range: FileLockRange {
                        start: 0,
                        end: u64::MAX,
                    },
                }),
                Some(other) => Err(MetaError::Internal(format!("flock Lua error: {other}"))),
                None if response.ok => Ok(()),
                None => Err(MetaError::Internal("unexpected flock Lua response".into())),
            };

            match result {
                Ok(()) => return Ok(()),
                Err(MetaError::LockConflict { .. }) if block => {
                    let delay_ms: u64 = if lock_type == FileLockType::Write {
                        1
                    } else {
                        10
                    };
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn get_flock(&self, inode: i64, owner: i64) -> Result<FileLockType, MetaError> {
        let sid = self.get_sid()?;
        let field = self.plock_field(&sid, owner);
        let flock_key = format!("flock:{inode}");

        let raw: Option<String> = redis::cmd("HGET")
            .arg(&flock_key)
            .arg(&field)
            .query_async(&mut self.conn.clone())
            .await
            .map_err(redis_err)?;

        Ok(match raw {
            Some(v) => {
                let parsed: serde_json::Value = serde_json::from_str(&v).unwrap_or_default();
                match parsed.get("val").and_then(|v| v.as_str()) {
                    Some("W") => FileLockType::Write,
                    Some("R") => FileLockType::Read,
                    _ => FileLockType::UnLock,
                }
            }
            None => FileLockType::UnLock,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredNode {
    ino: i64,
    parent: i64,
    name: String,
    kind: NodeKind,
    attr: StoredAttr,
    #[serde(default)]
    symlink_target: Option<String>,
    deleted: bool,
}

impl StoredNode {
    fn as_file_attr(&self) -> FileAttr {
        self.attr.to_file_attr(self.ino, self.kind.into())
    }
}

/// Deserializer that accepts both integer and floating-point numbers.
/// Redis cjson encodes large integers (like epoch millis) as scientific notation
/// floats (e.g., 1.7698324007242e+18), which serde_json rejects for i64 fields.
fn deserialize_i64_from_number<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Visitor};

    struct I64OrFloatVisitor;

    impl<'de> Visitor<'de> for I64OrFloatVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an integer or floating-point number")
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
            i64::try_from(v).map_err(|_| E::custom("u64 out of i64 range"))
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
            // Validate finite value
            if !v.is_finite() {
                return Err(E::custom("non-finite float for i64 field"));
            }

            // Truncate and validate range
            let truncated = v.trunc();
            if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
                return Err(E::custom("float out of i64 range"));
            }

            Ok(truncated as i64)
        }
    }

    deserializer.deserialize_any(I64OrFloatVisitor)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredAttr {
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    #[serde(deserialize_with = "deserialize_i64_from_number")]
    atime: i64,
    #[serde(deserialize_with = "deserialize_i64_from_number")]
    mtime: i64,
    #[serde(deserialize_with = "deserialize_i64_from_number")]
    ctime: i64,
    nlink: u32,
}

impl StoredAttr {
    fn to_file_attr(&self, ino: i64, kind: FileType) -> FileAttr {
        FileAttr {
            ino,
            size: self.size,
            blocks: self.size.div_ceil(512),
            kind,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            nlink: self.nlink,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum NodeKind {
    File,
    Dir,
    Symlink,
}

impl From<FileType> for NodeKind {
    fn from(value: FileType) -> Self {
        match value {
            FileType::File => NodeKind::File,
            FileType::Dir => NodeKind::Dir,
            FileType::Symlink => NodeKind::Symlink,
        }
    }
}

impl From<NodeKind> for FileType {
    fn from(value: NodeKind) -> Self {
        match value {
            NodeKind::File => FileType::File,
            NodeKind::Dir => FileType::Dir,
            NodeKind::Symlink => FileType::Symlink,
        }
    }
}

fn current_time() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

fn redis_err(err: redis::RedisError) -> MetaError {
    MetaError::Internal(format!("Redis error: {err}"))
}

#[cfg(test)]
mod tests;
