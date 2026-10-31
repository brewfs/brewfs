//! Redis transactional substrate for the workspace catalog.

#[cfg(test)]
#[path = "redis_operator_admin_tests.rs"]
mod operator_admin_tests;

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use super::kv_backend::{
    KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend, validate_cas_time_window,
};
use crate::workspace_overlay::error::WorkspaceError;

const CAS_LUA: &str = r#"
local check_count = tonumber(ARGV[1])
if check_count == nil or check_count < 0 or check_count % 1 ~= 0 then
    return redis.error_reply('invalid workspace check count')
end
local write_cursor = 2 + 2 * check_count
local write_count = tonumber(ARGV[write_cursor])
local empty_mode = ARGV[#ARGV - 5]
local extra_key_count = empty_mode == '1' and 2 or 1
if write_count == nil or write_count < 0 or write_count % 1 ~= 0
    or #KEYS ~= check_count + write_count + extra_key_count
    or #ARGV ~= write_cursor + 2 * write_count + 6 then
    return redis.error_reply('invalid workspace mutation arguments')
end
local index_key = KEYS[check_count + write_count + 1]
local function key_type(key)
    local reply = redis.call('TYPE', key)
    if type(reply) == 'table' then return reply.ok end
    return reply
end
-- Redis Lua cannot roll back an earlier write after a later type error.
-- Validate the complete command/key plan before the first SET/DEL.
local index_type = key_type(index_key)
if index_type ~= 'none' and index_type ~= 'zset' then
    return redis.error_reply('invalid workspace key index type')
end
for index = 1, check_count do
    local check_type = key_type(KEYS[index])
    if check_type ~= 'none' and check_type ~= 'string' then
        return redis.error_reply('invalid workspace condition key type')
    end
    local present = ARGV[2 + 2 * (index - 1)]
    if present ~= '0' and present ~= '1' then
        return redis.error_reply('invalid workspace condition flag')
    end
end
for index = 1, write_count do
    local operation = ARGV[write_cursor + 1 + 2 * (index - 1)]
    if (operation ~= 'put' and operation ~= 'delete')
        or KEYS[check_count + index] == index_key then
        return redis.error_reply('invalid workspace write plan')
    end
end
local cursor = 2
for index = 1, check_count do
    local expected_present = ARGV[cursor]
    local expected = ARGV[cursor + 1]
    cursor = cursor + 2
    local current = redis.call('GET', KEYS[index])
    if expected_present == '0' then
        if current then return 0 end
    elseif current ~= expected then
        return 0
    end
end

-- Bootstrap observes raw keys, not the optional/migrated sorted index. Keep
-- the scan and every mutation in this one script; an interleaved SCAN + CAS
-- would admit foreign rows. Exhausting the fixed work budget fails closed.
local namespace = ARGV[#ARGV - 4]
if empty_mode ~= '0' and empty_mode ~= '1' then
    return redis.error_reply('invalid empty namespace flag')
end
if empty_mode == '1' then
    -- Native headers live outside ws:v1/. The Manager preflight is only a
    -- diagnostic; this atomic predicate prevents a carrier from being created
    -- if another initializer installed a native header after that read.
    if redis.call('EXISTS', KEYS[#KEYS]) ~= 0 then return 0 end
    local ready_type = key_type(namespace .. '__index/ready')
    if ready_type ~= 'none' and ready_type ~= 'string' then
        return redis.error_reply('invalid workspace index marker type')
    end
    if ready_type == 'string' and redis.call('GET', namespace .. '__index/ready') ~= '1' then
        return redis.error_reply('invalid workspace index marker')
    end
    local owner_key = namespace .. 'manager/controller'
    local owner_checked = false
    for index = 1, check_count do
        if KEYS[index] == owner_key and ARGV[2 + 2 * (index - 1)] == '1' then
            owner_checked = true
        end
    end
    local cursor = '0'
    for page = 1, 4096 do
        local result = redis.call('SCAN', cursor, 'MATCH', namespace .. '*', 'COUNT', 256)
        cursor = result[1]
        for _, key in ipairs(result[2]) do
            if key ~= index_key and key ~= namespace .. '__index/ready'
                and not (key == owner_key and owner_checked) then
                return 0
            end
        end
        if cursor == '0' then break end
        if page == 4096 then return redis.error_reply('empty namespace scan budget exhausted') end
    end
end

-- Bound parts stay below 2^53. Converting full nanoseconds to a Lua double
-- would silently round either boundary at contemporary timestamps.
local function bound_parts(seconds_arg, nanos_arg)
    if seconds_arg == '' and nanos_arg == '' then return nil, nil, nil end
    local seconds = tonumber(seconds_arg)
    local nanos = tonumber(nanos_arg)
    if seconds == nil or seconds < 0 or seconds % 1 ~= 0
        or nanos == nil or nanos < 0 or nanos >= 1000000000 or nanos % 1 ~= 0
        or (seconds == 0 and nanos == 0) then
        return nil, nil, 'invalid workspace temporal bound'
    end
    return seconds, nanos, nil
end
local lower_seconds, lower_ns, lower_error = bound_parts(ARGV[#ARGV - 3], ARGV[#ARGV - 2])
local upper_seconds, upper_ns, upper_error = bound_parts(ARGV[#ARGV - 1], ARGV[#ARGV])
if lower_error or upper_error then return redis.error_reply(lower_error or upper_error) end
if lower_seconds and upper_seconds
    and (lower_seconds > upper_seconds or (lower_seconds == upper_seconds and lower_ns >= upper_ns)) then
    return redis.error_reply('invalid workspace temporal interval')
end
if lower_seconds or upper_seconds then
    local now = redis.call('TIME')
    local current_seconds = tonumber(now[1])
    local current_ns = tonumber(now[2]) * 1000
    if lower_seconds and (current_seconds < lower_seconds
        or (current_seconds == lower_seconds and current_ns < lower_ns)) then
        return -2
    end
    if upper_seconds and (current_seconds > upper_seconds
        or (current_seconds == upper_seconds and current_ns >= upper_ns)) then
        return -1
    end
end
cursor = cursor + 1
for index = 1, write_count do
    local operation = ARGV[cursor]
    local value = ARGV[cursor + 1]
    cursor = cursor + 2
    local key = KEYS[check_count + index]
    if operation == 'put' then
        redis.call('SET', key, value)
        redis.call('ZADD', index_key, 0, key)
    else
        redis.call('DEL', key)
        redis.call('ZREM', index_key, key)
    end
end
if empty_mode == '1' and write_count > 0 then
    redis.call('SET', namespace .. '__index/ready', '1')
end
return 1
"#;

const KEY_INDEX: &[u8] = b"__index/keys";

// STRLEN never copies the stored string into Lua or the client. All lengths
// are checked in the same indivisible script before the first GET. Therefore
// a corrupt oversized persistent value cannot be returned to the RESP decoder.
const BOUNDED_TIMED_GET_LUA: &str = r#"
local maximum_value = tonumber(ARGV[1])
local maximum_total = tonumber(ARGV[2])
local prefix_bytes = tonumber(ARGV[3])
local total = 0
local response_bytes = 128 + 32 * #KEYS
for _, key in ipairs(KEYS) do
    local bytes = redis.call('STRLEN', key)
    total = total + #key - prefix_bytes + bytes
    response_bytes = response_bytes + bytes
    if bytes > maximum_value or total > maximum_total
        or response_bytes > tonumber(ARGV[4]) then
        return redis.error_reply('workspace bounded value bytes exceeded')
    end
end
local values = {}
for index, key in ipairs(KEYS) do
    values[index] = redis.call('GET', key)
end
return {redis.call('TIME'), values}
"#;

const BOUNDED_PREFIX_LUA: &str = r#"
local keys = redis.call('ZRANGEBYLEX', KEYS[1], ARGV[1], ARGV[2],
    'LIMIT', 0, tonumber(ARGV[3]))
local maximum_key = tonumber(ARGV[4])
local maximum_value = tonumber(ARGV[5])
local maximum_total = tonumber(ARGV[6])
local namespace = ARGV[7]
local requested_prefix = ARGV[8]
local total = 0
local response_bytes = 32 + 64 * #keys
for _, key in ipairs(keys) do
    if string.sub(key, 1, #namespace) ~= namespace
        or string.sub(key, 1, #requested_prefix) ~= requested_prefix
        or #key - #namespace > maximum_key then
        return redis.error_reply('workspace bounded index key rejected')
    end
    -- An index ghost must fail closed; silently skipping it would turn a
    -- record sentinel limit into incomplete-prefix success.
    if redis.call('EXISTS', key) ~= 1 then
        return redis.error_reply('workspace bounded index references missing value')
    end
    local bytes = redis.call('STRLEN', key)
    total = total + #key - #namespace + bytes
    response_bytes = response_bytes + #key + bytes
    if bytes > maximum_value or total > maximum_total
        or response_bytes > tonumber(ARGV[9]) then
        return redis.error_reply('workspace bounded scan bytes exceeded')
    end
end
local result = {}
for index, key in ipairs(keys) do
    result[index] = {key, redis.call('GET', key)}
end
return result
"#;
const KEY_INDEX_READY: &[u8] = b"__index/ready";
const INDEX_BATCH_SIZE: usize = 1024;

#[derive(Clone)]
pub struct RedisWorkspaceBackend {
    connection: ConnectionManager,
    prefix: Vec<u8>,
    namespace: String,
    operator_admin: Option<RedisOperatorAdminIdentity>,
}

#[derive(Clone)]
struct RedisOperatorAdminIdentity {
    admin_principal: String,
    runtime_principal: String,
}

impl RedisWorkspaceBackend {
    pub async fn connect(url: &str, namespace: &str) -> Result<Self, WorkspaceError> {
        let backend = Self::connect_read_only(url, namespace).await?;
        backend.ensure_key_index().await?;
        Ok(backend)
    }

    /// Connect without creating or migrating any namespace keys. Manager uses
    /// this before durable admission and strict empty-namespace validation.
    pub async fn connect_read_only(url: &str, namespace: &str) -> Result<Self, WorkspaceError> {
        validate_namespace(namespace)?;
        let client = redis::Client::open(url).map_err(backend)?;
        let connection = ConnectionManager::new(client).await.map_err(backend)?;
        // The hash tag keeps every catalog key in one Redis Cluster slot, which
        // is required for the multi-key Lua transactions below.
        let prefix = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/").into_bytes();
        let backend = Self {
            connection,
            prefix,
            namespace: namespace.to_owned(),
            operator_admin: None,
        };
        Ok(backend)
    }

    /// Read the native locator outside the workspace key prefix even when the
    /// native feature is not compiled. A v1 Manager must not bypass native
    /// admission/deletion by treating its metadata carrier as a plain volume.
    pub(crate) async fn load_manager_native_locator(
        &self,
    ) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let mut connection = self.connection.clone();
        redis::cmd("GET")
            .arg(format!("nb2/header/{}", self.namespace))
            .query_async(&mut connection)
            .await
            .map_err(backend)
    }

    /// Registration must not silently scan an absent legacy key index. Its
    /// migration remains an explicit administrator action, outside Manager.
    pub async fn validate_manager_catalog_index(&self) -> Result<(), WorkspaceError> {
        let mut connection = self.connection.clone();
        let valid: bool = redis::Script::new(
            r#"
            local kind = redis.call('TYPE', KEYS[2])
            if type(kind) == 'table' then kind = kind.ok end
            return redis.call('GET', KEYS[1]) == '1' and kind == 'zset'
        "#,
        )
        .key(self.scoped(KEY_INDEX_READY))
        .key(self.scoped(KEY_INDEX))
        .invoke_async(&mut connection)
        .await
        .map_err(backend)?;
        if !valid {
            return Err(WorkspaceError::UnsupportedCapability(
                "Manager requires an existing complete Redis key index",
            ));
        }
        Ok(())
    }

    /// Read-only admission probe that also sees rows missing from the index.
    /// Lua returns one boolean, never arbitrary stored metadata values.
    pub async fn has_raw_prefix(&self, prefix: &[u8]) -> Result<bool, WorkspaceError> {
        let scoped = self.scoped(prefix);
        let mut pattern = Vec::with_capacity(scoped.len() + 1);
        for &byte in &scoped {
            if matches!(byte, b'*' | b'?' | b'[' | b']' | b'\\') {
                pattern.push(b'\\');
            }
            pattern.push(byte);
        }
        pattern.push(b'*');
        let mut connection = self.connection.clone();
        redis::Script::new(
            r#"
            local cursor = '0'
            for page = 1, 4096 do
                local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 256)
                cursor = result[1]
                for _, key in ipairs(result[2]) do
                    if string.sub(key, 1, #ARGV[2]) == ARGV[2] then return 1 end
                end
                if cursor == '0' then return 0 end
            end
            return redis.error_reply('Manager raw namespace probe budget exhausted')
        "#,
        )
        .arg(pattern)
        .arg(scoped)
        .invoke_async(&mut connection)
        .await
        .map_err(backend)
    }

    /// The trusted operator Secret resolver assigns roles to two independent
    /// named ACL credentials. Both actual AUTH/WHOAMI exchanges must succeed
    /// before catalog discovery or mutation. This does not claim Redis Lua can
    /// authorize individual business transitions.
    pub async fn connect_operator_admin(
        admin_url: &str,
        expected_admin_principal: &str,
        runtime_url: &str,
        expected_runtime_principal: &str,
        namespace: &str,
    ) -> Result<Self, WorkspaceError> {
        validate_namespace(namespace)?;
        validate_operator_principals(expected_admin_principal, expected_runtime_principal)?;
        let admin_client = redis::Client::open(admin_url).map_err(|_| {
            WorkspaceError::Backend("invalid operator Redis connection configuration".into())
        })?;
        let runtime_client = redis::Client::open(runtime_url).map_err(|_| {
            WorkspaceError::Backend("invalid runtime Redis connection configuration".into())
        })?;
        let admin_info = &admin_client.get_connection_info().redis;
        let runtime_info = &runtime_client.get_connection_info().redis;
        if admin_client.get_connection_info().addr != runtime_client.get_connection_info().addr
            || admin_info.db != runtime_info.db
            || admin_info.username.as_deref() != Some(expected_admin_principal)
            || runtime_info.username.as_deref() != Some(expected_runtime_principal)
            || admin_info.password.as_ref().is_none_or(String::is_empty)
            || runtime_info.password.as_ref().is_none_or(String::is_empty)
            || admin_info.password == runtime_info.password
        {
            return Err(WorkspaceError::UnsupportedCapability(
                "operator and runtime Redis ACL credentials must be independent",
            ));
        }
        let mut runtime = ConnectionManager::new(runtime_client)
            .await
            .map_err(|_| WorkspaceError::Backend("runtime Redis authentication failed".into()))?;
        authenticate_principal(&mut runtime, expected_runtime_principal).await?;
        drop(runtime);
        let mut connection = ConnectionManager::new(admin_client)
            .await
            .map_err(|_| WorkspaceError::Backend("operator Redis authentication failed".into()))?;
        authenticate_principal(&mut connection, expected_admin_principal).await?;
        let prefix = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/").into_bytes();
        let backend = Self {
            connection,
            prefix,
            namespace: namespace.to_owned(),
            operator_admin: Some(RedisOperatorAdminIdentity {
                admin_principal: expected_admin_principal.to_owned(),
                runtime_principal: expected_runtime_principal.to_owned(),
            }),
        };
        backend.ensure_key_index().await?;
        Ok(backend)
    }

    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        let mut scoped = Vec::with_capacity(self.prefix.len() + key.len());
        scoped.extend_from_slice(&self.prefix);
        scoped.extend_from_slice(key);
        scoped
    }

    async fn scan_prefix_with_byte_plan(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate()?;
        if prefix.len() > limits.max_key_bytes {
            return Err(WorkspaceError::InvalidReadPlan(
                "KV scan prefix exceeds key byte limit".into(),
            ));
        }
        let scoped_prefix = self.scoped(prefix);
        let mut minimum = match after_key_exclusive {
            Some(after) => {
                let mut bound = b"(".to_vec();
                bound.extend_from_slice(&self.scoped(after));
                bound
            }
            None => b"[".to_vec(),
        };
        if after_key_exclusive.is_none() {
            minimum.extend_from_slice(&scoped_prefix);
        }
        let maximum = match prefix_range_end(&scoped_prefix) {
            Some(upper) => {
                let mut bound = b"(".to_vec();
                bound.extend_from_slice(&upper);
                bound
            }
            None => b"+".to_vec(),
        };
        let mut connection = self.connection.clone();
        // ZRANGEBYLEX seeks strictly past the last logical key. The script
        // applies row and stored-byte limits before its first GET/RESP value.
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = redis::Script::new(BOUNDED_PREFIX_LUA)
            .key(self.scoped(KEY_INDEX))
            .arg(minimum)
            .arg(maximum)
            .arg(limits.max_records)
            .arg(limits.max_key_bytes)
            .arg(limits.max_value_bytes)
            .arg(limits.max_total_bytes)
            .arg(&self.prefix)
            .arg(scoped_prefix)
            .arg(limits.max_response_bytes)
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        if pairs.len() > limits.max_records {
            return Err(backend("bounded Redis scan exceeded record limit"));
        }
        let mut entries: Vec<KvEntry> = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let logical = key
                .strip_prefix(self.prefix.as_slice())
                .ok_or_else(|| backend("bounded Redis scan returned an out-of-namespace key"))?;
            let previous = entries
                .last()
                .map(|entry| entry.key.as_slice())
                .or(after_key_exclusive);
            if !logical.starts_with(prefix) || previous.is_some_and(|last| logical <= last) {
                return Err(backend(
                    "bounded Redis scan returned invalid key order/prefix",
                ));
            }
            entries.push(KvEntry {
                key: logical.to_vec(),
                value,
            });
        }
        Ok(entries)
    }

    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
        empty_namespace: bool,
    ) -> Result<bool, WorkspaceError> {
        let script = redis::Script::new(CAS_LUA);
        let mut invocation = script.prepare_invoke();
        for check in checks {
            invocation.key(self.scoped(&check.key));
        }
        for write in writes {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            invocation.key(self.scoped(key));
        }
        invocation.key(self.scoped(KEY_INDEX));
        if empty_namespace {
            invocation.key(format!("nb2/header/{}", self.namespace));
        }
        invocation.arg(checks.len());
        for check in checks {
            match &check.expected {
                Some(expected) => {
                    invocation.arg(1_u8).arg(expected);
                }
                None => {
                    invocation.arg(0_u8).arg(Vec::<u8>::new());
                }
            }
        }
        invocation.arg(writes.len());
        for write in writes {
            match write {
                KvWrite::Put { value, .. } => {
                    invocation.arg("put").arg(value);
                }
                KvWrite::Delete { .. } => {
                    invocation.arg("delete").arg(Vec::<u8>::new());
                }
            }
        }
        invocation.arg(u8::from(empty_namespace)).arg(&self.prefix);
        for bound in [not_before_ns, before_ns] {
            match bound {
                Some(value) if value > 0 => {
                    invocation
                        .arg(value / 1_000_000_000)
                        .arg(value % 1_000_000_000);
                }
                Some(_) => return Err(WorkspaceError::Fenced),
                None => {
                    invocation.arg("").arg("");
                }
            }
        }
        let mut connection = self.connection.clone();
        let result: i64 = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        match result {
            -2 => Err(WorkspaceError::Busy),
            -1 => Err(WorkspaceError::Fenced),
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(WorkspaceError::Backend("invalid Redis CAS result".into())),
        }
    }

    async fn ensure_key_index(&self) -> Result<(), WorkspaceError> {
        let marker = self.scoped(KEY_INDEX_READY);
        let index = self.scoped(KEY_INDEX);
        let mut connection = self.connection.clone();
        let ready: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&marker)
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        if ready.is_some() {
            return Ok(());
        }

        // This is a one-time migration for catalogs created before the
        // lexicographic index existed. Normal prefix reads never use SCAN.
        let mut pattern = self.prefix.clone();
        pattern.push(b'*');
        let mut cursor = 0_u64;
        loop {
            let (next, mut keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(INDEX_BATCH_SIZE)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            keys.retain(|key| key != &index && key != &marker);
            if !keys.is_empty() {
                redis::cmd("ZADD")
                    .arg(&index)
                    .arg(0_i64)
                    .arg(keys)
                    .query_async::<()>(&mut connection)
                    .await
                    .map_err(backend)?;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        redis::cmd("SET")
            .arg(marker)
            .arg(1_u8)
            .query_async::<()>(&mut connection)
            .await
            .map_err(backend)
    }
}

impl RedisWorkspaceBackend {
    async fn scan_prefix_with_limit(
        &self,
        prefix: &[u8],
        max_records: Option<usize>,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if max_records == Some(0) {
            return Err(WorkspaceError::InvalidReadPlan(
                "bounded prefix scan requires a positive row limit".into(),
            ));
        }
        let mut connection = self.connection.clone();
        let scoped_prefix = self.scoped(prefix);
        let mut minimum = Vec::with_capacity(scoped_prefix.len() + 1);
        minimum.push(b'[');
        minimum.extend_from_slice(&scoped_prefix);
        let maximum = match prefix_range_end(&scoped_prefix) {
            Some(upper) => {
                let mut bound = Vec::with_capacity(upper.len() + 1);
                bound.push(b'(');
                bound.extend_from_slice(&upper);
                bound
            }
            None => b"+".to_vec(),
        };
        let index = self.scoped(KEY_INDEX);
        let mut entries = Vec::new();
        loop {
            if max_records.is_some_and(|limit| entries.len() >= limit) {
                break;
            }
            let batch_limit = max_records
                .map(|limit| INDEX_BATCH_SIZE.min(limit - entries.len()))
                .unwrap_or(INDEX_BATCH_SIZE);
            let keys: Vec<Vec<u8>> = redis::cmd("ZRANGEBYLEX")
                .arg(&index)
                .arg(&minimum)
                .arg(&maximum)
                .arg("LIMIT")
                .arg(0_u8)
                .arg(batch_limit)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            if keys.is_empty() {
                break;
            }
            let batch: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            for (key, value) in keys.iter().zip(batch) {
                if let Some(value) = value {
                    let logical = key.strip_prefix(self.prefix.as_slice()).ok_or_else(|| {
                        WorkspaceError::Backend(
                            "Redis workspace index returned an out-of-namespace key".into(),
                        )
                    })?;
                    entries.push(KvEntry {
                        key: logical.to_vec(),
                        value,
                    });
                }
            }
            let Some(last) = keys.last() else {
                break;
            };
            minimum.clear();
            minimum.push(b'(');
            minimum.extend_from_slice(last);
            if keys.len() < batch_limit {
                break;
            }
        }
        Ok(entries)
    }
}

#[async_trait]
impl WorkspaceKvBackend for RedisWorkspaceBackend {
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        let identity =
            self.operator_admin
                .as_ref()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "independent operator Redis credentials",
                ))?;
        validate_operator_principals(&identity.admin_principal, &identity.runtime_principal)?;
        let mut connection = self.connection.clone();
        authenticate_principal(&mut connection, &identity.admin_principal).await
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    // BOUNDED_PREFIX_LUA is one ZRANGEBYLEX ... LIMIT max_records inside a
    // script; byte limits and index ghosts fail the call, never truncate it.
    fn short_scan_page_is_end(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "workspace-redis"
    }
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        // Redis has the same bounded keyset-page contract as the operator
        // adapter. Opt into the durable cursor so layer finalization never
        // falls back to materializing every native metadata family at once.
        Some(32)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let mut connection = self.connection.clone();
        redis::cmd("GET")
            .arg(self.scoped(key))
            .query_async(&mut connection)
            .await
            .map_err(backend)
    }

    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let scoped = keys.iter().map(|key| self.scoped(key)).collect::<Vec<_>>();
        let mut connection = self.connection.clone();
        redis::cmd("MGET")
            .arg(scoped)
            .query_async(&mut connection)
            .await
            .map_err(backend)
    }

    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        // One Redis MGET is indivisible relative to the Lua writer.
        self.get_many(keys).await
    }

    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if keys.is_empty() {
            return Ok((Vec::new(), self.server_time_ns().await?));
        }
        let scoped = keys.iter().map(|key| self.scoped(key)).collect::<Vec<_>>();
        let mut connection = self.connection.clone();
        let ((seconds, micros), values): ((i64, i64), Vec<Option<Vec<u8>>>) =
            timed_mget_pipeline(scoped)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
        Ok((values, redis_time_ns(seconds, micros)?))
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let script = redis::Script::new(BOUNDED_TIMED_GET_LUA);
        let mut invocation = script.prepare_invoke();
        for key in keys {
            invocation.key(self.scoped(key));
        }
        invocation
            .arg(limits.max_value_bytes)
            .arg(limits.max_total_bytes)
            .arg(self.prefix.len())
            .arg(limits.max_response_bytes);
        let mut connection = self.connection.clone();
        let ((seconds, micros), values): ((i64, i64), Vec<Option<Vec<u8>>>) = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        if values.len() != keys.len() {
            return Err(backend("bounded timed GET returned a short result"));
        }
        Ok((values, redis_time_ns(seconds, micros)?))
    }

    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let scoped = keys.iter().map(|key| self.scoped(key)).collect::<Vec<_>>();
        let mut connection = self.connection.clone();
        // TIME and MGET form one metadata read snapshot. Without MULTI/EXEC,
        // another workspace mutation can run between the two pipelined
        // commands, leaving the lease timestamp detached from the values used
        // to authorize an ACL/mode mutation.
        let ((seconds, micros), values): ((i64, i64), Vec<Option<Vec<u8>>>) =
            timed_mget_pipeline(scoped)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
        let now = redis_time_ns(seconds, micros)?;
        Ok((values, now))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_with_limit(prefix, None).await
    }

    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_with_limit(prefix, Some(max_records)).await
    }

    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_with_byte_plan(prefix, None, limits).await
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after_key_exclusive)?;
        self.scan_prefix_with_byte_plan(prefix, after_key_exclusive, limits)
            .await
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, None, false).await
    }

    async fn compare_and_swap_in_empty_namespace(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        validate_cas_time_window(None, Some(before_ns))?;
        self.cas(checks, writes, None, Some(before_ns), true).await
    }

    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        super::kv_backend::validate_bounded_authentication_checks(checks, limits)?;
        validate_cas_time_window(None, Some(expires_at_ns))?;
        // Existing Lua atomically compares all bytes and samples TIME; response
        // materializes only an integer. Expected input is bounded above.
        self.cas(checks, &[], None, Some(expires_at_ns), false)
            .await
    }

    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, Some(expires_at_ns), false)
            .await
    }

    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        validate_cas_time_window(not_before_ns, before_ns)?;
        self.cas(checks, writes, not_before_ns, before_ns, false)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        let mut connection = self.connection.clone();
        let (seconds, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        redis_time_ns(seconds, micros)
    }
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(upper);
        }
    }
    None
}

fn redis_time_ns(seconds: i64, micros: i64) -> Result<i64, WorkspaceError> {
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(micros.saturating_mul(1_000)))
        .ok_or_else(|| WorkspaceError::Backend("Redis TIME overflows i64 nanos".into()))
}

fn timed_mget_pipeline(scoped: Vec<Vec<u8>>) -> redis::Pipeline {
    let mut pipeline = redis::pipe();
    pipeline.atomic().cmd("TIME").cmd("MGET").arg(scoped);
    pipeline
}

fn validate_namespace(namespace: &str) -> Result<(), WorkspaceError> {
    if namespace.is_empty()
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(WorkspaceError::Backend(
            "Redis workspace namespace must contain only ASCII letters, digits, '-', '_' or '.'"
                .into(),
        ));
    }
    Ok(())
}

fn validate_operator_principals(admin: &str, runtime: &str) -> Result<(), WorkspaceError> {
    let valid = |value: &str| {
        !value.is_empty()
            && value.len() <= 128
            && value != "default"
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    };
    if !valid(admin) || !valid(runtime) || admin == runtime {
        return Err(WorkspaceError::UnsupportedCapability(
            "operator and runtime Redis principals must be distinct named ACL users",
        ));
    }
    Ok(())
}

async fn authenticate_principal(
    connection: &mut ConnectionManager,
    expected: &str,
) -> Result<(), WorkspaceError> {
    let actual: String = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        redis::cmd("ACL").arg("WHOAMI").query_async(connection),
    )
    .await
    .map_err(|_| WorkspaceError::Backend("Redis identity check timed out".into()))?
    .map_err(|_| WorkspaceError::Backend("Redis identity check failed".into()))?;
    if actual != expected {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn backend(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::Backend(format!("Redis workspace catalog: {error}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn timed_metadata_reads_are_transactional() {
        let packed =
            super::timed_mget_pipeline(vec![b"brewfs:test".to_vec()]).get_packed_pipeline();
        let wire = String::from_utf8_lossy(&packed);
        assert!(
            wire.starts_with("*1\r\n$5\r\nMULTI\r\n"),
            "TIME/MGET metadata reads must enter MULTI/EXEC"
        );
        assert!(wire.ends_with("*1\r\n$4\r\nEXEC\r\n"));
    }

    #[test]
    fn operator_principals_reject_shared_default_or_unbounded_identity() {
        assert!(super::validate_operator_principals("operator", "runtime").is_ok());
        for (admin, runtime) in [
            ("default", "runtime"),
            ("operator", "operator"),
            ("", "runtime"),
            ("operator", "runtime\n"),
        ] {
            assert!(super::validate_operator_principals(admin, runtime).is_err());
        }
        assert!(super::validate_operator_principals(&"a".repeat(129), "runtime").is_err());
    }
    use super::*;
    use futures::FutureExt;

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn real_redis_bounded_keyset_pages_complete_large_prefix() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
        let namespace = format!("kv-pages-{}", uuid::Uuid::new_v4().simple());
        let backend = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        super::super::kv_backend::scan_page_contract::assert_real_pages(&backend).await;
        let mut connection = backend.connection.clone();
        redis::cmd("DEL")
            .arg(backend.scoped(KEY_INDEX))
            .arg(backend.scoped(KEY_INDEX_READY))
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn real_redis_bounded_values_reject_corrupt_oversized_point_and_scan() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
        let namespace = format!("kv-bytes-{}", uuid::Uuid::new_v4().simple());
        let backend = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        let keys = [b"pin/a".to_vec(), b"pin/b".to_vec()];
        let limits = KvReadLimits {
            max_records: 2,
            max_key_bytes: 32,
            max_value_bytes: 4,
            max_total_bytes: 18,
            max_response_bytes: 1024,
            max_data_requests: 1,
        };
        let result = std::panic::AssertUnwindSafe(async {
            backend
                .compare_and_swap(
                    &[],
                    &[
                        KvWrite::Put {
                            key: keys[0].clone(),
                            value: vec![1; 4],
                        },
                        KvWrite::Put {
                            key: keys[1].clone(),
                            value: vec![2; 4],
                        },
                    ],
                )
                .await?;
            let (values, now) = backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await?;
            assert_eq!(values, vec![Some(vec![1; 4]), Some(vec![2; 4])]);
            assert!(now > 0);
            assert_eq!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", limits)
                    .await?
                    .len(),
                2
            );
            let aggregate_too_small = KvReadLimits {
                max_total_bytes: 17,
                ..limits
            };
            assert!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, aggregate_too_small)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", aggregate_too_small)
                    .await
                    .is_err()
            );

            // Inject corruption through the actual persistence backend. A GET
            // plus a Rust len check would transfer this 2 MiB string first.
            let mut connection = backend.connection.clone();
            redis::cmd("SET")
                .arg(backend.scoped(&keys[1]))
                .arg(vec![3_u8; 2 << 20])
                .query_async::<()>(&mut connection)
                .await
                .map_err(super::backend)?;
            assert!(
                backend
                    .get_many_consistent_with_time_bounded(&keys, limits)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", limits)
                    .await
                    .is_err()
            );
            Ok::<(), WorkspaceError>(())
        })
        .catch_unwind()
        .await;
        let mut connection = backend.connection.clone();
        let cleanup: Result<(), redis::RedisError> = redis::cmd("DEL")
            .arg(backend.scoped(KEY_INDEX))
            .arg(backend.scoped(KEY_INDEX_READY))
            .arg(backend.scoped(&keys[0]))
            .arg(backend.scoped(&keys[1]))
            .query_async(&mut connection)
            .await;
        cleanup.unwrap();
        result.unwrap().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn real_redis_short_page_is_complete_range_end() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
        let namespace = format!("kv-short-page-{}", uuid::Uuid::new_v4().simple());
        let backend = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        assert!(backend.short_scan_page_is_end());
        let keys = (0..5)
            .map(|index| format!("page/a/{index:03}").into_bytes())
            .chain([b"page/a0".to_vec(), b"page/b/000".to_vec()])
            .collect::<Vec<_>>();
        let writes = keys
            .iter()
            .map(|key| KvWrite::Put {
                key: key.clone(),
                value: vec![1],
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&[], &writes).await.unwrap());
        let limits = |records| KvReadLimits {
            max_records: records,
            max_key_bytes: 64,
            max_value_bytes: 4,
            max_total_bytes: 1024,
            max_response_bytes: 4096,
            max_data_requests: records,
        };
        // A full page may be followed by more rows; the short tail is the end
        // and an explicit next page after it is indeed empty.
        let first = backend
            .scan_prefix_page_with_byte_limits(b"page/a/", None, limits(3))
            .await
            .unwrap();
        assert_eq!(first.len(), 3);
        let tail = backend
            .scan_prefix_page_with_byte_limits(b"page/a/", Some(&first[2].key), limits(3))
            .await
            .unwrap();
        assert_eq!(tail.len(), 2);
        let after = backend
            .scan_prefix_page_with_byte_limits(b"page/a/", Some(&tail[1].key), limits(3))
            .await
            .unwrap();
        assert!(after.is_empty());
        let mut connection = backend.connection.clone();
        let mut del = redis::cmd("DEL");
        del.arg(backend.scoped(KEY_INDEX))
            .arg(backend.scoped(KEY_INDEX_READY));
        for key in &keys {
            del.arg(backend.scoped(key));
        }
        let _: () = del.query_async(&mut connection).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn real_redis_bounded_scan_rejects_oversized_index_key_and_ghost() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
        let namespace = format!("kv-index-bytes-{}", uuid::Uuid::new_v4().simple());
        let backend = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        let mut key = b"pin/".to_vec();
        key.extend_from_slice(&[b'x'; 2048]);
        let limits = KvReadLimits {
            max_records: 2,
            max_key_bytes: 32,
            max_value_bytes: 4,
            max_total_bytes: 72,
            max_response_bytes: 1024,
            max_data_requests: 1,
        };
        let result = std::panic::AssertUnwindSafe(async {
            backend
                .compare_and_swap(
                    &[],
                    &[KvWrite::Put {
                        key: key.clone(),
                        value: vec![1],
                    }],
                )
                .await?;
            assert!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", limits)
                    .await
                    .is_err()
            );
            let mut connection = backend.connection.clone();
            redis::cmd("ZREM")
                .arg(backend.scoped(KEY_INDEX))
                .arg(backend.scoped(&key))
                .query_async::<()>(&mut connection)
                .await
                .map_err(super::backend)?;
            redis::cmd("ZADD")
                .arg(backend.scoped(KEY_INDEX))
                .arg(0)
                .arg(backend.scoped(b"pin/ghost"))
                .query_async::<()>(&mut connection)
                .await
                .map_err(super::backend)?;
            assert!(
                backend
                    .scan_prefix_with_byte_limits(b"pin/", limits)
                    .await
                    .is_err()
            );
            Ok::<(), WorkspaceError>(())
        })
        .catch_unwind()
        .await;
        let mut connection = backend.connection.clone();
        redis::cmd("DEL")
            .arg(backend.scoped(KEY_INDEX))
            .arg(backend.scoped(KEY_INDEX_READY))
            .arg(backend.scoped(&key))
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        result.unwrap().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn real_redis_clock_window_fences_reap_and_advances_generation_atomically() {
        let backend = RedisWorkspaceBackend::connect(
            &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
            &format!("g12-pin-clock-{}", uuid::Uuid::new_v4().simple()),
        )
        .await
        .unwrap();
        super::super::kv_backend::time_window_contract::assert_real_window(&backend).await;
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn corrupt_key_index_rejects_cas_before_any_inode_acl_write() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
        let namespace = format!("acl-fault-{}", uuid::Uuid::new_v4().simple());
        let backend = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        let index = backend.scoped(KEY_INDEX);
        let mut connection = backend.connection.clone();
        redis::cmd("SET")
            .arg(&index)
            .arg("injected-wrong-type")
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let result = backend
            .compare_and_swap(
                &[],
                &[
                    KvWrite::Put {
                        key: b"test-inode".to_vec(),
                        value: b"mode640".to_vec(),
                    },
                    KvWrite::Put {
                        key: b"test-acl".to_vec(),
                        value: b"mask4".to_vec(),
                    },
                ],
            )
            .await;
        let inode = backend.get(b"test-inode").await.unwrap();
        let acl = backend.get(b"test-acl").await.unwrap();
        // Remove only this test's three explicit keys; no namespace-wide delete.
        redis::cmd("DEL")
            .arg(&index)
            .arg(backend.scoped(KEY_INDEX_READY))
            .arg(backend.scoped(b"test-inode"))
            .arg(backend.scoped(b"test-acl"))
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        assert!(result.is_err());
        assert_eq!(
            (inode, acl),
            (None, None),
            "Redis Lua error left half a permission update"
        );
    }
}
