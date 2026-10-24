//! Redis implementation of the bounded entity substrate, independent of layers.

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use uuid::Uuid;

use super::{
    EntityError, Result,
    backend::{
        CommitOutcome, CreateAllocation, EntityBackend, EntityPlan, MAX_KEY_BYTES,
        MAX_TRANSACTION_BYTES, RowCheck, RowSnapshot, RowWrite, SliceAllocation,
        TransactionAuthority, prepare_create, prepare_slices, validate_checked_read, validate_key,
        validate_plan, validate_scan, validate_scan_page,
    },
    codec::MAX_ROW_BYTES,
    keys::EntityKeys,
};

const READ_LUA: &str = r#"
local max_key, max_row, max_total = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local total = 0
for _, key in ipairs(KEYS) do
    local size = redis.pcall('STRLEN', key)
    if type(size) == 'table' then
        if string.sub(size.err, 1, 10) == 'WRONGTYPE ' then
            return redis.error_reply('ENTITY_CORRUPT invalid row type')
        end
        return size
    end
    total = total + #key + size
    if #key > max_key or size > max_row or total > max_total then
        return redis.error_reply('ENTITY_INVALID read exceeds byte bound')
    end
end
local rows = {}
local read_count, matched = #KEYS - tonumber(ARGV[4]), 1
for i, key in ipairs(KEYS) do
    local current = redis.call('GET', key)
    if i <= read_count then
        rows[i] = current
    else
        local cursor = 5 + 2 * (i - read_count - 1)
        local present, expected = ARGV[cursor], ARGV[cursor + 1]
        if (present == '0' and current) or (present == '1' and current ~= expected) then
            matched = 0
        end
    end
end
return {redis.call('TIME'), rows, matched}
"#;

// The first five args carry small counts and a split lease deadline. Full IDs
// and timestamps never pass through Lua's floating point integer conversion.
// All key types, predicates, operations, counters and bounds are validated
// before the first mutating command: Lua errors cannot roll back a prior SET.
const TRANSACTION_LUA: &str = r#"
local index = table.remove(KEYS, 1)
if redis.call('TYPE', index).ok ~= 'zset' or not redis.call('ZSCORE', index, '') then
    return redis.error_reply('ENTITY_CORRUPT missing entity key index')
end
local authorities, checks, writes = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local mode = ARGV[7]
local extra_keys = mode == 'create' and 2 or (mode == 'reserve' and 1 or 0)
if not authorities or not checks or not writes or authorities < 1 or checks < 0 or writes < 0
    or authorities % 1 ~= 0 or checks % 1 ~= 0 or writes % 1 ~= 0
    or authorities + checks + writes > 4096
    or #KEYS ~= authorities + checks + writes + extra_keys then
    return redis.error_reply('ENTITY_INVALID malformed transaction')
end
local create_at = 8 + 2 * (authorities + checks + writes)
local xattr_count, reservation_count = 0, 0
if mode == 'create' then
    xattr_count = tonumber(ARGV[create_at + 5])
    if not xattr_count or xattr_count < 0 or xattr_count > 2 or xattr_count % 1 ~= 0
        or authorities + checks + writes + 3 + xattr_count > 4096
        or #ARGV ~= create_at - 1 + 6 + 2 * xattr_count then
        return redis.error_reply('ENTITY_INVALID malformed create allocation')
    end
elseif mode == 'reserve' then
    reservation_count = tonumber(ARGV[create_at + 3])
    if checks ~= 0 or writes ~= 0 or not reservation_count or reservation_count < 1
        or reservation_count > 64 or reservation_count % 1 ~= 0
        or authorities + 1 + 2 * reservation_count > 4096
        or #ARGV ~= create_at - 1 + 4 + reservation_count then
        return redis.error_reply('ENTITY_INVALID malformed slice reservation')
    end
elseif #ARGV ~= create_at - 1 then
    return redis.error_reply('ENTITY_INVALID malformed transaction')
end
local expires_s, expires_ns
if ARGV[4] ~= '' or ARGV[5] ~= '' then
    expires_s, expires_ns = tonumber(ARGV[4]), tonumber(ARGV[5])
    if not expires_s or expires_s < 0 or expires_s > 9223372036 or expires_s % 1 ~= 0
        or not expires_ns or expires_ns < 0 or expires_ns >= 1000000000 or expires_ns % 1 ~= 0
        or (expires_s == 9223372036 and expires_ns > 854775807) then
        return redis.error_reply('ENTITY_INVALID invalid lease deadline')
    end
end
local prefix = ARGV[6]
if mode ~= 'commit' and mode ~= 'create' and mode ~= 'reserve' then
    return redis.error_reply('ENTITY_INVALID invalid transaction mode')
end
local total, total_stored = 0, 0
for _, key in ipairs(KEYS) do
    if key == index or #key == 0 or #key > 1024 or string.sub(key, 1, #prefix) ~= prefix then
        return redis.error_reply('ENTITY_INVALID invalid entity key')
    end
    local size = redis.pcall('STRLEN', key)
    if type(size) == 'table' then
        if string.sub(size.err, 1, 10) == 'WRONGTYPE ' then
            return redis.error_reply('ENTITY_CORRUPT invalid row type')
        end
        return size
    end
    total_stored = total_stored + #key + size
    if size > 4194304 or total_stored > 16777216 then
        return redis.error_reply('ENTITY_INVALID stored rows exceed byte bound')
    end
    total = total + #key
end
local cursor = 8
local expected = {}
for i = 1, authorities + checks do
    local present, value = ARGV[cursor], ARGV[cursor + 1]
    if present ~= '0' and present ~= '1' then
        return redis.error_reply('ENTITY_INVALID invalid condition flag')
    end
    if #value > 4194304 then return redis.error_reply('ENTITY_INVALID row exceeds byte bound') end
    local marker = present .. value
    if expected[KEYS[i]] and expected[KEYS[i]] ~= marker then
        return redis.error_reply('ENTITY_INVALID contradictory predicates')
    end
    expected[KEYS[i]] = marker
    total = total + #value
    cursor = cursor + 2
end
local seen = {}
for i = 1, writes do
    local key, operation, value = KEYS[authorities + checks + i], ARGV[cursor], ARGV[cursor + 1]
    if seen[key] or #value > 4194304 then
        return redis.error_reply('ENTITY_INVALID invalid or duplicate write')
    end
    seen[key] = operation
    if operation ~= 'put' and operation ~= 'delete' then
        return redis.error_reply('ENTITY_INVALID invalid write operation')
    end
    total = total + #value
    cursor = cursor + 2
end
if total > 16777216 then return redis.error_reply('ENTITY_INVALID transaction exceeds byte bound') end
local now = redis.call('TIME')
local now_s, now_ns = tonumber(now[1]), tonumber(now[2]) * 1000
if expires_s and (now_s > expires_s or (now_s == expires_s and now_ns >= expires_ns)) then
    return {-1, ''}
end
cursor = 8
for i = 1, authorities + checks do
    local present, value, current = ARGV[cursor], ARGV[cursor + 1], redis.call('GET', KEYS[i])
    if (present == '0' and current) or (present == '1' and current ~= value) then
        return {0, ''}
    end
    cursor = cursor + 2
end
local function valid_counter(counter)
    return counter and string.match(counter, '^%d+$')
        and not (#counter > 1 and string.sub(counter, 1, 1) == '0')
        and (#counter < 19 or (#counter == 19 and counter <= '9223372036854775807'))
end
local function next_decimal(counter)
    local digits, carry = {}, 1
    for i = #counter, 1, -1 do
        local digit = string.byte(counter, i) - 48 + carry
        if digit == 10 then digit, carry = 0, 1 else carry = 0 end
        digits[i] = string.char(48 + digit)
    end
    return (carry == 1 and '1' or '') .. table.concat(digits)
end
local generated, allocated = {}, nil
if mode == 'reserve' then
    local slice_prefix, descriptor_prefix, row_prefix = ARGV[create_at], ARGV[create_at + 1], ARGV[create_at + 2]
    local counter_key = KEYS[#KEYS]
    if counter_key ~= prefix .. 'counter/slice' or expected[counter_key]
        or slice_prefix ~= prefix .. 'slice/' or descriptor_prefix ~= prefix .. 'object/' then
        return redis.error_reply('ENTITY_INVALID invalid slice reservation scope')
    end
    local counter = redis.call('GET', counter_key)
    if not valid_counter(counter) then
        return redis.error_reply('ENTITY_CORRUPT invalid or uninitialized counter')
    end
    local ids = {}
    for i = 1, reservation_count do
        if counter == '9223372036854775807' then return {3, ''} end
        counter = next_decimal(counter)
        ids[i] = counter
        local key, descriptor = slice_prefix .. counter, descriptor_prefix .. counter
        local suffix = ARGV[create_at + 3 + i]
        local value = row_prefix .. counter .. suffix
        if #key > 1024 or #descriptor > 1024 or #value > 4194304 or string.sub(suffix, 1, 1) ~= ',' then
            return redis.error_reply('ENTITY_INVALID invalid pending row')
        end
        if redis.call('EXISTS', key) ~= 0 or redis.call('EXISTS', descriptor) ~= 0 then
            return redis.error_reply('ENTITY_CORRUPT slice allocator collision')
        end
        total = total + #key + #descriptor + #value
        total_stored = total_stored + #key + #descriptor
        generated[i] = {key, value}
    end
    total = total + #counter
    if total > 16777216 or total_stored > 16777216 then
        return redis.error_reply('ENTITY_INVALID slice reservation exceeds byte bound')
    end
    -- Validate the entire batch before consuming even its first identity.
    redis.call('INCRBY', counter_key, reservation_count)
    redis.call('ZADD', index, 0, counter_key)
    for _, row in ipairs(generated) do
        redis.call('SET', row[1], row[2])
        redis.call('ZADD', index, 0, row[1])
    end
    return {5, table.concat(ids, ',')}
end
if mode == 'create' then
    local namespace, parent, lower = ARGV[create_at], ARGV[create_at + 1], ARGV[create_at + 2]
    local inode_suffix, dentry_suffix = ARGV[create_at + 3], ARGV[create_at + 4]
    local counter_key, dentry_key = KEYS[#KEYS - 1], KEYS[#KEYS]
    if string.sub(namespace, 1, #prefix) ~= prefix or string.sub(namespace, -1) ~= '/'
        or counter_key ~= namespace .. 'counter/inode'
        or string.sub(parent, 1, #namespace + 6) ~= namespace .. 'inode/'
        or string.sub(dentry_key, 1, #namespace + 7) ~= namespace .. 'dentry/'
        or not expected[parent] or not expected[dentry_key] or seen[parent] ~= 'put'
        or expected[counter_key] or seen[counter_key] or seen[dentry_key]
        or not valid_counter(lower) or lower == '0'
        or string.sub(inode_suffix, 1, 1) ~= ',' or string.sub(dentry_suffix, 1, 1) ~= ',' then
        return redis.error_reply('ENTITY_INVALID invalid create allocation template')
    end
    if expected[dentry_key] ~= '0' and expected[dentry_key] ~= '1"Whiteout"' then
        return redis.error_reply('ENTITY_EXISTS create destination exists')
    end
    local counter = redis.call('GET', counter_key)
    if not valid_counter(counter) then
        return redis.error_reply('ENTITY_CORRUPT invalid or uninitialized counter')
    end
    if counter == '9223372036854775807' then return {3, ''} end
    -- Compute the exact successor without a floating point conversion. This
    -- previews generated keys for complete validation before INCR mutates state.
    allocated = next_decimal(counter)
    if #allocated < #lower or (#allocated == #lower and allocated <= lower) then
        return redis.error_reply('ENTITY_CORRUPT inode allocator overlaps lower')
    end
    local row_prefix = '{"Present":{"ino":' .. allocated
    generated[1] = {namespace .. 'inode/' .. allocated, row_prefix .. inode_suffix, true}
    generated[2] = {dentry_key, row_prefix .. dentry_suffix, false}
    for i = 1, xattr_count do
        local name, value = ARGV[create_at + 6 + 2 * (i - 1)], ARGV[create_at + 7 + 2 * (i - 1)]
        if #name == 0 or #name > 510 or #name % 2 ~= 0 or string.find(name, '[^0-9a-f]') then
            return redis.error_reply('ENTITY_INVALID invalid create xattr name')
        end
        generated[#generated + 1] = {namespace .. 'xattr/' .. allocated .. '/' .. name, value, true}
    end
    total = total + #allocated
    for _, row in ipairs(generated) do
        local key, value, require_absent = row[1], row[2], row[3]
        if #key > 1024 or #value > 4194304 or seen[key] then
            return redis.error_reply('ENTITY_INVALID invalid generated create row')
        end
        if require_absent then
            if redis.call('EXISTS', key) ~= 0 then
                return redis.error_reply('ENTITY_CORRUPT inode allocator collision')
            end
            total = total + #key
            total_stored = total_stored + #key
        end
        seen[key] = 'put'
        total = total + #value
    end
    if total > 16777216 or total_stored > 16777216 then
        return redis.error_reply('ENTITY_INVALID create exceeds byte bound')
    end
    -- No data-dependent validation remains after this point. INCR's rounded
    -- Lua reply is ignored; the exact decimal preview is the published identity.
    redis.call('INCR', counter_key)
    redis.call('ZADD', index, 0, counter_key)
end
for i = 1, writes do
    local key, operation, value = KEYS[authorities + checks + i], ARGV[cursor], ARGV[cursor + 1]
    if operation == 'put' then
        redis.call('SET', key, value)
        redis.call('ZADD', index, 0, key)
    else
        redis.call('DEL', key)
        redis.call('ZREM', index, key)
    end
    cursor = cursor + 2
end
for _, row in ipairs(generated) do
    redis.call('SET', row[1], row[2])
    redis.call('ZADD', index, 0, row[1])
end
if allocated then return {4, allocated} end
return {1, ''}
"#;

// Backfill only at first open, with SCAN turns outside Lua. Checking existence
// and adding each member atomically prevents a concurrent delete from leaving
// a stale index entry. The empty member marks completion and keeps an empty
// volume's index alive. Old writers must be stopped before upgrading.
const INDEX_LUA: &str = r#"
local index = KEYS[1]
local kind = redis.call('TYPE', index).ok
if kind ~= 'none' and kind ~= 'zset' then
    return redis.error_reply('ENTITY_CORRUPT invalid entity key index type')
end
if redis.call('ZSCORE', index, '') then return 1 end
for i = 2, #KEYS do
    if KEYS[i] ~= index and redis.call('EXISTS', KEYS[i]) == 1 then
        redis.call('ZADD', index, 0, KEYS[i])
    end
end
if ARGV[1] == '1' then redis.call('ZADD', index, 0, '') end
return 0
"#;

// One lexicographic range, bounded by matching rows rather than database size.
// Complete scans detect overflow; batch and paged scans return at most limit
// rows. Paged scans resume after an exclusive cursor. Parent/authority
// predicates fence decisions made from these observations.
const SCAN_LUA: &str = r#"
local index, prefix, limit = KEYS[1], ARGV[1], tonumber(ARGV[2])
local batch = ARGV[3] == 'batch'
local keys_only = ARGV[3] == 'keys'
local paged = keys_only or ARGV[3] == 'page'
if redis.call('TYPE', index).ok ~= 'zset' or not redis.call('ZSCORE', index, '') then
    return redis.error_reply('ENTITY_CORRUPT missing entity key index')
end
-- Find the exact exclusive prefix successor, including binary suffixes.
local upper = '+'
for i = #prefix, 1, -1 do
    local byte = string.byte(prefix, i)
    if byte < 255 then
        upper = '(' .. string.sub(prefix, 1, i - 1) .. string.char(byte + 1)
        break
    end
end
local lower, count = '[' .. prefix, (batch or paged) and limit or limit + 1
if paged and ARGV[4] ~= '' then
    lower = '(' .. ARGV[4]
end
local keys = redis.call('ZRANGEBYLEX', index, lower, upper, 'LIMIT', 0, count)
if #keys > limit then return redis.error_reply('ENTITY_INVALID scan exceeds row bound') end
local total, rows = 0, {}
for _, key in ipairs(keys) do
    if string.sub(key, 1, #prefix) ~= prefix or #key > 1024 then
        return redis.error_reply('ENTITY_INVALID invalid scanned key')
    end
    if keys_only then
        rows[#rows + 1] = key
    else
        if redis.call('TYPE', key).ok ~= 'string' then
            return redis.error_reply('ENTITY_CORRUPT invalid scanned row type')
        end
        local size = redis.call('STRLEN', key)
        total = total + #key + size
        if size > 4194304 or total > 16777216 then
            return redis.error_reply('ENTITY_INVALID scan exceeds byte bound')
        end
        rows[#rows + 1] = {key, redis.call('GET', key)}
    end
end
return rows
"#;

#[derive(Clone)]
pub struct RedisEntityBackend {
    connection: ConnectionManager,
    prefix: Vec<u8>,
    index: Vec<u8>,
}

impl RedisEntityBackend {
    pub async fn connect(url: &str, volume_id: Uuid) -> Result<Self> {
        let client = redis::Client::open(url).map_err(backend_error)?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(backend_error)?;
        let keys = EntityKeys::new(volume_id, Uuid::nil());
        let backend = Self {
            connection,
            prefix: keys.volume_prefix().to_vec(),
            index: keys.key_index(),
        };
        backend.initialize_index().await?;
        Ok(backend)
    }

    async fn initialize_index(&self) -> Result<()> {
        let script = redis::Script::new(INDEX_LUA);
        let mut connection = self.connection.clone();
        let ready: bool = script
            .key(&self.index)
            .arg(0)
            .invoke_async(&mut connection)
            .await
            .map_err(backend_error)?;
        if ready {
            return Ok(());
        }
        // This volume prefix contains only a fixed tag and UUID, no glob bytes.
        let pattern = [self.prefix.as_slice(), b"*"].concat();
        let mut cursor = 0_u64;
        loop {
            let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(128)
                .query_async(&mut connection)
                .await
                .map_err(backend_error)?;
            // COUNT is a hint: explicitly bound each atomic backfill batch.
            for batch in keys.chunks(128) {
                script
                    .key(&self.index)
                    .key(batch)
                    .arg(0)
                    .invoke_async::<bool>(&mut connection)
                    .await
                    .map_err(backend_error)?;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        script
            .key(&self.index)
            .arg(1)
            .invoke_async::<bool>(&mut connection)
            .await
            .map_err(backend_error)?;
        Ok(())
    }

    fn validate_scope(&self, key: &[u8]) -> Result<()> {
        validate_key(key)?;
        if !key.starts_with(&self.prefix) {
            return Err(EntityError::Invalid(
                "entity key belongs to another volume".into(),
            ));
        }
        if key == self.index {
            return Err(EntityError::Invalid("reserved entity key index".into()));
        }
        Ok(())
    }

    async fn transaction(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: Option<&CreateAllocation>,
        slices: Option<&SliceAllocation>,
    ) -> Result<(i64, Vec<u8>)> {
        if create.is_some() && slices.is_some() {
            return Err(EntityError::Invalid("conflicting allocation modes".into()));
        }
        let prepared = create
            .map(|create| prepare_create(authority, plan, create))
            .transpose()?;
        let pending = slices
            .map(|slices| prepare_slices(authority, slices))
            .transpose()?;
        validate_plan(authority, plan)?;
        for check in authority.checks.iter().chain(&plan.checks) {
            self.validate_scope(&check.key)?;
        }
        for write in &plan.writes {
            self.validate_scope(write.key())?;
        }
        if let Some(prepared) = &prepared {
            self.validate_scope(&prepared.counter_key)?;
            self.validate_scope(&prepared.dentry_key)?;
        }
        if let Some(pending) = &pending {
            self.validate_scope(&pending.counter_key)?;
        }
        let script = redis::Script::new(TRANSACTION_LUA);
        let mut invocation = script.prepare_invoke();
        invocation.key(&self.index);
        for check in authority.checks.iter().chain(&plan.checks) {
            invocation.key(&check.key);
        }
        for write in &plan.writes {
            invocation.key(write.key());
        }
        if let Some(prepared) = &prepared {
            invocation
                .key(&prepared.counter_key)
                .key(&prepared.dentry_key);
        }
        if let Some(pending) = &pending {
            invocation.key(&pending.counter_key);
        }
        invocation
            .arg(authority.checks.len())
            .arg(plan.checks.len())
            .arg(plan.writes.len());
        if let Some(deadline) = authority.expires_before_ns {
            invocation
                .arg(deadline / 1_000_000_000)
                .arg(deadline % 1_000_000_000);
        } else {
            invocation.arg("").arg("");
        }
        invocation.arg(&self.prefix).arg(if create.is_some() {
            "create"
        } else if slices.is_some() {
            "reserve"
        } else {
            "commit"
        });
        for check in authority.checks.iter().chain(&plan.checks) {
            invocation
                .arg(if check.expected.is_some() { "1" } else { "0" })
                .arg(check.expected.as_deref().unwrap_or_default());
        }
        for write in &plan.writes {
            match write {
                RowWrite::Put { value, .. } => {
                    invocation.arg("put").arg(value);
                }
                RowWrite::Delete { .. } => {
                    invocation.arg("delete").arg("");
                }
            }
        }
        if let (Some(create), Some(prepared)) = (create, &prepared) {
            invocation
                .arg(&prepared.namespace_prefix)
                .arg(&prepared.parent_key)
                .arg(create.lower_max_ino.to_string())
                .arg(&prepared.inode_suffix)
                .arg(&prepared.dentry_suffix)
                .arg(prepared.xattrs.len());
            for (name, value) in &prepared.xattrs {
                invocation.arg(name).arg(value);
            }
        }
        if let Some(pending) = &pending {
            invocation
                .arg(&pending.slice_prefix)
                .arg(&pending.descriptor_prefix)
                .arg(&pending.row_prefix)
                .arg(pending.suffixes.len());
            for suffix in &pending.suffixes {
                invocation.arg(suffix);
            }
        }
        invocation
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(backend_error)
    }
}

fn backend_error(error: redis::RedisError) -> EntityError {
    let message = error.to_string();
    if message.contains("ENTITY_EXISTS") {
        EntityError::Exists
    } else if message.contains("ENTITY_INVALID") {
        EntityError::Invalid(message)
    } else if message.contains("ENTITY_CORRUPT") {
        EntityError::Corrupt(message)
    } else {
        EntityError::Backend(message)
    }
}

#[async_trait]
impl EntityBackend for RedisEntityBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.read_checked(keys, &[])
            .await
            .map(|(snapshot, _)| snapshot)
    }

    async fn read_checked(
        &self,
        keys: &[Vec<u8>],
        checks: &[RowCheck],
    ) -> Result<(RowSnapshot, bool)> {
        validate_checked_read(keys, checks)?;
        for key in keys.iter().chain(checks.iter().map(|check| &check.key)) {
            self.validate_scope(key)?;
        }
        let script = redis::Script::new(READ_LUA);
        let mut invocation = script.prepare_invoke();
        for key in keys.iter().chain(checks.iter().map(|check| &check.key)) {
            invocation.key(key);
        }
        invocation
            .arg(MAX_KEY_BYTES)
            .arg(MAX_ROW_BYTES)
            .arg(MAX_TRANSACTION_BYTES)
            .arg(checks.len());
        for check in checks {
            invocation
                .arg(u8::from(check.expected.is_some()))
                .arg(check.expected.as_deref().unwrap_or_default());
        }
        let (time, rows, matched): ((i64, i64), Vec<Option<Vec<u8>>>, bool) = invocation
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(backend_error)?;
        if rows.len() != keys.len() || time.0 < 0 || !(0..1_000_000).contains(&time.1) {
            return Err(EntityError::Corrupt(
                "invalid backend clock/read response".into(),
            ));
        }
        let now_ns = time
            .0
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(time.1 * 1000))
            .ok_or(EntityError::Overflow)?;
        Ok((RowSnapshot { now_ns, rows }, matched))
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        match self.transaction(authority, plan, None, None).await?.0 {
            -1 => Err(EntityError::Fenced),
            0 => Ok(CommitOutcome::Conflict),
            1 => Ok(CommitOutcome::Committed),
            _ => Err(EntityError::Corrupt("invalid transaction response".into())),
        }
    }

    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        let (status, bytes) = self
            .transaction(authority, plan, Some(create), None)
            .await?;
        match status {
            -1 => Err(EntityError::Fenced),
            0 => Ok(None),
            3 => Err(EntityError::Exhausted),
            4 => {
                let ino = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|ino| *ino > create.lower_max_ino)
                    .ok_or_else(|| {
                        EntityError::Corrupt("invalid create allocation response".into())
                    })?;
                if ino.to_string().as_bytes() != bytes {
                    return Err(EntityError::Corrupt("noncanonical allocated inode".into()));
                }
                Ok(Some(ino))
            }
            _ => Err(EntityError::Corrupt(
                "invalid create transaction response".into(),
            )),
        }
    }

    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        let (status, bytes) = self
            .transaction(authority, &EntityPlan::default(), None, Some(allocation))
            .await?;
        match status {
            -1 => Err(EntityError::Fenced),
            0 => Ok(None),
            3 => Err(EntityError::Exhausted),
            5 => {
                let text = std::str::from_utf8(&bytes).map_err(|_| {
                    EntityError::Corrupt("invalid slice reservation response".into())
                })?;
                let mut ids = Vec::with_capacity(allocation.sizes.len());
                for decimal in text.split(',') {
                    let id = decimal
                        .parse::<u64>()
                        .ok()
                        .filter(|id| *id > 0 && *id <= i64::MAX as u64)
                        .ok_or_else(|| {
                            EntityError::Corrupt("invalid reserved slice identity".into())
                        })?;
                    if decimal != id.to_string()
                        || ids.len() >= allocation.sizes.len()
                        || ids.last().is_some_and(|previous| *previous + 1 != id)
                    {
                        return Err(EntityError::Corrupt(
                            "noncanonical slice reservation response".into(),
                        ));
                    }
                    ids.push(id);
                }
                if ids.len() != allocation.sizes.len() {
                    return Err(EntityError::Corrupt(
                        "short slice reservation response".into(),
                    ));
                }
                Ok(Some(ids))
            }
            _ => Err(EntityError::Corrupt(
                "invalid slice reservation status".into(),
            )),
        }
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_scan(prefix, limit)?;
        self.validate_scope(prefix)?;
        redis::Script::new(SCAN_LUA)
            .key(&self.index)
            .arg(prefix)
            .arg(limit)
            .arg("complete")
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(backend_error)
    }

    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_scan(prefix, limit)?;
        self.validate_scope(prefix)?;
        redis::Script::new(SCAN_LUA)
            .key(&self.index)
            .arg(prefix)
            .arg(limit)
            .arg("batch")
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(backend_error)
    }

    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        validate_scan_page(prefix, after, limit)?;
        self.validate_scope(prefix)?;
        redis::Script::new(SCAN_LUA)
            .key(&self.index)
            .arg(prefix)
            .arg(limit)
            .arg("keys")
            .arg(after.unwrap_or_default())
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(backend_error)
    }

    async fn scan_prefix_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_scan_page(prefix, after, limit)?;
        self.validate_scope(prefix)?;
        redis::Script::new(SCAN_LUA)
            .key(&self.index)
            .arg(prefix)
            .arg(limit)
            .arg("page")
            .arg(after.unwrap_or_default())
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(backend_error)
    }
}
