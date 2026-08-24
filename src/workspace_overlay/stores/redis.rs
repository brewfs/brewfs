//! Redis transactional substrate for the workspace catalog.

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use super::kv_backend::{KvCheck, KvEntry, KvWrite, WorkspaceKvBackend};
use crate::workspace_overlay::error::WorkspaceError;

const CAS_LUA: &str = r#"
local check_count = tonumber(ARGV[1])
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

local write_count = tonumber(ARGV[cursor])
cursor = cursor + 1
for index = 1, write_count do
    local operation = ARGV[cursor]
    local value = ARGV[cursor + 1]
    cursor = cursor + 2
    local key = KEYS[check_count + index]
    if operation == 'put' then
        redis.call('SET', key, value)
    else
        redis.call('DEL', key)
    end
end
return 1
"#;

#[derive(Clone)]
pub struct RedisWorkspaceBackend {
    connection: ConnectionManager,
    prefix: Vec<u8>,
}

impl RedisWorkspaceBackend {
    pub async fn connect(url: &str, namespace: &str) -> Result<Self, WorkspaceError> {
        validate_namespace(namespace)?;
        let client = redis::Client::open(url).map_err(backend)?;
        let connection = ConnectionManager::new(client).await.map_err(backend)?;
        // The hash tag keeps every catalog key in one Redis Cluster slot, which
        // is required for the multi-key Lua transactions below.
        let prefix = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/").into_bytes();
        Ok(Self { connection, prefix })
    }

    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        let mut scoped = Vec::with_capacity(self.prefix.len() + key.len());
        scoped.extend_from_slice(&self.prefix);
        scoped.extend_from_slice(key);
        scoped
    }
}

#[async_trait]
impl WorkspaceKvBackend for RedisWorkspaceBackend {
    fn name(&self) -> &'static str {
        "workspace-redis"
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let mut connection = self.connection.clone();
        redis::cmd("GET")
            .arg(self.scoped(key))
            .query_async(&mut connection)
            .await
            .map_err(backend)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        let mut connection = self.connection.clone();
        let mut pattern = self.scoped(prefix);
        pattern.push(b'*');
        let mut cursor = 0_u64;
        let mut entries = Vec::new();
        loop {
            let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1024_u32)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
            if !keys.is_empty() {
                let batch: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                    .arg(&keys)
                    .query_async(&mut connection)
                    .await
                    .map_err(backend)?;
                for (key, value) in keys.into_iter().zip(batch) {
                    if let Some(value) = value {
                        let logical =
                            key.strip_prefix(self.prefix.as_slice()).ok_or_else(|| {
                                WorkspaceError::Backend(
                                    "Redis workspace scan returned an out-of-namespace key".into(),
                                )
                            })?;
                        entries.push(KvEntry {
                            key: logical.to_vec(),
                            value,
                        });
                    }
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(entries)
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
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
        let mut connection = self.connection.clone();
        let result: i64 = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        Ok(result == 1)
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        let mut connection = self.connection.clone();
        let (seconds, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        seconds
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(micros.saturating_mul(1_000)))
            .ok_or_else(|| WorkspaceError::Backend("Redis TIME overflows i64 nanos".into()))
    }
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

fn backend(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::Backend(format!("Redis workspace catalog: {error}"))
}
