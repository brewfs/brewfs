//! Redis `ControlStore`: every transaction is one atomic Lua script.
//!
//! A [`Txn`](super::store::Txn) is exactly a compare-and-set over a batch of
//! keys, which is what `EVAL` gives us natively: all checks run, then all
//! writes apply, with no other client interleaving (Redis executes scripts
//! atomically). A failed check surfaces as the sentinel error
//! `BREWFS_CONFLICT`, mapped to [`StoreError::Conflict`] — retryable.
//!
//! Keys are used as-is: every key the pipeline builds already lives under
//! `nb2/{volume}/` (see [`super::keys`]), so an instance shared with other
//! subsystems never collides.

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;

use super::store::{ControlStore, Expect, StoreError, Txn};

/// The sentinel a failed check returns; distinguishing a genuine conflict
/// from every other Redis error is what makes `Conflict` retryable rather
/// than a backend failure.
const CONFLICT_MARK: &str = "BREWFS_CONFLICT";

/// ARGV layout (KEYS = check keys, then write keys, in order):
///
/// ```text
/// ARGV[1]                       = number of checks
/// per check i (1-based):        ARGV[2i] = tag, ARGV[2i+1] = expectation
/// ARGV[2*nc+2]                  = number of writes
/// per write i (1-based):        ARGV[2*nc+2i+1] = tag, ARGV[2*nc+2i+2] = value
/// ```
///
/// Tags: `A` absent, `V` expected bytes, `S` set, `D` delete. Absent and
/// delete still carry an empty placeholder, keeping the framing fixed-width.
/// A missing key reads as `false` in Lua, which never equals a string
/// expectation — an `Expect::Bytes` check on an absent key fails, as it must.
const TXN_LUA: &str = r#"
local nc = tonumber(ARGV[1])
for i = 1, nc do
  local k = KEYS[i]
  local tag = ARGV[2 * i]
  local expected = ARGV[2 * i + 1]
  if tag == 'A' then
    if redis.call('EXISTS', k) == 1 then
      return redis.error_reply('BREWFS_CONFLICT')
    end
  else
    local current = redis.call('GET', k)
    if current ~= expected then
      return redis.error_reply('BREWFS_CONFLICT')
    end
  end
end
local nw = tonumber(ARGV[2 * nc + 2])
for i = 1, nw do
  local k = KEYS[nc + i]
  local tag = ARGV[2 * nc + 1 + 2 * i]
  local value = ARGV[2 * nc + 2 + 2 * i]
  if tag == 'D' then
    redis.call('DEL', k)
  else
    redis.call('SET', k, value)
  end
end
return 'OK'
"#;

/// The control plane on Redis.
pub struct RedisControlStore {
    conn: MultiplexedConnection,
    txn_script: redis::Script,
}

impl RedisControlStore {
    /// Connect to a Redis instance at `url` (e.g. `redis://127.0.0.1:6379`).
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let client = redis::Client::open(url).map_err(backend)?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(backend)?;
        Ok(Self {
            conn,
            txn_script: redis::Script::new(TXN_LUA),
        })
    }
}

fn backend(err: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("redis control store: {err}"))
}

fn is_conflict(err: &redis::RedisError) -> bool {
    err.to_string().contains(CONFLICT_MARK)
        || err.detail().is_some_and(|d| d.contains(CONFLICT_MARK))
}

/// Escape glob metacharacters so a `SCAN MATCH` pattern matches exactly the
/// literal `prefix` (binary keys are matched byte-for-byte).
fn glob_escape(prefix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 8);
    for &byte in prefix {
        match byte {
            b'*' | b'?' | b'[' | b']' | b'\\' => {
                out.push(b'\\');
                out.push(byte);
            }
            _ => out.push(byte),
        }
    }
    out
}

#[async_trait]
impl ControlStore for RedisControlStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let mut conn = self.conn.clone();
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(backend)?;
        Ok(value)
    }

    async fn run(&self, txn: Txn) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let mut invocation = self.txn_script.prepare_invoke();
        invocation.arg(txn.checks.len());
        for (key, expect) in &txn.checks {
            invocation.key(key.as_slice());
            match expect {
                Expect::Absent => {
                    invocation.arg("A").arg(&[] as &[u8]);
                }
                Expect::Bytes(expected) => {
                    invocation.arg("V").arg(expected.as_slice());
                }
            }
        }
        invocation.arg(txn.writes.len());
        for (key, value) in &txn.writes {
            invocation.key(key.as_slice());
            match value {
                Some(v) => {
                    invocation.arg("S").arg(v.as_slice());
                }
                None => {
                    invocation.arg("D").arg(&[] as &[u8]);
                }
            }
        }
        let outcome: Result<String, redis::RedisError> = invocation.invoke_async(&mut conn).await;
        match outcome {
            Ok(_) => Ok(()),
            Err(err) if is_conflict(&err) => Err(StoreError::Conflict),
            Err(err) => Err(backend(err)),
        }
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let mut conn = self.conn.clone();
        let mut pattern = glob_escape(prefix);
        pattern.push(b'*');

        // SCAN is incremental and unordered: collect, filter back to the
        // literal prefix (MATCH is a glob), sort, then fetch the values.
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let (next, batch): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern.as_slice())
                .arg("COUNT")
                .arg(512u64)
                .query_async(&mut conn)
                .await
                .map_err(backend)?;
            keys.extend(batch.into_iter().filter(|key| key.starts_with(prefix)));
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        keys.sort();

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let value: Option<Vec<u8>> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(backend)?;
            if let Some(value) = value {
                out.push((key, value));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_escape_quotes_metacharacters() {
        assert_eq!(
            glob_escape(b"nb2/ab*c?[d]\\e"),
            br"nb2/ab\*c\?\[d\]\\e".to_vec()
        );
        assert_eq!(glob_escape(b"nb2/0909/"), b"nb2/0909/".to_vec());
    }
}
