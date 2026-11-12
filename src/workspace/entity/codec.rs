//! Bounded typed rows; counters use canonical decimal strings instead.

use serde::{Serialize, de::DeserializeOwned};

use super::{EntityError, Result};

pub const MAX_ROW_BYTES: usize = 4 * 1024 * 1024;

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|e| EntityError::Invalid(e.to_string()))?;
    if bytes.len() > MAX_ROW_BYTES {
        return Err(EntityError::Invalid("entity row exceeds size bound".into()));
    }
    Ok(bytes)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_ROW_BYTES {
        return Err(EntityError::Corrupt("entity row exceeds size bound".into()));
    }
    serde_json::from_slice(bytes).map_err(|e| EntityError::Corrupt(e.to_string()))
}
