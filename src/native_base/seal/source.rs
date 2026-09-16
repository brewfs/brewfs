//! The object backend abstraction the seal reader reads through.
//!
//! PR03 executes against any implementation of [`ObjectSource`]; the product
//! S3/block-store backend and its namespace/authorization binding arrive with
//! the PR07 integration. Tests use an in-memory counting fake so fetch
//! counts, short reads and missing objects are observable (`fake计数`).

use crate::native_base::wire::refs::ObjectId;

#[derive(Debug, thiserror::Error)]
pub enum ObjectSourceError {
    /// The referenced object does not exist. A *reference* failure, never a
    /// Hole (spec 06 §4: 404 is reference corruption, not zeros).
    #[error("object not found")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
    /// The backend returned fewer bytes than requested.
    #[error("short read: requested {requested}, received {received}")]
    ShortRead { requested: u64, received: u64 },
    #[error("backend error: {0}")]
    Backend(String),
}

/// Read `[start, end)` of the object identified by `object_id`.
///
/// Implementations must return exactly `end - start` bytes or an error; a
/// short read is an error, not a zero fill.
pub trait ObjectSource {
    fn get_range(
        &self,
        object_id: &ObjectId,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, ObjectSourceError>;
}

impl ObjectSource for &[(&ObjectId, &[u8])] {
    fn get_range(
        &self,
        object_id: &ObjectId,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, ObjectSourceError> {
        for (id, bytes) in self.iter() {
            if *id == object_id {
                let len = bytes.len() as u64;
                if end > len || start > end {
                    return Err(ObjectSourceError::ShortRead {
                        requested: end - start,
                        received: len.saturating_sub(start),
                    });
                }
                return Ok(bytes[start as usize..end as usize].to_vec());
            }
        }
        Err(ObjectSourceError::NotFound)
    }
}
