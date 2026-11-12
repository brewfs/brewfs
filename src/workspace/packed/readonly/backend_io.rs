//! Runtime backend operations and response bytes, excluding local cache reads.
//! Call starts belong inside admitted transport factories. An opening error or
//! the first body error counts one failed operation; repeated errors from that
//! body do not count again. Admission rejection and local file I/O are excluded.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cadapter::client::ObjectByteStream;
use futures_util::StreamExt;

#[derive(Debug, Default)]
pub(super) struct BackendIoCounters {
    pub(super) full_gets: AtomicU64,
    pub(super) heads: AtomicU64,
    pub(super) range_gets: AtomicU64,
    pub(super) requested_bytes: AtomicU64,
    pub(super) received_bytes: AtomicU64,
    pub(super) failures: AtomicU64,
}

impl BackendIoCounters {
    pub(super) fn full_started(&self, length: u64) {
        self.full_gets.fetch_add(1, Ordering::Relaxed);
        self.requested_bytes.fetch_add(length, Ordering::Relaxed);
    }

    pub(super) fn range_started(&self, length: u64) {
        self.range_gets.fetch_add(1, Ordering::Relaxed);
        self.requested_bytes.fetch_add(length, Ordering::Relaxed);
    }

    pub(super) fn head_started(&self) {
        self.heads.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn finish_open<T>(&self, result: anyhow::Result<T>) -> anyhow::Result<T> {
        if result.is_err() {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub(super) fn observe_stream(
        self: &Arc<Self>,
        result: anyhow::Result<ObjectByteStream>,
    ) -> anyhow::Result<ObjectByteStream> {
        let stream = self.finish_open(result)?;
        let counters = self.clone();
        let mut failed = false;
        Ok(Box::pin(stream.map(move |result| {
            match &result {
                Ok(bytes) => {
                    // Count bytes as they leave the actual response, before a
                    // downstream cache file write or integrity check can fail.
                    counters
                        .received_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                }
                Err(_) if !failed => {
                    failed = true;
                    counters.failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {}
            }
            result
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn body_consumption_counts_bytes_and_one_failure_without_a_cache_write() {
        let counters = Arc::new(BackendIoCounters::default());
        let stream = Box::pin(futures_util::stream::iter(vec![
            Ok(bytes::Bytes::from_static(b"ab")),
            Err(anyhow::anyhow!("first body error")),
            Err(anyhow::anyhow!("same failed body")),
        ]));
        let mut observed = counters.observe_stream(Ok(stream)).unwrap();
        assert_eq!(counters.received_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(observed.next().await.unwrap().unwrap().as_ref(), b"ab");
        assert_eq!(counters.received_bytes.load(Ordering::Relaxed), 2);
        assert!(observed.next().await.unwrap().is_err());
        assert!(observed.next().await.unwrap().is_err());
        assert_eq!(counters.failures.load(Ordering::Relaxed), 1);
        assert!(observed.next().await.is_none());
    }
}
