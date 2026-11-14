//! Let an independent Native writer make progress between rejected CAS attempts.
//! This changes scheduling only: the caller must reacquire its complete fresh
//! permissions/version and use the original guarded CAS on every next attempt.

use super::*;
use std::time::Duration;

/// There are still exactly 64 attempts. Do not wait after the final rejection.
/// Requested delays total 5727 ms; backend work and scheduler delay are separate.
fn retry_delay(attempt: usize) -> Option<Duration> {
    (attempt < 63).then(|| Duration::from_millis((1_u64 << attempt.min(7)).min(100)))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeRetryEvent {
    Scheduled { attempt: usize, millis: u64 },
    Pending { attempt: usize },
}

impl<W: WorkspaceStore + 'static> WorkspaceMetaLayer<W> {
    /// Called only by Native open/set_attr after their existing Busy result.
    /// Packed callers keep their existing yield and owner lifetime unchanged.
    pub(super) async fn wait_native_mutation_retry(&self, attempt: usize) {
        let Some(delay) = retry_delay(attempt) else {
            return;
        };
        let wait = tokio::time::sleep(delay);
        #[cfg(not(test))]
        wait.await;
        #[cfg(test)]
        {
            let events = self.native_retry_events.lock().unwrap().clone();
            if let Some(events) = &events {
                let _ = events.send(NativeRetryEvent::Scheduled {
                    attempt,
                    millis: delay.as_millis() as u64,
                });
            }
            tokio::pin!(wait);
            let mut observed_pending = false;
            std::future::poll_fn(|cx| {
                let result = std::future::Future::poll(wait.as_mut(), cx);
                if result.is_pending() && !observed_pending {
                    observed_pending = true;
                    if let Some(events) = &events {
                        let _ = events.send(NativeRetryEvent::Pending { attempt });
                    }
                }
                result
            })
            .await;
        }
    }

    #[cfg(test)]
    pub(super) fn observe_native_retry_waits(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<NativeRetryEvent> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let old = self.native_retry_events.lock().unwrap().replace(sender);
        assert!(old.is_none(), "one observer per test instance");
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_mutation_backoff_has_63_waits_and_bounded_total() {
        let waits = (0..64).filter_map(retry_delay).collect::<Vec<_>>();
        assert_eq!(waits.len(), 63);
        assert_eq!(
            waits[..8],
            [1, 2, 4, 8, 16, 32, 64, 100].map(Duration::from_millis)
        );
        assert!(
            waits
                .iter()
                .all(|delay| *delay <= Duration::from_millis(100))
        );
        assert_eq!(waits.iter().sum::<Duration>(), Duration::from_millis(5727));
        assert_eq!(retry_delay(63), None);
        assert_eq!(retry_delay(usize::MAX), None);
    }
}
