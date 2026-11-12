//! Local serialization must survive cancellation of the public caller. Backend
//! barriers retain the actual submitted CAS while the competing driver starts.

use super::*;
use std::sync::atomic::AtomicBool;

pub(super) struct Control {
    track: AtomicBool,
    pause: AtomicBool,
    reached: Semaphore,
    resume: Semaphore,
    pub(super) committed: Semaphore,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            track: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
            committed: Semaphore::new(0),
        }
    }
}

impl Control {
    pub(super) async fn before_commit(&self, writes: &[KvWrite]) -> bool {
        let tracked = self.track.load(Ordering::SeqCst)
            && writes.iter().any(|write| {
                matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"delta/inode/"))
            });
        if tracked && self.pause.swap(false, Ordering::SeqCst) {
            self.reached.add_permits(1);
            self.resume.acquire().await.unwrap().forget();
        }
        tracked
    }
}

#[tokio::test]
async fn packed_renewal_waits_for_cancelled_local_mutation_driver() {
    let f = fixture().await;
    let control = &f.control.renewal_mutation;
    let budget = f.authority.budget().clone();
    let baseline = budget.state().used;
    control.track.store(true, Ordering::SeqCst);
    control.pause.store(true, Ordering::SeqCst);
    let mut mutation = Box::pin(real_single_inode_permission_mutation(
        f.authority.view(),
        f.meta.store().as_ref(),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            () = &mut mutation => panic!("mutation must pause at its real CAS"),
            permit = control.reached.acquire() => permit.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    drop(mutation);

    let mut renewal = f.authority.clone().renew(60_000_000_000);
    // The mutation caller is gone; only its owned driver can retain the gate.
    let early = tokio::time::timeout(Duration::from_millis(100), &mut renewal).await;
    let waited = early.is_err();
    control.resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), control.committed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    match early {
        Ok(result) => result.unwrap(),
        Err(_) => tokio::time::timeout(Duration::from_secs(5), renewal)
            .await
            .unwrap()
            .unwrap(),
    }
    shutdown(&f).await;
    assert_eq!(budget.state().used, baseline);
    assert!(
        waited,
        "renewal crossed a cancelled caller's live mutation CAS"
    );
}

#[tokio::test]
async fn packed_mutation_waits_for_cancelled_local_renewal_driver() {
    let f = fixture().await;
    let budget = f.authority.budget().clone();
    let baseline = budget.state().used;
    f.control.arm(MOUNT_RENEW_HEAD_ONCE, 1);
    let mut renewal = f.authority.clone().renew(60_000_000_000);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut renewal => panic!("renewal must pause at its real CAS: {result:?}"),
            permit = f.control.reached.acquire() => permit.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    drop(renewal);

    let mut mutation = Box::pin(real_single_inode_permission_mutation(
        f.authority.view(),
        f.meta.store().as_ref(),
    ));
    let early = tokio::time::timeout(Duration::from_millis(100), &mut mutation).await;
    let waited = early.is_err();
    f.control.resume.add_permits(1);
    if waited {
        tokio::time::timeout(Duration::from_secs(5), mutation)
            .await
            .unwrap();
    }
    shutdown(&f).await;
    assert_eq!(budget.state().used, baseline);
    assert!(
        waited,
        "mutation crossed a cancelled caller's live renewal CAS"
    );
    assert_eq!(f.control.conflicts.load(Ordering::SeqCst), 0);
}
