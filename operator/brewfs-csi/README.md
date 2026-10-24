# BrewFS CSI adapter

This directory contains an experimental CSI v1.8 adapter for an existing
BrewFS workspace. It is deliberately standalone (`operator/brewfs-csi` has its
own Cargo workspace) so building the adapter does not change the BrewFS
filesystem binary or its on-disk formats.

## Design

* Driver name: `csi.brewfs.io`.
* Volume identity: `brewfs://<cluster>/<workspace-uuid>`. The cluster name
  prevents an accidental cross-cluster identity collision; the UUID identifies
  an existing operator-provisioned workspace. An administrator supplies this ID
  as a static PV's `spec.csi.volumeHandle`, with the mount configuration in
  `spec.csi.volumeAttributes`. Keep that handle and context fixed for the PV.
* The adapter supports **static volumes only**. It does not advertise
  `CONTROLLER_SERVICE` or `CREATE_DELETE_VOLUME`. `CreateVolume`, `DeleteVolume`,
  and `ValidateVolumeCapabilities` return `Unimplemented`; it has no controller
  inventory or capacity guarantees. Workspace lifecycle remains with
  `BrewFSWorkspace` and its operator. The example PVs use `Retain`.
* A static PV's `capacity.storage` is administrator-supplied Kubernetes binding
  metadata. The adapter does not allocate, reserve, or enforce that capacity.
  Administrators must manage backing storage capacity separately.
* `NodeStageVolume` starts the existing `brewfs mount` command with the
  configured file, workspace UUID, and privileged mount mode. One node plugin
  reuses one hash-derived mount directory for a volume. `NodePublishVolume`
  bind-mounts that directory into the actor target. The bind is remounted
  read-only when requested. The plugin needs `SYS_ADMIN`, `/dev/fuse`, and
  bidirectional propagation for the host paths it serves.
* State is an atomically replaced JSON file under `--state-dir`. It records the
  mount context, published targets, and the PID of a mount process started by
  this plugin, plus the shared mount's kernel filesystem identity and each
  target's read-only mode. Publish and unpublish inspect `/proc/self/mountinfo`
  and compare the device, filesystem root, type, source, and mode before
  accepting an existing target or unmounting it. Foreign mounts and mode
  mismatches return CSI `FailedPrecondition` and are left mounted. The same
  checks apply after a plugin restart; a target path in state alone is not
  proof of ownership.
  Publish checks an existing target before starting or persisting a shared
  mount, so rejecting a foreign target leaves prior state unchanged.
  Unpublish retains an `O_PATH` reference to prevent mount-ID reuse, pins the
  parent directory, and revalidates the exact mount ID and identity immediately
  before `umount2(..., UMOUNT_NOFOLLOW)`. This avoids resolving the caller's
  ancestor pathname again and rejects a final-component symlink swap. Linux
  does not provide an atomic unmount-by-handle operation here: a privileged
  overmount or rename can still race the final check and syscall. Host mount
  changes must be serialized with CSI operations to eliminate that remaining
  window. Passing a target fd through `/proc/self/fd` alone is insufficient;
  Linux can follow an overmount even through that path. Lazy detach is not used.
  Unstage refuses to remove a shared mount while a target is still published.
  If a recorded mount process is gone but its mountpoint remains, the stale
  mount is removed only if its identity still matches, before a new process
  is started. Mounted targets with missing state or legacy state lacking
  identity/mode information are rejected. Drain and unmount these volumes
  before upgrading, then stage and publish them with the new adapter. A
  missing or replaced shared mount also prevents target ownership validation;
  investigate the host mounts before retrying cleanup. A mount visibility
  timeout kills and reaps the child, then removes any mount that appeared
  during the final poll.
* Publish records a pending recovery intent before binding and commits the
  publish only after ownership and mode verification. Rejected verification
  rolls back the owned bind, any shared mount/process started by this attempt,
  and the intent, restoring prior state. If cleanup itself fails, CSI returns
  `Aborted` and keeps recovery state for `NodeUnpublishVolume`; pending binds
  still require source verification but may have a failed read-only remount.
  A changed mount identity at the final unmount re-check also returns `Aborted`.
  This re-check uses `statx(STATX_MNT_ID)` and `/proc/self/mountinfo` and shrinks
  the TOCTOU window to the syscall boundary; it does **not** eliminate it.
  See [the rejected fd/quarantine designs](tests/issue180/README.md).

The BrewFS process remains the lease authority. CSI reference tracking only
controls when the node plugin may unbind and unstage; it cannot grant a second
writer. A writable workspace therefore has exactly one lease-owning BrewFS
mount. Multiple AX actors must either use one mount pod with cooperating
containers (`BrewFSWorkspaceMount.spec.agent`) or use a single CSI-backed AX
Task. Independent writable Tasks for one workspace are rejected by the
workspace lease and are unsupported. The adapter advertises only
`SINGLE_NODE_WRITER` and `SINGLE_NODE_READER_ONLY`; it does not claim
multi-node reader access because each node would otherwise start a distinct
FUSE process.

The lease is acquired by normal BrewFS mount startup and released by its FUSE
shutdown when `NodeUnstageVolume` unmounts the shared path. If the CSI process
restarts, persisted PID/reference state lets it reuse a live mount or remove a
stale mount before retrying lease acquisition; a failed BrewFS lease
acquisition is returned as a failed stage request.

## Kubernetes resources

`manifests/` contains:

* `CSIDriver` (`attachRequired: false`, `podInfoOnMount: true`);
* a privileged node DaemonSet with the CSI node registrar, `/dev/fuse`,
  `/var/lib/kubelet/pods`, `/var/lib/ate`, and `/var/lib/brewfs-csi` mounted
  with bidirectional propagation;
* an example config Secret. Replace its Redis/S3 endpoints and credentials
  before use; it is a template, not a working credential;
* separate static PV/PVC/Pod examples under `manifests/examples/`. Replace the
  PV's cluster name and workspace UUID with an existing workspace before use.
  The PV and PVC use `storageClassName: ""` and explicit binding so Kubernetes
  does not invoke a default dynamic provisioner. Run one example per workspace.

There is no external provisioner, controller Deployment, or dynamic StorageClass
in the base installation. Substrate v0.5.0 `externalVolumeTemplate` and the AX
fork's `WorkspaceSpec.externalStorageClassName` path require `CreateVolume` and
are unsupported under this contract. Use the operator's mount-pod path or a
consumer that accepts an existing static PV instead.

When upgrading an earlier installation, move consumers to existing static PVs
and stop dynamic provisioning. Existing PVs can retain their volume handles and
attributes. Remove the old `brewfs-csi-controller` Deployment, Service, and
ServiceAccount, the `brewfs-csi-provisioner` ClusterRole and ClusterRoleBinding,
the `brewfs-workspace` StorageClass, and the `csi.brewfs.io` Substrate
`CSIDriverConfig` if installed. Applying the new kustomization does not prune
those old resources. Do not delete workspace data or bound PVs during migration.

## Build and local tests

```bash
cargo fmt --manifest-path operator/brewfs-csi/Cargo.toml --all --check
cargo check --manifest-path operator/brewfs-csi/Cargo.toml --locked
cargo test --manifest-path operator/brewfs-csi/Cargo.toml --locked
cargo clippy --manifest-path operator/brewfs-csi/Cargo.toml --locked --all-targets -- -D warnings
kubectl kustomize operator/brewfs-csi/manifests > /tmp/brewfs-csi.yaml
```

The image build expects a BrewFS image containing `/usr/local/bin/brewfs`.
The runtime stage uses Debian Trixie so it can run the current BrewFS images
built with glibc 2.39; the builder installs `protobuf-compiler` before running
the locked Rust build:

```bash
docker build \
  --build-arg BREWFS_IMAGE=ghcr.io/ivanbeethoven/brewfs:<pinned-tag> \
  -t ghcr.io/ivanbeethoven/brewfs-csi:<pinned-tag> \
  operator/brewfs-csi
```

## Verification matrix

The static-volume contract has local regression coverage for capability
discovery, rejection of 1 GiB/2 GiB `CreateVolume` requests with the same name,
and rejection of controller validation/deletion of unknown IDs. All five tests
fail against the earlier implementation. These tests do not exercise host
mounts or establish new cluster/csi-sanity evidence.

The following evidence was collected on the single-node k3s cluster
`brewfs-singapore-v2` on 2026-10-22. The existing `ax-system`, `ate-system`,
WorkerPool, and registry workloads were left running. The temporary resources
were in `brewfs-csi-ax-test` and `ax-csi-test`. This is historical evidence from
before the static-volume contract; the dynamic provisioning and AX
external-volume paths below are no longer supported.

| Area | Result | Evidence |
| --- | --- | --- |
| Rust unit tests | **Verified** | `cargo fmt --manifest-path operator/brewfs-csi/Cargo.toml --all --check`; `cargo test --manifest-path operator/brewfs-csi/Cargo.toml --locked`: 6 tests passed. |
| CSI image | **Verified** | Image `localhost:5001/brewfs-csi@sha256:91d446adb46e48e80b36bd8715440fc3fafb7e168d4de9a062adf2829e32ec96`, built from the committed Dockerfile with `protobuf-compiler` and pushed to the in-cluster registry. |
| Identity and node CSI calls | **Verified** | csi-sanity v5.3.0 full run: no identity or node failures after the target-removal fix. `NodeStageVolume`, `NodePublishVolume`, `NodeUnpublishVolume`, `NodeUnstageVolume`, and idempotent stage/publish paths exercised against the live Unix socket. |
| csi-sanity overall (before static-volume fix) | **Partial** | `Ran 33 of 92 Specs`: **30 passed, 3 failed, 1 pending, 58 skipped**. The first failure was `CreateVolume ... same name and different capacity` (expected an error, got nil). The other failures were invalid-volume `DeleteVolume` and nonexistent-volume `ValidateVolumeCapabilities`. The earlier controller lacked capacity enforcement and a volume inventory; issue #183 removes that unsupported provisioning contract. Snapshot, expansion, stats, and group-controller cases were skipped as unsupported. |
| AX external-volume selection | **Verified** | AX fork `a0a660a`; `WorkspaceSpec.externalStorageClassName: brewfs-ax-workspace` produced a Substrate `externalVolumeTemplate`. The patched runner image was `localhost:5001/ax-task-runner@sha256:1b89768c157514a8e83571abeea5de71828107186a4d776aa4e050571f99ab5e`. |
| AX task data path | **Verified** | `brewfs-csi-task-3` was resumed to `Running` with `Ready=True`, `WorkspaceReady=True`, worker `10.42.0.23`. `ax ssh ... -- sh -c 'cat /workspace/ax-proof; sha256sum /workspace/ax-proof; wc -c < /workspace/ax-proof'` returned `ax-csi-proof:workspace`, SHA-256 `4f6f4a5fcfde254f79f67bf571401928044d4f54b071f5e0a211ec210aecff38`, and `23` bytes. |
| RustFS publication | **Verified** | `aws s3 ls s3://brewfs-data/chunks-v2/ --recursive` showed `chunks-v2/129/0` (27 bytes). Reading that object through RustFS and hashing its 23-byte payload after the four-byte slice header returned the same SHA and `ax-csi-proof:workspace`. |
| Separate remount | **Unverified** | A prior separate remount could list the directory but returned `EIO` while reading the object. The successful AX read plus direct RustFS object check are the current data-path evidence; a clean independent remount remains to be repeated. |
| AX `ssh` guest tunnel | **Verified for this task** | `ax ssh` returned the exact file contents and hash above. The earlier EOF issue was resolved in the tested task path; no claim is made for unrelated guest-service configurations. |

The focused csi-sanity command used unique paths that did not exist before the
run, and cleanup helpers for paths left by the test harness:

```bash
kubectl -n brewfs-csi-ax-test exec <node-pod> -c brewfs-csi -- \
  /tmp/csi-sanity \
  -csi.endpoint unix:///csi/csi.sock \
  -csi.testvolumeparameters /tmp/csi-params.yaml \
  -csi.stagingdir /tmp/csi-full-stage-20261022b \
  -csi.mountdir /tmp/csi-full-target-20261022b \
  -csi.removemountpathcmd /tmp/remove-csi-path.sh \
  -csi.removestagingpathcmd /tmp/remove-csi-path.sh
```

The unit tests use a fake mount-operations boundary; they validate identity
parsing, status mapping, idempotent reference handling, and stale-target
protection without pretending to validate host mount propagation. The k3s
verification above is the only cluster evidence currently recorded.

The focused `ownership_kernel_replacements_are_not_unmounted` test also uses
real tmpfs bind mounts in a private Linux mount namespace. It checks overmounts,
symlink swaps, replacement by another bind of the same filesystem, busy mounts,
and successful cleanup. Build the CSI library test binary, then run that test
with `sudo unshare --mount --fork --propagation private env
CSI_PRIVATE_MOUNT_TEST=1 <test-binary> --exact
tests::ownership_kernel_replacements_are_not_unmounted --ignored --nocapture`.
The test refuses to run in the host mount namespace or with shared propagation.

For a kind smoke after replacing the config Secret values and the static PV's
`volumeHandle` with an existing operator-provisioned workspace:

```bash
kubectl apply -k operator/brewfs-csi/manifests
kubectl apply -f operator/brewfs-csi/manifests/examples/kind-smoke.yaml
kubectl -n brewfs-csi-smoke wait --for=condition=Ready pod/workspace-smoke --timeout=180s
kubectl -n brewfs-csi-smoke logs pod/workspace-smoke
```

The smoke command is a recipe only; it was not run here.
