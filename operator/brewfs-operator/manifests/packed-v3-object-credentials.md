# packed object roles

Configure `spec.workspace.objectRuntimeSecretRef.name` and
`spec.workspace.objectAdminSecretRef.name` on BrewFSCluster. Both Secrets are
namespace-local and contain `accessKey` and `secretKey`. Missing/invalid values,
the same access key in both roles, or either role using the configured RustFS
server root access key are rejected. The operator does not create permissive
replacement credentials. Mount sidecars receive only the runtime Secret; the
operator's authenticated object clients receive only the admin values.

Provision two actual, distinct server users through the storage administrator.
Render `packed-runtime-object-policy.json` for the cluster's bucket and attach
it to the runtime user using the deployed S3/RustFS server's supported IAM
administration mechanism. Give the separate admin user `s3:ListBucket` on the
exact bucket and `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and
`s3:DeleteObjectVersion` on that bucket's objects, plus only the multipart
actions required by its production paths. HEAD uses `s3:GetObject`. Do not
attach a broad policy that bypasses the runtime
deny, use a server root user as runtime, or infer enforcement from Secret names.
Rendering a JSON file does not attach it or prove the server implements it.

The runtime policy explicitly denies object deletion and version deletion.
It denies all bucket-level S3 actions using `s3:*` scoped to the exact bucket
ARN; object GET and required PUT/multipart permissions use the object ARN.
Create-only publication still uses the existing conditional-PUT implementation;
this identity policy does not claim to deny arbitrary unconditional overwrites.

Use `tools/testing/packed_object_permission_probe.py --bucket <bucket>
--render-policy <new-json-path>` to produce the deployable policy without any
network calls. For a real probe, provide `BREWFS_TEST_RUNTIME_ACCESS_KEY`,
`BREWFS_TEST_RUNTIME_SECRET_KEY`, `BREWFS_TEST_ADMIN_ACCESS_KEY` and
`BREWFS_TEST_ADMIN_SECRET_KEY` through a trusted environment, then run the script
with `--endpoint`, `--bucket`, `--metadata-backend redis|tikv` and `--out`.
Run separately for the actual Redis and TiKV workload identities. The backend
label records which deployment was tested; the S3 probe does not independently
verify metadata authorization or substitute for a mounted-workload test.

The probe creates one random owned 256-byte object, verifies full/range reads
and conditional overwrite refusal, requires a real HTTP 403 authorization
response to runtime DELETE, verifies unchanged bytes, then requires admin
DELETE plus an independently authorized admin HEAD returning 404. A bounded
admin listing of the fresh owned prefix and a positive HEAD before deletion
first prove its ListBucket and GetObject access. Runtime GET of a missing key
can return 403 without ListBucket and is not used as absence evidence. SDK
mutation replay is disabled. An ambiguous
admin DELETE is recorded and never replayed for cleanup. No bucket/container,
user, policy, metadata or unrelated object is created/deleted by the probe.

On 2026-10-08, the policy and probe passed against a disposable RustFS server
(`rustfs/rustfs@sha256:7bc4270ebbf1c17a76de8cdf59ec010bddac36d90f19be520b77d12eb256016c`).
Both probe labels verified runtime create/read/range, conditional overwrite
refusal, HTTP 403 on DELETE with unchanged bytes, and admin DELETE followed by
independently authorized absence checks. A second owned run enabled bucket
versioning, verified Enabled through the separate admin identity, and passed
actual VersionId DELETE denial with HTTP 403 for both probe labels. Admin
version-specific deletion and independent absence of all versions/delete markers
also passed. Both runs removed their exact owned container and temporary
credentials. They started no Redis/TiKV services and are object IAM evidence
only; runtime BlockStore capability separation, metadata authority, and mounted
workloads have separate gates.
