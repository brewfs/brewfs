# Packed-v3 metadata closeout development validation

Scope is packed-v3, with wire005 as the encoding module. Redis/TiKV remain
the distributed metadata backends. The temporary local authentication ledger
does not replace them. Public names and the private hardlink comparison domain
use packed-v3; wire005 is only an internal encoding identifier.

## 2026-10-08 results and remaining gates

Current completed checks: full stores289/0, real focused25/0 and full real
attempt12: 26 phases, 64 passed/0 failed/0 unexecuted. Every phase retained
identical source/config/Git inputs. Root consumed session40073 terminal exit0
and independently verified exact Redis/PD/TiKV IDs and proxy start ticks absent.
Evidence is `metadata-system-closeout07/actual-test-counts.json` under the existing
external recovery root; it binds every log and the frozen source snapshot.
Mounted/operator integration and the full repository gate remain open.

Predecessor iteration: real focused19 executed 12 passed/7 failed, zero unexecuted,
with unchanged inputs and exact owned-service cleanup/absence verified. All
seven failures still encountered TiKV Get/KeyError.locked with the strict local
marker false. The upper process case passed. `native-lock-focused-lock-green01`
retains those results without accepting failed phases.

An independent controlled lock on one fresh key in the same pinned TiKV image
produced Get KeyError plus scalar execution timing. Exact owned rollback and
a subsequent Get without error were verified; no keys, IDs or protobuf bodies
were recorded. `tikv-lock-shape-observation01` proves a supported response shape,
not the owner or origin of the prior failures. The bounded timing validator
retains the full error/LockInfo checks and private marker, and accepts only known
canonical scalar TimeDetail/TimeDetailV2 fields, with a whole16KiB/timing128B cap.
Seven appended transport tests preserved all original22 tests. Actual RED ran
26 passed/3 failed, log
`76dd63fc1bda560bf26ca362c81ab812a386d331f23aa6b95d2e65f005c13217`;
the complete ordinary SDK GREEN then passed113/0/1 real-backend ignored, log
`215dd7e38ee56a5d83dc30b7e531d6b92fca7c247292e18d10d53db0ff9e36cc`.
Both runs kept inputs unchanged. Unknown/malformed/duplicate/empty telemetry,
other payload/error shapes, Scan and forged remote markers remain rejected.
The subsequent focused25 and complete attempt12 reruns passed on that input.

The four stores failures below have exact test-only fixture repairs integrated:
positive rebind clock, workspace/native hold update in one actual fixture CAS,
and an exact five-key heartbeat check with both single-key allocators retained.
Production and G13 target assertions are unchanged. The full stores RED then
executed 283 passed/6 failed/71 ignored, with only new initial-history tests
failing. The Deleting-only initial1 retirement delta and two real initial-GC
tests are now integrated. Full stores GREEN01 executed 286 passed/3 failed/73
ignored, unchanged inputs, log
`bc02ddeadf7d7661f1a1119285bbf4e586b147882d9c3f80cf337fd86f381223`.
The repairs add deadline CAS to the test backend, prove one canonical native
owner before expecting its existing Fenced contract, and check durable Deleting
eligibility before routing-coherence Busy. The original anchor assertion and
all read/CAS authority checks remain. `stores-initial-history-green02` passed
the full stores regression: 289 passed/0 failed/73 ignored, unchanged inputs,
log `af70fb702ce621deea687c4bab72b2b979eff37cf3ed747d7cc80c78107eec38`.
Real focused25 completed 25 passed/0 failed/0 unexecuted across seven phases,
with unchanged source/config/Git and exact owned cleanup independently verified.
It covers factory2, recovery12, Prepare4, persistent-upper process1, reaper4 and
initial history-GC2. Artifact: `native-metadata-focused-metadata-green01`, with
`actual-test-counts.json` binding every log. The initial cases use real Redis/TiKV
metadata and LocalFS packed objects plus actual native GC with an empty native
block store; they do not certify FUSE or remote object storage.
Full attempt12's original 26 phases/64 tests completed with unchanged inputs:
64 passed/0 failed/0 unexecuted, including all original factory/recovery/Prepare,
actual process exits, history retirement, bounded metadata reads, durable pins,
public binding and no-replay tests. Owned cleanup was independently confirmed.
This iteration has not passed the full repository or all-SPEC gate.

The next development iteration has reproduced the native root-read defect:
14 tests executed, 6 passed/8 failed with unchanged inputs, log
`56467c15e8aff621ee83d60ec5405bd53051d8eafdf24f745f0fc7e52d561c7b`.
Four positive publication cases fail at the old root-only rejection; two fresh
read-error cases and two three-round churn cases cannot reach their intended
paths before that rejection. Tests and all original assertions are retained.
Root-read alignment, native lease-reaper02 plus census owner-reuse03 and
test-only hash diagnostics are integrated. Root-read GREEN passed all 14 with
unchanged source, log
`9efe673cffaeb61405b7bf92994c7ac0fb835a52b8e71a3d9d488d7d4853a2cf`;
retained mutation12 and Quiesced10 also passed. The read bound is per entry,
not a global bound across nested staged/phase authority. Reaper27 was 26/1:
the original missing-sentinel MAIN-only Fenced assertion still fails.
The first scratch run was 0/3 due to a fixture cleanup baseline error. Its
repaired baseline includes the real persistent coordinator Roots after a
plan-only operation and asserts zero payload fetches. All eight pool checks
remain exact. That first run is not claimed as behavioral RED. Corrected RED02
executed 0/3 at the intended payload assertions, with unchanged source, log
`def6364ba1e8988e2b9e5f9ce401b9a57f5d476b449a8908d0da3bde892250a9`.
The three cases demonstrate fetching before budget rejection and allocation
after token cancellation/caller abort. Payload production is now integrated;
GREEN passed all three with unchanged source, log
`693dbbef07e40d69e46ef7522d329ca09c6e9cb9e7377e2b62a01555a6efccba`.
The retained complete roundtrip passed one, log
`c2fae515c4a41b6675c2c0361e9c5a8a13951c2b43d0da47078fe2e6cd174342`.
Reaper classification04 moved the absent-sentinel check before protective Busy;
all 27 unchanged tests passed, log
`598fe0f3f4c2c97ff2a01f1abda676362095014cfc3b2ba43338c32d55c5a07f`.
The fixed seven-byte bounded SDK classifier passed the ordinary suite: 84/0,
one real-backend entry ignored, unchanged source, log
`3c2ff88140ee47dd50e3ae37534de71b19ae4be6fd1fd7963016ded8d55ef612`.
It changes diagnostic details, not retry policy; complete means shallow tag
presence only, never decoded validity or permission to retry.
Seed readonly RED executed 4 passed/4 failed, log
`dbc79c9857b1e9ba0e2094907aaa72d7f2410d4963d48a29b8436806fdad88d2`.
The single-function repair is integrated with all eight test bytes retained;
GREEN passed all eight with unchanged source, log
`6d36a26936bdef13f3690255317e88723f9a65dd3d5378733895864ef1d4418e`.
Cleanup request-limit and page-stage diagnostics are also integrated. Actual
real green03 completed 5 passed/0 failed/0 unexecuted: Redis Prepare2, Redis
Quiesced1, TiKV Quiesced1 and TiKV factory1. Source/config/Git were unchanged
and all exact owned services are verified absent. Its original assertions,
including TiKV Quiesced namespace cleanup, remain intact. The old Redis Fenced
and TiKV bounded error did not reproduce; their root causes are not claimed
resolved. `native-closeout-diagnostic-green03/actual-test-counts.json` records
terminal counts. The original 26-phase/64-test attempt11 then completed all 64:
56 passed/8 failed/0 unexecuted, with 39 conservative accepted passes from
wholly successful phases. Source/config/Git were unchanged, with exact owned
cleanup and independent absence verification. All eight failures are TiKV
Get/KeyError.locked, diagnosed by `[66,82,69,1,2,1,0]`; no lock owner is known.
Failed phases were factory6/2, recovery8/4, prepare3/1 and upper process0/1.
Several failed before their intended fault-injection assertions. Both history
retirement tests, Quiesced restarts and the later system/no-replay phases passed.
`metadata-system-closeout06/actual-test-counts.json` records terminal counts;
the single-use summarizer has run. Post-run identity map_err page observers
were changed to inspect_err with identical diagnostic/error behavior; fmt/diff
passed, but the prior runtime does not certify that new source inventory.
Strict local Get lock classification is integrated. The interface-only RED
executed 22 transport tests, 19 passed/3 failed with unchanged inputs, log
`5548484a0747e1373cb38ea0d7fe1e87a7671996a744b547bd15cc087898e8ef`.
After the validator-only delta, the complete ordinary SDK suite passed 93/0,
with one real-backend test ignored and unchanged inputs, log
`4ff6870c3cc6f12abf5e702063b6fd87af00d3f33a8576e73be24515d9935cfa`.
The original 13 transport tests are an exact unchanged prefix, followed by nine
tests through the generated client and tonic decoder using an in-process HTTP
Service. This is not a real TiKV or socket run. Classification is default-off
and requires complete error-only Get/KeyError/LockInfo validation; a private
local status source marker cannot be forged by a matching remote diagnostic.
It retains no keys and introduces no resolver or retry within the classifier.
The additive SDK fixed data deadline bridge passed ten deadline/HTTP2 tests and
three actual Transaction timestamp tests; the complete ordinary SDK suite then
passed 106/0, with one real-backend test ignored and unchanged inputs, log
`1c8e3d9de9cbbaeec37e89b583863d107e789abcbf378183d5f039f05f110531`.
BrewFS opt-in and bounded same-transaction/key continuation are now integrated.
The interface-only RED ran 12 passed/4 failed of sixteen point-batch tests. All
sixteen passed with the production continuation loop in the subsequent stores
run. That run is terminal at 277 passed/4 failed/71 ignored with unchanged inputs,
log `7cf5c5d0072e8fe36d0eabaa3a1d6bf3615ef78f79fdedab355fee63270fa922`.
The four retained failures are native rebind's bounded begin response, two
packed-root GC fixtures returning Fenced, and heartbeat/allocator single-hot-key
CAS checks; the stores gate remains failed pending diagnosis. The data deadline
starts after initial TSO acquisition, with at most two shared continuations and
the original caller request/byte bounds. Generic errors and Scan remain strict.
Real Redis/TiKV focused19 is running under `native-lock-focused-lock-green01`.
The complete gate and remaining system exits remain pending.
Actual diagnostic green02 was 3/2, all five executed, with unchanged source and
verified owned-service cleanup. Redis Prepare2 and Quiesced1 passed; the prior
Fenced has not been classified. TiKV Quiesced's child recovery and publication
passed, but its parent failed namespace cleanup because a 32-record page was
limited to one TiKV data request. TiKV factory failed at hash count table1 with
a cursor, using the SDK's generic bounded key/region error. Neither failed
parent is counted as accepted. Their logs and failed process files are preserved.
No all-SPEC acceptance is claimed.

Latest development batch uses packed-v3 throughout; no separate public version
was introduced. Targeted diagnostic RED completed five actual tests, one passed
and four failed, with unchanged source and verified owned-service cleanup.
Redis expired-lease Prepare passed. Original-lease Prepare reached its genuine
first-PNB owner handoff, then failed final owner validation. TiKV factory's
Building revision 19 CAS returned false with only the packed root generation
changed. Both original Quiesced process-restart cases remained Busy. Logs and
preserved failed-process diagnostics are in `native-closeout-diagnostic-red01`
and `redis-tikv-native-diagnostic-20261008-red01-v3` under the recovery root below.

The exact final-owner fix now derives ownership from opaque seed/PPJ recovery
authority, including recovery that retains the original lease. Quiesced takeover
production02 includes bounded MAIN census for every sentinel state, retaining
all exact checks through takeover and lost-reply confirmation. The original nine
Quiesced tests are byte-for-byte preserved; the MAIN-only remnant test is appended.
Native mutation CAS false permits at most three complete rebuild attempts, only
when every condition except packed root generation remains exact; deadlines never
increase, and unknown commits/read errors never enter that loop. Twelve actual
Memory VFS-to-publication tests cover reserve/dispatch, changed authority, bounded
churn and unknown replies. Exact integration and file-local formatting are recorded
in `native-closeout-integration01`. Development GREEN completed 22 passed/0 failed,
with unchanged source, log
`f67cc643b24b2c6eb790e1676a99e850b927458fe094d66f37c2a83fb56e970b`.
The targeted real-backend rerun completed all five tests: 3 passed/2 failed,
unchanged source, with exact services cleaned and verified absent. Redis original
lease Prepare, Redis Quiesced restart and the TiKV complete factory passed.
Expired Redis Prepare failed Fenced at native-hash-capture; TiKV Quiesced recovery
reached actual CAS-false rebuild, then encountered a root-only mismatch between
the native/seed and staged read snapshots. All original failures and the new
owned-process diagnostics are retained. Actual counts are in
`native-closeout-diagnostic-green01/actual-test-counts.json`; only two passes from
wholly successful phases are counted conservatively. Further read-conflict and
hash diagnosis work remains required. This is not a full gate or all-SPEC result.

This section supersedes the pending statuses in the historical notes below.
Attempt10 has now finished all 26 phases and all 64 expected tests: 44 passed,
20 failed, zero unexecuted. The conservative accepted count is 34. All source,
configuration and Git inputs were unchanged throughout the run; exact owned
proxy/Redis/PD/TiKV cleanup and absence verification passed. The terminal
receipts and actual per-phase counts are in `metadata-system-closeout05`, under
`C:/Codex-Recovery/brewfs-root-20261005/v3-plans-repair01`.
Failed owned process directories were preserved with per-file hashes in
`redis-tikv-closeout-20261008-attempt10-v3/owned-process-diagnostics`.
The exact real-backend configuration precompiled and passed six census tests;
log `8051e2804d15acdc8ac01406258e5685e45b34bf596bd6a3e5b3cd2271903f83`.
Both distributed catalog tests, immediate TiKV secondary/CAS/read/scan, public
bindings, pin reopen/expiry, clock windows, destructive revalidation and both
no-replay commit tests passed. Native TiKV factory/recovery/history still fail
Busy or bounded-read key/region errors; task-owner exhaustion was not observed.
Both Quiesced process recovery tests still fail Busy. Redis Prepare factory
inspection found the test recorder replacing first creation with later
Building updates. The exact predecessor-None recording correction is now
applied; all original owner/PPJ same-CAS assertions remain and await rerun.
Its test source SHA256 is
`94dddbd1225ef6ba0fe9ab771d1b1708927842b5213066456c4f214a89aa80c8`.
The Quiesced absence-proof and stage diagnostics remain external candidates.
This is not a full Rust, mounted FUSE, all-SPEC or experiment acceptance.

Actual attempt09 is terminal: 26 phases; 37 passed / 11 failed, with 16 planned
recovery/Prepare tests unexecuted due to two incorrect module selectors.
The conservative accepted count is 32 from wholly successful phases.
Source/config/Git remained unchanged and all exact owned services were cleaned.
Actual Redis history grace/reader drain/membership/DELETE passed, log
`2c96fdd4e52c2fa3abfcc43c31f048fdb7e3c5b530e847dce4c4c9715b9bda71`.
TiKV catalog/factory/process/history failures identify the SDK task-owner cap,
not an unexplained backend failure. Heartbeats retain completed transactions
until the next sleep interval. SDK heartbeat RED01 executed 0/3 and reproduced
exhaustion at commit ordinal 63, log
`f99db9d869fd0039f37810f735fd4b4d6f871806122bc0a898f7fb05610a04c4`.
The fix wakes terminal heartbeats using registered Notify, retains in-flight
RPCs to terminal state, and notifies Dropped before panic/unwind checks.
The strengthened panic RED02 ran 4/2; final GREEN02 passed the SDK ordinary
suite 79/0/1 ignored, unchanged source, log
`55b1c30abaaf0baff18808ee3eb92fa2576520eaa2ec641eff96f4ad7c5238e4`.
All six new lifecycle tests and original secondary/64-live-slot controls passed;
the 64-slot cap, interval and no-replay commit behavior are unchanged.
Redis Quiesced process recovery separately returns Busy and remains open.
Attempt10 corrects the two selector paths in a new run; original logs remain.
Quiesced-without-claim is still conservatively rejected because an original
PPJ may exist without that claim. Safe support requires bounded actual PPJ
absence and same-CAS epoch proof; this separate candidate remains external.
Public mount/fixture messages and codec help now use packed-v3; the internal
encoding flag is hidden from help. The existing eight fixture controls passed
with unchanged source, log
`50ac01549ca667334428c9cc30bf44b8b52888e1d2fd7e7c6d3a12f970484442`.
This is presentation/CLI regression evidence, separate from actual Redis/TiKV
and the final full gate. Only the CLI and fixture Rust inputs changed after
Deleting-head GREEN02; metadata implementation hashes stayed the same.
Latest mixed-VFS green07 executed 5/1 with unchanged source: candidate creation
and byte-for-byte comparison pass, but explicit prepared Hole sources were
absent. Log `2ebb2be3c8f96d6714c8bfbf6d83602c1a76c0084cd35afbf4594b9af1b2f966`.
The bounded data/gap/tail repair keeps default budgets and original assertions.
Green08 failed compilation on a missing test trait import, 0 tests, log
`3e012ca0a82f50e40970b76c760ada7794b614ac732333ceb5f46707d20d6513`;
root corrected it and green09 executed 6/0, unchanged source, log
`56196782fb092a9f6a760d5977a95c7a09fb99e63b8fdfa4834681f5806d38b9`.
The original effective roundtrip, exact holes/cold/ACL/native hash/publication
and four permission/copy-up contracts passed. The warm small suite passed
registry 35, readonly 5, sparse continuous coverage 1 and budget 12 tests.
The new all-hole test alone failed on Raw peak 120: legitimate GroupMetadata
decoding uses Raw. Root corrected the new assertion to require actual metadata
reads, zero typed PackedPayload/ExternalPayload requests and zero remaining
Raw ownership. Original assertions remain. Rerun small02 passed 1/0 with
unchanged source, log
`96b99d8f1096400480ab6892fdec52c7a58e569b75450c9971173bfa7ecdda79`.
The failed log remains
`26b53721682ac68a076371bc17dcdedbd134c266ba21fbfdd15c2ca5120eff34`.

The canonical mount observer lifetime regression passed 1/0, unchanged source,
log `07421cb4d282bfa45d401d6086eebc7ca6d7e0794e1b752feb50f2544229d9a0`.
Current-history census green01 executed 0/4 on its invalid object-length
fixture, log `a3fb94727c282a16f87f68e4d212f7cd675071268f9a452ca6bf34c931652b48`.
After exact coherence02/history-retirement01 integration, root changed only
that test reference to the header+footer minimum. Ten historical-retirement
tests passed as part of registry-small01 (35/0), log
`d9896c6fdeceb2a029859d33a7b528046c9a027438dfd2727eee8695842b5a83`.
Two persistent-upper process-death tests and two real authenticated
Redis/TiKV retirement tests are integrated. Attempt09 results are recorded
above; the current full Rust gate remains unexecuted. Actual
mark_workspace_deleting turns the writable head into
Deleting; RED02 reproduced the census rejection (5/1), log
`c11cec36d69f71e576a79876e09ba1eabc8326dc405419c5cdaa03588fffcfb7`.
The exact-target Deleting-head repair passed GREEN02 (6/0), unchanged source,
log `234be4e367cd681274c5a9e1fead1ff90ef5e09f0c3552984b4b4ffeb5fbc017`.
Other Deleting owners, stale authority and corrupt ancestry remain rejected.
Aliyun's two PowerShell entrypoints now accept/default/dispatch only v3,
with offline real parameter binding and pure command generator verification.
Generic compose packed fixture routing is now integrated, including explicit
zero-level trees. Offline routing/scanner checks passed. Formal fio datasets
and trace workload support remain open.

Native-copyup-effective-green02 executed 5 passed/1 failed with unchanged
source: four permission/copy-up tests and the other-VFS rejection passed;
the original mixed-VFS producer failed on mount-budget exhaustion after capture. Its
hardlink operation now passes and its assertions were not changed. Log SHA256:
`b4556436b474ec4bd4c08554c0433ba3d1ddaa2daf0a7c894c223dee563ebf57`.
Bounded native begin GREEN executed 7 passed/0 failed with unchanged source,
log `8b219b9f09ef598268adfb84aa4a75fd78ad77d763db01a6d5d1bd11e469ed68`.

Redis/TiKV attempt08 executed 21 passed/1 failed; the conservative accepted
count is 20 because the failed distributed-catalog phase is not signed off.
The actual 32-round TiKV immediate two-key CAS/bounded point/scan test passed,
as did both journal reopen contracts. Concurrent TiKV ACL mutation returned
an undetermined commit. Failure log SHA256:
`fb8108687da3861141246b59b10439e8ee3f5b349b93cc1b8f2cfd95c8a7f125`.
All exact owned proxy/Redis/PD/TiKV services were cleaned and verified absent.
The new commit-cause diagnostic preserves the error and never retries commit.

Retained Prepare seeds, unified recovery ownership, seed lease/reader handoff,
first-PNB owner binding and native owner holds are exact integrated development
inputs. Check04 passed unchanged source, log SHA256:
`a9f3d392c6a535c5066668f9e078fef34219fb88f37b2482cc61b417d3d11205`.
Producer administrative memory retains the same total charge under Metadata
instead of consuming the full decode Workspace pool. Defaults are unchanged.
Budget rejection diagnostics now expose pool/used/request/limit.
Green03 failed compilation with zero executed tests, log
`4bc517917df46bdb983f37920aaee8a9dcfe05f081e120cae0460328bb406225`.
Root fixed the test backend module path. Green04 executed 5 passed/1 failed:
capture and producer admission succeed, but cold attributes were emitted
before namespace inode locations. The test and strict producer validation
remain unchanged. Root moved cold emission to a bounded inode pass after
namespace containers and is running green05. Green04 log SHA256:
`0fd2c2474ef2a9d00052ac323c834443351692e4352321c90e7b792ee2ce38e1`.
All seven native holds tests and ten native begin/phase tests then passed with
unchanged inputs before the bounded owner census was integrated. The new
recovery/Prepare selectors have not yet executed on real backends. This is
development evidence; no all-SPEC, S/X or current full Rust gate is signed off.

## Historical development inputs and gates

The current continuation is recorded at the top of REMOTE_CODEX_HANDOFF.md.
SDK secondary-join RED executed 0 passed/1 failed; GREEN executed 6 passed,
zero failed, with unchanged source. The opt-in joins the existing secondary
task result and returns an undetermined error on post-primary failure or
timeout. A cancelled waiter leaves that owned task running to terminal state.
Default SDK behavior stays unchanged; BrewFS TiKV CAS enables the opt-in.
This local regression does not prove the real-backend immediate-read case.

Root precisely integrated factory/source recovery/typed recovery reader/new
lease recovery/claim bridge/merged permissions. Integrated production
check02 passed with unchanged inputs. The initial mixed-VFS regression run
failed on three factory-test compilation errors and executed zero tests.
After the twelve real recovery selectors were precisely integrated, root
fixed those paths and used actual shutdown_session cleanup. The next mixed
copy-up run is pending. Bounded native-begin RED executed 0 passed/3 failed
against the old production code; its owned bounded implementation is now
integrated but GREEN remains pending. All corresponding original failed
logs and exact integration receipts are retained under the external recovery
root; these inputs do not close the full gate or all SPEC requirements.

Root integrated pagination01, journal-bounds04, packed-object-registry05,
VFS publication-drain01, lower-transport-teardown02, CLI teardown03 and
drain-integrated-review02 using exact base/candidate hash receipts. Actual
producer PUTs reserve and dispatch durable object membership before remote I/O;
unknown replies retain holds. Publication drains retained mutation/upload
owners, including failures from writers already removed from the map. Lower
transport tasks retain session/pin/budget owners through terminal response state.
The subsequent CLI04 additionally requires captured mount-ID absence even when
the path-based unmount call succeeds. These are development inputs, not a
statement that native publication and physical GC are complete.

Development check03 completed with exit 0 and unchanged source/Git before
CLI04. Overlay workspace ordinary02 executed 1925 library tests and 8 fixture
tests: 1933 passed, zero failed, 259 ignored, unchanged source/Git. Artifact:
`kv-sdk-development/cli-drain-registry-ordinary02`; log SHA256:
`425744ee6039954fe01d672d253aad6a04ff48f7541b2f4d8fa9c5e64e414735`.
This evidence precedes CLI04 and the 64 KiB xattr transport successor.

Actual PD GetStore returned 3832 bytes with 71 CPU/read/write entries per array.
The former per-array 64 schema cap rejected valid TiKV metadata transactions.
The generated-client regression failed before the repair. The repair caps each
StoreStats metric array at 128 and all nested nodes at 512 while retaining the
16 KiB topology envelope and depth 8. SDK green01 executed 67 passed, zero
failed, one remote ignored with unchanged inputs; its log SHA256 is
`45caba323f399385940bd7f20ab32158be7eda1bb5f59f9382fcb013042b8fc3`.

The corrected owned-process runner's actual sdk-held-prewrite03 executed one
test and exited 0, with source/Git/config unchanged. Log SHA256:
`43a29aa5af5ea8fafae3841b6c583029df3ed4b88fc5c9beeab4f2925ef31976`.
It proves bounded reads reject the held prewrite and recover after an actual
rollback. It does not prove automatic lock resolution. Attempt06-sdk was
subsequently retired using exact owned service and process identities.

The user-facing fixture errors now name the current packed-v3 encoding and
its output explicitly includes `packed_version=v3`. `wire_version=5` identifies
encoding 005 within packed-v3. The private producer hardlink signature domain
now uses `BrewFS-packed-v3-inode-identity`. Its digest is only compared in the
disposable build inventory and is not serialized into packed objects. No
old-format aliases or readers were added.
`v3-hardlink-naming01` executed both existing producer hardlink contracts:
two passed, exit 0, unchanged source/Git. They retain the original inode IDs,
reject divergent aliases and prove inline-admission independence. Log SHA256:
`3715de3537ef7f6fe1ac13e455dd5c483c54cd8b2080a0454b9e279279d9862e`.
The workspace implementation consistently uses packed-v3 names. The operator's
unrelated UUID dependency feature is not a packed metadata version.
The real 64 KiB xattr regression xattr-64k-red01 executed zero passed/one failed
on the old 48 KiB bounded-read schema cap, with unchanged source/Git/config.
Its log SHA256 is
`4d08654166e1ab04314784693e9cf63410359349785e25eca69d0a25b3b92ed6`.
The fixed tier is now implemented: at most 96 KiB per stored value, a hard
128 KiB gRPC response, bounded Get/Scan schema enabled, and a lazily created
fourth client sharing the canonical resident budget. Drop cancels it and
explicit shutdown joins it before releasing the retained budget. Check01
actually exited 0 with unchanged source/Git (log
`2aa75fd74416c7ab9585d78a08d5241ba67ca468a8675ba2aeb20e2c09b0278d`).
The real xattr-64k-green01 actually passed one test, exited 0 and kept source,
Git and config unchanged. Log SHA256:
`320d74535617ab2b0b494b0f980d2fb89cc178ce51308a8b36b68abcebaeaacc`.
It roundtrips the actual native XattrDelta encoding with a 64 KiB value,
checks point/page bytes and EOF, rejects insufficient aggregate/value/response
limits and corrupt values through the actual 128 KiB transport cap, accounts
for all four resident clients and verifies zero remaining budget after shutdown.

Native-catalog-freeze02 is precisely applied on the actual current kv_store
base. Its opaque authority requires exact Sealing/Quiesced topology and
backend-time checks; its xattr pages use the fourth fixed tier. This does not
authorize full native publication, old history retirement, or recovery.
Its four focused native tests actually passed with unchanged source/Git in
native-freeze-ordinary01, log
`9e818e5c014360a633b91bceb6c93354556517a51b102342c31834051e9eaf7b`.
They prove actual quiescence, context/sequence fencing, locked-clock expiry
and refusal to treat a public phase flag as physical drain authority.
The complete Rust gate remains pending.

Root subsequently integrated the exact native-rebind03, native-graph02,
registry-gc02 and gc03 fixture corrections. The latter produces a second
distinct graph with the actual LocalFS producer; repeated history publication
cannot reuse the same manifest. Production check01 exited 0/source unchanged,
with two unused-reexport warnings still requiring correction before strict
Clippy. The first ordinary stores compile failed on an Arc fixture argument;
after its correction, ordinary02 executed 137 passed, 15 failed, 31 ignored.
Fourteen failures came from ClockBackend's missing bounded timed read and one
from a nil-UUID error assertion; root corrected those fixtures while retaining
the production bounds and error. All five actual collector regressions passed
within that run. Its failed log remains preserved:
`1036104ae889a065f8c508e0a4b5fb028e601f9f2eb00fce956c7b7136925162`.
The collector
is conservative: all existing history roots remain retained; full history
grace retirement, native snapshot/fork holds and complete native publication
are not established by this development check.

The local runner now defaults to encoding 005 and refuses 004 before starting
services. New result manifests bind `packed_version=v3` and encoding 005;
finalization rejects another product/encoding. Both Python suites passed four
tests each, and an actual old-encoding runner invocation exited 2 before any
services. Inputs remained unchanged. Artifact: `v3-naming-runner-only01`.

The exact native-effective-export01, tests01 and original-Quiesced journal
getter are integrated. Capture uses the actual mixed VFS and original inode
IDs, with hardlinks, hot/cold metadata and Data/Hole/Absent provenance. Registered
producer output is compared to the captured artifact, followed by two bounded
old-head table passes and owned DataDrained/Hashed CAS. Check01 passed with
unchanged inputs; this is not full native publication or recovery acceptance.
Complete workspace ordinary01 failed before any tests on a new fixture's
u64 block_size argument (log
`435b40fc42fa428087bce865e85b533d32e5c247619c223526763180f201aefd`).
Root changed only that field to a checked u32 conversion. Ordinary02 reached
terminal status: 1944 passed, one failed, 260 ignored, with unchanged source/Git.
Log SHA256:
`e3503fa43cb88ddefe5223a5ef2cabca2b04e1783a6bbc0cfdc8b7866affd6da`.
The actual mixed-VFS test can stat packed-only inode 400 but hardlink returns
NotFound. Native-only permission/mutation resolution has no authenticated
merged copy-up, and packed inode_permissions still fails closed. This is an
open implementation requirement; neither the assertion nor the capability
gate was weakened. The first real Redis/TiKV development suite completed six
phases with 19 passes, followed by one Redis pass and one TiKV failure in the
two-client distributed catalog contract. TiKV returned a typed precommit
pessimistic WriteConflict after its bounded retry allowance. Root changed only
definite precommit conflicts after successful rollback to return a CAS mismatch;
commit errors still return Err and are never replayed. Its classification test
passed one entrypoint, exit 0, with unchanged inputs (log
`e29ff8dc0c5af39a173b4ea0b096e5db4372f9e7d438fd35adefa38a76a338b0`).

The next real suite (metadata-system-closeout02 / attempt07-sdk) passed the two
keyset tests, two value-bound tests and Redis corrupt-index test. Journal reopen
then passed Redis and failed TiKV: the bounded SDK read rejected a key/region
error response during journal begin. The actual test summary is one pass/one
failure; the runner conservatively gives the failed phase zero accepted passes.
Failed log SHA256:
`fb3dc7333377046086a7848561e4c76de6638cf6d071849103c0982cbf216f59`.
Both attempts reached exact cleanup: their proxies retired and all three owned
container IDs were absent. These failed suites do not establish complete
mixed-workspace correctness or the remaining TiKV fault contracts.

External recovery01 and typed-reader01 candidates reconstruct the immutable
original Quiesced basis but currently only reopen while its original lease is
valid. New lease/holder takeover needs an actual recovery-claim CAS and separate
current authority while retaining the original canonical bytes. They are
uncompiled, unapplied and do not close fresh-process recovery. The complete
Rust, real Redis/TiKV and FUSE lifecycle gates must cover the resulting final tree.

Earlier checkpoints below describe their own frozen inputs only.

## Actual graph and content validation

The public index-context audit now reads every actual GM07 entry, compares
canonical/reverse locators and every hardlink alias's logical content, validates
External ownership/EOF/run commitments, and scans all cold owners and values.
It then joins every incoming container occurrence to all Groups and FI/FD
occurrences. Physical deduplication cannot skip a second semantic occurrence.
Every frame, including unused frames, decodes; descriptors cover contiguous
ordinals and the complete payload body through its footer. Actual observations
must equal all BP11 frame, group and inline provenance counters.

The audit has separate request-byte, decode-byte, logical-hash and frame
validation work quotas. Nonempty group frame intervals use one admitted sorted
index and a single cursor across FD pages. `pages` still means IP06 pages;
`objects` counts all physical objects. Full-page frame validation is charged
before each actual frame read. The twelve quota boundaries are tested.

The initial container RED executed six entry points: one valid control passed
and five adversaries failed on the old selected-content audit. The first GREEN
attempt had two wrapper-field type errors. The second executed 77 passed and
one failure while constructing an invalid BP11 metadata-count adversary; the
PM11 local validator rejected it before the audit. The test now claims zero
metadata groups, a locally valid but physically false count. Production PM11
validation was not weakened. All original failed logs remain preserved.

`index-context-development/green-container-occurrences03` actually executed
78 passed, zero failed, one ignored, with source/Git unchanged. Log SHA256:
`ae718c32a2c5b205c76b3a63707e4db6f05fe6425b889932b2b99498870985aa`.
The ignored real S3 entry point is not included in those passes. This audit
does not yet authorize publication or make its temporary inventory durable.

## Reader retention and lifecycle

PPR3 reader pins now use 256 fixed slots, increasing slot generations and
exact holder/nonce/revision identities. Active mirrors, counts and root
generations transition in the same backend CAS. Existing legitimate pins can
renew after publication; a stale new acquisition cannot grant an old binding.
Expiry forbids revival and successful delivery. Expired-plus-grace reaping
uses a backend-time window in the mutation itself, rather than client time.

The production reader session acquires before lower open, independently
renews, retains request owners, fences successful delivery, and shuts down by
stopping admission, joining heartbeat, draining owners and releasing the
latest exact pin. Cancellation of a shutdown future preserves its join handle.
The shared budget is explicit, including reopened administrative stores.
Native destructive CAS includes reader root observations and the absent
feature/count sentinel checks, so first insertion fences an older GC scan.

`index-context-development/green-reader-pins01` actually executed 13 passed,
zero failed, two ignored, with source/Git unchanged. Log SHA256:
`c5a7a818d1f1a9644c8cbdfde527c9feef862c374aea0f9699b8a9c887967a2c`.
Both ignored backend contracts were subsequently executed in the real suite.
These gates do not establish immutable old-upper/old-lower snapshot mapping.

## Real Redis/TiKV acceptance inputs

All nine phases below used the actual production backend and one source
inventory after reader integration. Each `result.json` records the executed
count, exit code, before/after input hashes and log hash. No zero-test run or
ignored entry point was counted as an executed pass.

| Phase | Executed | Result |
| --- | ---: | --- |
| reader-pins-real01 | 2 | passed |
| clock-window-real01 | 2 | passed |
| public-binding-real01 | 10 | passed |
| distributed-catalog-real01 | 2 | passed |
| clock-deadline-real01 | 2 | passed |
| redis-index-real01 | 1 | passed |
| gc-revalidation-real01 | 2 | passed |
| lostreply-after-pins01 | 1 | passed |
| commit-after-pins01 | 1 | passed |

Redis samples split seconds/nanoseconds from TIME in the same Lua mutation.
TiKV locks exact checked keys, samples fresh PD time immediately before commit
and evaluates the lower-inclusive/upper-exclusive interval there. This proves
locked validation time, not wall-clock time at the eventual commit timestamp.

TiKV no longer starts a new CAS transaction after any commit error. The actual
missing-pessimistic-lock RED previously started two attempts and returned
`Ok(false)`; the repair preserves the original backend error with one attempt.
The response-loss proxy verifies a real upstream successful KvCommit reply
before suppressing it with gRPC Unavailable and an injected retry-policy
triggering message. An independent connection reads the complete committed
new pair, while BrewFS returns the SDK error with one CAS attempt. This does
not assert immediate cleanup of already-prewritten locks. No blind rollback
after unknown commit was introduced.

Artifact roots, under
`C:/Codex-Recovery/brewfs-root-20261005/v3-plans-repair01/`:

- `g12-graph-validation-20261007/index-context-development/`
- `redis-tikv-closeout-20261007-attempt03/` for actual commit RED/GREEN
- `redis-tikv-closeout-20261007-attempt05-proxy/` for the nine real phases

Service images were pinned by local image SHA. Data uses at most 656 MiB of
tmpfs; Docker/proxy logs have fixed limits. Cleanup uses exact service IDs,
labels and names, and verified proxy process identity with pidfd.
Attempt03 and attempt05 cleanup actually completed; attempt05 confirms the
proxy retired and all three owned service IDs absent in `cleanup.json`.

## Remaining acceptance boundary

Strict overlay Clippy actually passed in `green-reader-pins-style04`, with
unchanged inputs. Earlier failures and their logs remain. Its last correction
uses two transparent test barrier type aliases; it changes no production
behavior. The full same-iteration Gate17 ended with 47/49 checks passed and
580 unchanged inputs. Overlay and all-feature library tests each failed the
same absent-binding ESTALE contract; default/overlay strict Clippy and all
operator checks passed. Earlier Gate15/Gate16 evidence does not certify this tree.

Root applied the default reader binding preflight and the source/journal
assembly03 candidate, checking all 14 exact resulting file hashes. The default
method fences a missing binding before reporting unsupported reader lifecycle;
present bindings still cannot attach without pins. The Redis/TiKV override is
unchanged and has no extra probe. The source importer now issues a private final
capture certificate; a complete staged graph audit joins every authenticated
full reference against exact durable ordinal and identity rows. Recording that
receipt stays AwaitingFullProof, and recovery clears its authority.

Development compile01 failed before running tests: one nested test module path
was missing and five journal imports used a private module. Root fixed those
sites using an explicit path and existing public re-exports. Compile02 and
the small regression runs passed against unchanged inputs: journal 5, final
source proof 2, packed attach/read 13, and complete graph 78 (98 passes total).
The two remote journal and one real S3 entry points were ignored and are not
counted as executed passes. Their artifact phases are green-packed-journal02,
green-final-source-proof01, green-packed-attach01 and
green-journal-graph-regression01 under index-context-development. The original
absent-binding ESTALE regression now passes, while a present binding without
pin support still refuses attachment. The changed tree still needs a new
complete gate and real remote journal tests before acceptance. Its real importer fixture uses
data, sparse holes, hardlinks, symlink and xattr bytes, with missing/extra/wrong
typed graph dependencies. Native freeze/drain/rotation, history/snapshot mapping, before-PUT
shared-object registration and typed packed GC remain separate requirements.
Strict overlay Clippy green-journal-style02 passed after the audit arguments
were grouped and a transparent journal-change alias was extracted, without
lint suppression. The 98 runtime passes above precede that parameter cleanup;
the complete gate must rerun the final tree.

The bounded Redis/TiKV transport and SDK-residency final-resident03 candidate
is now applied; root verified all 182 resulting candidate file hashes. Root
and operator use the direct local TiKV SDK dependency and updated lockfiles.
The first independent SDK compile failed before tests because resources.rs
did not import the macro used by its error formatter. The small import fix
then passed sdk-ordinary02: 66 executed tests, zero failures, one remote test
ignored, source/Git unchanged. The log SHA256 is
`616dc0a316ea6e409490aabe9d2371a0de0cdf5dfe23ad226f1b1e713a4c6773`.
Artifact: `kv-sdk-development/sdk-ordinary02` under the same recovery root.

The SDK tests cover message-header rejection, pre-protobuf schema bounds,
repeated empty-field expansion, disabled compression, bounded region scans,
and cancellation retaining task leases until terminal join. They do not
certify the changed workspace or actual Redis/TiKV services. Bounded data
lock/region errors currently fail closed; there is no automatic resolver.
Redis Lua checks stored lengths before GET but does not establish a cap on
a malicious RESP decoder. PD/TSO calls are outside the data-RPC counter.
Keyset pagination and bounded journal successors remain external inputs.
Post-materialization value-length checks alone do not prove transport bounds. FUSE system
gates and formal three-innovation experiments remain open. These development
passes are not a statement that every SPEC requirement is complete.
