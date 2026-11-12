# Packed metadata credentials

An enabled workspace requires namespace-local `metadataRuntimeSecretRef.name`
and `metadataAdminSecretRef.name`. Their names must differ. A runtime Secret must
not be referenced by either metadata or object administrative role. A Secret may
combine object and metadata credentials for the same role.

For Redis, each Secret supplies `username` and `password`. Usernames use 1–64
ASCII letters, digits, `_`, or `-`; passwords use 16–256 of the same characters.
The `default` principal is rejected. The two usernames and passwords must each
differ. This bounded syntax is safe for both CLI URLs and the server ACL parser.
The operator derives the Redis server ACL configuration from SHA256 password
hashes. The Redis Pod receives that derived configuration, not either original
role Secret. The runtime role receives only the required connection, WHOAMI,
read/write/CAS/index/script commands. The admin role is separate.

For TiKV, each Secret supplies `ca.crt`, `tls.crt`, and `tls.key`, each nonempty
and at most 64 KiB, in PEM format. Runtime and operator use different leaf client
certificates issued for the actual cluster. The operator copies both identities
to private temporary directories with mode-0600 files; the library retains the
directory owners through connection, lazy SDK work, cancellation, and drain.
It compares canonical leaf certificate identities and performs real bounded
backend reads using both roles before retaining an operator session. PEM syntax
and a Secret reference alone are not authentication evidence.

SPEC §10.6 includes the trusted BrewFS sidecar in the protection boundary. Only
the operator receives administrative metadata/object credentials. The sidecar
receives runtime credentials; the agent receives none, cannot share the sidecar
process namespace, and cannot escalate privileges. Redis EVAL and TiKV
transactions cannot independently recognize snapshot/discard/GC operation
intent. Those operations require the trusted process's typed operator entry
points, including a private authenticated session proof for native GC. Raw
backend credentials are therefore not a safe agent-facing interface.

Acceptance requires real Redis and TiKV authentication, distinct principal
checks, runtime/admin mount and recovery smoke tests, and negative operator
capability tests. These configuration checks do not replace that evidence.
