# Vendored `fuser` 0.17.0 with FUSE-T support

This directory is a vendored copy of [`fuser` 0.17.0](https://github.com/cberner/fuser)
(MIT licensed) with a small, macOS-only patch set that makes the crate work with
[FUSE-T](https://www.fuse-t.org/) — the kext-less FUSE implementation that backs
FUSE with a local NFS server.

BrewFS wires it in via `[patch.crates-io]` in the workspace `Cargo.toml`:

```toml
[patch.crates-io]
fuser = { path = "vendor/fuser" }
```

## Why the patch is needed

`ossmount` (the OSS direct-mount binary) uses `fuser` on macOS. fuser was written
against `/dev/fuse`, where the kernel guarantees exactly one FUSE request per
`read()`. FUSE-T instead hands the daemon a `SOCK_STREAM` socketpair, which has no
message boundaries. Two things break:

1. **Protocol negotiation.** fuser always replies to `FUSE_INIT` with the modern
   64-byte `fuse_init_out`. FUSE-T's NFS backend reads the reply size and selects
   profile **v3** (standard libfuse3 attr layout), but fuser serializes the
   macFUSE extended `fuse_attr` (with `crtime`/`flags`) on macOS. The 16-byte
   layout difference shifts every attribute and breaks OPEN/READ.

2. **Stream coalescing.** A single `read()` on the socket can return several
   coalesced FUSE requests (NFS readahead sends many READs back-to-back). fuser
   parsed only the first request per `read()` and silently dropped the rest,
   hanging the mount.

## What was changed (all gated on `feature = "fuse-t"` + macOS)

- `src/ll/request.rs` — when `fuse-t` is enabled on macOS, the `FUSE_INIT` reply
  is truncated to the 24-byte libfuse2-compatible size. FUSE-T then negotiates
  profile v2, which matches fuser's macFUSE attribute layout.
- `src/channel.rs` — when `fuse-t` is enabled on macOS, `receive()` reads the
  40-byte `fuse_in_header` first and then the request body, so every call returns
  exactly one complete request (stream-safe).
- `src/lib.rs` — when `fuse-t` is enabled on macOS, the advertised INIT flags are
  the plain libfuse2 set (`FUSE_ASYNC_READ | FUSE_BIG_WRITES`) instead of the
  macFUSE-only `FUSE_CASE_INSENSITIVE | FUSE_VOL_RENAME | FUSE_XTIMES`.

`Cargo.toml` gained an empty `fuse-t = []` feature. On Linux/other targets the
feature is inert.

## Updating

To refresh from upstream: copy `fuser-0.17.0` from crates.io into this directory
(keeping `Cargo.toml`, `build.rs`, `src/`, `LICENSE.md`, `README.md`, `deny.toml`),
then re-apply the three hunks above. Do not commit `.cargo-checksum.json` or the
macOS `._*` AppleDouble files (repo `.gitignore` ignores `._*`).
