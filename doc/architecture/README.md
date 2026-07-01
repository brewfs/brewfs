# Architecture Documents

This directory contains BrewFS design notes for core filesystem behavior and
data layout. Prefer adding new subsystem design documents here when the topic
changes runtime semantics rather than deployment or testing workflow.

## Core Model

- [arch.md](arch.md): high-level architecture and component boundaries.
- [meta.md](meta.md): metadata layer overview.
- [metadata.md](metadata.md): metadata structures and behavior notes.
- [chunk.md](chunk.md): chunk model and IDs.
- [data-layout.md](data-layout.md): persisted data layout.

## I/O Paths

- [read-path.md](read-path.md): read flow.
- [write-path.md](write-path.md): write flow.
- [caching.md](caching.md): cache layers and invalidation.
- [compaction-gc.md](compaction-gc.md): compaction and garbage collection.

## Semantics

- [consistency.md](consistency.md): consistency model.
- [redis-version-cas.md](redis-version-cas.md): Redis CAS/version strategy.
- [permissions.md](permissions.md): permission behavior.
- [link_symlink.md](link_symlink.md): link and symlink behavior.
- [rename_design.md](rename_design.md): rename semantics.
