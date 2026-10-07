# Release notice: workspace catalog format 2

The `brewfs workspace migrate` command changes the workspace catalog to the entity-key format. This migration is one-way: older BrewFS binaries cannot read the catalog after migration. Upgrade **all readers**, including every mount and workspace CLI instance, before running `migrate`.

Stop older readers and workspace writers. Run the command with the same metadata backend, endpoint, and `workspace-namespace` used by the volume:

```text
brewfs workspace --meta-backend redis --meta-url <url> --workspace-namespace <name> migrate
brewfs workspace --meta-backend sqlx --meta-url <url> --workspace-namespace <name> migrate
brewfs workspace --meta-backend tikv --meta-tikv-pd-endpoints <pd-endpoints> --workspace-namespace <name> migrate
```

The migration can resume after an interruption by running the same command again. Workspace mounts reject an unmigrated catalog and point to the migration command. Keep older readers stopped after migration.
