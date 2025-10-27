# Best Practices (ACE)

ACE (Active Consistency Engine) helps keep nodes in a pgEdge Distributed Postgres cluster consistent by detecting and repairing drift in data, schema, and Spock configuration.

## General Guidance

- **Validate your prerequisites.**

  - Ensure every table has a **primary key** (ACE requires it for block/range partitioning).
  - Confirm credentials and connection details in your `pg_service.conf` file.

- **Scope deliberately.** Use targeted `schema-diff`, `repset-diff`, or `table-diff` with `--table-filter` instead of cluster-wide runs when diagnosing a known issue.

  - **`table-diff`**: Performs a deep dive on a specific table (fastest to iterate).
  - **`schema-diff`**: Inventories and compares objects (tables/views/functions/indexes) and optionally run per-table diffs.
  - **`repset-diff`**: Sweep all tables in a replication set.
  - **`spock-diff`**: Validate Spock metadata across nodes.

- **Start safe, then tighten.** Begin with `--output json` and `--quiet` off; add `--output html` for human review. Use `--dry-run` for repair.
- **Control resource use.** Tune `--max-cpu-ratio`, `--block-rows`, and `--batch-size` to fit the host and workload.
- **Keep statistics fresh.** Run `ANALYZE` on large/hot tables before heavy comparisons (especially when using Merkle trees).
- **Use connection pooling.** Point ACE at pgBouncer or pgCat for stable, efficient connections.

## Adopt a Safe Repair Workflow

It's a good practice to schedule diff jobs (schema/repset/table) during low-traffic windows and after maintenance events.

1. **Detect Differences Often**: Run a diff and review the JSON/HTML report.
2. **Perform a Repair Dry-run**: `table-repair --dry-run` to preview actions.
3. **Perform Conservative Repairs when Possible**: Prefer `--upsert-only` or `--insert-only` on critical tables where deletes are risky.
4. **Verify the Repair**: `table-rerun` using the original diff to confirm resolution.
5. **Iterate in Batches** if the number of diffs is large (watch your `MAX_ALLOWED_DIFFS` guardrail).