# Best Practices (ACE)

ACE (Active Consistency Engine) helps keep nodes in a pgEdge Distributed Postgres cluster consistent by detecting and repairing drift in data, schema, and Spock configuration.

## General Guidance

- **Run checks regularly.** Schedule diff jobs (schema/repset/table) during low-traffic windows and after maintenance events.
- **Start safe, then tighten.** Begin with `--output json` and `--quiet` off; add `--output html` for human review. Use `--dry-run` for repair.
- **Scope deliberately.** Prefer targeted `schema-diff`, `repset-diff`, or `table-diff` with `--table-filter` over blanket cluster-wide runs when diagnosing a known issue.
- **Validate preconditions.**
  - Ensure every compared table has a **primary key** (ACE requires it for block/range partitioning).
  - Confirm credentials, connectivity, and that your `pg_service.conf` / cluster config are correct.
- **Control resource use.** Tune `--max-cpu-ratio`, `--block-rows`, and `--batch-size` to fit the host and workload.
- **Keep stats fresh.** Run `ANALYZE` on large/hot tables before heavy comparisons (especially when using Merkle trees).
- **Prefer connection pooling.** Point ACE at pgBouncer or pgCat for stable, efficient connections.

**When to Use Which Diff**

- **`table-diff`**: Deep dive on a specific table (fastest to iterate).
- **`schema-diff`**: Inventory & compare objects (tables/views/functions/indexes) and optionally run per-table diffs.
- **`repset-diff`**: Sweep all tables in a replication set.
- **`spock-diff`**: Validate Spock metadata across nodes.

Adopt a Safe Repair Workflow

1. **Detect**: Run a diff and review the JSON/HTML report.
2. **Dry-run**: `table-repair --dry-run` to preview actions.
3. **Conservative repair**: Prefer `--upsert-only` or `--insert-only` on critical tables where deletes are risky.
4. **Verify**: `table-rerun` using the original diff to confirm resolution.
5. **Iterate in batches** if the number of diffs is large (watch your `MAX_ALLOWED_DIFFS` guardrail).

## Known Limitations

- **Primary key required**: ACE cannot compare/repair tables without a primary key.
