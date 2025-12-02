# Best Practices (ACE)

ACE (Active Consistency Engine) helps keep nodes in a pgEdge Distributed Postgres cluster consistent by detecting and repairing drift in data, schema, and Spock configuration.

## General Guidance

**Validate your prerequisites.**

  - Ensure every table has a **primary key** (ACE requires it for block/range partitioning).
  
  - Confirm credentials and connection details in your `pg_service.conf` file.

**Scope deliberately.** Use targeted `schema-diff`, `repset-diff`, or `table-diff` with `--table-filter`/`-F` instead of cluster-wide runs when diagnosing a known issue.

  - **`table-diff`**: Performs a deep dive on a specific table (fastest to iterate).
  
  - **`schema-diff`**: Inventories and compares objects (tables/views/functions/indexes) and optionally run per-table diffs.
  
  - **`repset-diff`**: Sweep all tables in a replication set.
  
  - **`spock-diff`**: Validate Spock metadata across nodes.

**Start safe, then add options.** 

  - Begin with `--output json` and `--quiet`/`-q` off; add `--output html` for human review. 
  
  - Use `--dry-run`/`-y` when performing repairs.

**Control Resource Use When Possible.** 

  - Adjust `--block-size`, `--concurrency-factor`, and `--compare-unit-size` to match each host’s capacity. For Merkle operations, consider lowering or raising `--max-cpu-ratio` as needed.

**Keep your Statistics Fresh.** 

  - Run `ANALYZE` on large/cold tables before heavy comparisons, since ACE relies on probabilistic sampling (`TABLESAMPLE`) to speed things up. Especially useful for cold tables and when using Merkle trees.

**Use a Connection Pooler.** 

  - Point ACE at pgBouncer or pgCat for stable, efficient connections.

**Automate checks.**

  - Use `--schedule`/`-S` with `--every=<duration>`/`-e <duration>` for quick loops or configure jobs in `ace.yaml` and run `./ace start`. See [Scheduling ACE Runs](scheduling.md) for examples.


## Adopting a Safe Repair Workflow

It's a good practice to schedule diff jobs (schema/repset/table) during low-traffic windows and after maintenance events.

1. **Detect Differences Often**: Run a diff and review the JSON/HTML report.
2. **Perform a Repair Dry-run**: `table-repair --dry-run` to preview actions.
3. **Perform Conservative Repairs when Possible**: Prefer `--upsert-only` or `--insert-only` on critical tables where deletes are risky.
4. **Use `--fix-nulls` for NULL-only drifts**: When nodes differ only by missing values, cross-fill without picking a single source-of-truth.
4. **Verify the Repair**: `table-rerun` using the original diff to confirm resolution.
5. **Iterate in Batches** if the number of diffs is large, narrow the scope with `--table-filter`/`-F` or segment work by replication set/schema.
