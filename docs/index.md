# ACE Getting Started

ACE is a powerful tool designed to ensure and maintain consistency across nodes in a pgEdge Distributed Postgres cluster. ACE helps identify and resolve data inconsistencies, schema differences, and replication configuration mismatches across nodes in a cluster.

Key features of ACE include:

- Table-level data comparison and repair
- Replication set level verification
- Automated repair capabilities
- Schema comparison
- Spock configuration validation

## ACE Use Cases

In an eventually consistent system (like a cluster), nodes can diverge due to replication exceptions, lag, network partitions, or node failures. ACE reconciles such differences across databases by performing efficient comparisons and repairs in a controlled manner.


### Node Failures (Planned/Unplanned)
- **Problem:** A node rejoins the cluster but is out-of-sync.
- **Approach:** `table-diff` (or `repset-diff` / `schema-diff`) to assess drift; `table-repair` with `--dry-run` then apply.

### Network Partitions / Link Degradation
- **Problem:** Cross-region clusters experience Spock exceptions and partial replication.
- **Approach:** Identify impacted rows precisely with `table-diff`; repair with `--upsert-only` or `--insert-only` where appropriate to minimize risk.

### Planned Maintenance Windows
- **Problem:** Nodes fall behind during upgrades or maintenance.
- **Approach:** Run diffs and perform bulk `table-repair` to re-synchronize.

### Post-Repair Verification
- **Problem:** Need to confirm remediation success.
- **Approach:** Use `table-rerun --diff-file=<original-diff>` to verify that discrepancies no longer exist.

### Spock Configuration Validation
- **Problem:** Metadata/config drift can cause replication anomalies.
- **Approach:** Use `spock-diff` to compare Spock state across nodes; correct differences before they cause data drift.

### Large-Scale Integrity Checks
- **Problem:** Very large tables make full scans impractical.
- **Approach:** Use [Merkle trees](./merkle.md):
  - Initialize once (`mtree init`), build each table (`mtree build`), then use `mtree table-diff` for faster comparisons.
  - Optionally keep trees current with `mtree listen`.

## Simplifying ACE Operations

- **Automate** ACE to perform periodic checks (daily/weekly) and **alert** if diffs are found.
- **Segment** by schema or repset to keep runs predictable.
- **Record** ACE provides JSON/HTML reports for complete audit trails.
- **Guardrails**: Track `MAX_ALLOWED_DIFFS`; break large repairs into manageable batches.

## Known Limitations

* ACE cannot be used on a table without a primary key, because primary keys are the basis for range partitioning, hash calculations, and other critical functions in ACE.

