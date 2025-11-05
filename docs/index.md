# ACE Getting Started

ACE is a powerful tool designed to ensure and maintain consistency across nodes in a pgEdge Distributed Postgres cluster. ACE helps identify and resolve data inconsistencies, schema differences, and replication configuration mismatches across nodes.

Key features of ACE include:

- Table-level data comparison and repair
- Replication set level verification
- Diff-driven repair workflows
- Schema comparison
- Spock configuration validation

## ACE Use Cases

In an evntually consistent multi-master system, nodes may potentially diverge due to replication issues, network partitions, or node failures. ACE helps restore correctness by performing efficient, controlled comparisons and targeted repairs across nodes.


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
  - Initialize (`mtree init`) and build the tree (`mtree build`) once for each table, then use `mtree table-diff` for faster comparisons.
  - Optionally keep trees current with `mtree listen` for real-time tree updates, and save time during `mtree table-diff`.

## Simplifying ACE Operations

<!-- TODO: Fix this after merging the scheduler PR -->
- **Schedule** ACE via your orchestration tool of choice to perform periodic checks and alert if diffs are found.
- **Segment and Target** by schema, repset, or by using `table-filter` to keep runs predictable. 
- **Store** ACE-generated JSON/HTML reports for complete audit trails.

## Known Limitations

* ACE cannot be used on a table without a primary key, because primary keys are the basis for range partitioning, hash calculations, and other critical functions in ACE.
