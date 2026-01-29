# ACE Repair Commands

## Using the ACE table-repair Command

The `table-repair` command fixes data inconsistencies identified by `table-diff`. It uses a specified node as the *source of truth* to correct data on other nodes, and provides options to perform specific and targeted repairs. 

**Typical scenarios**

- **Spock exception repair**: Resolve conflicts from insert/update/delete exceptions.
- **NULL-only drift**: Use `--fix-nulls` to cross-fill NULL columns without a single source-of-truth.
- **Network partition repair**: Re‑align nodes after a partition.
- **Temporary node outage**: Catch a lagging node up.
- **Catastrophic node failure recovery**: Use origin-scoped diffs plus recovery-mode repair to reconcile survivors when a node fails mid-replication.

**Safety & audit features**

- **Dry run mode**: see proposed changes without modifying data.
- **Report generation**: write a detailed audit of actions taken.
- **Upsert‑only**: prevent deletions on divergent nodes.
- **Transaction safety**: changes are atomic; partial failures are rolled back.
- **Advanced repair plans**: drive repairs from a versioned YAML/JSON file with per-table rules, overrides, and custom rows. See [Advanced repair](advanced-repair.md) and [Examples](advanced-repair-examples.md).

**Helpful Tips**

- Use `--dry-run` first to review changes.
- Use `--upsert-only` or `--insert-only` for sensitive tables to avoid deletes.
- `table-repair` only fixes rows present in the diff file. If a diff is too large, consider breaking the table into filtered chunks (`table-diff ... --table-filter`/`-F`) and repairing iteratively.
- For very large, multi‑table incidents, a dump/restore may be more practical.

---
