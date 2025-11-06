# ACE Commands

This section documents the ACE (Active Consistency Engine) CLI commands. Be sure you review and meet all of the [ACE configuration requirements](../configuration.md) before using any ACE command:

## Basic ACE `diff` and `repair` Commands

| Command                         | Description                                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [repset-diff](diff/repset-diff.md)   | Runs table-diff on every table in a specified replication set and aggregates results.                     |
| [schema-diff](diff/schema-diff.md)   | Compares objects (and optionally data via per-table diffs) across nodes for an entire schema.             |
| [spock-diff](diff/spock-diff.md)     | Compares Spock metadata/state across nodes to find configuration divergences.                             |
| [table-diff](diff/table-diff.md)     | Compares a single table across cluster nodes and writes a diff report (JSON/HTML/CSV).                    |
| [table-repair](repair/table-repair.md) | Applies fixes from a diff file using a chosen source-of-truth node; supports dry-run, upsert/insert-only. |
| [table-rerun](diff/table-rerun.md)   | Re-runs a previous diff from a saved file to verify that inconsistencies were resolved.                   |


## Merkle Tree (`mtree`) Commands

| Command                                         | Description                                                                               |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [mtree build](mtree/mtree-build.md)                   | Builds Merkle trees for a specific table on all nodes (after `mtree init`).               |
| [mtree init](mtree/mtree-init.md)                     | Creates required schema/objects and sets up CDC (publication/slot) for Merkle operations. |
| [mtree listen](mtree/mtree-listen.md)                 | Long-running process that consumes CDC and continuously updates Merkle trees.             |
| [mtree table-diff](mtree/mtree-table-diff.md)         | Compares Merkle trees across nodes to detect inconsistencies; can emit JSON/HTML reports. |
| [mtree teardown](mtree/mtree-teardown.md)             | Removes all Merkle-related objects and CDC setup created by `mtree init`.                 |
| [mtree teardown-table](mtree/mtree-teardown-table.md) | Drops Merkle data/metadata for one table and removes it from CDC publication.             |
| [mtree update](mtree/mtree-update.md)                 | Applies captured CDC changes to refresh Merkle trees; optional rebalance.                 |

