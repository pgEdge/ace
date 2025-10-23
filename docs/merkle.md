# Using ACE with Merkle Trees

!!! info

    ACE Merkle trees are introduced as an experimental optimisation in pgEdge Distributed Postgres. Evaluate this feature carefully before enabling in production.

ACE can use Merkle trees to make very large table comparisons dramatically faster. Normal table-diff runs (with tuned parameters) often complete in seconds or minutes, but on huge tables a full scan may take hours. Merkle trees avoid full rescans by hashing ranges (blocks) and only drilling into blocks that differ.

**Before using a Merkle Tree**

You must perform two setup steps before using Merkle trees effectively.  The first command adds cluster-level operators used by Merkle trees:

`./ace mtree init cluster_name`

The second command creates the Merkle metadata table and triggers for the target table:

`./ace mtree build cluster_name schema.table_name`

Building the per-table Merkle tree is typically a one-time operation. After that, ACE tracks changes and updates the tree automatically during diffs (or via mtree listen / mtree update).

Because Merkle mode is designed for very large tables, ACE uses probabilistic estimates (row counts, PK ranges, etc.). For best results, ensure your table has fresh statistics with the Postgres ANALYZE command. You can pass `--analyse=true` during build, but on very large tables you may prefer to run ANALYZE manually at a more convenient time.

## Using Merkle Trees

The following steps walk you through initializing, building, and using Merkle trees to monitor your table.

First, initialize Merkle Tree objects with the command:

`./ace mtree init cluster_name`

Then, build the Merkle Tree with the command:

`./ace mtree build cluster_name schema.table_name`

!!! info

    You can force recreation of objects with --recreate-objects=true, and let ACE analyze the table first with --analyse=true.

Then, invoke ACE to compare Merkle trees across nodes and write a diff report (and optional HTML):

`./ace mtree table-diff cluster_name schema.table_name`

Finally, you can use the diff file to initiate table repair with the ACE [table-repair](./commands/table-repair.md) command:

`./ace table-repair --diff-file=<diff-file-from-mtree-diff> --source-of-truth=n1 cluster_name schema.table_name`

!!! note

    Running `mtree listen` can help keep trees current; every `mtree table-diff` also performs an on-demand update before comparing.


## Building Merkle Trees in Parallel (for Very Large Tables)

If a table is extremely large (e.g., ~1B rows or ~1 TB), remote building the Merkle tree from a single ACE node can be slowed by network latency. You can parallelize the build (per node) to speed up the process.

On one node, compute ranges and start hashing and writing the ranges to a file:

`./ace mtree build cluster_name schema.table_name --max-cpu-ratio=1 --write-ranges=true`

Then, copy the generated ranges file to other nodes (e.g., with scp).

On each other node, build using the shared ranges file, targeting only that node:

```bash
./ace mtree build cluster_name schema.table_name \
  --max-cpu-ratio=1 \
  --recreate-objects=true \
  --nodes=n2 \
  --ranges-file=/path/to/ranges-file.txt
```

Repeat this process on each node in your cluster, using exactly one node per run so ACE doesn’t attempt remote creation from that host.


## Using Merkle Tree Commands

For detailed information about the options that you can include when using Merkle tree commands, see the documentation listed below:

| Command   | Description                                  |
| --------------- | ---------------------------------------- |
| [mtree build](mtree-build.md)                   | Builds Merkle trees for a specific table on all nodes (after `mtree init`).               |
| [mtree init](mtree-init.md)                     | Creates required schema/objects and sets up CDC (publication/slot) for Merkle operations. |
| [mtree listen](mtree-listen.md)                 | Long-running process that consumes CDC and continuously updates Merkle trees.             |
| [mtree table-diff](mtree-table-diff.md)         | Compares Merkle trees across nodes to detect inconsistencies; can emit JSON/HTML reports. |
| [mtree teardown](mtree-teardown.md)             | Removes all Merkle-related objects and CDC setup created by `mtree init`.                 |
| [mtree teardown-table](mtree-teardown-table.md) | Drops Merkle data/metadata for one table and removes it from CDC publication.             |
| [mtree update](mtree-update.md)                 | Applies captured CDC changes to refresh Merkle trees; optional rebalance.                 |


