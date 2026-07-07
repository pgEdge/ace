# Merkle Tree Commands

!!! info

    ACE Merkle trees are introduced as an experimental optimisation in pgEdge Distributed Postgres. Evaluate this feature carefully before enabling in production.

Merkle trees provide a highly efficient method for verifying data consistency between nodes in a distributed system. By comparing cryptographic hashes of data ranges (leaf nodes) and their parent hashes up to a single root hash, ACE can quickly determine if tables are in sync without transferring and comparing the entire dataset. This dramatically reduces network traffic and computational load, making it ideal for verifying very large tables.

### When to Use Merkle Trees

Merkle trees are most beneficial when:

-   **Verifying Large Tables**: They are significantly faster than a full `table-diff` for tables with millions or billions of rows, as the amount of data exchanged is minimal.
-   **Frequent, Low-Impact Checks**: If you need to run consistency checks frequently, the lightweight nature of a Merkle tree diff is less impactful on system performance than repeated full data scans.
-   **Detecting Small Divergences**: They excel at quickly confirming that tables are identical or identifying that a small number of rows have diverged.

### Performance Considerations

While powerful, Merkle trees are not always the optimal solution. A traditional `table-diff` may be more performant in scenarios where:

-   **A high percentage of rows have diverged**: If a large portion of a table is out of sync, the Merkle tree comparison will identify differences in many branches, and the follow-up process to find the exact differing rows can be less efficient than a direct, full table scan.
-   **Tables are small**: For small tables, the overhead of building and maintaining Merkle trees can be greater than the time it takes to perform a quick `table-diff`.

### Using Merkle Trees

You must perform two setup steps before using Merkle trees effectively. The first command adds cluster-level operators used by Merkle trees:

`./ace mtree init cluster_name`

The second command creates the Merkle metadata table and triggers for the target table:

`./ace mtree build cluster_name schema.table_name`

Building the per-table Merkle tree is typically a one-time operation. After that, ACE tracks changes and updates the tree automatically during diffs (or via `mtree listen` / `mtree update`).

Because Merkle mode is designed for very large tables, ACE uses probabilistic estimates (row counts, primary key ranges, etc.). For best results, ensure your table has fresh statistics with the Postgres `ANALYZE` command. You can pass `--analyse=true` during build, but on very large tables you may prefer to run `ANALYZE` manually at a more convenient time.

Then, invoke ACE to compare Merkle trees across nodes and write a diff report (and optional HTML):

`./ace mtree table-diff cluster_name schema.table_name`

Finally, you can use the diff file to initiate table repair with the ACE [table-repair](../repair/table-repair.md) command:

`./ace table-repair --diff-file=<diff-file-from-mtree-diff> --source-of-truth=n1 cluster_name schema.table_name`

!!! note

    Running `mtree listen` can help keep trees current; every `mtree table-diff` also performs an on-demand update before comparing.

`mtree listen` and `mtree table-diff` / `mtree update` can run at the same time.
Each node has a single replication slot shared across all of its Merkle trees,
and PostgreSQL allows only one active consumer per slot. So whenever a diff or
update finds a node's slot already held by another consumer — normally a running
`mtree listen`, but also a concurrent `mtree table-diff`/`update` on the same
node (even for a different table) — ACE skips that node's CDC catch-up and
compares against the tree already maintained on that node. A warning is printed,
the skipped nodes are listed in the diff summary (`cdc_skipped_nodes`), and the
result is best-effort: it may omit changes newer than the last apply, so
divergence can be under-reported. For a diff that is guaranteed current to the
present moment, ensure no `mtree listen` or other mtree operation is holding the
node's slot, then re-run so the diff can perform its own bounded CDC drain.

Because the replication slot is shared, the bounded CDC drain that runs before a
diff or update processes the pending changes for **all** Merkle-tracked tables on
that node, not just the table named on the command line. For the other tables this
only records which of their tree blocks are now stale (marks them dirty); it never
recomputes their hashes. Each table's dirty blocks are rehashed when that table is
next the target of an `mtree table-diff`, `mtree update`, or `mtree listen` apply,
so a table-diff on one table may slightly speed up — never slow down or alter —
a later diff of another table.

When a single bounded drain encounters a very large number of row updates for one table (by default more than ~1% of its rows), ACE marks that table's whole Merkle tree dirty instead of tracking each update individually, and the next tree update for that table recomputes its leaf hashes in bulk — much faster for heavily-updated tables, with no effect on diff correctness (inserts and deletes are always tracked individually, so block structure maintenance is unaffected). This applies to any tracked table whose updates appear in the drained stream, including tables other than the one being diffed. Keeping `mtree listen` running still gives the best interactive `table-diff` latency, since the tree is then maintained continuously.

### Building Merkle Trees in Parallel (for Very Large Tables)

If a table is extremely large (e.g., ~1B rows or ~1 TB), remote building the Merkle tree from a single ACE node can be slowed by network latency. You can parallelize the build (per node) to speed up the process.

On one node, compute ranges and start hashing and writing the ranges to a file:

`./ace mtree build --max-cpu-ratio=1 --write-ranges=true cluster_name schema.table_name`

Then, copy the generated ranges file to other nodes (e.g., with scp).

On each other node, build using the shared ranges file, targeting only that node:

```bash
./ace mtree build --max-cpu-ratio=1 --nodes=n2 \
--ranges-file=/path/to/ranges-file.txt \
cluster_name schema.table_name
```

Repeat this process on each node in your cluster, using exactly one node per run so ACE doesn’t attempt remote creation from that host.

### Available Commands

Below is a summary of the available `mtree` commands.

-   [`mtree build`](mtree-build.md): Builds a Merkle tree for a table on all nodes.
-   [`mtree init`](mtree-init.md): Initializes database objects needed for Merkle tree operations.
-   [`mtree listen`](mtree-listen.md): Starts a long-running process to listen for changes and auto-update Merkle trees.
-   [`mtree table-diff`](mtree-table-diff.md): Compares Merkle trees of a table across nodes to find inconsistencies.
-   [`mtree teardown`](mtree-teardown.md): Drops all database objects created by `mtree init`.
-   [`mtree teardown-table`](mtree-teardown-table.md): Removes all database objects associated with a single table’s Merkle tree.
-   [`mtree update`](mtree-update.md): Manually applies changes to a table’s Merkle tree.
