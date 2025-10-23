Using ACE with Merkle Trees

!!! info

    ACE Merkle trees are introduced as an experimental optimisation in pgEdge Distributed Postgres. Evaluate this feature carefully before enabling in production.

ACE can use Merkle trees to make very large table comparisons dramatically faster. Normal table-diff runs (with tuned parameters) often complete in seconds or minutes, but on huge tables a full scan may take hours. Merkle trees avoid full rescans by hashing ranges (blocks) and only drilling into blocks that differ.

**Before using Merkle Trees**

You must perform two setup steps before using Merkle trees effectively.  The first command adds cluster-level operators used by Merkle trees:

`./ace mtree init cluster_name`

The second command creates the Merkle metadata table and triggers for the target table:

`./ace mtree build cluster_name schema.table_name`

Building the per-table Merkle tree is typically a one-time operation. After that, ACE tracks changes and updates the tree automatically during diffs (or via mtree listen / mtree update).

Because Merkle mode is designed for very large tables, ACE uses probabilistic estimates (row counts, PK ranges, etc.). For best results, ensure your table has fresh statistics with the Postgres ANALYZE command. You can pass `--analyse=true` during build, but on very large tables you may prefer to run ANALYZE manually at a more convenient time.

## Using Merkle Trees

1. Initialize Merkle Tree objects with the command:

`./ace mtree init cluster_name`


2. Build the Merkle Tree with the command:

`./ace mtree build cluster_name schema.table_name`


!!! info

    You can force recreation of objects with --recreate-objects=true, and let ACE analyze the table first with --analyse=true.

3. Invoke ACE to compare Merkle trees across nodes and write a diff report (and optional HTML):

`./ace mtree table-diff cluster_name schema.table_name`

4. Use the diff file to initiate table repair with the ACE table-repair command:

`./ace table-repair --diff-file=<diff-file-from-mtree-diff> --source-of-truth=n1 cluster_name schema.table_name`


!!! note

    Running `mtree listen` can help keep trees current; every `mtree table-diff` also performs an on-demand update before comparing.


## Building Merkle Trees in Parallel (Very Large Tables)

If a table is extremely large (e.g., ~1B rows or ~1 TB), remote building the Merkle tree from a single ACE node can be slowed by network latency. You can parallelize the build per node to speed up the process.

On one node, compute ranges and start hashing and writing the ranges to a file:

`./ace mtree build acctg public.customers_large --max-cpu-ratio=1 --write-ranges=true`

Copy the generated ranges file to other nodes (e.g., with scp).

On each other node, build using the shared ranges file, targeting only that node:

```bash
./ace mtree build acctg public.customers_large \
  --max-cpu-ratio=1 \
  --recreate-objects=true \
  --nodes=n2 \
  --ranges-file=/path/to/ranges-file.txt
```

Repeat this process on each node in your cluster, using exactly one node per run so ACE doesn’t attempt remote creation from that host.

## Day-to-Day Commands with Merkle Trees

mtree table-diff

Performs a Merkle-based comparison (updates the tree first unless told otherwise) and writes a diff report.

./ace mtree table-diff acctg public.customers_large


Useful flags:

--output {json|html}

--skip-update

--batch-size <N>

--max-cpu-ratio <0..1>

mtree update

Manually update (refresh) a table’s Merkle tree using captured CDC.

./ace mtree update acctg public.customers_large


--rebalance=true will split/merge blocks based on keyspace changes. The default update in mtree table-diff handles splits and updates, but defers merges; use --rebalance when you need merges.

mtree listen

Continuously consumes CDC (via pgoutput) and updates trees in real time.

./ace mtree listen acctg

mtree teardown (table-level)

Remove table-specific Merkle artifacts:

./ace mtree teardown acctg public.customers_large

mtree teardown (cluster-level)

Remove cluster-level objects (operators/functions) created by mtree init:

./ace mtree teardown acctg

Performance Considerations

Triggers: The Merkle build adds triggers to track changes; these can impact write performance. Measure and verify for your workload.

Tuning: For build/diff speed, adjust --max-cpu-ratio, relevant block/batch options, and keep the ACE node network-close to your databases.

Stats: Ensure fresh ANALYZE on large tables for accurate range estimates.

Rebalancing: Use --rebalance sparingly; merges are more expensive than splits.

Thinking