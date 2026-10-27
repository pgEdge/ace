# Performance Tuning (ACE)

ACE parallelizes work across multiple processes, hashing blocks of rows and only drilling into blocks that differ. ACE performance on your system will depend on a number of tunable factors:

**Hardware and host capacity impacts performance**: Having more cores and memory improves processing time; ensure ACE’s CPU cap is consistent with your resources.

**Table architecture can speed hashing**: Tables with very wide rows and large JSON/BLOB/embedded columns slow hashing; design tables for efficiency, and ensure that VACUUM/ANALYZE functions are performed regularly.

**Network distribution impacts diff distribution**: Keep ACE close to your databases. Many small, scattered mismatches force more data movement across the network than a few concentrated ones.

!!! note

    We recommend you experiment with the suggestions on this page on a non-production system.

If you notice a pronounced change in a node's performance:

* Review your ACE commands; experiment with the [table-diff tuning flags](#command-options-that-impact-performance) such as `--block-size`, `--max-connections`, `--concurrency-factor`, and `--compare-unit-size`.
* Confirm that your table has been `ANALYZE`d recently.
* Use `--table-filter`/`-F` and `--nodes` to target hot ranges.
* If you're using ACE on huge tables, use Merkle trees.
* After repairing differences, run `table-rerun` to validate the results.


## Command Options that Impact Performance

When invoking [ACE commands](commands/index.md), review the available command options; many of the options are designed to improve performance.  For example:

- **`--block-size`**
    Controls the number of rows hashed per block. Larger blocks reduce round-trips but can increase memory usage or hash time. Defaults to `100000`, and respects the guardrails defined in `ace.yaml` unless `--override-block-size`/`-B` is set.

- **`--concurrency-factor`**
    CPU ratio that controls how many comparisons of the blocks that differ ACE runs at the same time, relative to available CPUs (0.0–4.0). For example, `0.5` on a 16-CPU machine runs 8 comparisons. Higher values find the differing rows faster on machines with spare CPU, but can create contention on smaller or busier hosts. It does not change the speed of hashing; for that, see `--max-connections`. Default is `0.5`.

- **`--compare-unit-size`**
    Sets the minimum block size used when ACE recursively drills into mismatched blocks. Smaller values provide more granular comparisons at the cost of additional round-trips. Default is `10000`.

- **`--max-connections`**
    The number of connections that ACE opens to each node to cut and hash a table. The default is 4, or the value from the `max_connections` section of `ace.yaml`, which can also give a different value for each node. More connections hash faster but keep more backends busy on the server. If a server has fewer free connection slots, ACE works with the connections it gets and writes a warning. A value that you set is a strict limit for the whole task. Without one, comparing the blocks that differ can use more connections, up to the number that `--concurrency-factor` gives. See [Connections per node](configuration.md#connections-per-node).

- **`--override-block-size` (`-B`)**
    Override the safety limits defined in `ace.yaml`. Use cautiously: extremely large blocks can lead to `array_agg` memory pressure.

- **`--table-filter` (`-F`)**
    Use a table filter clause to narrow the comparison scope for large tables.

- **`--nodes`**  
    Compare a subset (e.g., `n1,n2`) of your cluster's nodes to reduce cross-node IO.


## Improving Performance with Merkle Trees

Using a [Merkle tree](commands/mtree/index.md) can improve performance when your tables are very large by storing statistics so you can avoid a full rescan between diff activities.  To improve performance when using Merkle trees:

- Keep your statistics fresh (`ANALYZE`) for accurate range estimation.
- For huge tables(billion-row/terabyte), [parallelize builds](commands/mtree/index.md#building-merkle-trees-in-parallel-for-very-large-tables). 
- Use `mtree listen` (CDC) or rely on the pre-diff update that `mtree table-diff` performs automatically to keep your data fresh automatically.
- Tune `--max-cpu-ratio` on Merkle commands (`mtree build`, `mtree table-diff`, `mtree update`) to control worker parallelism per host. The ratio is a share of the CPUs on the machine running ACE, not the database server, and each worker keeps one backend busy on every node. Use `--max-connections` (or `mtree.max_connections` in `ace.yaml`) for an absolute cap when ACE runs on a host with far more cores than the database servers; it works like the same flag on the diff commands, and since one connection holds the tree's transaction, `N` connections means `N - 1` hash workers on `mtree update` and `mtree table-diff`; `mtree build` also runs cutting workers out of the same pool, so it needs at least 3. ACE logs the worker count it settles on at the start of each phase.
- ACE sends `max_parallel_workers_per_gather = 0` on every connection it opens by default, for the diff and repair commands as well as the Merkle ones, so its queries do not also fan out into Postgres parallel workers. Change `postgres.max_parallel_workers_per_gather` in `ace.yaml` to allow them.

!!! note

    Merkle tree initialization adds triggers; measure the impact of this addition on write-heavy workloads.
