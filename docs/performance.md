# Performance Tuning (ACE)

ACE parallelizes work across multiple processes, hashing blocks of rows and only drilling into blocks that differ. ACE performance on your system will depend on a number of tunable factors:

**Hardware and host capacity impacts performance**: Having more cores and memory improves processing time; ensure ACE’s CPU cap is consistent with your resources.

**Table architecture can speed hashing**: Tables with very wide rows and large JSON/BLOB/embedded columns slow hashing; design tables for efficiency, and ensure that VACUUM/ANALYZE functions are performed regularly.

**Network distribution impacts diff distribution**: Keep ACE close to your databases. Many small, scattered mismatches force more data movement across the network than a few concentrated ones.

!!! note

    We recommend you experiment with the suggestions on this page on a non-production system.

If you notice a pronounced change in a node's performance:

* Review your ACE commands; experiment with the [table-diff tuning flags](#command-options-that-can-impact-performance) such as `--block-size`, `--concurrency-factor`, and `--compare-unit-size`.
* Confirm that your table has been `ANALYZE`d recently.
* Use `--table-filter` and `--nodes` to target hot ranges.
* If you're using ACE on huge tables, use Merkle trees.
* After repairing differences, run `table-rerun` to validate the results.


## Command Options that Impact Performance

When invoking [ACE commands](/commands/), review the available command options; many of the options are designed to improve performance.  For example:

- **`--block-size`**
    Controls the number of rows hashed per block. Larger blocks reduce round-trips but can increase memory usage or hash time. Defaults to `100000`, and respects the guardrails defined in `ace.yaml` unless `--override-block-size` is set.

- **`--concurrency-factor`**
    Increases the number of workers per node (1–10). Higher values improve throughput on machines with spare CPU, but can create contention on smaller hosts. Default is `1`.

- **`--compare-unit-size`**
    Sets the minimum block size used when ACE recursively drills into mismatched blocks. Smaller values provide more granular comparisons at the cost of additional round-trips. Default is `10000`.

- **`--override-block-size`**
    Override the safety limits defined in `ace.yaml`. Use cautiously: extremely large blocks can lead to `array_agg` memory pressure.

- **`--table-filter`**
    Use a table filter clause to narrow the comparison scope for large tables.

- **`--nodes`**  
    Compare a subset (e.g., `n1,n2`) of your cluster's nodes to reduce cross-node IO.


## Improving Performance with Merkle Trees

Using a [Merkle tree](/commands/mtree/) can improve performance when your tables are very large by storing statistics so you can avoid a full rescan between diff activities.  To improve performance when using Merkle trees:

- Keep your statistics fresh (`ANALYZE`) for accurate range estimation.
- For huge tables(billion-row/terabyte), [parallelize builds](commands/mtree/index.md#building-merkle-trees-in-parallel-for-very-large-tables). 
- Use `mtree listen` (CDC) or rely on the pre-diff update that `mtree table-diff` performs automatically to keep your data fresh automatically.
- Tune `--max-cpu-ratio` on Merkle commands (`mtree build`, `mtree table-diff`, `mtree update`) to control worker parallelism per host.

!!! note

    Merkle tree initialization adds triggers; measure the impact of this addition on write-heavy workloads.
