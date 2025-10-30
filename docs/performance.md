# Performance Tuning (ACE)

ACE parallelizes work across multiple processes, hashing blocks of rows and only drilling into blocks that differ. ACE performance on your system will depend on a number of tunable factors:

**Hardware and host capacity impacts performance**: Having more cores and memory improves processing time; ensure ACE’s CPU cap is consistent with your resources.

**Table architecture can speed hashing**: Tables with very wide rows and large JSON/BLOB/embedded columns slow hashing; design tables for efficiency, and ensure that VACUUM/ANALYZE functions are performed regularly.

**Network distribution impacts diff distribution**: Keep ACE close to your databases. Many small, scattered mismatches force more data movement across the network than a few concentrated ones.

!!! note

    We recommend you experiment with the suggestions on this page on a non-production system.

If you notice a pronounced change in a node's performance:

*  Review your ACE commands; include the [command options](#command-options-that-can-impact-performance) `--max-cpu-ratio=1.0`, `--block-rows=10000`, `--batch-size=1` when possible.
* Confirm that your table has been `ANALYZE`d recently.
* Use `--table-filter` and `--nodes` to target hot ranges.
* If you're using ACE on huge tables, use Merkle trees.
* After repairing differences, run `table-rerun` to validate the results.


## Command Options that Can Impact Performance

When invoking [ACE commands](/commands/index.md), review the available command options; many of the options are designed to improve performance.  For example:

- **`--max-cpu-ratio`**
    This option specifies the percentage of CPU power you are allotting for use by ACE. A value of `1` instructs the server to use all available CPUs, while `.5` means use half of the available CPUs. The default is `.6` (or 60% of the CPUs). Setting it to its maximum (`1.0`) will result in faster execution times. Modify this parameter as needed.

- **`--block-rows`**
    `--block-rows` specifies the number of tuples to be used at a time during table comparisons. ACE computes an MD5 sum on the full chunk of rows per block and compares it with the hash of the same chunk on the other nodes. If the hashes match up between nodes, then ACE moves on to the next block. Otherwise, the rows get pulled in and a set difference is computed. If block_rows is set to 1000, then a thousand tuples are compared per job across tables. 
  
    It is worth noting here that while it may appear that larger block sizes yield faster results, it may not always be the case. Using a larger block size will result in a speed up, but only up to a threshold. If the block size is too large, the Postgres `array_agg()` function may run out of memory, or the hash might take longer to compute, thus annulling the benefit of using a larger block size. The sweet spot is a block size that is large enough to yield quicker runtimes, but still small enough to avoid the issues listed above. ACE enforces that block sizes are between 10^3 and 10^5 rows.

- **`--batch-size`**
    `--batch_size` dictates how many sets of block_rows a single process should handle. By default, this is set to 1 to achieve the maximum possible parallelism – each process in the multiprocessing pool works on one block at a time. However, in some cases, you may want to limit process creation overheads and use a larger batch size. We recommend you leave this setting to its default value, unless there is a specific use-case that demands changing it.

- **`--table-filter`**
    Use a table filter clause to narrow the comparison scope for large tables.

- **`--nodes`**  
    Compare a subset (e.g., `n1,n2`) of your cluster's nodes to reduce cross-node IO.


## Improving Performance with Merkle Trees

Using a [Merkle tree](/merkle.md) can improve performance when your tables are very large by storing statistics so you can avoid a full rescan between diff activities.  To improve performance when using Merkle trees:

- Keep your statistics fresh (`ANALYZE`) for accurate range estimation.
- For huge tables(billion-row/terabyte), [parallelize builds](/merkle.md#building-merkle-trees-in-parallel-for-very-large-tables). 
- Use `mtree listen` (CDC) or rely on the pre-diff update that `mtree table-diff` performs automatically to keep your data fresh automatically.

!!! note

    Merkle tree initialization adds triggers; measure the impact of this addition on write-heavy workloads.


