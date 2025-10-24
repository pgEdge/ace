# Performance Tuning (ACE)

ACE parallelizes work across multiple processes, hashing blocks of rows and only drilling into blocks that differ. ACE performance on your system will depend on a number of tunable factors:

* **Hardware** and **Host capacity**: More cores and memory help; ensure ACE’s CPU cap matches reality.
* **Table characteristics**: Very wide rows, large JSON/BLOB/embedding columns slow hashing and transport.
* **Diff distribution**: Many small, scattered mismatches force more data movement than a few concentrated ones.
* **Network**: Keep ACE close (network-wise) to the databases.

## Command Options that Can Impact Performance

When invoking [ACE commands](/commands/index.md), review the available command options; many of the options are designed to improve performance.  For example:

- **`--max-cpu-ratio`**  
  Percentage of host CPU ACE can use (0.0–1.0).  
  *Default:* `0.6`. Higher values speed up runs; set to `1.0` when you can.

- **`--block-rows`**  
  Rows per block for hashing & comparison.  
  *Range enforced:* `1e3 .. 1e5`. Larger blocks are faster—until memory/hash costs (e.g., `array_agg`) negate the gains. Find your sweet spot empirically.

- **`--batch-size`**  
  Blocks per worker before handing off.  
  *Default:* `1` (max parallelism). Increase only to reduce process scheduling overhead in niche cases.

- **`--table-filter`**  
  SQL `WHERE` to narrow scope (e.g., recent ranges). Massive speedups when diffs are localized.

- **`--nodes`**  
  Compare a subset (e.g., `n1,n2`) to reduce cross-node IO.


## Use Merkle Trees (for Very Large Tables)

- Use **Merkle mode** (`mtree build`, `mtree table-diff`) to avoid full rescans.
- Keep statistics **fresh** (`ANALYZE`) for accurate range estimation.
- For billion-row/terabyte tables, **parallelize builds**:
  - Generate ranges with `--write-ranges=true`, copy the ranges file, then build per node with `--ranges-file` and `--nodes=<that-node>`.
- Continuous freshness: run `mtree listen` (CDC) or rely on the pre-diff update that `mtree table-diff` performs automatically.
- Note: Merkle initialization adds **triggers**; measure the impact on write-heavy workloads.

## Performance Tuning Checklist

1. Include the command options `--max-cpu-ratio=1.0`, `--block-rows=10000`, `--batch-size=1` when possible.
2. Confirm that your table has been `ANALYZE`d recently.
3. Use `--table-filter` to target hot ranges.
4. If you're using ACE on huge tables, use Merkle trees.
5. After repairing differences, run `table-rerun` to validate.
