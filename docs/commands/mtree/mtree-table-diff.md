# mtree table-diff

Compares Merkle trees of a table across nodes to find inconsistencies.  
By default, updates trees first using CDC.

If a node's replication slot is already held by another consumer — normally a
running `mtree listen`, but also a concurrent `table-diff`/`update` on the same
node (the slot is shared across all of a node's Merkle trees) — `table-diff`
skips the CDC drain for that node (printing a warning) and compares against the
already-maintained tree instead of failing. The comparison is best-effort and
may omit the most recent changes on those nodes, so divergence can be
under-reported; the skipped nodes are listed in the diff summary
(`cdc_skipped_nodes`). Ensure no `mtree listen` or other mtree operation is
holding the node's slot, then re-run, if you need a guaranteed-current drain.

**Usage**

```
./ace mtree table-diff [flags] [cluster] <schema.table>
```

**Arguments**

- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--max-cpu-ratio` | `-m` | Share of this host's CPUs to use as parallel workers per node | `0.5` |
| `--max-connections` | `-M` | Maximum database connections per node (`0` = use `mtree.max_connections`, else derive from `--max-cpu-ratio`) | `0` |
| `--output` | `-o` | `json` or `html` | `json` |
| `--max-html-rows` |  | Max rows per node pair in the HTML report (`0` = use `mtree.diff.max_html_rows`, or `10000`) | `0` |
| `--skip-cdc` | `-U` | Skip CDC processing (only rehash and compare) | `false` |
| `--cdc-timeout` |  | Seconds to drain CDC before giving up (`0` = use `cdc_processing_timeout` / default) | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace mtree table-diff --dbname=mydatabase my-cluster public.my_table
```

**Notes**

- When the diff finds differences, it writes a result directory and the JSON report with matching names; `--output html` adds the HTML report. The HTML report can be built again from the result directory with [diff-report](../diff/diff-report.md).
- The HTML report shows at most `max_html_rows` rows for each node pair
  (`mtree.diff.max_html_rows`, default `10000`). The JSON report contains
  every row that the diff collected; when `max_diff_rows` stops the diff
  early, its summary has `diff_row_limit_reached`, and more differences may
  exist. When the HTML report is truncated, it says so, and a
  repair plan built in it has rules only for the rows shown and
  `default_action: skip`, so `table-repair` does not change the other rows.
  With three or more nodes this holds per key, not per node pair: see the
  known limit in the `table-diff` docs.
- The number of differing rows of all node pairs together is bounded by
  `mtree.diff.max_diff_rows` (the shipped `ace.yaml` sets `1000000`; if the key
  is absent or `0`, the diff is unbounded). The bound is approximate: the
  ranges in work when it is reached are compared to the end. The pairs are
  compared in the order of their names, so the first pairs can take the whole
  bound. When the bound is reached, the ranges left are skipped, the report's
  `diff_row_limit_reached` flag is set, a warning is logged, and no stale
  block is refreshed. Re-run after repairing to surface the remaining
  differences. The differing rows go to disk as soon as they are found, so
  the bound does not protect the memory of the client; it limits the size of
  the result and the run time.
- If the comparison of a range fails, the diff fails and writes no result.
- Merkle trees do not order every primary key type the way the server does. `text`
  keys are compared byte by byte, so a collation other than `C`, and also `enum`
  or `citext` keys, can sort differently here than in the database; `numeric` and
  the types the driver decodes into a struct are not ordered correctly at all.
  When the two orders disagree, some blocks are skipped and the diff reports
  fewer differences than exist. Use `table-diff` for those tables: it does not
  sort block bounds outside the database. See
  [Primary Key Types and the Postgres Type Boundary](../../design/merkle.md#primary-key-types-and-the-postgres-type-boundary)
  for the full list and the reasons.
- When *all* mismatched blocks between a node pair turn out to hold no row
  differences, the tree hashes were stale rather than the data divergent. The
  diff reports this, refreshes those blocks from live data (recorded in the
  summary's `stale_blocks_refreshed`), and the next diff runs clean. A pair
  with any real differences, a diff that reached `max_diff_rows`, or an
  `--until` filter is not refreshed. If the trees still disagree after a refresh, the nodes'
  leaf ranges have diverged; run `ace mtree build` to realign them.
