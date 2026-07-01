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
| `--max-cpu-ratio` | `-m` | Max CPU ratio | `0.5` |
| `--output` | `-o` | `json` or `html` | `json` |
| `--skip-cdc` | `-U` | Skip CDC processing (only rehash and compare) | `false` |
| `--cdc-timeout` |  | Seconds to drain CDC before giving up (`0` = use `cdc_processing_timeout` / default) | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace mtree table-diff --dbname=mydatabase my-cluster public.my_table
```

**Notes**

- With `--output html`, both JSON and HTML reports are generated with matching timestamps.
- The number of differing rows collected per node pair is bounded by
  `mtree.diff.max_diff_rows` (the shipped `ace.yaml` sets `1000000`; if the key
  is absent or `0`, the diff is unbounded). When the cap is reached, enumeration
  for that pair stops, the report's `diff_row_limit_reached` flag is set, and a
  warning is logged. This keeps a heavily diverged table from exhausting memory;
  lower the value in `ace.yaml` on memory-constrained hosts, and re-run after
  repairing to surface the remaining differences.
