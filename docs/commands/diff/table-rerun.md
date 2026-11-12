# table-rerun

Re-runs a previous diff to confirm fixes or check if inconsistencies persist.

**Usage**
```
./ace table-rerun [flags] [cluster]
```

**Arguments**
- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--diff-file` | `-f` | Path to the result directory of the original diff, or to the JSON report next to it (**required**) |  |
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--run-timeout <int>` |  | Time limit in seconds for the whole rerun. `0` means no limit. | `table_diff.run_timeout`, or `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Tips**
- Run the rerun after replication lag clears so rows have had time to sync.

**Example**
```sh
./ace table-rerun --diff-file=public_customers_large_diffs-20250722120353.json --dbname=mydatabase my-cluster 
```

The rerun reads the key of each difference from the result directory, reads the rows of these keys again on both nodes of each pair, and compares them by row hash. A row is found by its key text, as in the diff, and the rows are read with the filter of the original diff (`--table-filter`, `--against-origin`, `--until`), which the rerun takes from the manifest of the result. If ACE still finds mismatches, it writes a new diff result in the same format: a directory `<schema>_<table>_diffs-<timestamp>` and the JSON report next to it. Otherwise it logs that all previously reported differences have been resolved. A result of an earlier ACE version is refused: run the diff again.
