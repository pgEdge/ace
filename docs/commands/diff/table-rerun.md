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
| `--diff-file` | `-f` | Path to original diff (**required**) |  |
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

If ACE still finds mismatches, it writes a new report named `<schema>_<table>_rerun-diffs-<timestamp>.json`; otherwise it logs that all previously reported differences have been resolved.
