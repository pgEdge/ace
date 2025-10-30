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
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Tips**
- For very large diffs, consider `hostdb` behavior in your workflow (temporary tables to accelerate comparisons).
- Compare results using the same diff file after replication lag clears.

**Example**
```sh
./ace table-rerun --diff-file=diffs/2025-04-08/diffs_090241529.json --dbname=mydatabase my-cluster public.my_table
```
