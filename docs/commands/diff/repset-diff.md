# repset-diff

Runs `table-diff` on every table in a replication set and reports differences.

**Usage**

```
./ace repset-diff [flags] [cluster] <repset>
```

**Arguments**

- `[cluster]` — Optional; overrides `default_cluster`.
- `<repset>` — Replication set name.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--skip-tables` |  | Comma list of tables to exclude |  |
| `--skip-file` |  | File with list of tables to exclude |  |
| `--block-size <int>` | `-b` | Rows per block when diffing each table. Default `100000`. |
| `--concurrency-factor <int>` | `-c` | Workers per node (1–10). Default `1`. |
| `--compare-unit-size <int>` | `-s` | Recursive split size for mismatched blocks. Default `10000`. |
| `--output <json\|html>` | `-o` | Per-table diff report format. Default `json`. |
| `--override-block-size` |  | Allow block sizes outside `ace.yaml` guardrails. |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace repset-diff --dbname=mydatabase my-cluster my_repset
```

Each table in the replication set is diffed with the same block size, concurrency factor, compare-unit size, output format, and override behaviour you provide here.
