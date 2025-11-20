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
| `--skip-tables` | `-T` | Comma list of tables to exclude |  |
| `--skip-file` | `-s` | File with list of tables to exclude |  |
| `--block-size <int>` | `-b` | Rows per block when diffing each table. Default `100000`. |
| `--concurrency-factor <int>` | `-c` | Workers per node (1–10). Default `1`. |
| `--compare-unit-size <int>` | `-u` | Recursive split size for mismatched blocks. Default `10000`. |
| `--output <json\|html>` | `-o` | Per-table diff report format. Default `json`. |
| `--override-block-size` | `-B` | Allow block sizes outside `ace.yaml` guardrails. |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |
| `--schedule` | `-S` | Run the diff repeatedly on a timer (requires `--every`). |
| `--every <duration>` | `-e` | Go duration string (for example, `30m`, `6h`). Used with `--schedule`. |

**Example**

```sh
./ace repset-diff --dbname=mydatabase my-cluster my_repset
```

Each table in the replication set is diffed with the same block size, concurrency factor, compare-unit size, output format, and override behaviour you provide here.

### Scheduling runs

Combine `--schedule` with `--every=<duration>` to keep the replication-set sweep running until you cancel it:

```sh
./ace repset-diff --schedule --every=4h --dbname=mydatabase my-cluster my_repset
```

ACE runs the job immediately, waits for the interval, then repeats in the same process.
