# schema-diff

Compares schemas across nodes. By default, runs `table-diff` on every table.  
Alternatively, `--ddl-only` compares only object presence (tables, views, functions, indexes).

**Usage**

```
./ace schema-diff [flags] [cluster_name] <schema_name>
```

**Arguments**

- `[cluster_name]` — Optional; overrides `default_cluster`.
- `<schema_name>` — Schema name.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--skip-tables` | `-T` | Comma list of tables to exclude |  |
| `--skip-file` | `-s` | File with list of tables to exclude |  |
| `--block-size <int>` | `-b` | Rows per block when diffing tables. Default `100000`. |
| `--concurrency-factor <int>` | `-c` | Workers per node (1–10). Default `1`. |
| `--compare-unit-size <int>` | `-u` | Recursive split size for mismatched blocks. Default `10000`. |
| `--output <json\|html>` | `-o` | Per-table diff report format. Default `json`. |
| `--override-block-size` | `-B` | Allow block sizes outside `ace.yaml` guardrails. |
| `--ddl-only` | `-L` | Compare object sets only (no per-table diff) | `false` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |
| `--schedule` | `-S` | Run the schema diff repeatedly on a timer (requires `--every`). Not compatible with `--ddl-only`. |
| `--every <duration>` | `-e` | Go duration string (for example, `24h`). Used with `--schedule`. |

**Example**

```sh
./ace schema-diff --dbname=mydatabase my-cluster public
```

When `--ddl-only` is **not** set, every qualifying table invokes `table-diff` using the same block size, concurrency factor, compare-unit size, output format, and override settings supplied here, so you get consistent behaviour between standalone and schema-driven comparisons.

### Scheduling runs

Use `--schedule --every=<duration>` to keep a schema comparison running on a loop. This mode is only supported when ACE can run per-table diffs (omit `--ddl-only`):

```sh
./ace schema-diff --schedule --every=24h --dbname=mydatabase my-cluster public
```

ACE performs the first comparison immediately, then waits for the given interval before repeating. Stop the process to end the loop.
