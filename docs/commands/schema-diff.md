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
| `--skip-tables` |  | Comma list of tables to exclude |  |
| `--skip-file` |  | File with list of tables to exclude |  |
| `--block-size <int>` | `-b` | Rows per block when diffing tables. Default `100000`. |
| `--concurrency-factor <int>` | `-c` | Workers per node (1–10). Default `1`. |
| `--compare-unit-size <int>` | `-s` | Recursive split size for mismatched blocks. Default `10000`. |
| `--output <json\|html>` | `-o` | Per-table diff report format. Default `json`. |
| `--override-block-size` |  | Allow block sizes outside `ace.yaml` guardrails. |
| `--ddl-only` |  | Compare object sets only (no per-table diff) | `false` |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace schema-diff --dbname=mydatabase my-cluster public
```

When `--ddl-only` is **not** set, every qualifying table invokes `table-diff` using the same block size, concurrency factor, compare-unit size, output format, and override settings supplied here, so you get consistent behaviour between standalone and schema-driven comparisons.
