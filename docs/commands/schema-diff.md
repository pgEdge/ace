# schema-diff

Compares schemas across nodes. By default, runs `table-diff` on every table.  
Alternatively, `--ddl-only` compares only object presence (tables, views, functions, indexes).

**Usage**
```
./ace schema-diff [flags] [cluster] <schema>
```

**Arguments**
- `[cluster]` — Optional; overrides `default_cluster`.
- `<schema>` — Schema name.

**Flags**
| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--skip-tables` |  | Comma list of tables to exclude |  |
| `--skip-file` |  | File with list of tables to exclude |  |
| `--ddl-only` |  | Compare object sets only (no per-table diff) | `false` |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace schema-diff --dbname=mydatabase my-cluster public
```
