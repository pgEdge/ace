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
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace repset-diff --dbname=mydatabase my-cluster my_repset
```
