# mtree teardown

Drops all database objects created by `mtree init` across the cluster (schema, Merkle data) and stops CDC.

**Usage**
```
./ace mtree teardown [flags] [cluster]
```

**Arguments**
- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**
| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree teardown --dbname=mydatabase my-cluster
```
