# mtree listen

Starts a long-running process to listen for CDC changes and auto-update Merkle trees for tracked tables.

**Usage**
```
./ace mtree listen [flags] [cluster]
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
./ace mtree listen --dbname=mydatabase my-cluster
```
