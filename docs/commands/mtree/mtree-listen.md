# mtree listen

Starts a long-running process to listen for changes and auto-update Merkle trees for tracked tables.

**Usage**

```
./ace mtree listen [flags] [cluster_name]
```

**Arguments**

- `[cluster_name]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree listen --dbname=mydatabase my-cluster
```
