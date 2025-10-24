# mtree init

Initializes database objects needed for Merkle tree operations on all nodes (schema, metadata tables, publications, logical slots).

**Usage**
```
./ace mtree init [flags] [cluster_name]
```

**Arguments**

- `[cluster_name]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree init --dbname=mydatabase my-cluster
```
