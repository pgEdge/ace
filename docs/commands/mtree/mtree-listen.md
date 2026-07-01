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

You no longer need to stop `mtree listen` before running `mtree table-diff` or
`mtree update`. While `listen` is active, those commands skip their own CDC
drain for the affected node and compare against the tree `listen` maintains
(best-effort freshness). Stop `listen` first only when you need a diff that is
guaranteed current to the present instant.
