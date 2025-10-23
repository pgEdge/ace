# mtree teardown-table

Removes all database objects associated with a table’s Merkle tree (tree data, metadata) and removes the table from CDC publication.

**Usage**
```
./ace mtree teardown-table [flags] [cluster] <schema.table>
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
./ace mtree teardown-table --dbname=mydatabase my-cluster public.my_table
```
