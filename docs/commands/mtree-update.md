# mtree update

Manually applies CDC changes to a table’s Merkle tree (also supports rebalancing).

**Usage**
```
./ace mtree update [flags] [cluster] <schema.table>
```

**Arguments**
- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**
| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--max-cpu-ratio` |  | Max CPU ratio | `0.5` |
| `--rebalance` |  | Merge small blocks to rebalance | `false` |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree update --rebalance --dbname=mydatabase my-cluster public.my_table
```
