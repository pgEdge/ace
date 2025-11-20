# mtree update

Manually applies changes to a table’s Merkle tree (also supports rebalancing).

**Usage**
```
./ace mtree update [flags] [cluster_name] <schema.table_name>
```

**Arguments**

- `[cluster_name]` — Optional; overrides `default_cluster`.
- `schema` is the name of the schema in which a table resides.
- `table_name` is the name of the table.


**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--max-cpu-ratio` | `-m` | Max CPU ratio | `0.5` |
| `--rebalance` | `-l` | Merge small blocks to rebalance | `false` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace mtree update --rebalance --dbname=mydatabase my-cluster public.my_table
```
