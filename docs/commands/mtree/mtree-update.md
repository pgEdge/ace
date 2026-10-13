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
| `--skip-cdc` | `-U` | Skip CDC processing (only rehash dirty blocks) | `false` |
| `--cdc-timeout` |  | Seconds to drain CDC before giving up (`0` = use `cdc_processing_timeout` / default) | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Hash version**

The metadata of every tree stores the version of the hash algorithm that
computed its leaves. When the stored version is older than the version of
this ACE, `mtree update` computes all leaf hashes again. When it is newer,
the tree was written by a newer ACE: `mtree update` and `mtree table-diff`
stop with an error and change nothing. Use the newer ACE, or rebuild the tree
with `mtree build`. ACE does not support a tree that holds hashes of two
algorithms, so to go back to an older ACE, run `mtree build` of that version
first.

**Example**

```sh
./ace mtree update --rebalance --dbname=mydatabase my-cluster public.my_table
```
