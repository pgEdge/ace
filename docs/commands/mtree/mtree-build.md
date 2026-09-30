# mtree build

Builds a Merkle tree for a table on all nodes (after `mtree init`).

**Usage**

```
./ace mtree build [flags] [cluster] <schema.table>
```

**Arguments**

- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--block-size` | `-b` | Rows per leaf block | `10000` |
| `--max-cpu-ratio` | `-m` | Share of this host's CPUs to use as parallel workers per node | `0.5` |
| `--max-connections` | `-M` | Maximum database connections per node (`0` = use `mtree.max_connections`, else derive from `--max-cpu-ratio`) | `0` |
| `--override-block-size` | `-B` | Skip safety checks | `false` |
| `--analyse` | `-a` | Run `ANALYZE` before build | `false` |
| `--recreate-objects` | `-R` | Drop & recreate Merkle objects | `false` |
| `--write-ranges` | `-w` | Write computed ranges JSON | `false` |
| `--ranges-file` | `-k` | Use pre-computed block ranges |  |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree build --dbname=mydatabase my-cluster public.my_table
```

**Partitioned tables and inheritance**

ACE builds a Merkle tree on a table that stores rows. It does not build one
on a partitioned table or on a table with child tables through inheritance
(`INHERITS`). The tree stays current through CDC, and CDC reports each change
under the name of the table that holds the row: the partition or the child.
A tree on the parent would not see these changes.

For a partitioned table, build a tree on each partition. To compare the
whole partitioned table in one run, use `table-diff`.
