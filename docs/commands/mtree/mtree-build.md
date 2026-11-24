# mtree build

Builds a Merkle tree for a table on all nodes (after `mtree init`).

## Usage

```
./ace mtree build [flags] [cluster] <schema.table>
```

## Arguments

- `[cluster]` — Optional; overrides `default_cluster`.

## Flags

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--block-size` | `-b` | Rows per leaf block | `10000` |
| `--max-cpu-ratio` | `-m` | Share of this host's CPUs to use as parallel workers per node | `0.5` |
| `--max-connections` | `-M` | Maximum database connections per node, at least 3 (`0` = use `mtree.max_connections`, else derive from `--max-cpu-ratio`) | `0` |
| `--override-block-size` | `-B` | Skip safety checks | `false` |
| `--analyse` | `-a` | Run `ANALYZE` before build | `false` |
| `--recreate-objects` | `-R` | Drop & recreate Merkle objects | `false` |
| `--write-ranges` | `-w` | Write computed ranges JSON | `false` |
| `--ranges-file` | `-k` | Use pre-computed block ranges |  |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

## Example
```sh
./ace mtree build --dbname=mydatabase my-cluster public.my_table
```

## Partitioned tables and inheritance

ACE builds a Merkle tree on a table that stores rows. It does not build one
on a partitioned table or on a table with child tables through inheritance
(`INHERITS`). The tree stays current through CDC, and CDC reports each change
under the name of the table that holds the row: the partition or the child.
A tree on the parent would not see these changes.

For a partitioned table, build a tree on each partition. To compare the
whole partitioned table in one run, use `table-diff`.

## Replica identity

CDC finds the leaf of a changed row by the columns of the replica identity of
the table, so they must be the primary key columns. `mtree build` accepts
`REPLICA IDENTITY DEFAULT` (the primary key), or `USING INDEX` on a unique
index with exactly the primary key columns. It refuses `FULL`, `NOTHING`, and
`USING INDEX` on other columns, and a replica identity that differs between
nodes. The check runs before the table goes into the CDC publication: with
`NOTHING`, a table in the publication makes every `UPDATE` and `DELETE` on it
fail.

## What the tree records

The metadata of the tree records the primary key it was built for (the
columns in key order, with their attribute numbers, types and collations),
the replica identity, and the columns of the table. `mtree update` and
`mtree table-diff` compare them with the table each time; see
[Changes of the table](mtree-update.md#changes-of-the-table). If the table
changes while `mtree build` runs, the build fails; run it again.

A tree built by an earlier ACE version does not have this record and is not
valid. Run `mtree build` for it again.
