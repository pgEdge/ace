# mtree update

Manually applies changes to a table’s Merkle tree (also supports rebalancing).

## Usage
```
./ace mtree update [flags] [cluster_name] <schema.table_name>
```

## Arguments

- `[cluster_name]` — Optional; overrides `default_cluster`.
- `schema` is the name of the schema in which a table resides.
- `table_name` is the name of the table.


## Flags

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--max-cpu-ratio` | `-m` | Share of this host's CPUs to use as parallel workers per node | `0.5` |
| `--max-connections` | `-M` | Maximum database connections per node (`0` = use `mtree.max_connections`, else derive from `--max-cpu-ratio`) | `0` |
| `--rebalance` | `-l` | Merge small blocks to rebalance | `false` |
| `--skip-cdc` | `-U` | Skip CDC processing (only rehash dirty blocks) | `false` |
| `--cdc-timeout` |  | Seconds to drain CDC before giving up (`0` = use `cdc_processing_timeout` / default) | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

## Hash version

The metadata of every tree stores the version of the hash algorithm that
computed its leaves. When the stored version is older than the version of
this ACE, `mtree update` computes all leaf hashes again. When it is newer,
the tree was written by a newer ACE: `mtree update` and `mtree table-diff`
stop with an error and change nothing. Use the newer ACE, or rebuild the tree
with `mtree build`. ACE does not support a tree that holds hashes of two
algorithms, so to go back to an older ACE, run `mtree build` of that version
first.

## Changes of the table

`mtree update` and `mtree table-diff` first run the same checks of the
table as `mtree build`: the table exists, has a primary key, and has the
same columns and key on all nodes. If one of these checks fails, for
example because the primary key was dropped or changed on only one node,
the command stops with that error and does not change the tree state. When
these checks pass, and before any other work, the command compares the
table on each node with what the tree recorded at `mtree build`:

- A change of the primary key or of the replica identity makes the tree not
  valid: other key columns, the same columns in another order, another type,
  typmod or collation of a key column, a key column dropped and added again
  (it gets a new attribute number), or another replica identity. The leaves
  were cut in the order of the old key, so the tree cannot be used. The
  command marks the tree as not valid on that node and stops with an error
  that says what changed. Every later `mtree update` and `mtree table-diff`
  stops with the same error until you run `mtree build`. `mtree build`
  accepts the table only if it meets the rules in
  [Replica identity](mtree-build.md#replica-identity) on every node; it
  checks them and says what is wrong.
- A change of the other columns (a new, dropped or renamed column, another
  type or typmod, a new expression of a generated column) keeps the tree
  valid, but its leaf hashes are stale: for example, a type change can round
  the stored values without any row change in the CDC stream. `mtree update`
  then computes all leaf hashes of that node again and records the new
  columns.
- A rewrite of the table has the same effect, also when the definition is
  the same as at the last run: for example, a type change that rounds the
  values and a second type change that brings back the old type. The
  command finds a rewrite by the file that stores the table, so a rewrite
  that keeps the values, such as `VACUUM FULL` or `CLUSTER`, also makes
  `mtree update` compute all leaf hashes of that node again.
- A tree built by an earlier ACE version has no record of the table, so it
  is not valid. Run `mtree build` for it.

If the table has no usable replica identity any more, `UPDATE` and `DELETE`
on the table fail while it is in the CDC publication. This is the case when
the replica identity is `NOTHING`, when it is `DEFAULT` and the primary key
was dropped, or when it is `USING INDEX` and that index was dropped. This
failure comes from PostgreSQL and is separate from the tree state. ACE does
not change the publication on its own. If the primary key was dropped, add
it again first. Then set a replica identity that `mtree build` accepts, with
the same mode on all nodes, and run `mtree build`. Or run
`mtree teardown-table` to remove the table from the publication.

## Example

```sh
./ace mtree update --rebalance --dbname=mydatabase my-cluster public.my_table
```
