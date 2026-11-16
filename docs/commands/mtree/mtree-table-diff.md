# mtree table-diff

Compares Merkle trees of a table across nodes to find inconsistencies.  
By default, updates trees first using CDC.

If a node's replication slot is already held by another consumer — normally a
running `mtree listen`, but also a concurrent `table-diff`/`update` on the same
node (the slot is shared across all of a node's Merkle trees) — `table-diff`
skips the CDC drain for that node (printing a warning) and compares against the
already-maintained tree instead of failing. The comparison is best-effort and
may omit the most recent changes on those nodes, so divergence can be
under-reported; the skipped nodes are listed in the diff summary
(`cdc_skipped_nodes`). Ensure no `mtree listen` or other mtree operation is
holding the node's slot, then re-run, if you need a guaranteed-current drain.

**Usage**

```
./ace mtree table-diff [flags] [cluster] <schema.table>
```

**Arguments**

- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--max-cpu-ratio` | `-m` | Share of this host's CPUs to use as parallel workers per node | `0.5` |
| `--max-connections` | `-M` | Maximum database connections per node (`0` = use `mtree.max_connections`, else derive from `--max-cpu-ratio`) | `0` |
| `--output` | `-o` | `json` or `html` | `json` |
| `--max-html-rows` |  | Max rows per node pair in the HTML report (`0` = use `mtree.diff.max_html_rows`, or `10000`) | `0` |
| `--max-html-value-length` |  | Max characters of a value in the HTML report (`0` = use `mtree.diff.max_html_value_length`, or `1024`) | `0` |
| `--skip-cdc` | `-U` | Skip CDC processing (only rehash and compare) | `false` |
| `--cdc-timeout` |  | Seconds to drain CDC before giving up (`0` = use `cdc_processing_timeout` / default) | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace mtree table-diff --dbname=mydatabase my-cluster public.my_table
```

**Notes**

- When the diff finds differences, it writes a result directory and the JSON report with matching names; `--output html` adds the HTML report. The HTML report can be built again from the result directory with [diff-report](../diff/diff-report.md).
- The HTML report shows at most `max_html_rows` rows for each node pair
  (`mtree.diff.max_html_rows`, default `10000`). The JSON report contains
  every row that the diff collected; when `max_diff_rows` stops the diff
  early, its summary has `diff_row_limit_reached`, and more differences may
  exist. When the HTML report is truncated, it says so, and a
  repair plan built in it has rules only for the rows shown and
  `default_action: skip`, so `table-repair` does not change the other rows.
  With three or more nodes this holds per key, not per node pair: see the
  known limit in the `table-diff` docs.
- The number of differing rows of all node pairs together is bounded by
  `mtree.diff.max_diff_rows` (the shipped `ace.yaml` sets `1000000`; if the key
  is absent or `0`, the diff is unbounded). The bound is approximate: the
  ranges in work when it is reached are compared to the end. The pairs are
  compared in the order of their names, so the first pairs can take the whole
  bound. When the bound is reached, the ranges left are skipped, the report's
  `diff_row_limit_reached` flag is set, a warning is logged, and no stale
  block is refreshed. Re-run after repairing to surface the remaining
  differences. The differing rows go to disk as soon as they are found, so
  the bound does not protect the memory of the client; it limits the size of
  the result and the run time.
- If the comparison of a range fails, the diff fails and writes no result.
- The primary key columns must have the same types and the same collations on every node. The command stops with an error that names the column, the types or the collations, and the nodes when the name of a key type or the collation of a key column differs. The collations are compared by the settings that decide the order: the provider and the determinism; for libc the locale of `LC_COLLATE`; for ICU the ICU locale and the ICU rules; for the builtin provider its locale. For the default collation these are the settings of the database. Spellings of one libc locale are the same locale: `en_US.UTF-8` and `en_US.utf8`, or `POSIX` and `C`. The `LC_COLLATE` of a database with the ICU or the builtin provider is not compared, because it does not change the order of the key. A collation other than the default one must also have the same schema-qualified name on every node: two collations with the same definition under other names stop the task too. The version of the collation library is not compared: another ICU or glibc version on a node can order some key values in another way. The ranges of the diff are built in the order of one node, so they can then miss rows of the other node, and the check does not see it. Keep the collation libraries of the nodes on the same version. The checks compare only the name of each key type, not its definition. Keep the definitions of the key types the same on every node: the label order of an `enum`, the base type of a domain, the fields of a composite type. The checks do not compare them, and with another definition on a node the ranges of the diff, built in the order of one node, can miss rows of the other node. Other columns may have other types and collations on the nodes.
- The diff builds the ranges to compare on the server, with the type and the collation of each key column, so the ranges follow the order of the primary key index for every key type whose cast from `text` gives the same value as its input function. Key columns of type `oid` or of a reg* type are not covered: ACE does not match catalog objects between nodes (see [the note on catalog objects](../../index.md)). If a key type has a user-defined cast from `text` (`CREATE CAST`) that gives another value, the bounds of the ranges can change, and the diff can miss differences; use `table-diff` for such tables. The rows of two nodes are paired by the text of the key: `1.0` and `1.00` of a `numeric` key are two keys, each on one node only. See [Primary Key Types and the Postgres Type Boundary](../../design/merkle.md#primary-key-types-and-the-postgres-type-boundary).
- If the collation of a primary key column differs from the collation of its type (a `COLLATE` clause on the column, also `COLLATE "default"` on a column of a domain that has its own collation), mtree can count a change for a wrong leaf. That leaf keeps an old hash, and a later diff can miss the change. Use `table-diff` for such tables.
- When *all* mismatched blocks between a node pair turn out to hold no row
  differences, the tree hashes were stale rather than the data divergent. The
  diff reports this, refreshes those blocks from live data (recorded in the
  summary's `stale_blocks_refreshed`), and the next diff runs clean. A pair
  with any real differences, a diff that reached `max_diff_rows`, or an
  `--until` filter is not refreshed. If the trees still disagree after a refresh, the nodes'
  leaf ranges have diverged; run `ace mtree build` to realign them.
