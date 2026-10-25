# Changelog

All notable changes to ACE will be captured in this document. This project follows semantic versioning; the latest changes appear first.

## [Unreleased]

### Changed
- **A numeric value in a diff report has no trailing zeros.** The row fetch
  now reads a numeric column with `trim_scale()`, as the row hash does, so
  the report shows `7.5`, not `7.50`. Repair writes this form: a `numeric`
  column without a fixed scale gets the value without the trailing zeros on
  the target. A column with a fixed scale, such as `numeric(10,2)`, is not
  affected.

### Fixed
- **A numeric primary key stored with another scale on two nodes was
  reported as two rows.** The diff reported the row `1.50` on one node and
  the row `1.5` on the other as a row missing on each node, so a repair plan
  applied the rules for missing rows to them, for example a `delete` rule.
  Now they are one row.

## [v3.0.0]

> **⚠️ This release has breaking changes. Read Breaking Change and
> Upgrading below before you upgrade.** In short:
> - Upgrade every host that runs ACE at the same time, before any of them
>   runs `mtree update` or `mtree table-diff`. The first update moves each
>   Merkle tree to a new hash format, and ACE 2.1.x can write leaves into
>   the new format without an error, which can hide differences.
> - Going back to 2.1.x after that needs `mtree build` of each tree.
> - `mtree` now refuses partitioned tables and inheritance parents. Earlier
>   versions could build a tree on one only in rare cases, and the tree broke
>   at the first change to a partition or child, so few installs have one.
> - Connections fail if a connection pooler does not pass ACE's session
>   settings through to the server.
> - Windows and Debian bullseye are no longer supported.

### ⚠️ Breaking Change
- **Merkle trees move from hash version 2 to hash version 4, and an earlier
  ACE cannot use them after that.** The hash version is an internal number
  of the Merkle tree format, not an ACE release: it is stored with each
  tree, on each node, and names the algorithm of every leaf hash in it.
  ACE 2.1.x writes version 2. This release changes the leaf hash twice, and
  both changes ship together (version 3 was never released):
  - Version 3: every ACE connection fixes the session settings that change
    how a value is printed (`TimeZone`, `DateStyle`, `IntervalStyle`,
    `extra_float_digits`, `bytea_output`, `lc_monetary`). With version 2,
    equal rows with a `timestamptz`, `money`, `float` or `interval` column
    could hash differently on nodes with different settings, and the diff
    reported rows that were equal.
  - Version 4: each row is hashed on its own with `sha256()`, and a leaf
    combines the row hashes with a count and four sums instead of joining
    the text of all its rows. Version 2 joined the column values with `|`
    and printed NULL as an empty string, so a NULL and an empty string, or
    `('a|b', 'c')` and `('a', 'b|c')`, gave the same leaf hash and
    `mtree table-diff` reported the nodes as equal. Its single string per
    leaf also failed with "out of memory" past 1 GB. Version 4 also gives
    the first leaf no lower bound: with version 2, rows that were already
    on another node at build time, with a key below the smallest key of
    the reference node, were in no leaf and were never compared.

  A version 2 tree goes to version 4 on its first `mtree update`, or on
  the first `mtree table-diff`, which runs an update first. That update
  marks every leaf, hashes the whole table again on each node and writes
  version 4, in one transaction per node; no rebuild is needed, and the
  leaf bounds stay as they are.

  The upgrade is one way. The tree lives in the database, not on the host
  that runs ACE, so any host with ACE and access to the cluster can update
  it: a scheduler, an API server, a cron job. ACE 2.1.x upgrades a tree
  whose version is older than its own, but accepts a newer one as it is.
  If a 2.1.x host updates a version 4 tree, it recomputes the changed
  leaves with the version 2 algorithm and leaves the version at 4, without
  an error. Those leaves bring back the false matches above, and no later
  run recomputes them, because the version says they are current. This
  release refuses a tree with a version newer than its own and changes
  nothing on any node, so this cannot happen again in later upgrades. To
  go back to 2.1.x, rebuild each tree with `mtree build` of 2.1.x. See
  Upgrading below, and the block hash and first block entries under Fixed.
- **Windows is no longer supported.** ACE now supports Linux and macOS
  only, and the release has no Windows archives. ACE protects its reports
  with POSIX file modes (see Security below), and on Windows these do not
  stop other local users from reading the table rows in them, so the build
  now stops on Windows. On a Windows host, run ACE in WSL or in the
  container image, with its output directory on the Linux file system.
  Reports written by earlier Windows builds keep their Windows ACLs after a
  move to WSL: delete them, or restrict their ACLs by hand.
- **Debian bullseye packages are no longer built.** Packages are built for
  Debian bookworm and trixie, and Ubuntu jammy, noble and resolute.
- **Connections fail when a session setting does not reach the server.**
  ACE sends fixed values for `TimeZone`, `DateStyle`, `IntervalStyle`,
  `standard_conforming_strings`, `client_encoding` and other settings when
  it connects. If the server reports another value, for example because a
  connection pooler dropped the parameter, the connection fails with an
  error that names the setting. See the session defaults entry under Fixed.
- **`mtree` refuses partitioned tables and tables with inheritance
  children.** This takes away nothing that kept working. Earlier versions
  built a tree on such a table only in rare cases, and the tree did not
  stay current:
  - A partitioned table stores no rows itself, and autovacuum never
    analyzes it, so its row estimate was 0 and `mtree build` failed with
    "0 rows on all nodes; add data". It built only if someone had run
    `ANALYZE` on the parent by hand.
  - An inheritance parent usually built only if it held rows of its own,
    not just in its children.
  - Once built, the tree broke at the first change to a partition or a
    child: CDC reports the change under the name of the partition or
    child, which has no tree, so the CDC drain failed, and with it
    `mtree update` and `mtree table-diff` for every table on that node.

  Build a tree on each partition instead; CDC reports its changes under
  its own name, so it stays current. `table-diff` still compares the whole
  partitioned table. A tree left by an earlier version must be removed
  with `mtree teardown-table`; see Upgrading below.
- **A role with `SELECT` on only some columns of a table is refused.** It
  used to compare only the columns it could read. See the catalog entry
  under Changed.
- **Output and defaults that scripts may depend on:**
  - `mtree table-diff` exits with an error when part of the comparison
    failed. It used to report "TABLES MATCH" and exit 0.
  - The `total_rows_checked` summary field is the exact number of rows of
    the anchor node that match the filter, not the planner estimate for
    the whole table.
  - The HTML report shows at most `max_html_rows` rows per node pair,
    10,000 by default. The JSON report still has every row.
  - `--max-cpu-ratio` starts half as many workers on `mtree build` and
    `mtree update` as before, for the same value.
  - ACE connections set `max_parallel_workers_per_gather = 0`, so the
    server no longer adds parallel workers to ACE's queries.
  - `table-diff --ensure-pgcrypto` has no effect and prints a warning.

### Upgrading
1. Upgrade every host that runs ACE, including the scheduler and the API
   server, before any of them runs `mtree update` or `mtree table-diff`
   with the new version. An ACE 2.1.x left running can write old leaves
   into a tree that the new version has upgraded.
2. If ACE connects through a connection pooler, check that the pooler
   passes the session parameters through, for example with a `table-diff`
   on a small table. If it does not, connections fail and the error names
   the setting.
3. If you have a Merkle tree on a partitioned table or on a table with
   inheritance children, remove it with `mtree teardown-table` of the new
   version, which also takes the children out of the CDC publication; the
   `teardown-table` of 2.1.x left an inheritance parent in it. Most
   installs have none: earlier versions built such a tree only in rare
   cases (see Breaking Change), and if you have one, `mtree update` has
   most likely been failing on that node since the first change to a
   partition or child. Build trees on the partitions instead, or use
   `table-diff` for the whole table.
4. Plan the first `mtree update` or `mtree table-diff` of each tree. It
   hashes the whole table on each node in one transaction, which takes
   about as long as the hashing part of `mtree build`. Trees keep their
   leaf bounds; rebuild a tree with `mtree build` if you want leaves of at
   most `block_size` rows.
5. Run `table-diff` again before `table-repair` with a diff file made by
   an earlier version if the table has `money` columns.
6. Check scripts and jobs that read the exit code of `mtree table-diff`,
   the `total_rows_checked` field, or the HTML report, or that set
   `--max-cpu-ratio`.
7. Grant `SELECT` on the whole table, and `USAGE` on its schema, to roles
   that ACE runs as.
8. Remove the leftover `#` operator before the next Spock `add_node`, as
   described in the `bytea_xor` entry under Fixed.

To roll back to an earlier ACE, rebuild each Merkle tree with `mtree build`
of that version, and recreate the `#` operator as described in the
`bytea_xor` entry.

### Added
- `--max-connections` / `-M` on `mtree build`, `mtree update`, and
  `mtree table-diff`, with `mtree.max_connections` in `ace.yaml` and
  `max_connections` on the matching HTTP API endpoints. It caps the
  connection pool per node the same way the flag already does on the diff
  commands, for when ACE runs on a host with many more cores than the
  database servers. One connection holds the tree's transaction, so `N`
  connections means `N - 1` hash workers on `mtree update` and
  `mtree table-diff`. `mtree build` also runs cutting workers on the
  reference node out of the same pool, so they count against the cap and a
  build needs at least 3.
- The mtree commands log the worker count they settle on, along with the
  host CPU count and the flags it was derived from.
- **`schema-diff --compare=structure` checks table definitions directly,
  without reading any data.** This new mode checks column type, `NOT NULL`,
  identity, generated, storage options, default, and collation; the replica
  identity key and its operator classes; `PRIMARY KEY`, `UNIQUE`, `CHECK`,
  `FOREIGN KEY`, and `EXCLUDE` constraints; partition bounds; and the full
  definition of every domain, range, composite, and enum type used by a
  compared column, not just its name. Types are matched by name and
  definition, never by OID, because two nodes set up on their own can give
  the same user-defined type a different OID. Each difference found gets one
  of five ranks (`cosmetic`, `equivalent-differing`, `narrowed`,
  `incompatible`, `absent`), and the process exits with the code of the
  single worst rank found (`0`, `16`, `32`, `48`, or `64`). `--skip-tables`
  and `--skip-file` work the same way as they do for the default data diff.
  `--output=json` prints a structured report instead of text, but only when
  `--output` is given by hand on the command line, since `json` is also
  `--output`'s default value for every other mode. Not yet supported:
  `--schedule` and `--output=html`. Not checked by this mode:
  non-constraint indexes, triggers, rules, sequences, views, materialized
  views, storage parameters, column order, comments, and ACLs. See the
  `schema-diff` command docs for the full list of findings and exit codes.
- **`table-diff` says what differs when the table definitions do not
  match.** It used to say only that the table schemas do not match between
  nodes. It now runs the same comparison as `--compare=structure` on the
  two nodes and prints the differences, naming the nodes and columns. If
  that comparison finds nothing or cannot connect, it prints the column and
  key lists of both nodes.
- **Foreign tables, views, and partitioned tables with foreign partitions
  are refused with a clear message.** `table-diff`, `table-repair`, and
  `mtree` previously failed on these with "no primary key found", or in the
  case of an inheritance parent with a foreign child, hashed the foreign rows
  and then failed on a system column. ACE now reads the table's partitions and
  children in one catalog query and says what the relation is and why it
  cannot be compared. Other relations that are not tables, such as a
  sequence or an index, get a message that names what they are. A view
  with an underscore-prefixed table of the same name beside it gets a
  message mentioning that table, since it may be the table behind the view
  (the coldfront tiered layout). `schema-diff` and `repset-diff` list the
  foreign tables and views they skip. `repset-diff` also leaves out any
  other relation it cannot compare, such as a partitioned table with a
  foreign partition, instead of failing the run on it; when one node
  refuses a relation, it is left out on every node, with a warning that
  names the node and the reason. Comparing the heap parts of a partitioned
  table that has foreign partitions is future work.

### Changed
- **ACE reads table metadata from the system catalogs, so a read-only role
  can run it.** The `information_schema` views show only objects the user
  has privileges on, and `table_constraints` ignores `SELECT`, so a role
  that could only read a table saw no primary key: `table-diff` and `mtree`
  failed with "no primary key found". ACE now reads `pg_class`,
  `pg_attribute` and `pg_constraint`, which show every object. With this:
  - `table-diff`, `mtree` and `table-repair` check `USAGE` on the schema
    and say when it is missing, instead of failing with a raw "permission
    denied for schema" error.
  - A role with `SELECT` on only some columns of a table is refused with a
    message that says so. Before, the columns it could not read were left
    out of the comparison without a warning.
  - `schema-diff` and `repset-diff` list the tables the role cannot read as
    skipped, naming the node, the same way as foreign tables and views.
    `schema-diff` used to leave these tables out without a word. A run that
    compares nothing because the role can read none of its tables fails.
- **The columns of a composite primary key are read in the order of the
  constraint.** ACE read them from `information_schema.key_column_usage`
  without an `ORDER BY`, so their order was not guaranteed, although
  PostgreSQL returned them in the order of the constraint in practice. ACE
  builds its `ROW(...)` comparisons and `ORDER BY` from that list, and in
  another order those queries could not use the primary key index. No
  wrong or slow result from this has been seen; the order is now
  guaranteed, and the key is read from `pg_constraint`.
- **`table-diff` no longer refuses a table with a `bytea` value larger than
  1 MB.** Before each run it read the longest value of every `bytea`
  column on every node, which read the whole table, and refused to run if
  one was over 1,000,000 bytes. The check was not needed: rows reach ACE
  only for blocks whose hashes differ.
- **The reference config in the repository is now `ace.sample.yaml`.** The
  tracked `ace.yaml` made `ace config init --path ace.yaml` fail in a
  checkout with "config file already exists". `ace.sample.yaml` is the same
  file that `ace config init` writes, which now also includes
  `max_connections` and the `adaptive_drain_*` settings. The example had
  `concurrency_factor: 1`; the default is `0.5`. Running `./ace` from a
  fresh checkout now needs `ace config init` first. The container still
  reads `/etc/ace/ace.yaml`.

### Fixed
- **`mtree table-diff` failed on a table with a `uuid` primary key** as soon
  as the nodes had a different row, with `invalid input syntax for type
  uuid`. It sent the Go rendering of the key back to the server instead of
  the key itself.
- **`mtree table-diff` missed rows on tables with a `uuid` or `bytea`
  primary key.** It sorted the bounds of the blocks that differ in Go, in an
  order that did not match the server's, and a block whose bounds came out
  reversed selected no rows. In one test it reported 6 of 9 differing rows,
  with no error. These keys are now sorted the way the server sorts them,
  and so is `NaN` in a float key. A `text` key with a collation other than
  `C`, and `enum` or `citext` keys, can still sort differently, and keys
  such as `numeric` are not ordered correctly at all; use `table-diff` for
  those tables. The `mtree table-diff` docs list the key types.
- **`table-diff` and `mtree table-diff` could miss a row with a composite
  key that holds the delimiter `|`.** The keys `('a|b', 'c')` and
  `('a', 'b|c')` were joined into the same text, so the two rows were taken
  for one and a change between them was not reported. `mtree` also dropped
  one of two block bounds that printed the same way, such as
  `('a b', 'c')` and `('a', 'b c')`, which could leave rows out of the
  comparison.
- **`mtree table-diff` reported "TABLES MATCH" and exited 0 when part of
  the comparison failed.** A worker that failed to compare a range logged
  the error and dropped the range. Now the summary lists the node pairs
  that were not fully compared and the command fails. The summary file is
  still written.
- **`mtree update` failed on every run once it had to split the last block
  of a table whose key column has upper-case letters**, such as `"UserId"`.
  The split queries did not quote the column name.
- **`mtree` said "0 rows on all nodes; add data" for a partitioned table that
  has data.** A partitioned table has no storage, so its row estimate was
  empty, and a tree on it would not stay current anyway: CDC reports changes
  under the name of the partition. `mtree` now refuses a partitioned table
  with a message that names its partitions; build a tree on each partition,
  or use `table-diff` for the whole table. `mtree` also refuses a table with
  child tables through inheritance, for the same reason.
  An earlier version could build a tree on such a table if the table was
  analyzed. Then a change in a partition or a child makes the CDC drain fail,
  and `mtree update` and `mtree table-diff` fail for every table on that
  node. Remove such a tree with `mtree teardown-table`.
- **`mtree teardown-table` left an inheritance parent in the CDC
  publication.** It dropped the table from the publication without `ONLY`,
  so PostgreSQL also tried to drop the child tables and failed on a child
  that was not in the publication; ACE then logged that the publication was
  not found. Teardown now drops the table with `ONLY`, and also takes out a
  child that is in the publication but has no tree of its own, as an
  earlier ACE added the children of a parent along with it. A child with
  its own tree stays.
- **A change to a key on the bound between two Merkle tree leaves was
  counted in both leaves.** CDC counters and leaf row counts treated
  `range_end` as part of the leaf, while leaf hashes did not. A key equal
  to `range_end` went to two leaves: both were marked dirty, and the
  insert and delete counters that drive split and merge grew twice. Now
  `range_end` is exclusive everywhere, and such a key goes to the next
  leaf only. Diff results did not change.
- **`mtree build` made leaves much larger than `block_size`.** It took the
  leaf bounds from the same kind of sample as `table-diff`. It now cuts the
  leaves with the same code, so a new leaf holds at most `block_size` rows
  on the reference node, and it hashes the leaves on all nodes at the same
  time instead of one node after another. On a 100M-row table the build
  took about half the time.
- **`table-diff` made blocks much larger than `block_size`.** The block
  bounds came from a `TABLESAMPLE` of the key split with
  `ntile(rows / block_size)`. On large tables the sample had fewer rows than
  buckets, and `SYSTEM` sampling takes whole pages, so with a key in
  physical order a block could hold a million rows. Such a block could hit
  the 60-second hash timeout and stop the diff, and it made the workers
  uneven. Now ACE walks the primary-key index on the anchor node and starts
  a new block every `block_size` rows, so a block holds at most
  `block_size` rows there when it is cut. Several workers cut the table at
  the same time, each in its own key range, and hashing starts with the
  first block. The key ranges come from the histogram of the first key
  column, or from a small sample if there is no histogram. On a 100M-row
  table with a key in physical order `table-diff` ran 5 to 9 times faster.
  The log shows how ACE built the blocks. The summary field
  `total_rows_checked` is now the exact number of rows of the anchor node
  that match the filter, counted by the cut; before, it was the planner
  estimate of the whole table.
- **`table-diff` reported a match when the anchor node had no rows.** It
  built no blocks, so rows of the other nodes were never compared. Now the
  whole key space is one open block in this case.
- **Every ACE connection now sets `max_parallel_workers_per_gather = 0`**
  (`postgres.max_parallel_workers_per_gather` in `ace.yaml`; `-1` keeps the
  server setting). ACE runs its hash queries from many workers already, and
  without this Postgres could add parallel workers to each of them.
- **Equal rows could have different hashes on nodes with different session
  defaults.** table-diff and mtree hash the text that the server prints for
  a row, and the CDC walsender prints the values of each change as text.
  This text depends on `TimeZone`, `DateStyle`, `IntervalStyle`,
  `extra_float_digits`, `bytea_output` and `lc_monetary`. ACE used the
  defaults of each server, so, for example, a `timestamptz` column on two
  nodes with different `TimeZone` gave a false difference. ACE also read
  text in the encoding of each database, while it expects UTF-8.
  Every ACE connection, including the replication connection, now sends
  fixed values for these settings when it connects: `TimeZone = UTC`,
  `DateStyle = 'ISO, MDY'`, `IntervalStyle = postgres`,
  `extra_float_digits = 3`, `bytea_output = hex`, `lc_monetary = C`,
  `standard_conforming_strings = on` and `client_encoding = UTF8`. They
  have priority over `ALTER ROLE ... SET`, `ALTER DATABASE ... SET` and
  `PGOPTIONS`. If the server reports another value for `TimeZone`,
  `DateStyle`, `IntervalStyle`, `standard_conforming_strings` or
  `client_encoding` (for example, because a connection pooler dropped the
  parameter), the connection fails with an error that names the setting.
  This change raised the internal Merkle tree hash version to 3, which was
  never released; this release ships version 4 (see Breaking Change), and
  the next `mtree update` recomputes all stored hashes of a tree once.
  A diff file made by an earlier version of ACE can hold `money` values in
  the format of the server's `lc_monetary`. Run `table-diff` again before
  `table-repair` with such a file.
- **Block hashes failed with "out of memory" (SQLSTATE 54000) on large blocks,
  and some different rows gave the same hash.**
  The hash of an mtree leaf or a `table-diff` block joined the text of all its
  rows into one string. When a block held a few million rows, this string grew
  past the 1 GB limit that Postgres has for one value. The text also lost
  information: a NULL and an empty string gave the same text, and so did
  `('a|b', 'c')` and `('a', 'b|c')`, so the diff did not see these
  differences.
  Each row is now hashed separately: the `sha256()` of the `ROW(...)::text`
  form of the row, converted to UTF8. The block hash is built from the row
  count and from the sums of the four 64-bit words of the row hashes, so its
  memory does not grow with the number or the width of the rows. It does not
  depend on the order of the rows, and a row that appears twice counts twice:
  in an inheritance tree, the parent and a child can hold rows with the same
  key, and a XOR of the row hashes would cancel two equal rows. The row form
  keeps NULL, empty strings and delimiters apart. Numeric values still hash
  without trailing zeros. The hash does not depend on the database encoding
  or on the byte order of the server.
  The internal Merkle tree hash version is now 4: the next `mtree update`
  computes all stored leaf hashes again, in one transaction per node.
  `mtree update` and `mtree table-diff` now refuse a tree with a hash version
  newer than the ACE that runs them, and change nothing on any node. Such a
  tree was written by a newer ACE, and an older algorithm must not recompute
  its leaves. ACE versions released before this one do not have this check.
  To go back to one of them after `mtree update` has moved a tree to
  version 4, rebuild the tree with `mtree build` of that version first.
- **`table-diff` and `mtree` failed on tables with a `numeric[]` column.**
  ACE wrapped such a column in `trim_scale()`, which has no array variant.
- **`table-diff` and `mtree` no longer need pgcrypto.** The block hash uses the
  built-in `sha256()`. `table-diff --ensure-pgcrypto` has no effect now and
  only prints a warning.
- **Merkle tree diff did not compare rows with keys below the smallest key of
  the reference node.** The tree takes its block bounds from one node, and the
  first block started at the smallest key on that node. Rows on other nodes
  with a smaller key were in no block, so no block hash covered them and the
  diff reported the nodes as equal. Rows inserted after the build were
  compared correctly. The first block now has no lower bound, in the same way
  as the last block has no upper bound. Hash version 4 (see above) also
  covers this change, so a tree built by an earlier version computes all its
  block hashes again on the next `mtree update` or `mtree table-diff`.
- **`mtree table-diff` opened a separate connection pool for every compare
  worker and every node pair, so a per-node connection cap did not hold.**
  With `W` workers on a three-node cluster, one node could see `W + 2` pools,
  each as large as the cap. The diff now opens one pool per node and shares
  it across the tree traversal, the range comparison, and the stale-block
  refresh, so `--max-connections` is the most connections ACE holds on a
  node during a diff. Without a cap the pool is one connection per worker
  plus one.
- **`--max-cpu-ratio` on `mtree build` and `mtree update` started twice as
  many workers as the ratio implied.** The build and update paths multiplied
  the ratio by two, so `-m 0.1` on a 14-core host ran 3 hash queries per node
  at once instead of 1. The ratio now means the same thing on every mtree
  command: that share of the CPUs on the machine running ACE, and never fewer
  than one worker. The default of `0.5` therefore runs half as many workers
  for `mtree build` and `mtree update` as before.
- **ACE's hash queries could each fan out into Postgres parallel workers,
  multiplying the load on the database server.** ACE now sends
  `max_parallel_workers_per_gather = 0` as a session setting on every
  connection, so this covers `table-diff`, `repset-diff`, `schema-diff` and
  `table-repair` as well as the mtree commands. Set `postgres.max_parallel_workers_per_gather` in `ace.yaml` to
  allow them again, or to `-1` to leave the server's own setting in place.
- **`table-diff --output html` was killed by the OOM killer on large diffs.**
  The HTML writer built the whole report in memory: a copy of the diff as
  JSON, the markup of every row, and the final document in one buffer. With
  several kilobytes of markup per row, a diff of 493,200 rows used about 4 GB
  before the process was killed.
  The writer now streams the report to disk one row at a time. It also shows
  at most `max_html_rows` rows for each node pair (default `10000`, set in
  `table_diff` and `mtree.diff` in `ace.yaml`, or with `--max-html-rows`),
  because a report with hundreds of thousands of rows is too large for a
  browser anyway. A truncated report says so at the top and in each node pair
  section. A repair plan built in it has rules only for the rows shown and
  `default_action: skip`, so `table-repair` does not change the rows that the
  report did not show; the YAML starts with a comment that says this. With
  three or more nodes this holds per key only: a plan rule is not tied to a
  node pair, so it also acts on a key that the report hides in another pair.
  The JSON report is not changed and always contains every row.
- **HTML report: repair plans named the wrong rows for some primary keys.**
  The page script read keys as JavaScript numbers, so a bigint above 2^53
  could name the neighbouring row, and a text key such as `"007"` became the
  number 7 and matched nothing. A text key with the character U+0085 also
  named the wrong row: YAML reads it as a line break and turned it into a
  space, and it did the same in the text of a custom row. The plan now uses
  the keys exactly as the diff file has them, and writes U+0085, U+2028 and
  U+2029, which YAML also reads as line breaks, as `\u` escapes.
- **HTML report: repair plans used node names where `table-repair` expects
  `n1` or `n2`.** In `apply_from`, `coalesce_priority` and the
  `pick_freshest` tie, `n1` and `n2` mean the first and second node of each
  pair, but the plan wrote the node names. With nodes named anything else,
  `table-repair` rejected the plan, and for a pair such as `n2/n3` a node name
  could be read as the wrong node of the pair.
- **HTML report: a repair plan built from selected rows also repaired the
  rows that were not selected.** Its `default_action` was `keep_n1`, and
  `table-repair` rejected the whole plan when an unselected row was missing
  on the first node of its pair. The plan now has `default_action: skip`, so
  `table-repair` changes only the selected rows.
- **HTML report: the action chosen for a row was lost when its key held a
  quote or a backslash.** The page looked up the row's control with the raw
  key in a CSS selector. A quote made building the plan fail, and a backslash
  matched no control, so the plan used the default action instead.
- **HTML report: rows with primary keys that mix numbers and text were sorted
  in a different order on each run.** The key comparison now puts numbers
  before text, so the order is stable.
- **Spock `add_node` failed on clusters where `mtree init` had been run.**
  `mtree init` created a `#` operator on `bytea_xor` without a schema name, so
  it landed in `public` while the function stayed in the ACE schema. Spock's
  node join dumps the source with `pg_dump --exclude-schema=pgedge_ace`, which
  kept the operator but dropped the function, so the restore failed with
  `schema "pgedge_ace" does not exist` and the new node never finished joining.
  ACE now calls `bytea_xor` directly and no longer creates the operator.
  Clusters initialized by an earlier version still have the operator.

  **Upgrading:** the leftover operator does not affect ACE; existing Merkle
  trees keep working after the upgrade. It only matters for Spock `add_node`,
  which fails while the operator exists. Once every host running ACE has been
  upgraded (earlier versions still use the operator), run this on each node to
  list the command that drops it:

  ```sql
  SELECT format('DROP OPERATOR %I.%s(%s, %s);', n.nspname, o.oprname,
                o.oprleft::regtype, o.oprright::regtype)
  FROM pg_operator o
  JOIN pg_namespace n ON n.oid = o.oprnamespace
  WHERE o.oprname = '#'
    AND o.oprcode = to_regprocedure('pgedge_ace.bytea_xor(bytea,bytea)');
  ```

  Run each command it prints as the operator's owner or a superuser. No rows
  means the node is ready for `add_node`. Replace `pgedge_ace` if you set a
  different `mtree.schema`. If you later roll back to an earlier ACE, recreate
  the operator on each node with
  `CREATE OPERATOR public.# (LEFTARG = bytea, RIGHTARG = bytea, PROCEDURE = pgedge_ace.bytea_xor);`

  `ace mtree init` also drops the operator on the nodes it runs on, but
  re-running init resets change tracking: changes not yet applied to the
  Merkle trees are lost. Don't re-run it just to remove the operator unless
  you plan to rebuild the trees.

### Security
- **Reports and the task database were readable by every local user.** ACE
  created diff, repair, dry-run and HTML reports, stale-skip logs, the
  `reports/` directories and `ace_tasks.db` with the mode the umask allowed,
  usually 0644, and the reports hold rows copied from the compared tables.
  ACE now sets 0600 on these files and 0700 on the directories, also on a
  file left by an earlier version when it writes it again. Files written by
  earlier versions keep their mode until then; restrict them by hand.
- Updated Go dependencies to address recent CVEs.
- **ACE is built with Go 1.26.10.** ACE 2.1.1 was built with Go
  1.26.5. The newer Go has security fixes to `crypto/tls`, `net/http`,
  `net/url`, `encoding/asn1` and `os`, and makes `crypto/x509` reject
  certificate chains through the compromised eMudhra intermediate CAs
  (CVE-2026-111943).

## [v2.1.1]

### Added
- **`repair` can now resolve `pick_freshest` by spock `commit_ts`
  (latest-commit-wins).** `pick_freshest` previously only compared ordinary data
  columns (e.g. `updated_at`); keying it on `commit_ts` silently fell back to
  `tie` because that field is held in the diff's spock metadata, not the row. It
  now resolves the key from row data or metadata, so a plan can keep the side
  with the newer commit timestamp. See the latest-commit-wins repair example.

### Changed
- **`mtree build` on an empty table now fails with a clear message.** A table
  with 0 rows on every node previously errored with a misleading "could not
  determine a reference node" (as if row-estimate collection had failed). It now
  reports that the table is empty and to add data before building, and correctly
  distinguishes an all-empty table from an actual estimate failure.

### Fixed
- **`mtree table-diff` could report divergent nodes as identical (silent
  false-negative).** Leaf hashing used a closed (`<=`) upper bound, so rows on
  block boundaries were hashed into two adjacent leaves; the XOR-based parent
  hash then cancelled the duplicate siblings, letting the same primary key with
  *different* non-key data produce matching root hashes — the core UPDATE-UPDATE
  conflict active-active clusters produce. Leaf hashing now uses an exclusive
  (`<`) upper bound, so each row belongs to exactly one leaf. **Merkle trees
  built before this fix must be rebuilt (`mtree build`).**

## [v2.1.0]

This release focuses primarily on Merkle tree functionality.

### Added
- `--version` / `-V` flag to print the ACE version. Works without an
  `ace.yaml` present.
- `max_diff_rows` cap for `mtree table-diff` (mtree → diff, default 1000000,
  0 = unbounded). Previously the Merkle-tree engine collected every differing
  row in memory, so a heavily diverged table could OOM the process. A capped
  run is flagged in the report and warned about, matching `table-diff`.
- `mtree table-diff` and `mtree update` can now run while `mtree listen` is
  active. Nodes whose replication slot is busy skip the CDC refresh
  (best-effort, comparing against the tree the listener keeps warm) instead of
  failing with SQLSTATE 55006; skipped nodes are listed in the diff report
  under `cdc_skipped_nodes`.
- `--cdc-timeout` flag on `mtree update` and `mtree table-diff` to override the
  CDC drain budget per invocation.
- New configuration keys under mtree → cdc: `cdc_flush_batch_size`,
  `adaptive_drain_fraction`, and `adaptive_drain_min_changes`.

### Changed
- **CDC drain now absorbs large transactions instead of forcing a rebuild.** A
  bounded `mtree update` / `mtree table-diff` drain applies changes in
  sub-batches (`cdc_flush_batch_size`, default 10000) and buffers only primary
  keys, so memory scales with key size — not row width — regardless of
  transaction size. The previous hard `MaxBufferedChanges` cap (which erred and
  recommended a rebuild) is removed.
- **Heavily-updated tables escalate to a bulk rehash.** When a bounded drain
  decodes more UPDATEs for a table than
  `max(adaptive_drain_min_changes, adaptive_drain_fraction × row estimate)`
  (defaults 1000 / 0.01), all of the table's blocks are marked dirty once and
  rehashed in bulk instead of tracked per change, keeping drain time flat
  rather than linear in the backlog (measured 16.85s → 2.13s for a 250k-row
  backlog on a 2M-row table). Correctness is unaffected — hashes are always
  recomputed from live table data. Set `adaptive_drain_fraction: -1` to
  disable.
- `mtree update` / `mtree table-diff` drain CDC streams for all nodes
  concurrently and stop as soon as the server confirms the target LSN, instead
  of sequentially with a fixed ~1s idle timeout per node (measured 3.4s → 1.2s
  for a no-change diff on a 3-node cluster).
- `table-diff` applies `max_diff_rows` per node pair instead of as one budget
  shared across all pairs. On clusters with more than two nodes, one pair's
  divergence no longer starves the others; note the worst-case report size is
  now `max_diff_rows × number of pairs`.
- Bounded drains apply synchronously so the slot's confirmed LSN never advances
  ahead of durably-applied work; a crash mid-drain is recovered by re-streaming
  the in-flight transaction on the next run.
- `cdc_processing_timeout` default raised from 30s to **300s** (a timeout now
  means "re-run or raise", since drain progress is durable).

### Fixed
- **`mtree table-diff` could report divergent nodes as identical (silent
  false-negative).** Leaf hashing used a closed (`<=`) upper bound, so rows on
  block boundaries were hashed into two adjacent leaves; the XOR-based parent
  hash then cancelled the duplicate siblings, letting the same primary key with
  *different* non-key data produce matching root hashes — the core UPDATE-UPDATE
  conflict active-active clusters produce. Leaf hashing now uses an exclusive
  (`<`) upper bound, so each row belongs to exactly one leaf. **Merkle trees
  built before this fix must be rebuilt (`mtree build`).**
- **A bounded CDC drain could silently under-report divergence.** The drain
  treated a 1-second idle timeout as "caught up", so a node with a large
  backlog could leave its Merkle tree stale and the diff would report the
  tables as identical despite millions of differing rows. The drain now stops
  only once it has verifiably reached the target LSN, and fails with an
  actionable error otherwise.
- Merkle-tree diffs missed rows lying beyond the reference node's largest
  primary key (the open-ended tail range was never queried). A subsequent
  `table-repair` could then delete rows instead of converging the nodes.
- Phantom "trees differ … TABLES MATCH" results after clean replication. The
  drain target could sit beyond the WAL flush pointer under asynchronous
  commit (as used by spock apply), leaving subscriber trees stale behind
  already-visible data, and a drain could stop mid-transaction and skip it
  permanently. Both causes are fixed, and diffs now self-heal stale leaves by
  rehashing them from live data (reported as `stale_blocks_refreshed` in the
  summary) instead of leaving a contradictory verdict.
- Diff reports produced by the Merkle-tree engine are now repairable for the
  same column types as the classic engine. Time, uuid, interval, money,
  geometric, network, range, bit, enum, and xml values previously reached the
  report as raw driver structs or Go-formatted text, aborting the entire
  repair batch on the first unparseable value. Both engines now fetch and
  normalise values identically (as PostgreSQL text forms).
- `spock-diff` failed outright against spock clusters (OID scan errors, NULL
  replication-set names) and matched reciprocal subscriptions by
  reconstructing default names. Subscriptions are now matched by origin node,
  so renamed subscriptions compare correctly.
- `mtree init` created the replication slot before the publication commit was
  durable, which could later fail `mtree table-diff` with `publication
  "ace_mtree_pub" does not exist` (SQLSTATE 42704). Init now orders the
  publication strictly before the slot. Previously-broken installs are
  repaired with `mtree teardown` + `init` + `build`.
- ACE connections now disable spock DDL replication and repset auto-add, so
  ACE's per-node metadata objects no longer replicate to peer nodes (which
  caused cross-node races and leaked `ace_cdc_metadata` into default
  replication sets). No effect on vanilla PostgreSQL.
- `mtree build` could hang instead of reporting a per-node error, and leaked
  connection pools when it aborted early.
- `mtree table-diff` / `mtree update` on a table whose tree was never built
  now fail fast with a message pointing at `ace mtree build`, instead of
  draining the replication stream and erroring with "no rows in result set".
- Clusters upgraded from older versions whose `ace_cdc_metadata` table predates
  the `pub_commit_lsn` column no longer fail at CDC startup; ACE warns and
  continues, and the next `mtree init` adds the column.
- A bounded drain now fails with a clear, actionable error when a tracked
  table's `REPLICA IDENTITY` exposes no primary key, instead of panicking.

## [v2.0.0] - 2026-04-22

### Added
- **Native PostgreSQL support.** `table-diff`, `table-repair`, and Merkle tree
  operations now work on vanilla PostgreSQL (14+) without the spock extension.
  ACE auto-detects whether spock is installed on each node and branches
  accordingly; `spock-diff` and `repset-diff` return a clear error when spock
  is not present.
- Native PG alternatives for replication origin and slot LSN queries using
  `pg_subscription` and `pg_replication_origin` catalog tables.
- `--against-origin` now works on native PG logical replication setups by
  resolving replication origin IDs to subscription names.
- Integration test suite for native PostgreSQL covering table-diff, table-repair
  (unidirectional, bidirectional, fix-nulls, dry-run), Merkle tree operations,
  and origin-tracked replication with repair.

### ⚠️ Breaking Change
- **Default `mtree.schema` changed from `"spock"` to `"pgedge_ace"`.** If you have
  been using Merkle tree operations (`mtree build`, `mtree diff`, continuous CDC)
  in prior versions, your existing metadata tables live in the `spock` schema. To
  preserve compatibility, set `schema: "spock"` in the following locations:
  - `ace.yaml` → `mtree.schema`
  - `visualise.sh` → `-S spock` flag (or edit the default in the script)

### Changed
- ACE schema name in SQL templates is now quoted with `pgx.Identifier.Sanitize()`
  to prevent SQL breakage with non-simple schema names (e.g., mixed case,
  hyphens).
- `spock.xact_commit_timestamp_origin()` replaced with the standard PostgreSQL
  function `pg_xact_commit_timestamp_origin()` in the `--against-origin` filter.
  The two are functionally identical (spock's implementation is a thin wrapper
  around the same PG core function).
- Spock detection is now per-node and lazy, supporting mixed clusters where some
  nodes have spock and others do not.
- `SpockNodeNames` renamed to `NodeOriginNames` throughout the codebase to
  reflect dual-mode (spock + native PG) usage.

### Fixed
- Recovery-mode auto-selection (`autoSelectSourceOfTruth`) silently used native
  PG LSN queries on spock clusters because the connection pool was stored after
  `fetchLSNsForNode` returned instead of before.
- `aceSchema` template function used `config.Cfg` (not thread-safe) instead of
  `config.Get()`, risking data races during concurrent SIGHUP reloads.
- `repset-diff` child table-diff tasks did not inherit the parent's TaskStore,
  causing each to open its own SQLite connection.
- `isSpockAvailable` swallowed errors, causing callers to silently fall back to
  native PG mode on detection failures.
- Native PG subscription matching used LIKE substring patterns that could
  collide on similar node names (e.g., `n1` matching `n10`); now uses regex
  word boundaries.

## [v1.9.0] - 2026-04-17

### Added
- `--until` flag for `mtree` build/diff commands to bound operations by commit timestamp. 

### Changed
- Switched SQLite driver from cgo `mattn/go-sqlite3` to pure-Go `modernc.org/sqlite`. Enables fully static binaries on every platform and fixes a latent bug where darwin/windows builds silently dropped the sqlite driver at runtime.

### Fixed
- `table-diff` could OOM when a node became unresponsive mid-run. Workers had no way to stop and would grind through every remaining sub-range (each waiting ~60 s for context deadline), accumulating errors and log output. Added a circuit breaker that short-circuits all workers — including the initial hash phase — as soon as any node error is recorded. Backed by `atomic.Bool` to avoid mutex overhead on the common path.
- `--until` correctly handles frozen rows (NULL commit timestamps after freeze).
- Merkle tree row-hash and fetch-rows queries now quote identifiers via `pgx.Identifier.Sanitize()` instead of interpolating raw qualified names.
- Spock origin filter is validated as an integer via `strconv.Atoi` rather than string-escaped; non-numeric values are rejected.

### Security
- Go directive bumped 1.25.4 → 1.26.0; release builds use Go 1.26.2, resolving stdlib CVEs including CVE-2025-68121 (CRITICAL).
- Upgraded `moby/buildkit` v0.27.1 → v0.28.1 (CVE-2026-33747, CVE-2026-33748).
- Upgraded `go.opentelemetry.io/otel/sdk` + exporters to v1.43.0 (CVE-2026-39882, CVE-2026-39883).
- Upgraded `google.golang.org/grpc` v1.79.1 → v1.80.0 (CVE-2026-33186).
- Switching to distroless/static-debian12 removes libc6 and libssl3 from the image entirely, eliminating the class of CVEs reported against those packages (including CVE-2025-27587).
- GitHub Actions pinned to commit SHAs; `goreleaser` image pinned to v2.15.2.

## [v1.8.1] - 2026-04-10

### Changed
- Prevent overlapping scheduled job runs.
  The scheduler now uses singleton mode so a job that exceeds its
  `run_frequency` will not start a second concurrent instance.

### Fixed
- Fix connection pool leak when table validation fails.
  `RunChecks()` did not close its connection pool on error paths. Leaked
  connections accumulated across scheduled runs — one per failing table
  per tick — and could exhaust database connection limits over time.

- Apply max_connections cap to discovery pools.
  Repset-diff and schema-diff metadata queries now respect the configured
  `max_connections` limit instead of using the pgxpool default.

## [v1.8.0] - 2026-04-08

### Added
- `--max-connections` / `-M` flag for `table-diff`, `repset-diff`, and `schema-diff` to cap the number of database connections per node. Also configurable via `table_diff.max_connections` in `ace.yaml` and the HTTP API. Workers queue for connections when the pool is full rather than failing.

### Fixed
- Connection pool size for `table-diff` (and by extension `repset-diff` and `schema-diff`) was unbounded — defaulting to `max(4, NumCPU)` per node regardless of the concurrency factor. On high-core machines this could exhaust database connections. Pool size is now derived from the concurrency factor (`NumCPU × ConcurrencyFactor`, minimum 4).

### Changed
- Added missing integration tests to CI: repset-diff, schema-diff, Merkle numeric scale invariance, catastrophic single-node failure recovery, and timestamp comparison tests.

## [v1.7.2] - 2026-04-01

### Fixed
- Fix precision loss for large bigint and numeric primary keys during table repair. Previously it was possible that type conversions may cause the incorrect key to be used.

## [v1.7.1] - 2026-03-23

### Added
- End-of-run summary for `schema-diff` and `repset-diff` listing identical, skipped, differed, missing, and errored tables with error reasons.

### Changed
- `schema-diff` and `repset-diff` now query tables from all nodes and report tables not present on every node, instead of silently using only the first node's table list.  still compared across all nodes.
- `repset-diff` reports asymmetric repset membership when a table is in the repset on some nodes but not others.

### Fixed
- `schema-diff` and `repset-diff` silently excluded tables that failed during per-table comparison (e.g. missing primary key). Failed tables now appear in the summary with the error reason, and the task status is set to FAILED.

## [v1.7.0] - 2026-03-18

### Added
- `table-repair --preserve-origin` flag to preserve replication origin node ID, LSN, and per-row commit timestamps during repair operations. Repaired rows retain the original source node's origin metadata instead of being stamped with the local node's identity, preventing replication conflicts when a failed node rejoins the cluster. Upserts are grouped by (origin, timestamp) into separate transactions to satisfy PostgreSQL's per-transaction replication origin session constraint; deletes commit in a preceding transaction.
- Runtime configuration reload via SIGHUP for all long-running ACE modes. The scheduler waits for in-flight jobs to complete before swapping config; the API server reloads immediately, including mTLS security config (CRL and allowed CN list). Works for `ace start`, `ace server`, and `ace start --component=all`.
- End-of-run summary for `schema-diff` and `repset-diff` listing identical, skipped, differed, missing, and errored tables with error reasons.

### Changed
- CLI migrated from urfave/cli v2 to v3 for native interspersed flag support, allowing flags to be placed before or after subcommands.
- Config reads are now thread-safe and snapshotted per HTTP request, per Merkle-tree task, and per CDC replication stream to prevent mid-operation drift during concurrent SIGHUP reloads.
- mTLS certificate validator is swapped atomically on SIGHUP using `atomic.Pointer` for lock-free reads on the request path.
- `schema-diff` and `repset-diff` now query tables from all nodes and report tables not present on every node, instead of silently using only the first node's table list.  still compared across all nodes.
- `repset-diff` reports asymmetric repset membership when a table is in the repset on some nodes but not others.

### Fixed
- `repset-diff` was not working. Fixed and added tests.
- `schema-diff --skip-tables` now actually filters tables. Schema-qualified names (e.g. `myschema.mytable`) are also accepted; the schema prefix is validated against the target schema and stripped for matching.
- Replication origin LSN lookup rewritten to join through `spock.subscription` and `spock.node` instead of a broken LIKE pattern that never matched.
- `executeUpserts` no longer calls `resetReplicationOriginSession` before `tx.Commit()`, which was clearing the origin from WAL commit records.
- Unexpected scheduler exit in the SIGHUP reload loop is now handled gracefully.
- `schema-diff` and `repset-diff` silently excluded tables that failed during per-table comparison (e.g. missing primary key). Failed tables now appear in the summary with the error reason, and the task status is set to FAILED.

## [v1.6.0] 2026-02-25

### Changed
- To accommodate value comparison fix below, changed row hash algorithm and added algorithm versioning
    - If using Merkle Trees, it will automatically update to the new format when detected.
      Alternatively, you can reinitialize the trees.
- Batch concat_ws calls to support tales with 100+ columns
- Avoid extra memory usage when writing JSON table diff to output file

### Fixed
- Fixed OOM in recursive diff by limiting goroutine concurrency
- Fixed column comparisons with equal values but different scales (e.g., 3000.00 vs 3000.0).
- Fixed timestamp without timezone column value repair
- Close connections in table diff sooner

## [v1.5.5] - 2026-02-12

### Changed
- Change --concurrency-factor from int multiplier to float64 CPU ratio. The number of workers is based on the number of cores times this factor. The default is now 0.5.

## [v1.5.4] - 2026-01-28

### Changed
- Add 3rd party license notice

## [v1.5.3] - 2026-01-15

### Changed
- Update copyrights to 2026

### Fixed
- UUID data type handling for table diff and repair fixed.

## [v1.5.2] - 2025-12-30

### Added
- Repair plans can set `allow_stale_repairs: false` per action to skip repairs when the target row has a newer commit timestamp than the diff snapshot, with skipped rows logged to `reports/<YYYY-MM-DD>/stale_repair_skips_<HHMMSS.mmm>.json`.
- HTTP API documentation plus an OpenAPI 3.1 spec covering async task endpoints.
- Certificate/mTLS setup guide for the HTTP API server and Postgres client cert auth.

### Changed
- Documentation navigation now links the HTTP API docs, certificate guide, and OpenAPI spec.

## [v1.5.1] - 2025-12-22

### Changed
- Advanced repair terminology updated from `--repair-file` to `--repair-plan` across CLI/docs and HTML diff report export text.

## [v1.5.0] - 2025-12-18

### Added
- Advanced repair plans (`--repair-file`) for `table-repair`, including rule-based actions, row overrides, and custom rows, plus new docs and examples.
- Catastrophic node failure recovery workflow: `table-diff --against-origin` with optional `--until` fencing and `table-repair --recovery-mode` auto-selecting a source of truth using Spock origin/slot LSNs.
- HTML diff reports can now build and export starter repair plans with bulk actions, custom helpers, and copy/download controls.

### Changed
- Table filters are applied inline (no filtered views), and diff summaries now record the effective filter that combines table filters with origin/until constraints.
- Spock origin identifiers in diff metadata are resolved to node names when available.

### Fixed
- Dockerfile honors `TARGETARCH` for multi-arch builds.
- Repair-plan row overrides now compare numeric primary keys reliably.

## [v1.4.2] - 2025-12-03

### Added
- Installation guide covering `go install`, release tarballs, and a Docker quickstart that links to the full container docs.

### Changed
- CLI entrypoint moved from `cmd/server` to `cmd/ace`, so `go install github.com/pgedge/ace/cmd/ace@…` produces an `ace` binary (no more `server`).
- GoReleaser, README build snippets, and helper scripts updated to the new `cmd/ace` path.

### Fixed
- Avoids user confusion where `go install` previously emitted a `server` binary.

## [v1.4.1] - 2025-12-03

### Added
- `table-repair --fix-nulls` mode (with dry-run support) to cross-fill NULL-only drifts without a single source-of-truth, plus broader datatype coverage (arrays, JSON/JSONB, bytea, intervals) and dedicated integration tests.
- CDC can run snapshot-style drains to the current WAL flush LSN, periodically flushes metadata via the new `mtree.cdc.cdc_metadata_flush_seconds` setting, and exposes `--skip-cdc` to bypass replication updates before Merkle diffs.
- Documentation updates: Docker usage guide (GHCR images, config generation, ephemeral and long-running modes), design docs promoted into the docs nav, and mermaid diagrams enabled for architecture pages.

### Changed
- Connection pool lifecycle and privilege handling tightened across CLI/API paths; API handlers now require client roles, repairs set roles per transaction, and connection options avoid unnecessary privilege drops.
- Table-diff now records the base table and table filter in diff summaries, cleans up filtered materialized views automatically, and handles JSON/bytea/interval/array casting more defensively.

### Fixed
- CDC processing handles slot `confirmed_flush_lsn` fallbacks, stops once it reaches the caller’s high-water mark, and flushes metadata during/after streaming to avoid re-chasing new WAL.
- Table-repair validates diff-file schema/table metadata, enforces consistent column definitions across nodes, and closes connection pools reliably across all repair paths.

## [v1.4.0] - 2025-11-24

### Added
- REST API server secured with mutual TLS (configurable allowed CNs and optional CRL) that runs table-diff, table-rerun, table-repair, schema/repset/spock diffs, and Merkle tree init/build/update/diff/teardown as tracked tasks in a SQLite-backed store; runnable via `ace server` or `ace start --component api`.
- Client certificate authentication for PostgreSQL connections via the new `cert_auth` settings, allowing ACE to use user cert/key/CA pairs instead of passwords when talking to mTLS-secured nodes.
- `max_diff_rows` guardrail for table-diff runs (configurable in `ace.yaml` or API payloads) that stops comparisons once the threshold is hit and marks JSON/HTML reports when early termination occurs.

### Changed
- All CLI commands now expose short flag aliases and updated command reference docs; `ace start` also gained a `--component` selector to run just the scheduler, just the API server, or both.
- Release automation and packaging were reworked: GoReleaser now builds CGO-enabled Linux binaries (amd64/arm64) alongside non-CGO macOS/Windows artifacts, and a GitHub Actions workflow publishes tagged releases.

### Fixed
- Table filters now create deterministic, sanitised filtered view names per task to avoid collisions and invalid identifiers when `--table-filter` is used.

## [v1.3.5] - 2025-11-06

### Added
- Background task scheduler capable of running table-diff, schema-diff, and repset-diff jobs on a cron or fixed interval cadence, including run-on-start semantics and shared job definitions.
- Persistent SQLite-backed task store that records job metadata, execution status, and timing so schedules and operators have historical visibility.
- `default_cluster` config option plus supporting docs to eliminate the need to pass the cluster name on every CLI invocation once set.
- Official multi-stage Dockerfile that fetches a published ACE release tarball, installs it into a distroless base image, and wires up `/etc/ace/ace.yaml`.
- Comprehensive Go API and scheduling documentation covering the new automation flows.

## [v1.3.4] - 2025-10-22

### Fixed
- `ace --help`, `ace help …`, `ace config init`, and `ace cluster init` now skip configuration checks, so bootstrapping ACE no longer fails when `ace.yaml` is missing.
- All other commands still abort early with a clear error if a config file is not found, making misconfiguration obvious.

## [v1.3.3] - 2025-10-21

### Added
- Native support for PostgreSQL `pg_service.conf` files (including `ACE_PGSERVICEFILE` and `PGSERVICEFILE` overrides) so ACE can auto-discover cluster and node definitions without JSON stubs.

### Changed
- Service-file parsing merges base and per-node settings, validates required host/database attributes, and respects node filters before running diffs.
- Updated CLI options and documentation to explain the new service-file workflow.

## [v1.3.2] - 2025-10-18

### Changed
- Configuration discovery now walks `ACE_CONFIG`, the working directory, `$HOME/.config/ace/ace.yaml`, and `/etc/ace/ace.yaml`, dramatically simplifying shared installs.
- When no config file is present, ACE exits immediately with `config file 'ace.yaml' not found`, preventing confusing downstream failures.

## [v1.3.1] - 2025-10-17

### Added
- HTML diff report output (paired with JSON/CSV) that presents per-node summaries, highlights differing cells, and bundles metadata for sharing with stakeholders.

### Changed
- Table-diff runtime received major performance optimisations: per-task contexts, parallel leaf-hash recomputation, and more efficient goroutine usage cut latency for large tables.
- GoReleaser settings were updated so the new artifacts ship with every tagged release.

## [v1.3.0] - 2025-09-30

### Added
- Merkle tree subsystem (init/build/update/listen/table-diff/teardown) that maintains per-table rolling hashes to detect drift without rescanning entire relations.
- CDC listener now streams from PostgreSQL’s native `pgoutput` plugin for lower overhead Merkle updates.
- Automatic Merkle range split/merge plus transactional writes keep trees balanced and consistent as workloads fluctuate.
- Merkle objects moved into the dedicated `spock` schema and a `visualise.sh` helper script renders tree hierarchies for debugging.
- Additional docs and integration tests covering Merkle workflows and a key boundary-calculation fix for accurate diff windows.

## [v1.2.0] - 2025-07-23

### Added
- Centralised logger abstraction adopted across the CLI/server so every module emits consistent, leveled output.
- Shared helper functions consolidated into `pkg/common/utils.go`, reducing duplication and paving the way for future configuration improvements.
- GitHub Actions CI now runs the full integration test suite on every push and pull request.

### Changed
- Repository licensing and documentation were aligned with PostgreSQL License requirements: added LICENSE, propagated headers to all files, and reorganised long-form API docs.

## [v1.1.0] - 2025-07-16

### Added
- `schema-diff` for whole-schema comparisons (with optional per-table diffs) plus a DDL-only inspection mode.
- `repset-diff` to iterate table-diff across all tables in a replication set and aggregate the findings.
- `spock-diff` to validate Spock metadata, slots, and replication state across nodes.
- `table-rerun` to re-check only the rows captured in a previous diff report and confirm whether repairs succeeded.
- Diff reports now include commit timestamps and origin metadata for better provenance tracking.

## [v1.0.0] - 2025-06-30

### Added
- Initial ACE release featuring `table-diff` with rich tuning flags (block size, concurrency, compare units, table filters, and JSON/CSV output).
- `table-repair` workflow that consumes a diff report and applies repairs with safeguards such as dry-run, insert-only/upsert-only, and trigger controls.
- Baseline configuration workflow using `ace.yaml` plus cluster definitions, establishing the foundation for subsequent automation.
