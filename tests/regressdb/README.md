# ACE on the regression database

This test runs ACE on two nodes that hold the regression database of PostgreSQL, the database that `make installcheck` leaves behind. It has many data types and objects that the integration tests do not have: geometric types, `money`, `tsvector`, `xml`, range and composite types, domains, arrays, partitioned and inherited tables, and many tables without a primary key.

The GitHub workflow `.github/workflows/regressdb.yml` runs it on each pull request to `main`, in three cases:

| Case | Node n1 | Node n2 |
|------|---------|---------|
| `old` | oldest supported major version | same version |
| `new` | newest major version | same version |
| `mixed` | oldest supported major version | newest major version |

In each case node n1 is a UTF8 server with default settings, and node n2 is a LATIN1 server with other defaults for `DateStyle`, `IntervalStyle`, `TimeZone`, `extra_float_digits`, `bytea_output`, `lc_monetary`, `search_path` and `xmloption` (set with `ALTER DATABASE` after the regression tests). Each node runs the regression tests of its own version. The oldest and the newest version are set in the workflow (`PG_OLD`, `PG_NEW`).

### Second run on a restored database

After the first run, the regression database on node n2 is replaced with a copy made by `pg_dump` and `pg_restore` (`dump-restore.sh`), and the same checks run again. In the cases `old` and `new`, the copy is the database of node n1; in the case `mixed`, it is the database that node n2 had before the first run. The dump is made before the first run, so it has no deleted rows. The copy keeps the encoding, the locale and the database settings of its source.

A restored database has the same objects as its source, but not the same history: the objects get new OIDs from a counter that the regression tests on node n2 have already moved, they are created in the order of the dump, and a table or a composite type has no dropped columns. ACE must not report a difference because of this.

The copy must be complete. Roles that node n2 does not have are created first, and the rest of `pg_dumpall --globals-only` is loaded after them. Any error of `pg_restore` stops the case. The restored database is dumped again, and this dump must have the same lines as the first one. The only difference allowed is the column order of a table with inherited columns: `pg_dump` does not keep it, and the restored table has the inherited columns first. So in the second run such tables have their columns in another order on the two nodes. The regression tests leave such tables only without a primary key, so `setup-node.sh` adds one with a primary key: `regressdb_inh_child`. In `known.tsv` and in the output, the second run has the case name with `-restored` at the end, for example `old-restored`.

## What is checked

`run.py` computes a reference result for each table with plain SQL and checks the result of each ACE command against it:

- `table-diff` for each table, with `--max-connections` 1, the default, and 32, and blocks of 50 rows.
- `mtree build` and `mtree table-diff` for each table with a primary key, with the minimum (3 for `mtree build`, 2 for `mtree table-diff`), the default and 32 connections. Each limit builds its own tree, compares with it and removes it (`mtree teardown-table`). A table with child tables and an empty table must be refused with their own messages.
- `schema-diff --ddl-only` and `schema-diff --compare=structure` for each schema, and `schema-diff` in data mode for `public` with `--max-connections` 1, the default and 32.

The reference result of a table with a primary key comes from both nodes: each row is read as the text of its key and the text of the whole row, with the columns in the order of their names and with the same output settings that ACE sets on its connections. A table that has the same columns in another order on the two nodes is compared as any other table: ACE must match the columns by name, and an error about the schema fails the run. The number of keys that are on one node only or have a different row text is the number of differences that ACE must report. In a table with inheritance children one key can have several rows; there the rows of a key are compared as a multiset, and only match or diff is checked. A table without columns or a primary key, a table with other key or other columns on the two nodes, or a table on one node only, must give the error that names this problem; when the two nodes have different problems, the error of either node is right. `schema-diff --ddl-only` and `--compare=structure` must find no differences when both nodes run the same major version. `schema-diff` in data mode must report as many matching, differing, missing and failed tables as the reference results give. No command may panic, be ended by a signal, or run longer than the time limit.

Results that differ from this, are known and are not problems of ACE are listed in `known.tsv`, with the reason. A problem of ACE is not listed there: it fails the run until it is fixed. The run also fails on a line of `known.tsv` that no longer matches the result, so that the list stays short and correct.

## Run it locally

On Linux, with the packages that the workflow installs:

```sh
tests/regressdb/build-pg.sh "$(tests/regressdb/pg-version.sh 15)" ~/pg/15
tests/regressdb/build-pg.sh "$(tests/regressdb/pg-version.sh 19)" ~/pg/19
go build -o /tmp/ace ./cmd/ace
tests/regressdb/run-cell.sh mixed ~/pg/15 ~/pg/19 /tmp/regressdb /tmp/ace
```

`RUN_ARGS` passes options to `run.py`, for example `RUN_ARGS='--tables ^public\.gtest --commands table-diff'`. `--tables`, `--commands` and `--conns` are for repeating a part of the run by hand. Such a limited run checks each command it runs in the same way as a full run, but it does not check `known.tsv` for unused lines, and with `--tables` it skips the data mode of `schema-diff`, which always compares the whole schema. The results are in `<workdir>/run/results.tsv` for the first run and in `<workdir>/run-restored/results.tsv` for the second run, and the full output of each failed command is in the `logs` directory next to them. The dump files and the logs of `pg_dump` and `pg_restore` are in `<workdir>/dump`.
