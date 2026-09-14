# schema-diff

Compares schemas across nodes. By default, runs `table-diff` on every table.
Alternatively, `--ddl-only` compares only object presence (tables, views, functions, indexes).
A third mode, `--compare=structure`, checks the actual definition of each common
table instead of its data or its mere presence.

**Usage**

```
./ace schema-diff [flags] [cluster_name] <schema_name>
```

**Arguments**

- `[cluster_name]` — Optional; overrides `default_cluster`.
- `<schema_name>` — Schema name.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--compare <data\|structure>` |  | What to compare: `data` (default, per-table data diff) or `structure` (compares table definitions instead of data; see below). |
| `--skip-tables` | `-T` | Comma list of tables to exclude. Applies to `--compare=structure` too. |  |
| `--skip-file` | `-s` | File with list of tables to exclude. Applies to `--compare=structure` too. |  |
| `--block-size <int>` | `-b` | Rows per block when diffing tables. Default `100000`. |
| `--concurrency-factor <float>` | `-c` | CPU ratio for concurrency (0.0–4.0). Default `0.5`. |
| `--compare-unit-size <int>` | `-u` | Recursive split size for mismatched blocks. Default `10000`. |
| `--output <json\|html>` | `-o` | Per-table diff report format. Default `json`. With `--compare=structure`, only `json` is accepted, and it only takes effect when given by hand (see below). |
| `--override-block-size` | `-B` | Allow block sizes outside `ace.yaml` guardrails. |
| `--ddl-only` | `-L` | Compare object sets only (no per-table diff) | `false` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |
| `--schedule` | `-S` | Run the schema diff repeatedly on a timer (requires `--every`). Not compatible with `--ddl-only` or `--compare=structure`. |
| `--every <duration>` | `-e` | Go duration string (for example, `24h`). Used with `--schedule`. |

**Example**

```sh
./ace schema-diff --dbname=mydatabase my-cluster public
```

When `--ddl-only` is **not** set, every qualifying table invokes `table-diff` using the same block size, concurrency factor, compare-unit size, output format, and override settings supplied here, so you get consistent behaviour between standalone and schema-driven comparisons.

### Scheduling runs

Use `--schedule --every=<duration>` to keep a schema comparison running on a loop. This mode is only supported when ACE can run per-table diffs (omit `--ddl-only` and `--compare=structure`):

```sh
./ace schema-diff --schedule --every=24h --dbname=mydatabase my-cluster public
```

ACE performs the first comparison immediately, then waits for the given interval before repeating. Stop the process to end the loop.

### Structure mode (`--compare=structure`)

```sh
./ace schema-diff --compare=structure --dbname=mydatabase my-cluster public
```

This mode does not read table data. It reads the real table definition on
each node — columns, replica identity key, constraints, partition bounds, and
every domain, range, composite, and enum type a column uses — and compares
them directly. It is a fast check. Run it before the data diff (which is much
slower), or use it alone to check that the schema DDL is still the same on
every node.

**What is compared:** column type, `NOT NULL`, identity, generated, storage
options, default, and collation; the replica identity key and its operator
classes; `PRIMARY KEY`, `UNIQUE`, `CHECK`, `FOREIGN KEY`, and `EXCLUDE`
constraints; partition bound and partition key; and the full definition of
every domain, range, composite, and enum type used by a compared column, not
just its name. It also compares the two databases' collation settings,
because a mismatch there changes how the same text sorts and compares on
each node.

**What is not compared:** non-constraint indexes, triggers, rules, sequences,
views, materialized views, storage parameters, column order, comments, and
ACLs. A skipped view is named in a log line, not left out silently.

Types are matched by name and definition, never by OID. Two nodes created by
separate `initdb` runs give different OIDs to the same user-defined type, so
matching by OID would either miss a real difference or report one that does
not exist.

**Findings and exit code.** Each difference found gets one of five ranks. The
process exits with the code of the single worst rank found in the whole run:

| Exit code | Rank | Meaning |
|---|---|---|
| `0` | (none) | The schemas are identical, or both agree that the schema is empty. |
| `16` | `cosmetic` | Does not change the shape of the data. This mode does not produce this rank today. |
| `32` | `equivalent-differing` | The same values fit on both sides, but are stored or handled differently — for example, a different collation on the same type. |
| `48` | `narrowed` | One side accepts a strict subset of the values the other side accepts — for example, `int4` vs `int8`, or a stricter `CHECK`. The report names the narrow side. |
| `64` | `incompatible` | Neither side's set of values contains the other's, or the object exists on only one node. |

A table missing on some nodes is reported first, on its own, before the
per-table findings. It also counts toward the `64` exit code.

**Getting a JSON report.** Pass `--output=json` on the command line to get a
structured report (schema name, node names, missing tables, and each finding)
instead of the plain-text report shown above. The flag must be given by hand:
running the command with no `--output` at all still prints text, even though
`json` is `--output`'s own default value for every other schema-diff mode. If
this mode did not check for that, every run would print JSON by default, even
one that never named `--output`.

**Not yet supported with this mode:**

- `--schedule` — rejected with a clear error. Use a loop around the command
  instead, or use `--compare=data`, which does support scheduling.
- `--output=html` — rejected when given by hand, since this mode has no
  per-table diff files to turn into HTML, only a list of findings.
