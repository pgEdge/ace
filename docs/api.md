# API Reference

This document provides a detailed reference for the commands available in the Active Consistency Engine (ACE).

## Commands

### `table-diff`

Compares a table between nodes and generates a diff report.

**Usage:**
`./ace table-diff [flags] <cluster> <schema.table>`

**Arguments:**
-   `<cluster>`: The name of the cluster to connect to (must match a cluster name in the configuration file).
-   `<schema.table>`: The fully qualified name of the table to compare.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--block-size`        | `-b`  | Number of rows per block                                           | 100000   |
| `--concurrency-factor`| `-c`  | Concurrency factor                                                 | 1        |
| `--compare-unit-size` | `-s`  | Max size of the smallest block to use when diffs are present       | 10000    |
| `--output`            | `-o`  | Output format                                                      | json     |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--table-filter`      |       | `WHERE` clause expression to use while diffing tables              |          |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--override-block-size`|      | Override block size                                                | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace table-diff --nodes="n1,n2" --dbname=mydatabase my-cluster public.my_table 
```

### `table-repair`

Repairs table inconsistencies using a diff file.

**Usage:**
`./ace table-repair <cluster> <schema.table> [flags]`

**Arguments:**
-   `<cluster>`: The name of the cluster to connect to.
-   `<schema.table>`: The fully qualified name of the table to repair.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--diff-file`         | `-f`  | Path to the diff file (**required**)                               |          |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--source-of-truth`   | `-s`  | Name of the node to be considered the source of truth              |          |
| `--nodes`             | `-n`  | Nodes to include for cluster info (comma-separated, or "all")      | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |
| `--dry-run`           |       | Show what would be done without executing                          | false    |
| `--generate-report`   |       | Generate a report of the repair operation                          | false    |
| `--insert-only`       |       | Only perform inserts, no updates or deletes                        | false    |
| `--upsert-only`       |       | Only perform upserts (insert or update), no deletes                | false    |
| `--fire-triggers`     |       | Fire triggers during repairs                                       | false    |
| `--bidirectional`     |       | Perform repairs in both directions (use with insert-only)          | false    |

**Example:**
```sh
./ace table-repair --diff-file=public_my_table_diffs-20231027100000.json --source-of-truth=n1 --dbname=mydatabase my-cluster public.my_table 
``` 

### `table-rerun`

Re-runs a diff from a file to check for persistent differences.

**Usage:**
`./ace table-rerun [flags] <cluster>`

**Arguments:**
-   `<cluster>`: The name of the cluster to connect to.
-   `<schema.table>`: The fully qualified name of the table.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--diff-file`         | `-f`  | Path to the diff file to re-run (**required**)                     |          |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace table-rerun --diff-file=public_my_table_diffs-20231027100000.json --dbname=mydatabase my-cluster public.my_table
```
 
### `schema-diff`

Compares schemas across nodes in a pgEdge cluster. By default, `schema-diff` performs a `table-diff` on every table in the schema and reports differences. Alternatively, `schema-diff` could also be used to compare if two or more nodes have the same set of tables, views, functions, and indices, which can be achieved by passing in `ddl-only`

**Usage:**
`./ace schema-diff [flags] <cluster> <schema>`

**Arguments:**
-   `<cluster>`: The name of the cluster to connect to.
-   `<schema>`: The name of the schema to compare.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--skip-tables`       |       | Tables to exclude from the diff (comma-separated)                  |          |
| `--skip-file`         |       | File containing a list of tables to exclude                        |          |
| `--ddl-only`          |       | Only compare if objects (tables, views, functions, and indices) are the same, not individual tables                                          | false    |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace schema-diff --dbname=mydatabase my-cluster public
```

### `repset-diff`

Performs a `table-diff` on every table in a replication set and reports differences.

**Usage:**
`./ace repset-diff [flags] <cluster> <repset>`

**Arguments:**
-   `<cluster>`: The name of the cluster to connect to.
-   `<repset>`: The name of the replication set to compare.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--skip-tables`       |       | Tables to exclude from the diff (comma-separated)                  |          |
| `--skip-file`         |       | File containing a list of tables to exclude                        |          |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace repset-diff --dbname=mydatabase my-cluster my_repset
```

### `spock-diff`

Compares spock metadata across cluster nodes.

**Usage:**
`./ace spock-diff [flags] <cluster>`

**Arguments:**
-   `<cluster>`: The name of the cluster to connect to.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--output`            | `-o`  | Output format                                                      | json     |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace spock-diff --dbname=mydatabase my-cluster
``` 