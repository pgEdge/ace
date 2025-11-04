# ACE Command Reference

ACE API commands are available in two flavors: 

* ACE commands for those users who are not using Merkle trees.
* ACE [`mtree` commands](#merkle-tree-commands) for those users who use Merkle trees.

## Commands

The commands in the first section of this page are designed for use without Merkle trees.

### `table-diff`

Compares a table between nodes and generates a diff report.

**Usage:**
`./ace table-diff [flags] [cluster] <schema.table>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.
-   `<schema.table>`: The fully qualified name of the table to compare.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--block-size`        | `-b`  | Number of rows per block                                           | 100000   |
| `--concurrency-factor`| `-c`  | Concurrency factor                                                 | 1        |
| `--compare-unit-size` | `-s`  | Max size of the smallest block to use when diffs are present       | 10000    |
| `--output`            | `-o`  | Output format (`json` or `html`)                                     | json     |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--table-filter`      |       | `WHERE` clause expression to use while diffing tables              |          |
| `--override-block-size`|      | Allow block sizes outside `ace.yaml` guardrails                    | false    |
| `--quiet`             |       | Suppress progress output                                           | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace table-diff --nodes="n1,n2" --dbname=mydatabase my-cluster public.my_table 
```

Diff files follow the pattern `<schema>_<table>_diffs-<timestamp>.json`. When `--output html` is used, ACE writes an additional HTML file using the same prefix (for example `public_my_table_diffs-20231027100000.html`). The HTML diff report provides an easy-to-read, colour coded table of differences between nodes.


### `table-repair`

Repairs table inconsistencies using a diff file.

**Usage:**
`./ace table-repair [cluster] <schema.table> [flags]`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.
-   `<schema.table>`: The fully qualified name of the table to repair.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--diff-file`         | `-f`  | Path to the diff file (**required**)                               |          |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--source-of-truth`   | `-s`  | Name of the node to be considered the source of truth              |          |
| `--nodes`             | `-n`  | Nodes to include for cluster info (comma-separated, or "all")      | all      |
| `--dry-run`           |       | Show what would be done without executing                          | false    |
| `--generate-report`   |       | Write a JSON audit report under `reports/<date>/`                  | false    |
| `--insert-only`       |       | Only perform inserts, no updates or deletes                        | false    |
| `--upsert-only`       |       | Only perform upserts (insert or update), no deletes                | false    |
| `--bidirectional`     |       | Perform insert-only repairs in both directions                     | false    |
| `--fire-triggers`     |       | Fire triggers during repairs                                       | false    |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace table-repair my-cluster public.my_table \
  --diff-file=public_my_table_diffs-20231027100000.json \
  --source-of-truth=n1
``` 

Repair reports (when requested) are written to `reports/<YYYY-MM-DD>/repair_report_<HHMMSS.mmm>.json`, while dry runs use the same directory with a `dry_run_report_` prefix.

### `table-rerun`

Re-runs a diff from a file to check for persistent differences.

**Usage:**
`./ace table-rerun [flags] [cluster]`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--diff-file`         |       | Path to the diff file to re-run (**required**)                     |          |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace table-rerun --diff-file=public_my_table_diffs-20231027100000.json --dbname=mydatabase my-cluster
```

If any rows still differ, `table-rerun` writes a fresh report named `<schema>_<table>_rerun-diffs-<timestamp>.json`; otherwise it logs a success message without producing a new file.
 
### `schema-diff`

Compares schemas across nodes in a pgEdge cluster. By default, `schema-diff` performs a `table-diff` on every table in the schema and reports differences. Alternatively, `schema-diff` could also be used to compare if two or more nodes have the same set of tables, views, functions, and indices, which can be achieved by passing in `ddl-only`

**Usage:**
`./ace schema-diff [flags] [cluster] <schema>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.
-   `<schema>`: The name of the schema to compare.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--skip-tables`       |       | Tables to exclude from the diff (comma-separated)                  |          |
| `--skip-file`         |       | File containing a list of tables to exclude                        |          |
| `--block-size`        | `-b`  | Rows per block when diffing tables                                 | 100000   |
| `--concurrency-factor`| `-c`  | Workers per node                                                    | 1        |
| `--compare-unit-size` | `-s`  | Recursive split size for mismatched blocks                         | 10000    |
| `--output`            | `-o`  | Per-table diff output format (`json` or `html`)                    | json     |
| `--override-block-size`|      | Allow block sizes outside `ace.yaml` guardrails                    | false    |
| `--ddl-only`          |       | Only compare if objects (tables, views, functions, and indices) are the same, not individual tables                                          | false    |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace schema-diff --dbname=mydatabase my-cluster public
```

When `--ddl-only` is omitted, each table uses `table-diff` with the same block size, concurrency factor, compare-unit size, output format, and override behaviour supplied here.

### `repset-diff`

Performs a `table-diff` on every table in a replication set and reports differences.

**Usage:**
`./ace repset-diff [flags] [cluster] <repset>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.
-   `<repset>`: The name of the replication set to compare.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--skip-tables`       |       | Tables to exclude from the diff (comma-separated)                  |          |
| `--skip-file`         |       | File containing a list of tables to exclude                        |          |
| `--block-size`        | `-b`  | Rows per block when diffing tables                                 | 100000   |
| `--concurrency-factor`| `-c`  | Workers per node                                                    | 1        |
| `--compare-unit-size` | `-s`  | Recursive split size for mismatched blocks                         | 10000    |
| `--output`            | `-o`  | Per-table diff output format (`json` or `html`)                    | json     |
| `--override-block-size`|      | Allow block sizes outside `ace.yaml` guardrails                    | false    |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace repset-diff --dbname=mydatabase my-cluster my_repset
```

Each table in the replication set is diffed with the same block size, concurrency factor, compare-unit size, output format, and override behaviour provided on the command line.

### `spock-diff`

Compares spock metadata across cluster nodes.

**Usage:**
`./ace spock-diff [flags] [cluster]`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include in the diff (comma-separated, or "all")           | all      |
| `--output`            | `-o`  | Output format                                                      | json     |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace spock-diff --dbname=mydatabase my-cluster
``` 


## Merkle Tree Commands

`mtree` commands provide a more advanced and efficient way to compare tables using Merkle trees. This method is suitable for very large tables where a full table scan is too slow.

#### `mtree init`

Initialises the required database objects for Merkle tree operations on all nodes in a cluster. This includes creating a dedicated schema, tables for metadata, and setting up publications and replication slots for change data capture (CDC).

**Usage:**
`./ace mtree init [flags] [cluster]`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree init --dbname=mydatabase my-cluster
```

#### `mtree build`

Builds a Merkle tree for a specific table on all nodes in the cluster. This command should be run after `mtree init`.

**Usage:**
`./ace mtree build [flags] [cluster] <schema.table>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--block-size`        | `-b`  | Number of rows per leaf block                                      | 10000    |
| `--max-cpu-ratio`     |       | Max CPU ratio for parallel operations                              | 0.5      |
| `--override-block-size`|      | Skip block size check and allow potentially unsafe block sizes     | false    |
| `--analyse`           |       | Run `ANALYZE` on the table before building the tree                | false    |
| `--recreate-objects`  |       | Drop and recreate Merkle tree objects if they already exist        | false    |
| `--write-ranges`      |       | Write the calculated block ranges to a JSON file                   | false    |
| `--ranges-file`       |       | Path to a file with pre-computed block ranges to use for the build |          |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree build --dbname=mydatabase my-cluster public.my_table
```

#### `mtree table-diff`

Compares the Merkle trees of a table across nodes to find inconsistencies. It generates a diff report similar to the standard `table-diff` command. By default, it first updates the Merkle trees with the latest changes using CDC before performing the diff.

**Usage:**
`./ace mtree table-diff [flags] [cluster] <schema.table>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--max-cpu-ratio`     |       | Max CPU ratio for parallel operations                              | 0.5      |
| `--batch-size`        |       | Number of ranges to process in a batch when diffing                | 100      |
| `--output`            | `-o`  | Output format for the diff report (`json` or `html`)               | json     |
| `--skip-update`       | `-s`  | Skip updating the Merkle tree with CDC changes before the diff     | false    |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree table-diff --dbname=mydatabase my-cluster public.my_table
```

HTML output is also available via `--output html`; the command produces both JSON and HTML reports with matching timestamps.

#### `mtree update`

Manually triggers an update of a Merkle tree for a table using the captured changes from CDC. This can also be used to rebalance the tree.

**Usage:**
`./ace mtree update [flags] [cluster] <schema.table>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--max-cpu-ratio`     |       | Max CPU ratio for parallel operations                              | 0.5      |
| `--rebalance`         |       | Rebalance the tree by merging small blocks                         | false    |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree update --rebalance --dbname=mydatabase my-cluster public.my_table
```

#### `mtree listen`

Starts a long-running process that listens for database changes via CDC and automatically updates the Merkle trees for all tracked tables.

**Usage:**
`./ace mtree listen [flags] [cluster]`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree listen --dbname=mydatabase my-cluster
```

#### `mtree teardown-table`

Removes all database objects associated with a Merkle tree for a specific table. This includes the tree data, metadata, and removing the table from the CDC publication.

**Usage:**
`./ace mtree teardown-table [flags] [cluster] <schema.table>`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree teardown-table --dbname=mydatabase my-cluster public.my_table
```

#### `mtree teardown`

Removes all database objects created by `mtree init` from all nodes in the cluster. This will drop the dedicated schema, all Merkle tree data, and stop CDC.

**Usage:**
`./ace mtree teardown [flags] [cluster]`

**Arguments:**
-   `[cluster]`: Optional cluster override. When omitted, ACE uses the `default_cluster` from `ace.yaml`.

**Flags:**
| Flag                  | Alias | Description                                                        | Default  |
| --------------------- | ----- | ------------------------------------------------------------------ | -------- |
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--nodes`             | `-n`  | Nodes to include (comma-separated, or "all")                       | all      |
| `--quiet`             |       | Suppress output                                                    | false    |
| `--debug`             | `-v`  | Enable debug logging                                               | false    |

**Example:**
```sh
./ace mtree teardown --dbname=mydatabase my-cluster
``` 
