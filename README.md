
The Active Consistency Engine (ACE) is a tool designed to ensure eventual consistency between nodes in a pgEdge cluster. For more information, please refer to the official [pgEdge docs on ACE](https://docs.pgedge.com/platform/ace).


## Installation

To install ACE, you need to have Go (version 1.18 or higher) installed on your system.

1.  Clone the repository:
    ```sh
    git clone https://github.com/pgedge/ace
    cd ace
    ```

2.  Build the executable:
    ```sh
    go build -o ace ./cmd/server/
    ```
    This will create an executable file named `ace` in the current directory. You can move this file to a directory in your `PATH` (e.g., `/usr/local/bin`) to make it accessible from anywhere.

## Configuration

ACE requires a cluster configuration file to connect to the database nodes. Please refer to the [pgEdge docs](https://docs.pgedge.com/platform/installing_pgedge/json) on how to create this file.

## Usage

ACE currently provides two main commands: `table-diff` and `table-repair`.

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
| `--dbname`            | `-d`  | Name of the database                                               |          |
| `--diff-file`         | `-f`  | Path to the diff file (**required**)                               |          |
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
 