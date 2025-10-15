# ACE Functions

ACE provides functions that compare the data from one object to the data on other object, and optionally repairs the differences it finds.  ACE functions include:

| Command | Description |
|---------|-------------|
| [ACE table-diff](#ace-table-diff) | Compare two tables to identify differences. |
| [ACE repset-diff](#ace-repset-diff) | Compare two replication sets to identify differences. | 
| [ACE schema-diff](#ace-schema-diff) | Compare two schemas to identify differences |
| [ACE spock-diff](#ace-spock-diff) | Compare two sets of spock meta-data to identify differences. |
| [ACE table-repair](#ace-table-repair) | Repair data inconsistencies identified by the table-diff function. |
| [ACE table-rerun](#ace-table-rerun) | Rerun a diff to confirm that a fix has been correctly applied. |


## ACE Diff Functions

ACE diff functions compare two objects and identify the differences; the output is a report that contains a:

- Summary of compared rows
- Mismatched data details
- Node-specific statistics
- Error logs (if any)

If you generate an html report, ACE generates an interactive report with:
- Colour-coded differences
- Expandable row details
- Primary key highlighting
- Missing row indicators

Common use cases for the ACE diff functions include:

 * Performing routine content verification.
 * Performing a performance-optimized large table scan.
 * Performing a focused comparison between nodes, tables, or schemas.

As a best practice, you should experiment with different block sizes and CPU utilisation to find the best performance/resource-usage balance for your workload. Making use `--table-filter` for large tables to reduce comparison scope and generating HTML reports will make analysis of differences easier.

As you work, ensure that diffs have not overrun the `MAX_ALLOWED_DIFFS` limit; if your diffs surpass this limit, `table-repair` will only be able to partially repair the table.

### ACE table-diff

Use the `table-diff` command to compare the tables in a cluster and produce a csv, json, or html report showing any differences.

The syntax is:

`$ ./pgedge ace table-diff cluster_name schema.table_name [options]`

* `cluster_name` is the name of the pgEdge cluster in which the table resides.
* `schema.table_name` is the schema-qualified name of the table that you are comparing across cluster nodes.

**Optional Arguments**

Include the following optional arguments to customize ACE table-diff behavior:

* `-d` or `--dbname` is a string value that specifies the database name; `dbname` defaults to the name of the first database in the cluster configuration.
* `--block-rows` is an integer value that specifies the number of rows to process per block.
    - Min: 1000
    - Max: 100000
    - Default: 10000
    - Higher values improve performance but increase memory usage.
    - This is a configurable parameter in `ace_config.py`.
* `-m` or `--max-cpu-ratio` is a float value that specifies the maximum CPU utilisation; the accepted range is 0.0-1.0.  The default is 0.6.
    - This value is configurable in `ace_config.py`.
* `--batch-size` is an integer value that specifies the number of blocks to process per multiprocessing worker (default: `1`).
    - The higher the number, the lower the parallelism.
    - This value is configurable in `ace_config.py`.
* `-o` or `--output` specifies the output type; choose from `html`, `json`, or `csv` when including the `--output` option to select the output type for a report. By default, the report is written to `diffs/<YYYY-MM-DD>/diffs_<HHMMSSmmmm>.json`. If the output mode is csv or html, ACE will generate colored diff files to highlight differences.
* `-n` or `--nodes` specifies a comma-delimited subset of nodes on which the command will be executed. ACE allows up to a three-way node comparison. We do not recommend simultaneously comparing more than three nodes at once.
* `-q` or `--quiet` suppresses messages about sanity checks and the progress bar in `stdout`. If ACE encounters no differences, ACE will exit without messages. Otherwise, it will print the differences to JSON in `stdout` (without writing to a file).
* `-t` or `--table-filter` is a `SQL WHERE` clause that allows you to filter rows for comparison.

**ACE table-diff Command Examples**

The following example reports a difference when comparing a table (`public.foo`) across all nodes and generates an html report:

```bash
$ ./pgedge ace table-diff demo public.foo --output=html
✔ Cluster demo exists
✔ Connections successful to nodes in cluster
✔ Table public.foo is comparable across nodes
Getting primary key offsets for table...
Starting jobs to compare tables...

 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3  [ 0:00:00 < 0:00:00 , ? it/s ]
⚠ TABLES DO NOT MATCH
⚠ FOUND 1 DIFFS BETWEEN n1 AND n2
Diffs written out to diffs/2025-04-08/diffs_072159340.json
HTML report generated: diffs/2025-04-08/diffs_072159340.html
TOTAL ROWS CHECKED = 5
RUN TIME = 0.40 seconds
```

The following example reports a difference when comparing a table (`public.foo`) across nodes `n1` and `n2`, with a custom block size (`50000`):

```bash
$ ./pgedge ace table-diff demo public.foo --nodes="n1,n2" --block-rows=50000
✔ Cluster demo exists
✔ Connections successful to nodes in cluster
✔ Table public.foo is comparable across nodes
Getting primary key offsets for table...
Starting jobs to compare tables...

 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3  [ 0:00:00 < 0:00:00 , ? it/s ]
⚠ TABLES DO NOT MATCH
⚠ FOUND 1 DIFFS BETWEEN n1 AND n2
Diffs written out to diffs/2025-04-08/diffs_072804313.json
TOTAL ROWS CHECKED = 5
RUN TIME = 0.40 seconds
```

### ACE repset-diff

Use the `repset-diff` command to loop through the tables in a replication set and produce a csv, json, or html report showing any differences.  The syntax is:

`$ ./pgedge ace repset-diff cluster_name repset_name [options]`

* `cluster_name` is the name of the cluster in which the replication set is a member.
* `repset_name` is the name of the replication set in which the tables being compared reside.

**Optional Arguments**
* `-d` or `--dbname=db_name` is the name of the database in which to run the `repset-diff` command; the default is `none`.
* `-m` or `--max_cpu_ratio` specifies the percentage of CPU power you are allotting for use by ACE. A value of `1` instructs the server to use all available CPUs, while `.5` means use half of the available CPUs. The default is `.6` (or 60% of the CPUs).
* `--block_rows` specifies the number of tuples to be used at a time during table comparisons. If `block_rows` is set to `1000`, then a thousand tuples are compared per job across tables. 
* `-o` or `--output` specifies the output type; choose from `html`, `json`, or `csv` when including the `--output` parameter to select the output type for a report. By default, the report is written to `diffs/<YYYY-MM-DD>/diffs_<HHMMSSmmmm>.json`.  If the output type is csv or html, ACE will generate coloured diff files to highlight differences.
* `-n` or `--nodes` specifies a comma-delimited list of nodes on which the command will be executed.
* `--batch-size` is an integer value that specifies the number of blocks to process per multiprocessing worker (default: `1`).
* `-q` or `--quiet` suppresses output from ACE; this defaults to `False`.
* `--skip_tables=table_name` instructs ACE to not evaluate the specified table for differences.
* `--skip_file=file_name` allows you to specify the name of a file that contains a list of tables that you would like to skip.

**ACE repset-diff Example**

The following example reports a difference when comparing the `default` repset across all nodes:

```bash
$ ./pgedge ace repset-diff demo default
✔ Cluster demo exists
✔ Connections successful to nodes in cluster

CHECKING TABLE public.foo...

✔ Cluster demo exists
✔ Connections successful to nodes in cluster
✔ Table public.foo is comparable across nodes
Getting primary key offsets for table...
Starting jobs to compare tables...

 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3  [ 0:00:00 < 0:00:00 , ? it/s ]
⚠ TABLES DO NOT MATCH
⚠ FOUND 1 DIFFS BETWEEN n1 AND n2
Diffs written out to diffs/2025-04-08/diffs_090241529.json
TOTAL ROWS CHECKED = 5
RUN TIME = 0.40 seconds
```


### ACE schema-diff

Use the `schema-diff` command to compare the schemas in a cluster and report differences in a .json format report; the syntax is:

`$ ./pgedge ace schema-diff cluster_name schema_name [options]`

* `schema_name` is the name of the schema you will be comparing.
* `cluster_name` is the name of the cluster in which the table resides.

**Optional Arguments**
* `-n=node_list` specifies a list of nodes on which the schema will be compared; `node_list` is a comma-delimited list of node names. If omitted, the default is all nodes.
* `--dbname=db_name` specifies the name of the database in which you would like to run the diff; defaults to `none`.
* `--ddl_only` instructs ACE to check for only DDL differences.
* `--skip_tables=table_name` instructs ACE to not evaluate the specified table for differences.
* `--skip_file=file_name` allows you to specify the name of a file that contains a list of tables that you would like to skip.
* `-q` or `--quiet` suppresses output from ACE; this defaults to `False`.

**ACE schema-diff Example**

The following example demonstrates using the `schema-diff` command to check for differences in the `public` schema in a cluster named `demo` on nodes `n1` and `n2`:

```bash
$ ./pgedge ace schema-diff demo public -nodes=n1,n2
✔ Cluster demo exists
✔ Connections successful to nodes in cluster

Comparing nodes 127.0.0.1:6432 and 127.0.0.1:6433:
✔   No differences found
```

### ACE spock-diff

Use the `spock-diff` command to compare the meta-data on two cluster nodes, and produce a report showing any differences.  The syntax is:

`$ ./pgedge ace spock-diff cluster_name [options]`

* `cluster_name` is the name of the cluster in which the table resides.

**Optional Arguments**
* `-n=node_list` specifies a list of nodes on which spock will be compared; `node_list` is a comma-delimited list of node names. If omitted, the default is all nodes.
* `-q` or `--quiet` suppresses output from ACE; this defaults to `False`.


## ACE table-repair

The `ACE table-repair` function fixes data inconsistencies identified by the `table-diff` functions. ACE table-repair uses a specified node as the source of truth to correct data on other nodes. Common use cases for `table-repair` include:

  * **Spock Exception Repair** for exceptions arising from insert/update/delete conflicts
during replication.
  * **Network Partition Repair** to restore consistency across nodes after a network partition fails.
  * **Temporary Node Outage Repair** to bring a node up to speed after a temporary outage.

The function has a number of safety and audit features that you should consider before invoking the command:

  * **Dry run mode** allows you to test repairs without making changes.
  * **Report generation** produces a detailed repair audit trail of all changes made.
  * **Include the Upsert-Only option** to prevent data deletion.
  * **Transaction safety** ensures that all changes are atomic. If, for some reason your repair fails midway, the entire transaction will be rolled back, and no changes will be made to the database.

When using `table-repair`, remember that:

  * Table-repair is intended to be used to repair differences that arise from incidents such as spock exceptions, network partition irregularities, temporary node outages, etc. If the 'blast radius' of a failure event is too large -- say, millions of records across several tables, even though table-repair can handle this, we recommend that instead you do a dump and restore using native Postgres tooling.
  * Table-repair can only repair rows found in the diff file. If your diff exceeds `MAX_ALLOWED_DIFFS`, table-repair will only be able to partially repair the table.  This may even be desirable if you want to repair the table in batches; you can perform a `diff->repair->diff->repair` cycle until no more differences are reported.
  * You should invoke `ACE table-repair` with `--dry-run` first to review proposed changes.
  * Use `--upsert-only` or `--insert-only` for critical tables where data deletion may be risky.
  * You should verify your table structure and constraints before repair.

The command syntax is:

```bash
./pgedge ace table-repair <cluster_name> <schema.table_name> --diff-file=<diff_file> <--source-of-truth>[options]
```

* `cluster_name` is the name of the cluster in which the table resides.
* `diff_file` is the path and name of the file that contains the table differences.
* `schema.table_name` is the schema-qualified name of the table that you are repairing.
* `-s` or `--source-of-truth` is a string value specifying the node name to use as the source of truth for repairs. Note: If you are performing a repair that specifies the `--bidirectional` or `--fix-nulls` option, the `--source-of-truth` is not required.

**Optional Arguments**
* `--dry-run` is a boolean value that simulates repair operations without making changes. The default is `false`.
* `--upsert_only` (or `-u`) - Set this option to `true` to specify that ACE should make only additions to the *non-source of truth nodes*, skipping any `DELETE` statements that may be needed to make the data match. This option does not guarantee that nodes will match when the command completes, but can be useful if you want to merge the contents of different nodes. The default value is `false`. 
* `--generate_report` (or `-g`) - Set this option to `true` to generate a .json report of the actions performed; Reports are written to files identified by a timestamp in the format: `reports/<YYYY-MM-DD>/report_<HHMMSSmmmm>`.json. The default is `false`.
* `--dbname` is a string value that specifies the database name; dbname defaults to `none`.
* `--quiet` is a boolean value that suppresses non-essential output. The default is `false`.
* `--generate-report` is a boolean value that instructs the server to create a detailed report of repair operations. The default is `false`.
* `--upsert-only` is a boolean value that instructs the server to only perform inserts/updates, and skip deletions. The default is `false`.
* `-i` or `--insert-only` is a boolean value that instructs the server to only perform inserts, and skip updates and deletions. Note: This option uses `INSERT INTO ... ON CONFLICT DO NOTHING`. If there are identical rows with different values, this option alone is not enough to fully repair the table. The default is `false`.
* `-b` or `--bidirectional` is a boolean value that must be used with `--insert-only`. Similar to `--insert-only`, but inserts missing rows in a bidirectional manner. For example, if you specify `--bidirectional` is a boolean value that instructs ACE to apply differences found between nodes to create a *distinct union* of the content. In a distinct union, each row that is missing is recreated on the node from which it is missing, eventually leading to a data set (on all nodes) in which all rows are represented exactly once. For example, if you are performing a repair in a case where node A has rows with IDs 1, 2, 3 and node B has rows with IDs 2, 3, 4, the repair will ensure that both node A and node B have rows with IDs 1, 2, 3, and 4.
- `--fix-nulls` is a boolean value that instructs the server to fix NULL values by comparing values across nodes.  For example, if you have an issue where a column is not being replicated, you can use this option to fix the NULL values on the target nodes. This does not need a source of truth node as it consults the diff file to determine which rows have NULL values. However, it should be used for this special case only, and should not be used for other types of data inconsistencies.
- `--fire-triggers` is a boolean value that instructs triggers to fire when ACE performs a repair; note that `ENABLE ALWAYS` triggers will fire regardless of the value of `--fire-triggers`. The default is `false`.


**ACE table-repair Command Examples**

The following commands first perform a table-repair dry run of the `public.foo` table, specifying a diff file (`--diff-file=diffs/2025-04-08/diffs_090241529.json`) and using node `n1` as the source of truth:

```bash
[rocky@ip-172-31-15-12 pgedge]$ ./pgedge ace table-repair demo public.foo --diff-file=diffs/2025-04-08/diffs_090241529.json --source-of-truth=n1 --dry_run=True
✔ Cluster demo exists
✔ Connections successful to nodes in cluster
######## DRY RUN ########

Repair would have attempted to upsert 0 rows and delete 1 row on n2

######## END DRY RUN ########
```
After performing the dry run, we change the `--dry_run` flag to `False`, confirming that we want to apply the changes we reviewed in the first command iteration:

```bash
$ ./pgedge ace table-repair demo public.foo --diff-file=diffs/2025-04-08/diffs_090241529.json -s=n1 --dry_run=False
✔ Cluster demo exists
✔ Connections successful to nodes in cluster
✔ Successfully applied diffs to public.foo in cluster demo

*** SUMMARY ***

n2 UPSERTED = 0 rows

n2 DELETED = 1 rows
RUN TIME = 0.00 seconds
```

The following example performs a unidirectional insert-only repair on the `public.foo` table. In a situation where node 2 is missing a row when compared to node 1, including the `--insert-only` option inserts the missing rows from `node 1` to `node 2`:

```bash
$ ./pgedge ace table-repair demo public.foo diffs/2025-04-09/diffs_101804246.json --source-of-truth=n1 --insert-only=True
✔ Cluster demo exists
✔ Connections successful to nodes in cluster
✔ Successfully applied diffs to public.foo in cluster demo

*** SUMMARY ***

n2 INSERTED = 1 rows

RUN TIME = 0.00 seconds
```

The following example performs a bidirectional insert-only repair.  If you have a network partition between `node 1` and `node 2`, and they each separately received new records, including the `--bidirectional` option will insert the missing records from `node 1` to `node 2` and vice versa:

```bash
$ ./pgedge ace table-repair demo public.foo diffs/2025-04-09/diffs_103544698.json --source-of-truth=n1 --insert-only=True --bidirectional=True
✔ Cluster demo exists
✔ Connections successful to nodes in cluster

Performing bidirectional repair:
Overall progress:   0%|                                                                                           | 0/100 [00:00<?, ?%/s]
Processing node pair n1/n2
Processing node n1
Processing node n2
Overall progress: 100%|██████████████████████████████████████████████████████████████████████████████| 100/100 [00:00<00:00, 16786.62%/s]
✔ 
Successfully completed bidirectional repair in public.foo in cluster demo
```

**ACE table-repair Report Example**

```json
{
  "time_stamp": "08/07/2024, 13:20:19",
  "arguments": {
    "cluster_name": "demo",
    "diff_file": "diffs/2024-08-07/diffs_131919688.json",
    "source_of_truth": "n1",
    "table_name": "public.acctg_diff_data",
    "dbname": null,
    "dry_run": false,
    "quiet": false,
    "upsert_only": false,
    "generate_report": true
  },
  "database": "lcdb",
  "changes": {
    "n2": {
      "upserted_rows": [],
      "deleted_rows": [],
      "missing_rows": [
        {
          "employeeid": 1,
          "employeename": "Carol",
          "employeemail": "carol@pgedge.com"
        },
        {
          "employeeid": 2,
          "employeename": "Bob",
          "employeemail": "bob@pgedge.com"
        }
      ]
    }
  },
  "run_time": 0.1
}
```

Within the report:

* `time_stamp` displays when the function was called. 
* `arguments` lists the syntax used when performing the repair. 
* `database` identifies the database the function connected to. 
* `runtime` tells you how many seconds the function took to complete.

The `changes` property details the differences found by ACE on each node of your cluster. The changes are identified by node name (for example, `n2`) and by type:

* The `upserted_rows` section lists the rows upserted. Note that if you have specified `UPSERT only`, the report will include those rows in the `missing_rows` section.
* The `deleted_rows` section lists the rows deleted on the node.
* The `missing_rows` section lists the rows that were missing from the node. You will need to manually add any missing rows to your node.


## ACE table-rerun

The table-rerun function allows you to rerun a previous `table-diff` operation to verify fixes or check if inconsistencies persist after repairs. You can use table-rerun to:

* perform a post-repair verification to confirm that a table-repair run has resolved the diffs identified previously.
* Verify if diffs identified by the table-diff function still exist after the replication lag window has elapsed.

When using ACE table-rerun, you should:

* Include the `hostdb` processing option for very large tables and diffs to improve performance.
* Compare results using the original diff file to confirm that differences were resolved after a replication lag window.

 The syntax is:

`$ ./pgedge ace table-rerun <cluster_name> --diff_file=/path/to/diff_file.json schema.table_name`

* `cluster_name` is a string value that specifies the name of the cluster as defined in your configuration file.
* `schema.table_name` is a string value that specifies the fully qualified table name (e.g., "public.users")'.
* `diff_file` is a string value that specifies the path to the JSON diff file from a previous table-diff operation.

**Optional Arguments**

* `-d` or `--dbname` is a string value that specifies the database name; this defaults to the first database in the cluster config file.
* `-q` or `--quiet` is a boolean value that suppresses non-essential output.
* `-b` or `--behavior`is a string value that specifies the processing behavior [`multiprocessing` or `hostdb`].  
    - `multiprocessing` (the default) uses parallel processing for faster execution.
    - `hostdb` uses the host database to create temporary tables for faster comparisons. This is useful for very large tables and diffs.

**ACE table-rerun Command Examples**

To perform a table-rerun of a previous diff (specifying a diff file with the `--diff-file=diffs/2025-04-08/diffs_090241529.json` clause):

```bash
$ ./pgedge ace table-rerun demo --diff-file=diffs/2025-04-08/diffs_090241529.json public.foo
✔ Cluster demo exists
✔ Connections successful to nodes in cluster
✔ Table public.foo is comparable across nodes
Starting jobs to compare tables ...

 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1  [ 0:00:00 < 0:00:00 , ? it/s ]
✔ TABLES MATCH OK

TOTAL ROWS CHECKED = 2
RUN TIME = 0.40 seconds
```



