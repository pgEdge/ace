# Using the ACE table-diff and ACE table-repair Commands

!!! note

    Before using ACE commands, you must [create and modify the ACE configuration files](configuration.md).

For a full list of commands and their options, please refer to the [API Reference](api.md).

## Using the ACE table-diff Command

ACE diff commands can identify the differences between repset, schema, and table content on different nodes of your replication cluster. In the example that follows, we'll use the `table-diff` command to find differences between nodes for a specific table. This command will compare the data in the specified table across all nodes in the cluster and generate a diff report if any inconsistencies are found.

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

To find differences between nodes for a specific table, use the `table-diff` command. This command will compare the data in the specified table across all nodes in the cluster and generate a diff report if any inconsistencies are found.

**Example:**
```sh
./ace table-diff acctg public.customers_large
```

Add `--output html` to emit a colour-coded HTML diff report alongside the JSON diff. 

**Sample Output (with differences):**
```
2025/07/22 12:03:51 INFO Cluster acctg exists
2025/07/22 12:03:51 INFO Connections successful to nodes in cluster
2025/07/22 12:03:51 INFO Table public.customers_large is comparable across nodes
2025/07/22 12:03:51 INFO Using 16 CPUs, max concurrent workers = 16
Hashing initial ranges: 171 / 177 [=======================================================================>---] 1s | 0s
Analysing mismatches: 2 / 2 [=================================================================================] 0s | done
2025/07/22 12:03:53 INFO Table diff comparison completed for public.customers_large
2025/07/22 12:03:53 WARN ✘ TABLES DO NOT MATCH
2025/07/22 12:03:53 WARN Found 99 differences between n1/n2
2025/07/22 12:03:53 WARN Found 99 differences between n2/n3
2025/07/22 12:03:53 INFO Diff report written to public_customers_large_diffs-20250722120353.json
```

If no differences are found, ACE will indicate that, and no diff file is created.

**Sample Output (no differences):**
```
2025/07/22 12:05:59 INFO Cluster acctg exists
2025/07/22 12:05:59 INFO Connections successful to nodes in cluster
2025/07/22 12:05:59 INFO Table public.customers_large is comparable across nodes
2025/07/22 12:05:59 INFO Using 16 CPUs, max concurrent workers = 16
Hashing initial ranges: 168 / 177 [======================================================================>----] 1s | 0s
2025/07/22 12:06:01 INFO Table diff comparison completed for public.customers_large
2025/07/22 12:06:01 INFO ✔ TABLES MATCH
```

ACE is very efficient when it comes to comparing tables, and uses a lot of optimisations to speed up the process. However, it is important to consider certain factors that can affect the runtime of a `table-diff` command.
ACE first looks at table sizes, and then based on the specified runtime options, splits up the task into multiple processes and executes them in parallel. Each multiprocessing worker initially computes a hash of the data block, and if it finds there is a hash mismatch, attempts to fetch those records to generate a report.
How fast it can execute a `table-diff` command depends on the:

* configuration of the machine you're running ACE on – how many cores, how much memory, etc.
* resources you allow ACE to use (`max_cpu_ratio`). This is a strong determinant of runtime performance, even in the absence of other tuning options.
* runtime tuning options: `block-rows`, `batch-size`, `table-filter`, and `nodes`.
* size of your table, size of individual rows, and column datatypes. Sometimes performing a table-diff on tables with a very large number of records may take just a few minutes, while a smaller table with fewer rows, but with larger row sizes may take much longer. An example of the latter case is when embeddings or binary data is stored in the table.
* distribution of differences: Any differences in blocks of data identified by ACE require ACE to pull all those records together to generate a report. So, the smaller the data transfer between the database nodes and ACE, the faster it will run. If diffs are spread across numerous data blocks throughout the key space, it will take longer for ACE to be able to pull all the records. If you expect to see differences in certain blocks, using a table-filter and adjusting the block size can greatly speed up the process.
* network latency between the ACE node and your database nodes: The closer the ACE node is to the database nodes, the faster it can run.

ACE uses the [pg_service.conf file](./configuration.md) to connect to nodes and execute SQL statements. On your system, it may be desirable to set up a connection pooler like pgBouncer or pgCat separately and point to the configuration file for faster runtime performance.

### Improving ACE Performance when Invoking Diff Functions

The following runtime options can impact ACE performance during a `table-diff`:

* `--block-rows` specifies the number of tuples to be used at a time during table comparisons. ACE computes an MD5 sum on the full chunk of rows per block and compares it with the hash of the same chunk on the other nodes. If the hashes match up between nodes, then ACE moves on to the next block. Otherwise, the rows get pulled in and a set difference is computed. If `block_rows` is set to `1000`, then a thousand tuples are compared per job across tables. 
It is worth noting here that while it may appear that larger block sizes yield faster results, it may not always be the case. Using a larger block size will result in a speed up, but only up to a threshold. If the block size is too large, the Postgres [array_agg()](https://www.postgresql.org/docs/16/functions-aggregate.html) function may run out of memory, or the hash might take longer to compute, thus annulling the benefit of using a larger block size. The sweet spot is a block size that is large enough to yield quicker runtimes, but still small enough to avoid the issues listed above. ACE enforces that block sizes are between 10^3 and 10^5 rows.
* `batch_size` dictates how many sets of `block_rows` a single process should handle. By default, this is set to `1` to achieve the maximum possible parallelism – each process in the multiprocessing pool works on one block at a time. However, in some cases, you may want to limit process creation overheads and use a larger batch size. We recommend you leave this setting to its default value, unless there is a specific use-case that demands changing it.
* `--max_cpu_ratio` specifies the percentage of CPU power you are allotting for use by ACE. A value of `1` instructs the server to use all available CPUs, while `.5` means use half of the available CPUs. The default is `.6` (or 60% of the CPUs). Setting it to its maximum (1.0) will result in faster execution times. This should be modified as needed.

To evaluate and improve ACE performance:

1. Experiment with different block sizes and CPU utilisation to find the best performance/resource-usage balance for your workload.
2. Use `--table-filter` for large tables to reduce comparison scope.
3. Generate HTML reports for easier analysis of differences.
4. Ensure the diffs have not overrun the MAX_ALLOWED_DIFFS limit--otherwise, table-repair will only be able to partially repair the table.


## Using the ACE table-repair Command

The `ACE table-repair` function fixes data inconsistencies identified by the `table-diff` functions. ACE table-repair uses a specified node as the *source of truth* to correct data on other nodes. Common use cases for `table-repair` include:

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

### ACE table-repair

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

**Example:**
```sh
./ace table-repair --diff-file=public_customers_large_diffs-20250718134542.json --source-of-truth=n1 acctg public.customers_large
```

**Sample Output:**
```
2025/07/22 12:05:24 INFO Starting table repair for public.customers_large on cluster acctg
2025/07/22 12:05:24 INFO Processing repairs for divergent node: n2
2025/07/22 12:05:24 INFO Executed 99 upsert operations on n2
2025/07/22 12:05:24 INFO Repair of public.customers_large complete in 0.003s. Nodes n2 repaired (99 upserted).
```






