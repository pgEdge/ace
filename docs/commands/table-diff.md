# table-diff

## Using the ACE table-diff Command

ACE diff commands can identify the differences between replication set (repset), schema, and table content on different nodes of your replication cluster. In the example that follows, we'll use the `table-diff` command to find differences between nodes for a specific table. This command compares the data in the specified table across all nodes in the cluster and generates a diff report if any inconsistencies are found.

ACE diff functions compare two objects and identify the differences; the output is a report that contains a:

- Summary of compared rows
- Mismatched data details
- Node-specific statistics
- Error logs (if any)

If you generate an HTML report, ACE generates an interactive report with:

- Color‑coded differences
- Expandable row details
- Primary key highlighting
- Missing row indicators

**Common use cases:**

- Performing routine content verification
- Performance‑optimized large table scans
- Focused comparisons between nodes, tables, or schemas

!!! hint

    Experiment with different block sizes and CPU utilisation to balance performance and resource usage. Use `--table-filter` for large tables to reduce comparison scope, and generate HTML reports to simplify analysis. Also ensure diffs do not exceed `MAX_ALLOWED_DIFFS`; otherwise, `table-repair` can only partially repair the table.

## Syntax

```
./ace table-diff <cluster_name> <schema.table_name> [options]
```

- `cluster_name`: The name of the pgEdge cluster in which the table resides.
- `schema.table_name`: The schema‑qualified name of the table to compare across nodes.

## Options

| Option | Alias | Description |
|---|---|---|
| `-d, --dbname <name>` |  | Database name. Defaults to the first DB in the cluster config. |
| `--block-rows <int>` |  | Number of rows processed per block. Min: `1000`, Max: `100000`, Default: `10000`. Larger blocks can be faster but use more memory. Configurable in `ace_config.py`. |
| `-m, --max-cpu-ratio <float>` |  | Maximum CPU utilisation (0.0–1.0). Default: `0.6`. Configurable in `ace_config.py`. |
| `--batch-size <int>` |  | Blocks per worker per batch (default `1`). Higher values reduce parallelism overhead, but typically `1` is best. Configurable in `ace_config.py`. |
| `-o, --output <type>` |  | Output format: `html`, `json`, or `csv`. Default: `json`. Reports default to `diffs/<YYYY-MM-DD>/diffs_<HHMMSSmmmm>.json`. HTML/CSV highlight differences. |
| `-n, --nodes <list>` |  | Comma‑separated nodes to compare (or `all`). Up to three nodes recommended for clarity. |
| `-q, --quiet` |  | Suppress progress and sanity‑check output. If no differences are found, exits quietly; otherwise prints JSON to `stdout`. |
| `-t, --table-filter <WHERE>` |  | SQL `WHERE` clause to narrow the row set being compared. |

## Example

Compare a table across all nodes and write a JSON report (add `--output html` for a colour‑coded HTML report alongside JSON):

```sh
./ace table-diff acctg public.customers_large
```

## Sample Output (with differences)

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

## Sample Output (no differences)

```
2025/07/22 12:05:59 INFO Cluster acctg exists
2025/07/22 12:05:59 INFO Connections successful to nodes in cluster
2025/07/22 12:05:59 INFO Table public.customers_large is comparable across nodes
2025/07/22 12:05:59 INFO Using 16 CPUs, max concurrent workers = 16
Hashing initial ranges: 168 / 177 [======================================================================>----] 1s | 0s
2025/07/22 12:06:01 INFO Table diff comparison completed for public.customers_large
2025/07/22 12:06:01 INFO ✔ TABLES MATCH
```

### How `table-diff` Works

ACE optimises comparisons with multiprocessing and block hashing:

- It splits work into blocks (`--block-rows`) and uses multiple workers (bounded by `--max-cpu-ratio`) to compute hashes. If hashes mismatch for a block, rows are materialised to produce the diff report.
- Runtime factors include host resources (CPU/memory), allowed CPU utilisation, table size and row width (e.g., large JSON/bytea/embedding columns can slow hashing), distribution of differences (widely scattered diffs trigger more block fetches), and network latency to database nodes.

### Tuning tips
1. Tune `--block-rows` and `--max-cpu-ratio` for your hardware and data profile.
2. Use `--table-filter` to narrow scope on very large tables.
3. Prefer `--output html` when you’ll manually review diffs.
4. Keep diffs below `MAX_ALLOWED_DIFFS` to enable full repair via `table-repair`.
