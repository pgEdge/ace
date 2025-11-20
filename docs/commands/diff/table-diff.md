# table-diff

This command compares the data in the specified table across nodes in a cluster and generates a diff report if any inconsistencies are found. 

## Syntax

```
./ace table-diff <cluster_name> <schema.table_name> [options]
```

- `cluster_name`: The name of the pgEdge cluster in which the table resides.
- `schema.table_name`: The schema‑qualified name of the table to compare across nodes.

## Options

| Option | Alias | Description |
|---|---|---|
| `--dbname <name>` | `-d` | Database name. Defaults to the first DB in the cluster config. |
| `--block-size <int>` | `-b` | Rows processed per comparison block. Default `100000`. Honours `ace.yaml` limits unless `--override-block-size` is set. |
| `--concurrency-factor <int>` | `-c` | Number of workers per node (1–10). Default `1`. |
| `--compare-unit-size <int>` | `-u` | Recursive split size for mismatched blocks. Default `10000`. |
| `--output <json\|html>` | `-o` | Report format. Default `json`. When `html`, both JSON and HTML files share the same timestamped prefix. |
| `--nodes <list>` | `-n` | Comma-separated node list or `all`. Up to three-way diffs are supported. |
| `--table-filter <WHERE>` | `-F` | Optional SQL `WHERE` clause applied on every node before hashing. |
| `--override-block-size` | `-B` | Skip block-size safety checks defined in `ace.yaml`. |
| `--quiet` | `-q` | Suppress progress output. Results still write to the diff file. |
| `--debug` | `-v` | Enable verbose logging. |
| `--schedule` | `-S` | Run the diff repeatedly on a timer (requires `--every`). |
| `--every <duration>` | `-e` | Go duration string (for example, `15m`, `1h30m`). Used with `--schedule`. |

## Example

Compare a table across all nodes and write a JSON report (add `--output html` to emit both JSON and HTML using the same `<schema>_<table>_diffs-<timestamp>` prefix):

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

- It splits work into blocks (`--block-size`) and uses multiple workers per node (`--concurrency-factor`) to compute hashes. If hashes mismatch for a block, rows are materialised and, if necessary, recursively split using `--compare-unit-size`.
- Runtime factors include host resources (CPU/memory), allowed parallelism, table size and row width (e.g., large JSON/bytea/embedding columns can slow hashing), distribution of differences (widely scattered diffs trigger more block fetches), and network latency to database nodes.

### Tuning tips
1. Tune `--block-size` and `--concurrency-factor` for your hardware and data profile.
2. Use `--table-filter`/`-F` to narrow scope on very large tables.
3. Prefer `--output html` when you’ll manually review diffs.
4. Use `--override-block-size` sparingly; the guardrails in `ace.yaml` prevent allocations that can overwhelm memory.

### Scheduling runs

Add `--schedule --every=<duration>` to keep the diff running on a loop. The command performs an initial comparison immediately, then repeats after each interval until you stop the process:

```sh
./ace table-diff --schedule --every=1h my-cluster public.orders
```

All other flags (`--nodes`, `--block-size`, `--output`, and so on) are reused for every iteration.
