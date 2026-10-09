# Table Diff Architecture

The design of table diff is driven by the following requirements and constraints:

- Whether a table is kept consistent via logical replication, physical replication, backup/restore, or bulk copy, operators need a fast, low-impact way to verify that the replicas really match.
- Full-table comparisons that take hours are operationally risky (locks, lag, disk and network blow-ups). We need a method that scales down to quick “am I in sync?” checks and scales up to multi-billion-row tables without bringing the cluster to its knees.
- The outcome should be actionable: pinpoint what differs (adds/deletes/changes) rather than a binary “good/bad,” so remediation can be targeted.

## Existing Approaches and Their Limitations

- **Dump + file diff (e.g., `pg_dump` then `diff`)**
  - Pros: simple tooling, easy to script.
  - Limitations: requires extracting the entire table; dumps must be sorted or canonicalised to be diffable; produces huge intermediates; easily runs for hours on large tables; false positives from ordering/format differences.
- **Whole-table checksum**
  - Pros: single pass, compact result.
  - Limitations: still scans the whole table; a single checksum gives no locality of differences; any drift forces another full scan to localise; sensitive to scan order; changes during the scan can invalidate results.
- **Row-by-row comparison script**
  - Pros: can respect primary-key ordering and return precise differences.
  - Limitations: typically single-threaded and network-heavy; must stream or materialise the entire table; slow on wide rows; prone to timeouts and snapshot drift if not carefully managed; bespoke scripts vary in correctness and performance.

## Algorithm (Block-Hash Diff)

The table is partitioned into primary-key ranges (blocks). Identical block boundaries are applied on every node. Each range/node pair is hashed by parallel workers; only blocks whose hashes disagree are split further and, eventually, materialised for row-level diffing.

```mermaid
flowchart LR

  %% Styles
  classDef blk fill:#eef,stroke:#6b7a99,color:#1f2a44;
  classDef worker fill:#f5f9ff,stroke:#5b8def,stroke-dasharray:3 2,color:#0f2b5b;
  classDef cmp fill:#fff7e6,stroke:#e0a800,color:#5a4300;
  classDef split fill:#fdecea,stroke:#de4c4c,color:#5b0f0f;
  classDef skip fill:#e9f7ef,stroke:#2ca24c,color:#1f5b32;
  classDef fetch fill:#ffffff,stroke:#6b7a99,color:#1f2a44;
  classDef diff fill:#ffffff,stroke:#6b7a99,color:#1f2a44;
  classDef report fill:#ffffff,stroke:#6b7a99,color:#1f2a44;

  %% Blocks on Node A
  subgraph Blocks_NodeA["Node A: PK-ordered blocks"]
    A1["Block 1<br>PK a..b"]:::blk --> A2["Block 2<br>PK b..c"]:::blk --> A3["Block 3<br>PK c..d"]:::blk
  end

  %% Blocks on Node B
  subgraph Blocks_NodeB["Node B: same boundaries"]
    B1["Block 1<br>PK a..b"]:::blk --> B2["Block 2<br>PK b..c"]:::blk --> B3["Block 3<br>PK c..d"]:::blk
  end

  %% Hashing in parallel
  A1 -.-> WA1["Worker hashes<br>Block 1 @A"]:::worker
  B1 -.-> WB1["Worker hashes<br>Block 1 @B"]:::worker
  A2 -.-> WA2["Worker hashes<br>Block 2 @A"]:::worker
  B2 -.-> WB2["Worker hashes<br>Block 2 @B"]:::worker
  A3 -.-> WA3["Worker hashes<br>Block 3 @A"]:::worker
  B3 -.-> WB3["Worker hashes<br>Block 3 @B"]:::worker

  %% Hash compare per block
  subgraph CompareQ["Hash compare per block"]
    WA1 --> C1{"Block 1<br>hash match?"}:::cmp
    WB1 --> C1
    WA2 --> C2{"Block 2<br>hash match?"}:::cmp
    WB2 --> C2
    WA3 --> C3{"Block 3<br>hash match?"}:::cmp
    WB3 --> C3
  end

  %% Block-level decisions
  C1 -->|match| S1["Skip block<br>(no fetch)"]:::skip
  C2 -->|mismatch| Split2["Split Block 2<br>into subranges"]:::split
  C3 -->|match| S3["Skip block<br>(no fetch)"]:::skip

  %% Recursive path for mismatched block
  Split2 --> SA["Sub-block 2a"]:::blk
  Split2 --> SB["Sub-block 2b"]:::blk

  SA -.-> WSA["Worker hashes<br>2a"]:::worker
  SB -.-> WSB["Worker hashes<br>2b"]:::worker

  WSA --> RC2{"Sub-block hashes<br>match?"}:::cmp
  WSB --> RC2

  RC2 -->|match| RS2["Skip sub-block"]:::skip
  RC2 -->|mismatch| Fetch2["Fetch ordered rows<br>for sub-block(s)"]:::fetch

  Fetch2 --> Diff2["Row diff: add/del/mod<br>respect max_diff_rows"]:::diff
  Diff2 --> Report["Report & summary<br>(JSON/HTML, taskstore)"]:::report
```

### Block Boundaries

ACE takes the block boundaries from one anchor node: the node with the
highest estimated row count. The estimate is used only to choose this node;
the number and the size of blocks do not depend on it. The code is in
`internal/consistency/slicer`; `mtree build` uses the same code.

1. **Parts.** ACE splits the key space of the anchor node into K parts, the
   units of work for the cutting workers. There is one cutting worker per
   four hash workers (at most four), and four parts per cutting worker, so
   K does not depend on the size of the table. The part bounds come from
   `pg_stats.histogram_bounds` of the first primary-key column: each chosen
   value becomes the first key of the anchor node that is not less than it.
   If the column has no histogram (the table was never analyzed, or the
   first column of a composite key has few distinct values), the bounds
   come from a `TABLESAMPLE SYSTEM ... REPEATABLE` sample of the key, split
   with `ntile(K)`. The sample reads about 100 pages per part, at least
   1000; a smaller table is read in full. ACE does not check how old the
   histogram is: an old histogram makes some parts larger (on an
   append-only table, the last one), but the blocks stay the same.
2. **Blocks.** A cutting worker walks its part in key order on the anchor
   node and starts a new block every `block_size` rows. One query cuts a
   chunk of blocks by jumping `block_size` rows forward from the current
   block start:

   ```sql
   WITH RECURSIVE b(i, <key>) AS (
       (SELECT 0, <key> FROM t
        WHERE <key> >= <block start> AND <key> < <hi> AND <filter>
        ORDER BY <key> LIMIT 1)
     UNION ALL
       SELECT b.i + 1, x.*
       FROM b CROSS JOIN LATERAL (
           SELECT <key> FROM t
           WHERE <key> >= b.<key> AND <key> < <hi> AND <filter>
           ORDER BY <key> OFFSET <block_size> LIMIT 1) x
       WHERE b.i < <blocks in chunk>
   )
   SELECT i, <key> FROM b ORDER BY i
   ```

   Each jump is one descent of the primary-key index that reads
   `block_size` entries, and only one key per block goes back to ACE. When
   a jump finds no row, the part ends, and ACE counts the rows of its last
   block. So no block holds more than `block_size` rows on the anchor node,
   whatever the key distribution, the physical row order or the size of the
   table. Rows that `table_filter` removes are not counted.

   A chunk runs in a read-only `REPEATABLE READ` transaction with
   `enable_seqscan` and `enable_sort` off; otherwise, with a selective
   filter, the planner can choose a sequential scan and a sort for every
   jump. The first chunk of a part cuts one block; each next chunk cuts
   twice as many, up to about one million rows. So the first block is ready
   at once, and no query holds a snapshot for long. On a laptop one cutting
   worker cuts 5–6 million `bigint` keys per second, and the rate grows
   linearly with the number of workers.

Workers send each block to the hash queue as soon as they cut it, so
hashing does not wait for a full pass over the key. Blocks of different
parts can arrive in any order; the comparison step sorts them by key.

The log shows how ACE built the boundaries: the part source and why, the
anchor node, the number of parts and workers, and, at the end, the number
of blocks, the largest block, the time to the first block and the time to
cut all blocks.

### Range Alignment Across Nodes

Challenge: block boundaries come from the anchor node. Other nodes may have
rows that sort before the anchor’s first PK or after its last PK. If we only
used closed intervals from the anchor, those edge rows would never be
hashed.

- The first block has no lower bound: it is `(NULL, k_first)`, where
  `k_first` is the first key of the anchor node. On the anchor node it is
  empty.
- The last block has no upper bound: it is `[k_last, NULL)`. On the anchor
  node it holds at most `block_size` rows.
- Each block is applied identically to every node. In `hashRange`, `nil`
  means “no bound,” so rows of another node outside the anchor’s key range
  fall into the first or the last block and surface as hash mismatches.
  On such a node these two blocks can hold more than `block_size` rows:
  the extra rows are differences, and recursion splits the block until the
  discrepant rows are isolated.

```mermaid
flowchart LR
  subgraph Anchor["Anchor node (range generation)"]
    A0["Start=nil<br>End=a"] --> A1["a..b"] --> A2["b..c"] --> A3["c..nil"]
  end
  subgraph Other["Other node (extra rows possible)"]
    B0["Rows < a"] --> B1["a..b"] --> B2["b..c"] --> B3["Rows > c"]
  end
  A0 -.same bounds hashed.-> B0
  A1 -.same bounds hashed.-> B1
  A2 -.same bounds hashed.-> B2
  A3 -.same bounds hashed.-> B3
  classDef default fill:#eef,stroke:#6b7a99,color:#1f2a44;
```

### Comparison Notes: Simple vs Composite Primary Keys

- **Ordering and hashing**
  - Simple PK: ranges use scalar bounds; hashing binds one value for lower/upper when present.
  - Composite PK: ranges are tuples (`ROW(col1, col2, ...)`); hashing binds each component, and comparisons rely on the tuple sort order.
- **Range splitting**
  - Simple PK: median discovery and splits are straightforward using a single OFFSET/LIMIT on the ordered PK.
  - Composite PK: splits still pivot on PK order but must scan and bind every key component; more columns mean more bind parameters and slightly heavier queries.
- **Row materialisation**
  - `fetchRows` always orders by the PK columns (single or composite) so `CompareRowSets` can align rows deterministically.
  - Arrays/UDTs get cast to `TEXT` to avoid OID/scan issues regardless of PK shape.
- **What users should watch for**
  - Ensure the declared PK is the true business key; otherwise, composite drift can hide behind non-unique or misordered keys.
  - If the first key column has few distinct values, ACE cannot use its histogram and takes the part bounds from a sample. This is a little slower to start, but the blocks keep their size.
  - Avoid nullable or unstable key components (e.g., keys derived from timestamps that can change) to keep comparisons consistent over time.

```mermaid
flowchart LR
  %% Styles
  classDef note fill:#fff7e6,stroke:#e0a800,color:#5a4300;
  classDef pk fill:#eef,stroke:#6b7a99,color:#1f2a44;

  %% Composite PK
  subgraph CompositePK["Composite PK (lexical ordering)"]
    direction LR
    C1["(dept=10, id=1)"]:::pk --> C2["(dept=10, id=2)"]:::pk --> C3["(dept=20, id=1)"]:::pk --> C4["(dept=20, id=3)"]:::pk
    C2 -.-> CNOTE["Sorted by ROW(dept,id)<br>(dept then id)"]:::note
  end

  %% Simple PK
  subgraph SimplePK["Simple PK (scalar ordering)"]
    direction LR
    S1["pk=1"]:::pk --> S2["pk=2"]:::pk --> S3["pk=3"]:::pk
    S2 -.-> SNOTE["Compare with scalar <, >, ="]:::note
  end
```

### Resource Utilisation and Tuning

- **block_size**: Larger blocks reduce hash tasks and recursion but increase memory/IO per hash and slow mismatch localisation; smaller blocks do the opposite (more queries, finer locality).
- **concurrency_factor**: CPU ratio (0.0–4.0) that scales workers relative to `NumCPU` (e.g. 0.5 on a 16-CPU host spawns 8 workers). Higher = faster hashing but more load on DB backends, network, and local CPU; can contend with other workloads and connection limits. The connection pool per node is sized to match the worker count (minimum 4).
- **max_connections**: Hard cap on the connection pool size per node. When set, overrides the concurrency-derived pool size. Useful for environments with limited `max_connections` on the database server. Workers that exceed the pool size will queue for a connection rather than fail.
- **compare_unit_size**: Lower values push recursion deeper (more queries, smaller fetches); higher values stop earlier (fewer queries, larger fetches on mismatched ranges).
- **max_diff_rows**: Early-exit guardrail. Lower caps keep runs short and reports small on divergent tables; raising/removing can grow memory and report size when drift is large.
- **table_filter**: Narrows scope and cost; rows that do not match the filter do not count toward `block_size`. Must be identical across nodes to avoid false positives.
- **override_block_size**: Skips safety rails from `ace.yaml`. Oversized blocks can spike memory and slow hashes, especially on wide rows.
- **output (json/html)**: HTML adds minor post-processing; DB load is unaffected.

### Failure Modes and Safeguards

- **Diff limit hit**: `max_diff_rows` stops recursion early and marks the report; more differences may exist.
- **Permission or schema/PK mismatch**: Validation fails before work starts; nothing is executed against the DB.
- **Bytea >1 MB**: `CheckColumnSize` aborts to avoid runaway memory/IO.
- **Timeouts/slow ranges**: Hashing is wrapped in timeouts; the first error is recorded and surfaced after workers finish.
- **Stale stats**: Some parts are larger, so the cutting work is less even. Block size does not depend on statistics.

### Consistency Caveats
- No cross-node snapshot coordination. Concurrent writes during a run can appear as drift.
- Prefer quiescent windows or use `table_filter` to target stable partitions.
- PK stability matters: changing PK values mid-run can reshuffle ordering and produce noisy diffs.

### Operational Tuning Playbook

- Start conservative on busy systems: smaller `concurrency_factor`, moderate `block_size`.
- For large but mostly consistent tables: increase `block_size` and `concurrency_factor` to hash faster; keep `compare_unit_size` reasonable to localise mismatches.
- For drift-heavy tables: lower `block_size`/`compare_unit_size` to localise quickly; keep `max_diff_rows` low to bound runtime and report size.

### Limits and Edge Cases

- Requires a declared PK; up to three-way diffs only.
- `table_filter` creates per-node materialized views; filters must match exactly across nodes.
- Parts from an old histogram or a sample can be uneven. This changes only how the cutting work is shared between workers, not the block size.
- Wide JSON/bytea/UDT columns increase hash and fetch cost; oversized bytea (>1 MB) blocks execution.

### Observability

- Logs show range hashing progress, mismatches, recursion, and diff limits.
- Progress bars (mpb) reflect hash and mismatch-analysis stages.
- Task status and summary are persisted to SQLite (`ace_tasks.db` by default, or the `ACE_TASKS_DB` path) via `taskstore` in table `ace_tasks` with columns: `task_id`, `task_type`, `task_status`, `cluster_name`, `task_context` (JSON), `schema`, `table_name`, `repset_name`, `diff_file_path`, `started_at`, `finished_at`, `time_taken`. Example rows:

  | task_id       | task_status | cluster | schema | table_name       | diff_file_path                                      | started_at           | finished_at          | time_taken | task_context (truncated)                                                                                  |
  |---------------|-------------|---------|--------|------------------|------------------------------------------------------|----------------------|----------------------|------------|-----------------------------------------------------------------------------------------------------------|
  | 9f7f…e21b     | COMPLETED   | acctg   | public | customers_large  | public_customers_large_diffs-20250722120353.json    | 2025-07-22T12:03:51Z | 2025-07-22T12:03:53Z | 2.1        | {"qualified_table":"public.customers_large","mode":"diff","nodes":"all","diff_summary":{...}}            |
  | 3b2c…9aa4     | FAILED      | acctg   | public | orders           |                                                      | 2025-07-21T10:15:00Z | 2025-07-21T10:15:08Z | 8.0        | {"qualified_table":"public.orders","mode":"diff","nodes":"n1,n2","error":"user \"replicator\" lacks…"}   |
  | 1a4d…c7f2     | COMPLETED   | acctg   | public | invoices         | public_invoices_diffs-20250720115900.json           | 2025-07-20T11:58:55Z | 2025-07-20T11:59:00Z | 5.0        | {"qualified_table":"public.invoices","mode":"diff","nodes":"n1,n2","table_filter":"billing_cycle = …"}   |

- Diff reports write to timestamped JSON (and HTML if selected); paths are logged on completion.
- Sample analytics (SQLite):

  - Recent table-diff runs:
    ```sql
    SELECT task_id, task_status, started_at, finished_at, time_taken, diff_file_path
    FROM ace_tasks
    WHERE task_type = 'TABLE_DIFF'
    ORDER BY started_at DESC
    LIMIT 20;
    ```

  - Success/fail counts over the last 7 days:
    ```sql
    SELECT task_status, COUNT(*)
    FROM ace_tasks
    WHERE task_type = 'TABLE_DIFF'
      AND started_at >= datetime('now','-7 day')
    GROUP BY task_status;
    ```

  - Drift summary by run (JSON fields from `task_context.diff_summary`):
    ```sql
    SELECT
      task_id,
      json_extract(task_context, '$.diff_summary.total_rows_checked')   AS rows_checked,
      json_extract(task_context, '$.diff_summary.mismatched_ranges_count') AS mismatched_ranges,
      json_extract(task_context, '$.diff_summary.diff_row_limit_reached')  AS limit_hit
    FROM ace_tasks
    WHERE task_type = 'TABLE_DIFF'
    ORDER BY started_at DESC
    LIMIT 20;
    ```
