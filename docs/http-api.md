# ACE HTTP API

ACE exposes a REST-style HTTP API that mirrors a subset of CLI commands. All operations are asynchronous: requests enqueue a task and return a task ID for polling.

## Base URL and authentication

- Base path: `/api/v1`
- Transport: HTTPS only.
- Mutual TLS is required. The server requires a client certificate signed by `cert_auth.ca_cert_file`.
- Optional allowlist: `server.allowed_common_names`. If set, the client certificate CN must be in this list.
- Optional revocation: `server.client_crl_file` is checked and must be valid.
- The client certificate common name (CN) becomes the DB role used by API-initiated tasks that require a role (for example, `table-repair` uses `SET ROLE`).

Server-side settings used by the API:

- `server.listen_address` (default `0.0.0.0`)
- `server.listen_port` (default `5000`)
- `server.tls_cert_file`, `server.tls_key_file` (required)
- `server.client_crl_file` (optional)
- `server.allowed_common_names` (optional)
- `server.taskstore_path` (optional; defaults to `ACE_TASKS_DB` or `./ace_tasks.db`)

For certificate generation, PostgreSQL SSL setup, and role mapping examples, see [Certificate setup](certs.md).

## Common request/response behavior

- All write endpoints are `POST` and accept `application/json`.
- Successful submissions return `202 Accepted`:

```json
{
  "task_id": "uuid",
  "status": "QUEUED"
}
```

- Errors return a JSON body:

```json
{
  "error": "message"
}
```

Typical error codes: `400`, `401`, `404`, `405`, `500`.

## Task status

`GET /api/v1/tasks/{task_id}`

Returns the current task status. Status values are: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`.

```json
{
  "task_id": "uuid",
  "task_type": "TABLE_DIFF",
  "status": "COMPLETED",
  "cluster": "my-cluster",
  "schema": "public",
  "table": "customers",
  "repset": "",
  "started_at": "2025-01-01T10:00:00Z",
  "finished_at": "2025-01-01T10:02:12Z",
  "time_taken": 132.5,
  "task_context": {
    "diff_summary": {
      "...": "..."
    }
  }
}
```

Notes:

- `task_context` is task-specific and may include summaries, counts, or error details.
- Diff and report files are written on the server filesystem; the HTTP API does not expose file download endpoints.

## Endpoints

### POST /api/v1/table-diff

Compares a table between nodes and generates a JSON diff report.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster` from `ace.yaml`. |
| `table` | string | yes | `schema.table`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `block_size` | int | no | Defaults to `table_diff.diff_block_size` or `100000`. |
| `concurrency_factor` | int | no | Defaults to `table_diff.concurrency_factor` or `1`. |
| `compare_unit_size` | int | no | Defaults to `table_diff.compare_unit_size` or `10000`. |
| `max_diff_rows` | int64 | no | Defaults to `table_diff.max_diff_rows` or `0` (no limit). |
| `table_filter` | string | no | SQL `WHERE` predicate (without `WHERE`). |
| `override_block_size` | bool | no | Bypass block-size guardrails. |
| `quiet` | bool | no | Suppress progress output. |

Notes:

- API forces `output=json`; HTML output is not exposed via HTTP.
- `--against-origin` and `--until` are not available via HTTP.

Example:

```sh
curl --cert client.crt --key client.key --cacert ca.crt \
  https://ace-host:5000/api/v1/table-diff \
  -H 'Content-Type: application/json' \
  -d '{
    "cluster": "my-cluster",
    "table": "public.customers",
    "nodes": ["n2","n3"],
    "table_filter": "region = '\''us-east'\''"
  }'
```

### POST /api/v1/table-rerun

Re-runs a diff from an existing diff file.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `diff_file` | string | yes | Path on the server filesystem. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/table-repair

Repairs table inconsistencies using a diff file.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `table` | string | yes | `schema.table`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `diff_file` | string | yes | Path on the server filesystem. |
| `repair_plan` | string | no | Repair plan file path on the server. |
| `source_of_truth` | string | no | Required unless using `fix_nulls` or bidirectional insert-only. |
| `quiet` | bool | no | Suppress output. |
| `dry_run` | bool | no | Generate plan without applying changes. |
| `insert_only` | bool | no | Only insert missing rows. |
| `upsert_only` | bool | no | Only upsert missing/different rows. |
| `fire_triggers` | bool | no | Use `session_replication_role=local`. |
| `generate_report` | bool | no | Write a JSON report. |
| `fix_nulls` | bool | no | Fill NULLs using peer values. |
| `bidirectional` | bool | no | Insert-only in both directions. |
| `preserve_origin` | bool | no | Preserve replication origin node ID and LSN with per-row timestamp accuracy. Default: `false` |

Notes:

- Recovery-mode is not exposed via HTTP; origin-only diff files will be rejected.
- The client certificate CN must map to a DB role that can run `SET ROLE` and perform required DML.
- Preserve-origin maintains microsecond-precision timestamps for each repaired row, ensuring accurate temporal ordering in recovery scenarios.

### POST /api/v1/spock-diff

Diffs Spock metadata across nodes.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"` (must resolve to 2+ nodes). |
| `output` | string | no | `json` (default). |

### POST /api/v1/schema-diff

Diffs all tables in a schema or compares schema DDL only.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `schema` | string | yes | Schema name. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `skip_tables` | string | no | Comma-separated list. |
| `skip_file` | string | no | File path (server-side). |
| `ddl_only` | bool | no | Compare schema objects only. |
| `block_size` | int | no | Defaults to `table_diff.diff_block_size` or `100000`. |
| `concurrency_factor` | int | no | Defaults to `table_diff.concurrency_factor` or `1`. |
| `compare_unit_size` | int | no | Defaults to `table_diff.compare_unit_size` or `10000`. |
| `output` | string | no | `json` (default) or `html`. |
| `override_block_size` | bool | no | Bypass block-size guardrails. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/repset-diff

Diffs all tables in a replication set.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `repset` | string | yes | Replication set name. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `skip_tables` | string | no | Comma-separated list. |
| `skip_file` | string | no | File path (server-side). |
| `block_size` | int | no | Defaults to `table_diff.diff_block_size` or `100000`. |
| `concurrency_factor` | int | no | Defaults to `table_diff.concurrency_factor` or `1`. |
| `compare_unit_size` | int | no | Defaults to `table_diff.compare_unit_size` or `10000`. |
| `output` | string | no | `json` (default) or `html`. |
| `override_block_size` | bool | no | Bypass block-size guardrails. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/mtree/init

Initializes Merkle tree metadata for the cluster.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/mtree/teardown

Removes Merkle tree metadata for the cluster.

Request body: same as `/mtree/init`.

### POST /api/v1/mtree/teardown-table

Removes Merkle tree metadata for a specific table.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `table` | string | yes | `schema.table`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/mtree/build

Builds Merkle tree blocks for a table.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `table` | string | yes | `schema.table`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `block_size` | int | no | Defaults to `10000`. |
| `max_cpu_ratio` | float | no | Defaults to `0.5`. |
| `override_block_size` | bool | no | Bypass block-size guardrails. |
| `analyse` | bool | no | Analyze table statistics. |
| `recreate_objects` | bool | no | Drop/recreate Merkle tree objects. |
| `write_ranges` | bool | no | Persist computed ranges. |
| `ranges_file` | string | no | File path for reading/writing ranges. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/mtree/update

Updates Merkle tree blocks for a table.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `table` | string | yes | `schema.table`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `max_cpu_ratio` | float | no | Defaults to `0.5`. |
| `rebalance` | bool | no | Rebalance small blocks. |
| `quiet` | bool | no | Suppress output. |

### POST /api/v1/mtree/diff

Diffs Merkle tree blocks for a table.

Request body:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `cluster` | string | no | Defaults to `default_cluster`. |
| `table` | string | yes | `schema.table`. |
| `dbname` | string | no | Overrides DB name from config. |
| `nodes` | array[string] | no | Defaults to `"all"`. |
| `max_cpu_ratio` | float | no | Defaults to `0.5`. |
| `output` | string | no | `json` (default) or `html`. |
| `skip_update` | bool | no | Skip CDC/update step. |
| `quiet` | bool | no | Suppress output. |
