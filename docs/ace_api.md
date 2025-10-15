
## ACE API Endpoints

ACE includes API endpoints for some of its most frequently used functions.

## API Reference

ACE provides a REST API for programmatic access. The API server runs on localhost:5000 by default. An SSH tunnel is required to access the API from outside the host machine for security purposes.  

### Configuring Authentication for ACE API Use

You must also configure client-based certificate authentication before using the ACE API.

You should create a client certificate separately for ACE with all necessary privileges on tables, schemas, and databases that you want to use with ACE. This user should preferably be a superuser since ACE may need elevated privileges during diffs and repairs. 

Each external user should have their own client certificate to use the API, typically with lower privileges. ACE will attempt to use `SET ROLE` to switch to the external user's role before performing any operations, thus ensuring that diffs and repairs happen with the external user's privileges when possible.

**Creating a Certificate File**

Please refer to [Postgres documentation](https://www.postgresql.org/docs/17/ssl-tcp.html#SSL-CERTIFICATE-CREATION) for information about setting up server and client certificates. Please note that after creating your certificate, you must provide details in the following [postgresql.conf parameters](https://www.postgresql.org/docs/17/ssl-tcp.html#SSL-SERVER-FILES):

* `ssl = on`
* `ssl_ca_file`
* `ssl_cert_file`
* `ssl_key_file`

You must also enable `cert` authentication in the [pg_hba.conf](https://www.postgresql.org/docs/17/auth-pg-hba-conf.html) file:

`hostssl	all				all				192.168.0.0/16			cert`

After configuring cert authentication for pgEdge Postgres, restart the Postgres server for the changes to be applied. 

Then, provide the following certificate information in the [ace_config.py file](../ace/installing_ace.md#the-ace-configuration-file-ace_configpy) (by default, located in `$PGEDGE_HOME/hub/scripts/`). Client-cert-based auth is a *required* option for using the ACE APIs. It can optionally be used with the CLI modules as well. Specify your preferences in the `Cert-based auth options` section:

```bash
"""
USE_CERT_AUTH = False
ACE_USER_CERT_FILE = ""
ACE_USER_KEY_FILE = ""
CA_CERT_FILE = ""
```

* `USE_CERT_AUTH` (default=False) a boolean value that indicates if ACE should use client-cert based authentication when connecting to the Postgres nodes.
* `ACE_USER_CERT_FILE` (default="") is the path to the certificate file (.crt) of the certificate bundle issued to the ACE user.
* `ACE_USER_KEY_FILE` (default="") is the path to the key file (.key) of the certificate bundle issued to the ACE user.
* `CA_CERT_FILE` (default="") is the path to the certificate file (.crt) of the certificate authority that was used to issue certificates.

After creating the certificates and providing information in the configuration files, you're ready to use the ACE API.

!!! note

    If you're already running the ACE process, and need to modify the `ace_config.py` file, use `Ctrl+C` to stop the process before making changes.

### The table-diff API

Initiates a table diff operation.

**Endpoint:** `GET /ace/table-diff`

**Request Body:**
```json
{
    "cluster_name": "my_cluster",        // required
    "table_name": "public.users",        // required
    "dbname": "mydb",                    // optional
    "block_rows": 10000,                 // optional, default: 10000
    "max_cpu_ratio": 0.8,                // optional, default: 0.6
    "output": "json",                    // optional, default: "json"
    "nodes": "all",                      // optional, default: "all"
    "batch_size": 50,                    // optional, default: 1
    "table_filter": "id < 1000",         // optional
    "quiet": false                       // optional, default: false
}
```

**Parameters:**
- `cluster_name` (required): Name of the cluster
- `table_name` (required): Fully qualified table name (schema.table)
- `dbname` (optional): Database name
- `block_rows` (optional): Number of rows per block (default: 10000)
- `max_cpu_ratio` (optional): Maximum CPU usage ratio (default: 0.8)
- `output` (optional): Output format ["json", "csv", "html"] (default: "json")
- `nodes` (optional): Nodes to include ("all" or comma-separated list)
- `batch_size` (optional): Batch size for processing (default: 50)
- `table_filter` (optional): SQL WHERE clause to filter rows for comparison
- `quiet` (optional): Suppress output (default: false)

**Example Request:**
```bash
curl -X POST "http://localhost:5000/ace/table-diff" \
  -H "Content-Type: application/json" \
  --cert /path/to/client.crt \
  --key /path/to/client.key \
  -d '{
    "cluster_name": "my_cluster",
    "table_name": "public.users",
    "output": "html"
  }'
```

**Example Response:**
```json
{
    "task_id": "td_20240315_123456",
    "submitted_at": "2024-03-15T12:34:56.789Z"
}
```

### The table-repair API

Initiates a table repair operation.

**Endpoint:** `GET /ace/table-repair`

**Request Body:**
```json
{
    "cluster_name": "my_cluster",        // required
    "diff_file": "/path/to/diff.json",   // required
    "source_of_truth": "primary",        // required unless fix_nulls is true
    "table_name": "public.users",        // required
    "dbname": "mydb",                    // optional
    "dry_run": false,                    // optional, default: false
    "quiet": false,                      // optional, default: false
    "generate_report": false,            // optional, default: false
    "upsert_only": false,                // optional, default: false
    "insert_only": false,                // optional, default: false
    "bidirectional": false,              // optional, default: false
    "fix_nulls": false,                  // optional, default: false
    "fire_triggers": false               // optional, default: false
}
```

**Parameters:**
- `cluster_name` (required): Name of the cluster
- `diff_file` (required): Path to the diff file
- `source_of_truth` (required): Source node for repairs
- `table_name` (required): Fully qualified table name
- `dbname` (optional): Database name
- `dry_run` (optional): Simulate repairs (default: false)
- `quiet` (optional): Suppress output (default: false)
- `generate_report` (optional): Create detailed report (default: false)
- `upsert_only` (optional): Skip deletions (default: false)
- `insert_only` (optional): Repair INSERT statements only (default: false)
- `bidirectional` (optional): Insert missing rows in a bidirectional manner (default: false)
- `fix_nulls` (optional): Fix NULL values by comparing across nodes (default: false)
- `fire_triggers` (optional): fire triggers when ACE performs a repair; note that `ENABLE ALWAYS` triggers will always fire. (default: false)


**Example Request:**
```bash
curl -X POST "http://localhost:5000/ace/table-repair" \
  -H "Content-Type: application/json" \
  --cert /path/to/client.crt \
  --key /path/to/client.key \
  -d '{
    "cluster_name": "my_cluster",
    "diff_file": "/path/to/diff.json",
    "source_of_truth": "primary",
    "table_name": "public.users"
  }'
```

**Example Response:**
```json
{
    "task_id": "tr_20240315_123456",
    "submitted_at": "2024-03-15T12:34:56.789Z"
}
```

### The table-rerun API

Reruns a previous table diff operation.

**Endpoint:** `POST /ace/table-rerun`

**Request Body:**
```json
{
    "cluster_name": "my_cluster",        // required
    "diff_file": "/path/to/diff.json",   // required
    "table_name": "public.users",        // required
    "dbname": "mydb",                    // optional
    "quiet": false,                      // optional, default: false
    "behavior": "multiprocessing"        // optional, default: "multiprocessing"
}
```

**Parameters:**
- `cluster_name` (required): Name of the cluster
- `diff_file` (required): Path to the previous diff file
- `table_name` (required): Fully qualified table name
- `dbname` (optional): Database name
- `quiet` (optional): Suppress output (default: false)
- `behavior` (optional): Processing behavior ["multiprocessing", "hostdb"]

**Example Request:**
```bash
curl -X POST "http://localhost:5000/ace/table-rerun" \
  -H "Content-Type: application/json" \
  --cert /path/to/client.crt \
  --key /path/to/client.key \
  -d '{
    "cluster_name": "my_cluster",
    "diff_file": "/path/to/diff.json",
    "table_name": "public.users"
  }'
```

**Example Response:**
```json
{
    "task_id": "tr_20240315_123456",
    "submitted_at": "2024-03-15T12:34:56.789Z"
}
```

### The task-status API

Retrieves the status of a submitted task.

**Endpoint:** `GET /ace/task-status/<task_id>`

**Parameters:**
- `task_id` (required): The ID of the task to check

**Example Request:**
```bash
curl "http://localhost:5000/ace/task-status?task_id=td_20240315_123456" \
  --cert /path/to/client.crt \
  --key /path/to/client.key
```

**Example Response:**
```json
{
    "task_id": "td_20240315_123456",
    "task_type": "table-diff",
    "status": "COMPLETED",
    "started_at": "2024-03-15T12:34:56.789Z",
    "finished_at": "2024-03-15T12:35:01.234Z",
    "time_taken": 4.445,
    "result": {
        "diff_file": "/path/to/output.json",
        "total_rows": 10000,
        "mismatched_rows": 5,
        "summary": {
            // Additional task-specific details
        }
    }
}
```

### Spock Exception Update API

Updates the status of a Spock exception.

**Endpoint:** `POST /ace/update-spock-exception`

**Request Body:**
```json
{
    "cluster_name": "my_cluster",                   // required
    "node_name": "node1",                           // required
    "dbname": "mydb",                               // optional
    "exception_details": {                          // required
        "remote_origin": "origin_oid",              // required
        "remote_commit_ts": "2024-03-15T12:34:56Z", // required
        "remote_xid": "123456",                     // required
        "command_counter": 1,                       // optional
        "status": "RESOLVED",                       // required
        "resolution_details": {                     // optional
            "details": "Issue fixed"
        }
    }
}
```

**Parameters:**
- `cluster_name` (required): Name of the cluster
- `node_name` (required): The name of the node
- `dbname` (optional): The name of the database
- `exception_details` (required)
    - `remote_origin` (optional): The OID of the origin 
    - `remote_commit_ts` (optional): The timestamp of the exception
    - `remote_xid` (optional): The XID of the transaction
    - `command_counter` (optional): The number of commands executed
    - `status` (optional): The current state of the exception
    - `resolution_details` (optional):
        - `details`: Include details about the exception

**Example Request:**
```bash
curl -X POST "http://localhost:5000/ace/update-spock-exception" \
  -H "Content-Type: application/json" \
  --cert /path/to/client.crt \
  --key /path/to/client.key \
  -d '{
    "cluster_name": "my_cluster",
    "node_name": "node1",
    "exception_details": {
        "remote_origin": "origin1",
        "remote_commit_ts": "2024-03-15T12:34:56Z",
        "remote_xid": "123456",
        "status": "RESOLVED"
    }
  }'
```

**Example Response:**
```json
{
    "message": "Exception status updated successfully"
}
```

## API Error Responses

ACE API endpoints return error responses in the following format:

```json
{
    "error": "Description of what went wrong"
}
```

Common HTTP status codes:
- 200: Success
- 400: Bad Request (missing or invalid parameters)
- 401: Unauthorized (missing or invalid client certificate)
- 415: Unsupported Media Type (request body is not JSON)
- 500: Internal Server Error




