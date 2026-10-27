# ACE Configuration

Before using ACE commands, you must provide connection and cluster information in the `ace.yaml` and `pg_service.conf` files.

ACE first attempts to use the Postgres service file to resolve connection information before falling back to the (legacy) `<cluster>.json` file for cluster details. Before invoking any ACE commands, use the following commands to create the configuration files:

```sh
./ace cluster init --path pg_service.conf
./ace config init --path ace.yaml
```

!!! info

    You must create and modify the configuration files before using either standard ACE API calls or mtree calls. A Postgres service file (or a legacy cluster definition JSON file) and a configuration file named `ace.yaml` are both required when running ACE.


## The ace.yaml file

The [`ace.sample.yaml` file](https://github.com/pgEdge/ace/blob/main/ace.sample.yaml) is a reference copy of the configuration that ACE ships, showing the default values used when calling the ACE commands.  The file contains properties that control the resources used by ACE commands; after creating your own `ace.yaml` file, use your choice of editor to customize the properties for your system:


<!-- TODO: Remove and/or update inapplicable fields -->
| Section --> Property | Description |
|---|---|
| postgres --> statement_timeout | **Equivalent to Postgres `statement_timeout`.** Aborts any query that exceeds the specified time (ms). `0` disables and is the default. Set a non-zero value to guard long-running operations. |
| postgres --> connection_timeout | **Equivalent to Postgres `connect_timeout`.** Max wait to connect (s). `0` = wait indefinitely. **Default: 10** |
| postgres --> application_name | Value reported in `application_name`. **Default: "ACE"** |
| postgres --> tcp_keepalives_idle | TCP keepalive idle time (s). **Default: 30** |
| postgres --> tcp_keepalives_interval | TCP keepalive interval (s). **Default: 10** |
| postgres --> tcp_keepalives_count | Number of keepalive probes before drop. **Default: 5** |
| postgres --> max_parallel_workers_per_gather | Sent as a session setting on every ACE connection. ACE already runs its hash queries from several client-side workers, so `0` stops Postgres from adding parallel workers to each of them. Set to `-1` to leave the server's own setting in place. **Default: 0** |
| table_diff --> concurrency_factor | CPU ratio for the comparison of the blocks that differ (0.0–4.0, e.g. 0.5 uses half of available CPUs): the number of comparison tasks that run at the same time. The number of hash workers comes from `max_connections`. **Default: 0.5** |
| table_diff --> min_diff_block_size | Minimum diff block (row chunk) size. **Default: 1** |
| table_diff --> max_diff_block_size | Maximum diff block size. **Default: 1000000** |
| table_diff --> compare_unit_size | Unit size for smallest comparison chunk. **Default: 10000** |
| table_diff --> max_connections | Old name of `max_connections --> default`. ACE uses it only when `max_connections.default` is not set. **Default: 0** (not set) |
| max_connections --> default | Number of connections that one `table-diff` task opens to each node, and a strict limit for the whole task (see [Connections per node](#connections-per-node)). `0` means not set: then the task opens 4 connections for cutting and hashing, and the comparison can open more. **Default: 0** (not set) |
| max_connections --> nodes | Map from `<cluster>.<node>` to the number of connections for that node. It has priority over `max_connections.default`. **Default: none** |
| mtree --> max_connections | Maximum database connections per node for `mtree build`, `mtree update`, and `mtree table-diff`. It caps the pool; the `max_connections` section does not apply to mtree commands. One connection holds the tree's transaction and the rest run hash queries, so this also caps the busy backends per node. Must be at least `2` when set, and `3` for `mtree build`, which also runs cutting workers on the reference node out of the same pool. `0` derives the worker count from `--max-cpu-ratio` alone. **Default: 0** |
| mtree → cdc --> slot_name | Logical decoding slot name for mtree CDC. **Default: "ace_mtree_slot"** |
| mtree → cdc --> publication_name | Publication used for mtree CDC. **Default: "ace_mtree_pub"** |
| mtree → cdc --> cdc_processing_timeout | Wall-clock budget (s) for a CDC catch-up drain before it gives up. Progress is durable, so a timeout means "re-run or raise", not "rebuild". **Default: 300** |
| mtree → cdc --> cdc_metadata_flush_seconds | How often (s) CDC metadata is flushed to disk. **Default: 10** |
| mtree → cdc --> cdc_flush_batch_size | Peak un-committed CDC changes a bounded drain buffers before applying them to the tree and freeing memory. Only primary keys are buffered, so memory scales with key size, not row width. **Default: 10000** |
| mtree → cdc --> adaptive_drain_fraction | During a bounded CDC drain (`mtree update` / `mtree table-diff`), a table whose decoded UPDATE count exceeds `max(adaptive_drain_min_changes, adaptive_drain_fraction × table row estimate)` is escalated: instead of per-update block marking, all of its Merkle-tree blocks are marked dirty once and rehashed in bulk from live data on the following tree update. Inserts and deletes are always tracked individually regardless of escalation, so block split/merge maintenance is unaffected. This bounds drain cost for heavily-updated tables (a bulk rehash costs about as much as a tree build, while per-change marking grows linearly with the backlog). Set to `-1` to disable escalation; `0` is treated as use-the-default (`0.01`), so to escalate on the `adaptive_drain_min_changes` floor alone, set a very small positive fraction rather than `0`. Escalation never affects correctness — block hashes are always recomputed from live table data. **Default: 0.01** |
| mtree → cdc --> adaptive_drain_min_changes | Floor for the escalation threshold, so small tables and modest change volumes always use the precise per-change path. **Default: 1000** |
| mtree --> schema | Schema used for mtree metadata/objects. **Default: "pgedge_ace"** |
| mtree → diff --> min_block_size | Minimum Merkle diff block size. **Default: 1000** |
| mtree → diff --> block_size | Target Merkle diff block size. **Default: 100000** |
| mtree → diff --> max_block_size | Maximum Merkle diff block size. **Default: 1000000** |
| server --> listen_address | Address the ACE server listens on. **Default: "0.0.0.0"** |
| server --> listen_port | Port the ACE server listens on. **Default: 5000** |
| (root) --> schedule_jobs | Array of job definitions consumed by the scheduler (see [Scheduling ACE Runs](scheduling.md)). **Default: []** |
| (root) --> schedule_config | Array of cadence settings (`run_frequency` or `crontab_schedule`) that reference entries in `schedule_jobs`. **Default: []** |
| cert_auth --> use_cert_auth | Use client certificate authentication. **Default: true** |
| cert_auth --> ace_user_cert_file | Path to user/client certificate. **Default: "data/pg16/pki/admin-cert/admin.crt"** |
| cert_auth --> ace_user_key_file | Path to user/client private key. **Default: "data/pg16/pki/admin-cert/admin.key"** |
| cert_auth --> ca_cert_file | Path to CA certificate. **Default: "data/pg16/pki/ca.crt"** |
| (root) --> debug_mode | Enable verbose/diagnostic logging. **Default: false** |

### Connections per node

`table-diff` (also when `repset-diff` or `schema-diff` runs it) opens a fixed number of connections N to each node before it starts to cut and hash a table, and keeps them open until the task ends. One node, the anchor, cuts the table into blocks: there, one connection in five goes to a cutting worker (at least one), and the rest go to hash workers. On the other nodes all N connections go to hash workers. When a cutting worker ends, its connection becomes one more hash worker.

N for a node comes from, in this order: `--max-connections` on the command line (or `max_connections` in the HTTP API or in a scheduled job), `max_connections.nodes.<cluster>.<node>`, `max_connections.default`, `table_diff.max_connections`, and 4.

```yaml
max_connections:
  default: 4
  nodes:
    demo.n1: 8
```

Some rules to know:

- The limit is per task. An API server or the scheduler can run several tasks at the same time in one process, and each task opens its own connections, so two tasks can use up to 2N connections to one node.
- A value that you set, on the command line or in this section, is a strict limit for the whole task: the comparison of the blocks that differ also runs on these N connections, with no more comparison tasks at the same time than N. If no value is set, the task opens 4 connections for cutting and hashing, and the comparison can open more connections to a node, up to the number that `table_diff.concurrency_factor` gives (one connection per comparison task, at least 4).
- If a server refuses a connection because it has no free connection slot (SQLSTATE 53300: the limit of the server, of the role or of the database), ACE goes on with the connections that it already has and writes a WARNING with the node, the number of connections it got and N. The comparison phase then runs no more tasks at the same time than the smallest number of connections over all nodes, and does not open new connections. If a node accepts no connection at all, the run stops with an error that names the node.
- The service file (`pg_service.conf`) is not used for this setting: libpq reads the same file and refuses unknown keys.
- `mtree` commands do not use this section. For them, `--max-connections` and `mtree.max_connections` keep their old meaning: an upper bound for the pool, where `0` means "compute from `--max-cpu-ratio`".



## The pg_service.conf File

ACE uses the [`pg_service.conf` file](https://www.postgresql.org/docs/current/libpq-pgservice.html) to find cluster and node details.  

ACE checks the following locations in this order:

1. The `ACE_PGSERVICEFILE` environment variable.
2. The `PGSERVICEFILE` environment variable.
3. The `pg_service.conf` file in the current directory.
4. `$HOME/.pg_service.conf`.
5. `/etc/pg_service.conf`.

After creating the file, add a base section named after the cluster (for example `[acctg]`) to capture shared options, and one section per node named in the form: `<cluster>.<node>`, such as `[acctg.n1]`. Then, update the template with the `host`, `port`, `database`, and credentials for each node before running ACE commands.

```bash
[acctg]
host=10.0.0.1
port=5432
dbname=acctg
user=rocky
password=1safepassword!
[acctg.n1]
host=10.0.0.2
port=5432
dbname=acctg
user=rocky
password=1safepassword!
[acctg.n2]
host=10.0.0.3
port=5432
dbname=acctg
user=rocky
password=1safepassword!
```

If none of these files contain entries for the requested cluster, ACE attempts to read the `<cluster>.json` file.
