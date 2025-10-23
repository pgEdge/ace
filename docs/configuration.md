# ACE Configuration

Before using ACE commands, you must provide connection and cluster information in the `ace.yaml` and `pg_service.conf` files.

ACE first attempts to use the Postgres service file to resolve connection information before falling back to the (legacy) `<cluster>.json` file for cluster details. Before invoking any ACE commands, use the following commands to create the configuration files:

```sh
./ace cluster init --path pg_service.conf
./ace config init --path ace.yaml
```

!!! info

    You must create and modify the configuration commands before using either standard ACE API calls or mtree calls. A Postgres service file (or a legacy cluster definition JSON file) and a configuration file named `ace.yaml` are both required for running ACE.


## The ace.yaml file

The `ace.yaml` file defines defaults used for Postgres connections when calling the ACE commands like `table-diff` or `mtree table-diff` as well as configuration details for ACE commands.  You can modify properties that control ACE execution details like:

* the listen_address.
* the listen_port.
* timeout values.
* certificate information.

... and more.

| Section --> Property                    | Description |
| --------------------------------------- | ------------|
| postgres --> statement_timeout          | Statement execution timeout in milliseconds (0 = no timeout). Default: 0 |
| postgres --> connection_timeout         | Client connection timeout in seconds. Default: 10 |
| postgres --> application_name           | Value reported in application_name. Default: "ACE" |
| postgres --> tcp_keepalives_idle        | TCP keepalive idle time (seconds). Default: 30 |
| postgres --> tcp_keepalives_interval    | TCP keepalive interval (seconds). Default: 10 |
| postgres --> tcp_keepalives_count       | Number of keepalive probes before drop. Default: 5 |
| table_diff --> concurrency_factor       | Parallelism used for table diff operations. Default: 1 |
| table_diff --> max_diff_rows            | Max rows compared during a diff. Default: 1000000 |
| table_diff --> min_diff_block_size      | Minimum diff block (row chunk) size. Default: 1 |
| table_diff --> max_diff_block_size      | Maximum diff block size. Default: 1000000 |
| table_diff --> diff_block_size          | Target diff block size. Default: 1000 |
| table_diff --> diff_batch_size          | Rows per diff batch. Default: 1 |
| table_diff --> max_diff_batch_size      | Maximum rows per diff batch. Default: 1000 |
| table_diff --> compare_unit_size        | Unit size for compare chunks. Default: 10000 |
| mtree → cdc --> slot_name               | Logical decoding slot name for mtree CDC. Default: "ace_mtree_slot" |
| mtree → cdc --> publication_name        | Publication used for mtree CDC. Default: "ace_mtree_pub" |
| mtree → cdc --> cdc_processing_timeout  | CDC processing timeout (seconds). Default: 30 |
| mtree --> schema                        | Schema used for mtree metadata/objects. Default: "spock" |
| mtree → diff --> min_block_size         | Minimum diff block size. Default: 1000 |
| mtree → diff --> block_size             | Target diff block size. Default: 100000 |
| mtree → diff --> max_block_size         | Maximum diff block size. Default: 1000000 |
| server --> listen_address               | Address the ACE server listens on. Default: "0.0.0.0" |
| server --> listen_port                  | Port the ACE server listens on. Default: 5000 |
| (root) --> schedule_jobs                | List of scheduled jobs. Default: [] |
| (root) --> schedule_config              | Global scheduling settings. Default: [] |
| auto_repair_config --> enabled          | Toggle automatic repair workflow. Default: false |
| auto_repair_config --> cluster_name     | Target cluster name. Default: "" |
| auto_repair_config --> dbname           | Target database name. Default: "" |
| auto_repair_config --> poll_frequency   | How often to poll for issues (e.g., "10m"). Default: "10m" |
| auto_repair_config --> repair_frequency | How often to run repairs (e.g., "15m"). Default: "15m" |
| cert_auth --> use_cert_auth             | Use client certificate authentication. Default: true |
| cert_auth --> user_cert_file            | Path to user/client certificate. Default: "data/pg16/pki/admin-cert/admin.crt" |
| cert_auth --> user_key_file             | Path to user/client private key. Default: "data/pg16/pki/admin-cert/admin.key" |
| cert_auth --> ca_cert_file              | Path to CA certificate. Default: "data/pg16/pki/ca.crt" |
| cert_auth --> use_naive_datetime        | Use naive datetimes (no timezone) when parsing cert times. Default: false |
| (root) --> debug_mode                   | Enable verbose/diagnostic logging. Default: false |


## The pg_service.conf file

ACE uses the [`pg_service.conf` file](https://www.postgresql.org/docs/current/libpq-pgservice.html) to find cluster and node details.  

ACE checks the following locations in this order:

1. The `ACE_PGSERVICEFILE` environment variable.
2. The `PGSERVICEFILE` environment variable.
3. The `pg_service.conf` file in the current directory.
4. `$HOME/.pg_service.conf`.
5. `/etc/pg_service.conf`.

After creating the file, add a base section named after the cluster (for example `[acctg]`) to capture shared options, and one section per node named in the form: `<cluster>.<node>`, such as `[acctg.n1]`. Then, update the template with the `host`, `port`, `database`, and credentials for each node before running ACE commands.


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


If none of these files contain entries for the requested cluster, ACE attempts to read the `<cluster>.json` file.
