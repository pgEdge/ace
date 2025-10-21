# ACE Configuration

Before using the ACE Quickstart or ACE commands, you must provide connection and cluster information in the `ace.yaml` and `pg_service.conf` files.

ACE first attempts to use the Postgres service file to resolve connection information before falling back to the (legacy) `<cluster>.json` file for cluster details. Before invoking any ACE commands, use the following commands to create the configuration files:

```sh
./ace cluster init --path pg_service.conf
./ace config init --path ace.yaml
```

!!! info

    You must invoke both commands to initialize ACE before using either standard ACE API calls or mtree calls. A Postgres service file (or a legacy cluster definition JSON file) and a configuration file named `ace.yaml` are both necessary for running ACE.

The `ace.yaml` file defines defaults used for Postgres connections when calling the ACE commands like `table-diff` or `mtree table-diff` as well as configuration details for ACE commands.  You can modify properties that control ACE execution details like:

* the listen_address.
* the listen_port.
* timeout values.
* certificate information.

... and more.

The `pg_service.conf` file contains cluster details that help ACE locate nodes.  After creating the file, define a base section named after the cluster (for example `[acctg]`) to capture shared options, and one section per node using `<cluster>.<node>` such as `[acctg.n1]`. Then, update the template with the `host`, `port`, `database`, and credentials for each node before running ACE commands.

The following locations are checked (in order):

1. The `ACE_PGSERVICEFILE` environment variable.
2. The `PGSERVICEFILE` environment variable.
3. The `pg_service.conf` file in the current directory.
4. `$HOME/.pg_service.conf`.
5. `/etc/pg_service.conf`.

If none of these files contain entries for the requested cluster, ACE attempts to read the `<cluster>.json` file.
