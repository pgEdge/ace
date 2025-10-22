# Active Consistency Engine (ACE)
[![Go Integration Tests](https://github.com/pgEdge/ace/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/pgEdge/ace/actions/workflows/test.yml)

## Table of Contents
- [Building ACE](README.md#building-ace)
- [Configuring ACE](README.md#ace-configuration)
- [ACE Quickstart](./docs/using_ace.md)
- [Using Merkle Trees to Improve ACE Performance](./docs/merkle.md)
- [Using the ACE API](./docs/api.md)

The Active Consistency Engine (ACE) is a tool designed to ensure eventual consistency between nodes in a pgEdge cluster. For more information, please refer to the official [pgEdge docs on ACE](https://docs.pgedge.com/ace).


## Building ACE

To build ACE, you need to have Go (version 1.18 or higher) installed.

1.  Clone the repository:
    ```sh
    git clone https://github.com/pgedge/ace
    cd ace
    ```

2.  Build the executable:
    ```sh
    go build -o ace ./cmd/server/
    ```
    This will create an executable file named `ace` in the current directory. You can move this file to a directory in your `PATH` (e.g., `/usr/local/bin`) to make it accessible from anywhere.

## ACE Configuration

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

After creating the ACE configuration files, you're ready to use [ACE](/docs/quickstart.md).