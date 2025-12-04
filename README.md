# Active Consistency Engine (ACE)
[![Go Integration Tests](https://github.com/pgEdge/ace/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/pgEdge/ace/actions/workflows/test.yml)

## Table of Contents
- [ACE Best Practices](./docs/best_practices.md)
- [Building ACE](README.md#building-ace)
- [Installing ACE](./docs/install.md)
- [Configuring ACE](./docs/configuration.md)
- [Running ACE with Docker](./docs/docker.md)
- [Using Merkle Trees to Improve ACE Performance](./docs/merkle.md)
- [Command Reference](./docs/commands/index.md)
- [Performance Considerations](./docs/performance.md)
- [Architecture and Design Documents](./docs/design/index.md)

The Active Consistency Engine (ACE) is a tool designed to ensure eventual consistency between nodes in a cluster.

## Building ACE

Before building ACE, you need to install Go (version 1.18 or higher).

1.  Clone the repository:
    ```sh
    git clone https://github.com/pgedge/ace
    cd ace
    ```

2.  Build the executable:
    ```sh
    go build -o ace ./cmd/ace/
    ```
    This will create an executable file named `ace` in the current directory. You can move this file to a directory in your `PATH` (e.g., `/usr/local/bin`) to make it accessible from anywhere.

## ACE Configuration

Before invoking any ACE commands, use the following commands to create the configuration files:

```sh
./ace cluster init --path pg_service.conf
./ace config init --path ace.yaml
```

Set the `default_cluster` key in `ace.yaml` to the cluster name you most frequently target. When this value is present, CLI commands will use it automatically unless you provide an explicit cluster argument.

!!! info

    For detailed information about creating and modifying ACE configuration files, visit [here](/docs/configuration.md).

The [`ace.yaml` file](ace.yaml) defines default values used when executing ACE commands like `table-diff` or `mtree table-diff`.  You can modify properties that influence ACE performance and execution like timeout values and certificate information.

The `pg_service.conf` file contains cluster connection details that help ACE locate nodes.  After creating the file: 

* define a base section named after the cluster (for example `[acctg]`) for cluster details
* define one section per node, named in the form `[cluster.node]` (for example, `[acctg.n1]`). 

Then, update the file with the `host`, `port`, `database`, and credentials for each node before running ACE commands.

ACE checks the following locations in order for a pg_service.conf file:

1. The `ACE_PGSERVICEFILE` environment variable.
2. The `PGSERVICEFILE` environment variable.
3. The `pg_service.conf` file in the current directory.
4. `$HOME/.pg_service.conf`.
5. `/etc/pg_service.conf`.

If none of these files contain entries for the requested cluster, ACE attempts to read the `<cluster>.json` file.
