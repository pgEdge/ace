# Active Consistency Engine (ACE)
[![Go Integration Tests](https://github.com/pgEdge/ace/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/pgEdge/ace/actions/workflows/test.yml)

## Table of Contents
- [Building ACE](README.md#building-ace)
- [Configuring ACE](README.md#ace-configuration)
- [ACE Quickstart](./docs/quickstart.md)
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

ACE discovers cluster connection details from a PostgreSQL service file before falling back to the [legacy JSON format](https://docs.pgedge.com/platform/installing_pgedge/json). Service names must follow the pattern `<cluster>` for shared defaults and `<cluster>.<node>` for each node entry (for example, `app_db.n1`, `app_db.n2`). The following locations are checked (in order): 

1. The `ACE_PGSERVICEFILE` environment variable. 
2. The `PGSERVICEFILE` environment variable.
3. The `pg_service.conf` file in the current directory. 
4. `$HOME/.pg_service.conf`.
5. `/etc/pg_service.conf`.

Use the following command to bootstrap a template:

```sh
./ace cluster init --path pg_service.conf
```
Then adjust the host, port, database, and credentials for each node. If you still rely on the older JSON files, ACE will automatically read `<cluster>.json` when no matching service entries are present.

ACE also needs an `ace.yaml` for runtime defaults such as `connection_timeout` for Postgres or for ACE-specific defaults such as `max_diff_rows`. To generate an ace.yaml file, use the following command:

```sh
./ace config init --path ace.yaml
```

