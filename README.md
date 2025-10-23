# Active Consistency Engine (ACE)
[![Go Integration Tests](https://github.com/pgEdge/ace/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/pgEdge/ace/actions/workflows/test.yml)

The Active Consistency Engine (ACE) is a tool designed to ensure eventual consistency between nodes in a pgEdge cluster. For more information, please refer to the official [pgEdge docs on ACE](https://docs.pgedge.com/platform/ace).




## Building

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

## Configuration

ACE discovers cluster connection details from a PostgreSQL service file before falling back to the [legacy JSON format](https://docs.pgedge.com/platform/installing_pgedge/json). Service names must follow the pattern `<cluster>` for shared defaults and `<cluster>.<node>` for each node entry (for example, `app_db.n1`, `app_db.n2`). The following locations are checked in order: the `ACE_PGSERVICEFILE` environment variable, the `PGSERVICEFILE` environment variable, `pg_service.conf` file in the current directory, `$HOME/.pg_service.conf`, and finally `/etc/pg_service.conf`.

You can bootstrap a template with:
```sh
./ace cluster init --path pg_service.conf
```
Then adjust the host, port, database, and credentials for each node. If you still rely on the older JSON files, ACE will automatically read `<cluster>.json` when no matching service entries are present.

ACE also needs an `ace.yaml` for runtime defaults such as `connection_timeout` for Postgres or for ACE-specific defaults such as `max_diff_rows`. It can be generated with:
```sh
./ace config init --path ace.yaml
```

Set the `default_cluster` key in `ace.yaml` to the cluster name you most frequently target. When this value is present, CLI commands will use it automatically unless you provide an explicit cluster argument.

## Quickstart

This section provides a quick guide to get started with ACE. For a full list of commands and their options, please refer to the [API Reference](docs/api.md).

### 1. Finding Differences

To find differences between nodes for a specific table, use the `table-diff` command. This command will compare the data in the specified table across all nodes in the cluster and generate a diff report if any inconsistencies are found.

If you set `default_cluster` in `ace.yaml`, you can omit the cluster name in these examples and ACE will use that cluster automatically.

**Example:**
```sh
./ace table-diff demo_cluster public.customers_large
# or simply
./ace table-diff public.customers_large
```

Add `--output html` to emit a colour-coded HTML diff report alongside the JSON diff. 


**Sample Output (with differences):**
```
2025/07/22 12:03:51 INFO Cluster demo_cluster exists
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

If no differences are found, ACE will indicate that, and no diff file will be created.

**Sample Output (no differences):**
```
2025/07/22 12:05:59 INFO Cluster demo_cluster exists
2025/07/22 12:05:59 INFO Connections successful to nodes in cluster
2025/07/22 12:05:59 INFO Table public.customers_large is comparable across nodes
2025/07/22 12:05:59 INFO Using 16 CPUs, max concurrent workers = 16
Hashing initial ranges: 168 / 177 [======================================================================>----] 1s | 0s
2025/07/22 12:06:01 INFO Table diff comparison completed for public.customers_large
2025/07/22 12:06:01 INFO ✔ TABLES MATCH
```

### 2. Repairing Differences

Once a diff file has been generated, you can use the `table-repair` command to resolve the inconsistencies. You will need to specify the diff file and a "source of truth" node, which is the node that has the correct data.

**Example:**
```sh
./ace table-repair --diff-file=public_customers_large_diffs-20250718134542.json --source-of-truth=n1 demo_cluster public.customers_large
```

**Sample Output:**
```
2025/07/22 12:05:24 INFO Starting table repair for public.customers_large on cluster demo_cluster
2025/07/22 12:05:24 INFO Processing repairs for divergent node: n2
2025/07/22 12:05:24 INFO Executed 99 upsert operations on n2
2025/07/22 12:05:24 INFO Repair of public.customers_large complete in 0.003s. Nodes n2 repaired (99 upserted).
```

### 3. Finding Differences with Merkle Trees

For very large tables, you can use Merkle trees to find differences more efficiently. This method is faster because it doesn't require a full table scan on every comparison. Here's a quick guide to using Merkle trees.

**Step 1: Initialize Merkle Tree Objects**

First, you need to initialize the necessary database objects on all nodes in your cluster.

```sh
./ace mtree init demo_cluster
# or omit the cluster when default_cluster is set
./ace mtree init
```

**Step 2: Build the Merkle Tree**

Next, build the Merkle tree for the table you want to compare. This process will divide the table into blocks and calculate hashes for each block.

```sh
./ace mtree build demo_cluster public.customers_large
# or
./ace mtree build public.customers_large
```

**Step 3: Find Differences**

Now you can run the Merkle tree diff command. This will compare the trees on each node and report any inconsistencies. This will also create a diff file that can be used with the `table-repair` command.

```sh
./ace mtree table-diff demo_cluster public.customers_large
# or
./ace mtree table-diff public.customers_large
```

**Step 4: Repair Differences (Optional)**

If differences are found, you can repair them using the `table-repair` command, just like with a standard `table-diff`.

```sh
./ace table-repair --diff-file=<diff-file-from-mtree-diff> --source-of-truth=n1 demo_cluster public.customers_large
```

The Merkle trees can be kept up-to-date automatically by running the `mtree listen` command, which uses Change Data Capture (CDC) with the `pgoutput` output plugin to track row changes. Performing the `mtree table-diff` will update the Merkle tree even if `mtree listen` is not used.
