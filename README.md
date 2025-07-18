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

ACE requires a cluster configuration file to connect to the database nodes. Please refer to the [pgEdge docs](https://docs.pgedge.com/platform/installing_pgedge/json) on how to create this file.

## Quickstart

This section provides a quick guide to get started with ACE. For a full list of commands and their options, please refer to the [API Reference](docs/api.md).

### 1. Finding Differences

To find differences between nodes for a specific table, use the `table-diff` command. This command will compare the data in the specified table across all nodes in the cluster and generate a diff report if any inconsistencies are found.

**Example:**
```sh
./ace table-diff hetzner public.customers_large
```

**Sample Output (with differences):**
```
2025/07/18 13:45:40 [INFO] Cluster hetzner exists
2025/07/18 13:45:40 [INFO] Connections successful to nodes in cluster
2025/07/18 13:45:40 [INFO] Table public.customers_large is comparable across nodes
2025/07/18 13:45:40 [INFO] Using 16 CPUs, max concurrent workers = 16
2025/07/18 13:45:40 [INFO] Created 59 initial ranges to compare
  Hashing ranges: 177 / 177 [========================================================================================================================================================================] 1s | done
2025/07/18 13:45:42 [INFO] Initial hash calculations complete. Proceeding with comparisons for mismatches...
2025/07/18 13:45:42 [INFO] Table diff comparison completed for public.customers_large
2025/07/18 13:45:42 [INFO] Diff report written to public_customers_large_diffs-20250718134542.json
Table diff completed
```

If no differences are found, ACE will indicate that, and no diff file will be created.

**Sample Output (no differences):**
```
2025/07/18 13:47:55 [INFO] Cluster hetzner exists
2025/07/18 13:47:55 [INFO] Connections successful to nodes in cluster
2025/07/18 13:47:55 [INFO] Table public.customers_large is comparable across nodes
2025/07/18 13:47:55 [INFO] Using 16 CPUs, max concurrent workers = 16
2025/07/18 13:47:55 [INFO] Created 59 initial ranges to compare
  Hashing ranges: 177 / 177 [========================================================================================================================================================================] 1s | done
2025/07/18 13:47:57 [INFO] Initial hash calculations complete. Proceeding with comparisons for mismatches...
2025/07/18 13:47:57 [INFO] Table diff comparison completed for public.customers_large
2025/07/18 13:47:57 [INFO] No differences found. Diff file not created.
Table diff completed
```

### 2. Repairing Differences

Once a diff file has been generated, you can use the `table-repair` command to resolve the inconsistencies. You will need to specify the diff file and a "source of truth" node, which is the node that has the correct data.

**Example:**
```sh
./ace table-repair --diff-file=public_customers_large_diffs-20250718134542.json --source-of-truth=n1 hetzner public.customers_large
```

**Sample Output:**
```
2025/07/18 13:46:36 Table repair task validated and prepared successfully.
2025/07/18 13:46:36 Starting table repair for public.customers_large on cluster hetzner
2025/07/18 13:46:36 Processing repairs for divergent node: n2
2025/07/18 13:46:36 spock.repair_mode(true) set on n2
2025/07/18 13:46:36 session_replication_role set on n2 (fire_triggers: false)
2025/07/18 13:46:36 Executed 99 upsert operations on n2
2025/07/18 13:46:36 spock.repair_mode(false) set on n2
2025/07/18 13:46:36 Transaction committed successfully on n2
2025/07/18 13:46:36 Table repair for public.customers_large completed successfully.
2025/07/18 13:46:36 Total operations: map[n2:map[deleted:0 upserted:99]]
2025/07/18 13:46:36 *** SUMMARY ***
2025/07/18 13:46:36 n2 UPSERTED = 99 rows

2025/07/18 13:46:36 n2 DELETED = 0 rows
2025/07/18 13:46:36 RUN TIME = 0.00 seconds
```