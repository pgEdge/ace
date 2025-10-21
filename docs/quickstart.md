# Quickstart

!!! note

    Before performing the steps in the Quickstart, you must [create and modify the ACE configuration files](configuration.md).

This section provides a quick guide to get started with ACE. For a full list of commands and their options, please refer to the [API Reference](api.md).

### 1. Finding Differences

To find differences between nodes for a specific table, use the `table-diff` command. This command will compare the data in the specified table across all nodes in the cluster and generate a diff report if any inconsistencies are found.

**Example:**
```sh
./ace table-diff acctg public.customers_large
```

Add `--output html` to emit a colour-coded HTML diff report alongside the JSON diff. 


**Sample Output (with differences):**
```
2025/07/22 12:03:51 INFO Cluster acctg exists
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
2025/07/22 12:05:59 INFO Cluster acctg exists
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
./ace table-repair --diff-file=public_customers_large_diffs-20250718134542.json --source-of-truth=n1 acctg public.customers_large
```

**Sample Output:**
```
2025/07/22 12:05:24 INFO Starting table repair for public.customers_large on cluster acctg
2025/07/22 12:05:24 INFO Processing repairs for divergent node: n2
2025/07/22 12:05:24 INFO Executed 99 upsert operations on n2
2025/07/22 12:05:24 INFO Repair of public.customers_large complete in 0.003s. Nodes n2 repaired (99 upserted).
```

