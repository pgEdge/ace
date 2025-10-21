# Using ACE with Merkle Trees

For very large tables, you can use Merkle trees to find differences more efficiently. This method is faster because it doesn't require a full table scan on every comparison. Here's a quick guide to using Merkle trees.

**Step 1: Initialize Merkle Tree Objects**

First, you need to initialize the necessary database objects on all nodes in your cluster.

```sh
./ace mtree init hetzner
```

**Step 2: Build the Merkle Tree**

Next, build the Merkle tree for the table you want to compare. This process will divide the table into blocks and calculate hashes for each block.

```sh
./ace mtree build hetzner public.customers_large
```

**Step 3: Find Differences**

Now you can run the Merkle tree diff command. This will compare the trees on each node and report any inconsistencies. This will also create a diff file that can be used with the `table-repair` command.

```sh
./ace mtree table-diff hetzner public.customers_large
```

**Step 4: Repair Differences (Optional)**

If differences are found, you can repair them using the `table-repair` command, just like with a standard `table-diff`.

```sh
./ace table-repair --diff-file=<diff-file-from-mtree-diff> --source-of-truth=n1 hetzner public.customers_large
```

The Merkle trees can be kept up-to-date automatically by running the `mtree listen` command, which uses Change Data Capture (CDC) with the `pgoutput` output plugin to track row changes. Performing the `mtree table-diff` will update the Merkle tree even if `mtree listen` is not used.
