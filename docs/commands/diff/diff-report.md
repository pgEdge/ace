# diff-report

Builds the HTML report of a finished diff from its result directory. The command does not connect to a database, so you can run it on any host that has the result directory, and run it again with other limits.

`table-diff --output html` and `mtree table-diff --output html` run this step at the end of the diff, so you need the command only to build the report again, or for a diff that ran with `--output json`. It also reads the per-table results of `repset-diff` and `schema-diff`.

**Usage**
```
./ace diff-report --diff-file <path> [flags]
```

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--diff-file` | `-f` | The result directory (`<schema>_<table>_diffs-<timestamp>/`), or the JSON report next to it; for a JSON report the command reads the directory with the same base name (**required**) |  |
| `--max-html-rows <int>` |  | How many rows, at most, the report shows for each node pair. `0` means `table_diff.max_html_rows` from `ace.yaml`, or `10000`. | `0` |
| `--max-html-value-length <int>` |  | The number of characters of a value that the report shows. A longer value is cut at the right side, with a mark that gives its full length. Key values are never cut. `0` means `table_diff.max_html_value_length` from `ace.yaml`, or `1024`. | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Output**

The HTML file is written next to the result directory, with the same base name and the `.html` extension. A new run overwrites it.

**What the report shows**

- For each node pair, the rows of the blocks in block order, and inside a block sorted by the text of the primary key. This order is only for display: it can differ from the order of the database (for example, `10` comes before `9`), but the same diff always gives the same order. Rows of all kinds are shown together: value differences and rows missing on one node.
- At most `--max-html-rows` rows for each pair. The last block that the report reads can be cut: it shows the first rows of that block file, which are not in key order. A row with no visible difference (see below) does not count toward the limit.
- When a value of a row is cut, the editor of a custom repair action for that row starts empty, so that a repair does not write a cut value.
- The number of rows that the report does not show, by kind.
- A row that exists on both nodes but shows no difference after the values are turned into text (for example `1` against `"1"`) is not shown; the report gives the number of such rows of the blocks it read.

The command needs the result directory. A JSON report without its result directory, for example one written by an older ACE, cannot be read.

**Example**
```sh
./ace diff-report --diff-file=public_customers_diffs-20260106120353 --max-html-rows=50000
```

**The result directory**

A diff that finds differences writes, next to the JSON report, a directory with the same base name:

- `manifest.json`: the table, the key, the columns and their types, the nodes, the format version, the version of the row hash, the counts of each node pair and the diff summary. A result is complete when its directory has a manifest and the JSON report with the same base name is next to it. `diff-report` does not read a directory without the JSON report: such a directory is left by a run that stopped before it published the report.
- One directory for each node pair, with one file for each compared block that has differences, for example `n1_n2/00003-000000017.jsonl`: the part of the table and the number of the block in that part. The names sort in block order.
- Each line of a block file is one differing row: its kind (`node1_only`, `node2_only` or `changed`), the row of each node and the row hash of each node.

A diff that fails, times out or is stopped writes no result. The directory and its files are readable only by their owner, because they hold table data.
