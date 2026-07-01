# mtree table-diff

Compares Merkle trees of a table across nodes to find inconsistencies.  
By default, updates trees first using CDC.

If `mtree listen` is running and holds the replication slot, `table-diff` skips
the CDC drain for that node (printing a warning) and compares against the
listen-maintained tree instead of failing. The comparison reflects `listen`'s
last-applied state. Stop `mtree listen` first if you need a guaranteed-current
drain.

**Usage**

```
./ace mtree table-diff [flags] [cluster] <schema.table>
```

**Arguments**

- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--max-cpu-ratio` | `-m` | Max CPU ratio | `0.5` |
| `--output` | `-o` | `json` or `html` | `json` |
| `--skip-cdc` | `-U` | Skip CDC processing (only rehash and compare) | `false` |
| `--cdc-timeout` |  | Seconds to drain CDC before giving up (`0` = use `cdc_processing_timeout` / default) | `0` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace mtree table-diff --dbname=mydatabase my-cluster public.my_table
```

**Notes**

- With `--output html`, both JSON and HTML reports are generated with matching timestamps.
