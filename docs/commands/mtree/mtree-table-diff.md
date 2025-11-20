# mtree table-diff

Compares Merkle trees of a table across nodes to find inconsistencies.  
By default, updates trees first using CDC.

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
| `--batch-size` | `-h` | Ranges per batch during diff | `100` |
| `--output` | `-o` | `json` or `html` | `json` |
| `--skip-update` | `-U` | Don’t apply CDC before diff | `false` |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**

```sh
./ace mtree table-diff --dbname=mydatabase my-cluster public.my_table
```

**Notes**

- With `--output html`, both JSON and HTML reports are generated with matching timestamps.
