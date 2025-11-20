# mtree build

Builds a Merkle tree for a table on all nodes (after `mtree init`).

**Usage**

```
./ace mtree build [flags] [cluster] <schema.table>
```

**Arguments**

- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**

| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--block-size` | `-b` | Rows per leaf block | `10000` |
| `--max-cpu-ratio` | `-m` | Max CPU ratio for parallel ops | `0.5` |
| `--override-block-size` | `-B` | Skip safety checks | `false` |
| `--analyse` | `-a` | Run `ANALYZE` before build | `false` |
| `--recreate-objects` | `-R` | Drop & recreate Merkle objects | `false` |
| `--write-ranges` | `-w` | Write computed ranges JSON | `false` |
| `--ranges-file` | `-k` | Use pre-computed block ranges |  |
| `--quiet` | `-q` | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree build --dbname=mydatabase my-cluster public.my_table
```
