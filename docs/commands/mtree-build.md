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
| `--max-cpu-ratio` |  | Max CPU ratio for parallel ops | `0.5` |
| `--override-block-size` |  | Skip safety checks | `false` |
| `--analyse` |  | Run `ANALYZE` before build | `false` |
| `--recreate-objects` |  | Drop & recreate Merkle objects | `false` |
| `--write-ranges` |  | Write computed ranges JSON | `false` |
| `--ranges-file` |  | Use pre-computed block ranges |  |
| `--quiet` |  | Suppress output | `false` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace mtree build --dbname=mydatabase my-cluster public.my_table
```
