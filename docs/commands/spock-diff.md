# spock-diff

Compares Spock metadata across nodes.

**Usage**
```
./ace spock-diff [flags] [cluster]
```

**Arguments**
- `[cluster]` — Optional; overrides `default_cluster`.

**Flags**
| Flag | Alias | Description | Default |
|------|-------|-------------|---------|
| `--dbname` | `-d` | Database name |  |
| `--nodes` | `-n` | Nodes to include (comma or `all`) | `all` |
| `--output` | `-o` | Output format | `json` |
| `--debug` | `-v` | Debug logging | `false` |

**Example**
```sh
./ace spock-diff --dbname=mydatabase my-cluster
```
