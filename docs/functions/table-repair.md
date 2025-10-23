# table-repair

## Using the ACE table-repair Command

The `table-repair` command fixes data inconsistencies identified by `table-diff`. It uses a specified node as the *source of truth* to correct data on other nodes.

**Typical scenarios**

- **Spock exception repair**: Resolve conflicts from insert/update/delete exceptions.
- **Network partition repair**: Re‑align nodes after a partition.
- **Temporary node outage**: Catch a lagging node up.

**Safety & audit features**

- **Dry run mode** — see proposed changes without modifying data.
- **Report generation** — write a detailed audit of actions taken.
- **Upsert‑only** — prevent deletions on non‑source nodes.
- **Transaction safety** — changes are atomic; partial failures are rolled back.

**Remember**

- Use `--dry-run` first to review changes.
- Use `--upsert-only` or `--insert-only` for sensitive tables to avoid deletes.
- `table-repair` only fixes rows present in the diff file. If the diff exceeds `MAX_ALLOWED_DIFFS`, only a partial repair is possible (consider repairing in batches: diff → repair → diff).
- For very large, multi‑table incidents, a dump/restore may be more practical.

---

## Syntax

```bash
./pgedge ace table-repair <cluster_name> <schema.table_name> --diff-file=<diff_file> [--source-of-truth=<node>] [options]
```

- `cluster_name`: Name of the cluster.
- `schema.table_name`: Schema‑qualified table name to repair.
- `--diff-file`: Path to the diff JSON file.
- `--source-of-truth` (`-s`): Authoritative node for repairs. *Not required* when using `--bidirectional` or `--fix-nulls`.

## Options

| Option | Alias | Description | Default |
|---|---|---|---|
| `--dry-run` |  | Simulate repairs without changes | `false` |
| `--upsert_only` | `-u` | Only add/modify rows on non‑source nodes; skip deletes | `false` |
| `--generate_report` | `-g` | Write a JSON repair report to `reports/<YYYY-MM-DD>/report_<HHMMSSmmmm>.json` | `false` |
| `--dbname <name>` | `-d` | Database name |  |
| `--quiet` |  | Suppress non‑essential output | `false` |
| `--upsert-only` |  | Only inserts/updates; skip deletes | `false` |
| `--insert-only` | `-i` | Only inserts; skip updates/deletes (`ON CONFLICT DO NOTHING`) | `false` |
| `--bidirectional` | `-b` | With `--insert-only`, insert missing rows both ways (distinct union) | `false` |
| `--fix-nulls` |  | Repair NULLs using values observed across nodes (special case) | `false` |
| `--fire-triggers` |  | Allow triggers to fire; `ENABLE ALWAYS` triggers always fire | `false` |

## Example

```sh
./ace table-repair --diff-file=public_customers_large_diffs-20250718134542.json --source-of-truth=n1 acctg public.customers_large
```

## Sample Output

```
2025/07/22 12:05:24 INFO Starting table repair for public.customers_large on cluster acctg
2025/07/22 12:05:24 INFO Processing repairs for divergent node: n2
2025/07/22 12:05:24 INFO Executed 99 upsert operations on n2
2025/07/22 12:05:24 INFO Repair of public.customers_large complete in 0.003s. Nodes n2 repaired (99 upserted).
```
