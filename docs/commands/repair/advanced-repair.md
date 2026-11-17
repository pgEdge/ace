# Advanced repair (repair plans)

`table-repair` can run from a repair plan instead of a single source-of-truth flag. The repair plan is a versioned YAML/JSON document that describes per-table defaults, ordered rules, and explicit row overrides. This lets you pick different sources of truth (or custom rows) per batch, and keep a reproducible plan on disk.

## File structure and precedence

- Top level: `version`, optional `default_action`, `tables` map (`schema.table` keys).  
- Per table: `default_action`, ordered `rules`, and `row_overrides`.  
- Precedence: `row_overrides` (exact PK) > first matching `rule` > `table.default_action` > global `default_action`. Rules must declare at least one selector to avoid match‑all accidents.

### Values and keys

A value in a plan is the text of a YAML scalar, as the file writes it: `1.50`, `007` and `true` stay `1.50`, `007` and `true`. `null`, `~` or no value is NULL; a quoted `"null"` is the text `null`. The rows of a diff hold every value as the text that the server prints for it, so:

- A key in `pk_in.equals` or `row_overrides` matches a row when its text is the key text of the row, byte for byte. Write the key as the server prints it: `1.50` does not match the key `1.5` of a `numeric` column, and a timestamp key must be written in the form of the diff, such as `"2026-01-02 03:04:05+00"`. Quote a key that YAML could read in another way.
- A range in `pk_in`, the comparisons of `when` and `pick_freshest` are computed on a node with the type of the column: ACE sends both texts to the server, and the server reads them with the input function of the type, for example `SELECT $1::timestamptz < $2::timestamptz`. So `n1.amount < 10` compares numbers, and `n1.updated_at > '2026-01-01'` compares timestamps. A comparison needs a column on at least one side. `IS NULL` and a comparison with `NULL` are decided without the server.
- A value of `custom_row` is sent as text, and the server reads it with the type of its column.

### Selectors
- `pk_in`: list of PK values, and/or ranges (`from`/`to`) for simple PKs; composite PKs use ordered tuples.  
- `diff_type`: one or more of `row_mismatch`, `missing_on_n1`, `missing_on_n2`, `deleted_on_n1`, `deleted_on_n2`.  
- `columns_changed`: match if any listed column differs.  
- `when`: predicate over `n1.<col>` / `n2.<col>`; supports `= != < <= > >=`, `IN (...)`, `IS [NOT] NULL`, `AND/OR/NOT`, parentheses, string/number/bool/null literals.

### Actions (verbs)
- `keep_n1`, `keep_n2`: copy that side.  
- `apply_from` with `from: n1|n2` and `mode: replace|upsert|insert` (default `replace`). `insert` is only valid on missing rows.  
- `bidirectional`: copy a row that only one node has to the other node, as `--bidirectional` does. It is refused on `row_mismatch`: to make two different rows equal, choose a winner with `keep_n1`, `keep_n2`, `apply_from` or `custom`.  
- `custom`: provide `custom_row` (with optional `{{n1.col}}` / `{{n2.col}}` templating) and/or helpers:  
  - A template is a whole value that starts with `{{` and ends with `}}`; spaces around it are ignored. Any other value is written as text, as it is: a value with `{{` but no closing `}}`, such as `"{{n1.email"`, and a value with a template inside other text, such as `"id {{n1.id}}"`. There is no substitution inside text, so a value can hold literal braces with no escape.  
  - A template gives the value of the column on that node, or NULL if the value is NULL. Inside the braces it must be `n1.column` or `n2.column`. A template of another form, or one that names a column the table does not have, or a node on which the row is missing, makes table-repair refuse the plan before any write. table-repair checks a template only when it resolves it for a diff row, that is, in the action that a row of this diff selects. A template in a rule or an override that no row selects is not checked, so a run without errors, a dry run too, does not show that every template of the plan is valid. A YAML `null` writes NULL. A column that `custom_row` sets, even to NULL, is not filled by the helpers.  
  - `helpers.coalesce_priority: [n1, n2]` fills remaining columns from the first non‑NULL source.  
  - `helpers.pick_freshest: { key: updated_at, tie: n1|n2 }` fills remaining columns from the side with the greater value of `key`, compared with the type of the column (or of `commit_ts`). If the row exists on one node only, that node wins. With rows on both nodes, a side with a value wins over a side with NULL; on equal values or two NULLs the `tie` side wins. `tie` is required.  
- `skip`: do nothing.  
- `delete`: remove the row (from the side(s) where it exists).
- Action option: `allow_stale_repairs` (default `true`) set to `false` to skip repairs when the current row on the target node has a newer commit timestamp than the diff snapshot. Skipped rows are logged to `reports/<YYYY-MM-DD>/stale_repair_skips_<HHMMSS.mmm>.json`.
- Spock metadata in plans: you can reference `commit_ts` and `node_origin` in `when` predicates and `custom_row` templates (e.g., `{{n1.commit_ts}}`, `when: "n2.node_origin = 'n3'"`). These fields are injected during diff collection (via `pg_xact_commit_timestamp(xmin)` and `spock.xact_commit_timestamp_origin(xmin)`), not stored in your table.

### Compatibility checks (fail fast)
- `keep_n1`/`apply_from n1` require a row on n1; `keep_n2`/`apply_from n2` require a row on n2.  
- `apply_from mode: insert` and `bidirectional` are invalid on `row_mismatch`.  
- `diff_type` is validated; incompatible action/diff combos are rejected at parse time and at execution.  
- `custom` requires `custom_row` or `helpers`.
- `custom` cannot change the primary key: a key column in `custom_row` must have the key text of the diff row, such as `id: 42` for the row with key `42`, or a template such as `{{n1.id}}`. Otherwise the plan is refused before any write.

### Diff timing and stale rows
Repair plans (and `table-repair` in general) only act on the rows captured in the diff file. The plan engine does not re-fetch current row values at repair time, and `when` predicates/helpers only see `n1`/`n2` values (plus optional `commit_ts`/`node_origin`) from the diff snapshot. If you need protection against stale repairs, set `allow_stale_repairs: false` on the relevant action: ACE will compare the target row's current commit timestamp with the diff snapshot and skip repairs where the current row is newer. Skipped rows are logged (see above). Re-running `table-diff` close to the repair window is still recommended when consistency is critical.

## Running with a repair plan

```bash
./ace table-repair <cluster> <schema.table> \
  --diff-file=public_customers_diffs-20251210.json \
  --repair-plan=repair.yaml \
  --dry-run  # optional
```

Notes:
- When `--repair-plan` is provided, `--source-of-truth` is not required.  
- Dry-run and reports include rule usage counts per node.  
- `--fix-nulls` and `--bidirectional` cannot be combined with a repair plan (they’re separate modes).

## Minimal skeleton

```yaml
version: 1
default_action:
  type: skip
tables:
  public.customers:
    default_action:
      type: keep_n1
    rules:
      - name: eu_to_n2
        diff_type: [row_mismatch, missing_on_n2]
        when: "n1.region = 'eu'"
        action:
          type: keep_n2
      - name: coalesce_contact
        columns_changed: [email, phone]
        action:
          type: custom
          helpers:
            coalesce_priority: [n1, n2]
    row_overrides:
      - name: fix_customer_42
        pk: { id: 42 }
        action:
          type: custom
          custom_row:
            id: 42
            status: "vip"
            email: "{{n1.email}}"
```
