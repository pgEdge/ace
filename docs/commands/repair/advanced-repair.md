# Advanced repair (repair files)

`table-repair` can run from a repair file instead of a single source-of-truth flag. The repair file is a versioned YAML/JSON document that describes per-table defaults, ordered rules, and explicit row overrides. This lets you pick different sources of truth (or custom rows) per batch, and keep a reproducible plan on disk.

## File structure and precedence

- Top level: `version`, optional `default_action`, `tables` map (`schema.table` keys).  
- Per table: `default_action`, ordered `rules`, and `row_overrides`.  
- Precedence: `row_overrides` (exact PK) > first matching `rule` > `table.default_action` > global `default_action`. Rules must declare at least one selector to avoid match‑all accidents.

### Selectors
- `pk_in`: list of PK values, and/or ranges (`from`/`to`) for simple PKs; composite PKs use ordered tuples.  
- `diff_type`: one or more of `row_mismatch`, `missing_on_n1`, `missing_on_n2`, `deleted_on_n1`, `deleted_on_n2`.  
- `columns_changed`: match if any listed column differs.  
- `when`: predicate over `n1.<col>` / `n2.<col>`; supports `= != < <= > >=`, `IN (...)`, `IS [NOT] NULL`, `AND/OR/NOT`, parentheses, string/number/bool/null literals.

### Actions (verbs)
- `keep_n1`, `keep_n2`: copy that side.  
- `apply_from` with `from: n1|n2` and `mode: replace|upsert|insert` (default `replace`). `insert` is only valid on missing rows.  
- `bidirectional`: copy both ways (only meaningful on mismatches/missing rows).  
- `custom`: provide `custom_row` (with optional `{{n1.col}}` / `{{n2.col}}` templating) and/or helpers:  
  - `helpers.coalesce_priority: [n1, n2]` fills remaining columns from the first non‑NULL source.  
  - `helpers.pick_freshest: { key: updated_at, tie: n1|n2 }` fills remaining columns from the fresher side (numeric, string, or RFC3339 timestamps).  
- `skip`: do nothing.  
- `delete`: remove the row (from the side(s) where it exists).

### Compatibility checks (fail fast)
- `keep_n1`/`apply_from n1` require a row on n1; `keep_n2`/`apply_from n2` require a row on n2.  
- `apply_from mode: insert` is invalid on `row_mismatch`.  
- `diff_type` is validated; incompatible action/diff combos are rejected at parse time and at execution.  
- `custom` requires `custom_row` or `helpers`.

## Running with a repair file

```bash
./ace table-repair <cluster> <schema.table> \
  --diff-file=public_customers_diffs-20251210.json \
  --repair-file=repair.yaml \
  --dry-run  # optional
```

Notes:
- When `--repair-file` is provided, `--source-of-truth` is not required.  
- Dry-run and reports include rule usage counts per node.  
- `--fix-nulls` and `--bidirectional` cannot be combined with a repair file (they’re separate modes).

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
