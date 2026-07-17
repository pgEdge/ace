# Advanced repair examples

This page shows practical repair-plan snippets you can adapt. All examples assume `version: 1` at the top and a `tables:` section; only the relevant table entry is shown.

## 1) Classic source-of-truth per batch

Take most rows from `n1`, but EU rows from `n2`:
```yaml
tables:
  public.accounts:
    default_action: { type: keep_n1 }
    rules:
      - name: eu_from_n2
        diff_type: [row_mismatch, missing_on_n2]
        when: "n1.region = 'eu'"
        action: { type: keep_n2 }
```

## 2) Insert-only or upsert-only without global flags

Insert missing rows into `n2`; skip updates/deletes:
```yaml
tables:
  public.orders:
    default_action: { type: skip }
    rules:
      - name: insert_missing_n2
        diff_type: [missing_on_n2]
        action:
          type: apply_from
          from: n1
          mode: insert
```

Upsert (insert or update) into `n2`, but never delete:
```yaml
tables:
  public.orders:
    default_action: { type: skip }
    rules:
      - name: upsert_into_n2
        diff_type: [row_mismatch, missing_on_n2]
        action:
          type: apply_from
          from: n1
          mode: upsert
```

## 3) Bidirectional convergence for mismatches only

Copy rows both ways when they differ; leave single-sided misses untouched:
```yaml
tables:
  public.features:
    default_action: { type: skip }
    rules:
      - name: converge_mismatches
        diff_type: [row_mismatch]
        action: { type: bidirectional }
```

## 4) Coalesce (fix-nulls style) with helpers

Fill NULLs using non-NULL values preferring `n1`, then `n2`:
```yaml
tables:
  public.customers:
    rules:
      - name: coalesce_contact
        columns_changed: [email, phone]
        action:
          type: custom
          helpers:
            coalesce_priority: [n1, n2]
```

## 5) Pick freshest based on a timestamp

Keep the row with the newer `updated_at`; tie-break to `n1`:
```yaml
tables:
  public.inventory:
    rules:
      - name: pick_newer
        diff_type: [row_mismatch]
        action:
          type: custom
          helpers:
            pick_freshest:
              key: updated_at
              tie: n1
```

## 5b) Use Spock commit metadata

Pick the row with the newer replication commit timestamp (Spock metadata), else tie to n1:
```yaml
tables:
  public.inventory:
    rules:
      - name: pick_newer_commit
        diff_type: [row_mismatch]
        action:
          type: custom
          helpers:
            pick_freshest:
              key: commit_ts     # from _spock_metadata_
              tie: n1
```

## 5c) Use Spock node origin (route by producer)

Treat rows produced on node `n3` as authoritative; otherwise fall back to n1:
```yaml
tables:
  public.inventory:
    default_action: { type: keep_n1 }
    rules:
      - name: prefer_n3_origin
        diff_type: [row_mismatch, missing_on_n2]
        when: "n1.node_origin = 'n3'"
        action: { type: keep_n1 }
      - name: prefer_n3_origin_missing
        diff_type: [missing_on_n1]
        when: "n2.node_origin = 'n3'"
        action: { type: keep_n2 }
```

## 5d) Split by origin for batch decisions

Use n2 when the origin is n2, otherwise use n1:
```yaml
tables:
  public.orders:
    rules:
      - name: origin_n2
        diff_type: [row_mismatch, missing_on_n1, missing_on_n2]
        when: "n1.node_origin = 'n2' OR n2.node_origin = 'n2'"
        action: { type: keep_n2 }
      - name: default_to_n1
        action: { type: keep_n1 }
```

## 5e) Skip if target looks newer (snapshot-based)

Avoid overwriting rows that already appear newer on the target side in the diff snapshot (does not re-check current DB state):
```yaml
tables:
  public.orders:
    default_action: { type: keep_n1 }
    rules:
      - name: skip_if_target_newer
        diff_type: [row_mismatch]
        when: "n2.updated_at > n1.updated_at"
        action: { type: skip }
```

## 5f) Skip stale repairs (current commit timestamp)

Protect against overwriting rows that changed after the diff:
```yaml
tables:
  public.orders:
    rules:
      - name: n1_to_n2_no_stale
        diff_type: [row_mismatch, missing_on_n2]
        action:
          type: keep_n1
          allow_stale_repairs: false
```

## 5g) One-pass "latest commit wins" (complete, multi-table)

A ready-to-run plan that resolves every `row_mismatch` in a single pass by
keeping the row with the newer Spock commit timestamp, reused across several
tables via a YAML anchor. See
[`examples/repair-plan-latest-wins.yaml`](examples/repair-plan-latest-wins.yaml)
for the fully-commented version (opt-in alternatives, per-table override, and
the caveats in detail).

The core rule, shared by every table via the `&latest_wins` anchor:
```yaml
tables:
  public.inventory:
    rules: &latest_wins
      - name: latest_commit_wins
        diff_type: [row_mismatch]
        action:
          type: custom
          helpers:
            pick_freshest: { key: commit_ts, tie: n1 }
  public.orders:
    rules: *latest_wins
```

How each case resolves, all in one pass:

| Situation | Result |
| --- | --- |
| Both sides have `commit_ts`, and they differ | The newer one wins |
| One side's `commit_ts` is NULL | The side that still has a timestamp wins |
| Timestamps are equal, or both are NULL | The positional `tie` node |

Caveats:

- **`n1`/`n2` are positional, not node names.** For each compared pair, `n1`
  is the alphabetically-first node name and `n2` the second. `tie: n1` means
  "prefer the alphabetically-first node on a tie", not a node you can name; the
  plan DSL cannot target a physical node (that is what `--source-of-truth` is
  for).
- **`track_commit_timestamp` must be on** across all nodes, or a node's rows
  always read a NULL `commit_ts` regardless of recency.
- **`commit_ts` is compared as a string**, so "newer wins" is only reliable
  when all nodes emit it with the same UTC offset (ideally UTC).
- **Last-write-wins is not the same as always-correct** — a recent errant
  update beats an older good value.
- **A NULL `commit_ts` is rare.** PostgreSQL preserves a tuple's raw `xmin`
  across freezing (the frozen bit is set but `xmin` still returns the original
  xid), so a frozen row keeps a resolvable `commit_ts`. It only goes NULL once
  the xid ages past the commit-timestamp retention horizon — roughly 50M–200M
  transactions, tied to `vacuum_freeze_min_age` / `autovacuum_freeze_max_age` —
  and `pg_commit_ts` is truncated, or if `track_commit_timestamp` was off when
  the row was written.

Always run with `--dry-run --generate-report` first and review the per-row
decisions before applying.

## 6) Custom row per PK

Pin a specific PK to a hand-crafted row:
```yaml
tables:
  public.products:
    row_overrides:
      - name: fix_widget42
        pk: { id: 42 }
        action:
          type: custom
          custom_row:
            id: 42
            status: "retired"
            notes: "manual override"
```

## 7) Mixed SOT by ranges

PK 1–100 from `n1`, 101–200 from `n2`, everything else from `n1`:
```yaml
tables:
  public.accounts:
    default_action: { type: keep_n1 }
    rules:
      - name: range_101_200_n2
        pk_in:
          - range: { from: 101, to: 200 }
        action: { type: keep_n2 }
```

## 8) Delete stray rows on a target

Delete rows present only on `n2`:
```yaml
tables:
  public.logs:
    rules:
      - name: delete_extras_n2
        diff_type: [missing_on_n1]
        action: { type: delete }
```

## 9) Combine predicates

Only take `n2` for VIPs in EU where status changed:
```yaml
tables:
  public.users:
    rules:
      - name: vip_eu_from_n2
        diff_type: [row_mismatch]
        columns_changed: [status]
        when: "n1.region = 'eu' AND n1.tier = 'vip'"
        action: { type: keep_n2 }
```

## 10) Coalesce with templating

Build a row with an explicit status and templated columns:
```yaml
tables:
  public.tasks:
    rules:
      - name: coalesce_with_template
        diff_type: [row_mismatch, missing_on_n2]
        action:
          type: custom
          custom_row:
            id: "{{n1.id}}"
            status: "active"
            title: "{{n2.title}}"
          helpers:
            coalesce_priority: [n2, n1]
```
