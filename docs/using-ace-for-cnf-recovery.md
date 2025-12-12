# Using ACE for Catastrophic Node Failure Recovery

Catastrophic node failures (CNFs) leave a cluster with one node abruptly down mid‑replication. The failed node’s transactions may be partially replicated; survivors can drift. ACE helps you scope and repair the drift by focusing on the failed node’s origin and an agreed cutoff, then repairing from the best survivor.

## What you need
- Spock metadata available on survivors (node names, origins, slots).
- A cutoff for the failed node’s commits (timestamp/LSN) so you can ignore churn after failure.
- Optional: a repair plan (YAML/JSON) if you need rule‑based actions (upsert‑only, coalescing, etc.).

## Workflow
1) **Capture an origin-scoped diff**  
   Run `table-diff` on survivors with `--only-origin <failed-node>` and optionally `--until <timestamp>` to fence at the last known commit from the failed node. You can combine a `--table-filter` to limit scope. The diff summary records `only_origin`, resolved node name, `until`, raw/effective filters, and an `origin_only` flag.

2) **Choose source of truth (SoT)**  
   In `table-repair --recovery-mode`, ACE:
   - Refuses origin-only diffs unless recovery-mode is set.
   - If `--source-of-truth` is not provided, probes survivors for Spock origin LSN (preferred) and slot LSN (fallback) for the failed node and picks the highest. Ties or missing LSNs require an explicit SoT.
   - Records the SoT choice and LSN probes in the repair report.

3) **Repair**  
   Run `table-repair --recovery-mode ... --diff-file=<origin diff> [--repair-file plan.yaml]`. A repair plan is optional; if provided, it is applied after SoT selection.

4) **Validate**  
   Re-run `table-diff` (optionally without `--only-origin`) to confirm survivors converge. Repeat per table or filter chunk if needed.

## Notes and cautions
- If origin/slot LSNs are absent on survivors, auto-selection will fail; provide `--source-of-truth` explicitly.
- The `until` fence should reflect the failed node’s last trusted commit to avoid including churn after failure.
- Advanced plans are allowed in recovery-mode; use them for upsert-only/coalesce patterns instead of default delete/update behavior.
