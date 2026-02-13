# Changelog

All notable changes to ACE will be captured in this document. This project follows semantic versioning; the latest changes appear first.

## [v1.6.0-beta] 2026-02-14

### Changed
- To accommodate value comparison fix below, changed row hash algorithm and added algorithm versioning
    - If using Merkle Trees, it will automatically update to the new format when detected.
      Alternatively, you can reinitialize the trees.

### Fixed
- Fixed column comparisons with equal values but different scales (e.g., 3000.00 vs 3000.0).

## [v1.5.5] - 2026-02-12

### Changed
- Change --concurrency-factor from int multiplier to float64 CPU ratio. The number of workers is based on the number of cores times this factor. The default is now 0.5.

## [v1.5.4] - 2026-01-28

### Changed
- Add 3rd party license notice

## [v1.5.3] - 2026-01-15

### Changed
- Update copyrights to 2026

### Fixed
- UUID data type handling for table diff and repair fixed.

## [v1.5.2] - 2025-12-30

### Added
- Repair plans can set `allow_stale_repairs: false` per action to skip repairs when the target row has a newer commit timestamp than the diff snapshot, with skipped rows logged to `reports/<YYYY-MM-DD>/stale_repair_skips_<HHMMSS.mmm>.json`.
- HTTP API documentation plus an OpenAPI 3.1 spec covering async task endpoints.
- Certificate/mTLS setup guide for the HTTP API server and Postgres client cert auth.

### Changed
- Documentation navigation now links the HTTP API docs, certificate guide, and OpenAPI spec.

## [v1.5.1] - 2025-12-22

### Changed
- Advanced repair terminology updated from `--repair-file` to `--repair-plan` across CLI/docs and HTML diff report export text.

## [v1.5.0] - 2025-12-18

### Added
- Advanced repair plans (`--repair-file`) for `table-repair`, including rule-based actions, row overrides, and custom rows, plus new docs and examples.
- Catastrophic node failure recovery workflow: `table-diff --against-origin` with optional `--until` fencing and `table-repair --recovery-mode` auto-selecting a source of truth using Spock origin/slot LSNs.
- HTML diff reports can now build and export starter repair plans with bulk actions, custom helpers, and copy/download controls.

### Changed
- Table filters are applied inline (no filtered views), and diff summaries now record the effective filter that combines table filters with origin/until constraints.
- Spock origin identifiers in diff metadata are resolved to node names when available.

### Fixed
- Dockerfile honors `TARGETARCH` for multi-arch builds.
- Repair-plan row overrides now compare numeric primary keys reliably.

## [v1.4.2] - 2025-12-03

### Added
- Installation guide covering `go install`, release tarballs, and a Docker quickstart that links to the full container docs.

### Changed
- CLI entrypoint moved from `cmd/server` to `cmd/ace`, so `go install github.com/pgedge/ace/cmd/ace@…` produces an `ace` binary (no more `server`).
- GoReleaser, README build snippets, and helper scripts updated to the new `cmd/ace` path.

### Fixed
- Avoids user confusion where `go install` previously emitted a `server` binary.

## [v1.4.1] - 2025-12-03

### Added
- `table-repair --fix-nulls` mode (with dry-run support) to cross-fill NULL-only drifts without a single source-of-truth, plus broader datatype coverage (arrays, JSON/JSONB, bytea, intervals) and dedicated integration tests.
- CDC can run snapshot-style drains to the current WAL flush LSN, periodically flushes metadata via the new `mtree.cdc.cdc_metadata_flush_seconds` setting, and exposes `--skip-cdc` to bypass replication updates before Merkle diffs.
- Documentation updates: Docker usage guide (GHCR images, config generation, ephemeral and long-running modes), design docs promoted into the docs nav, and mermaid diagrams enabled for architecture pages.

### Changed
- Connection pool lifecycle and privilege handling tightened across CLI/API paths; API handlers now require client roles, repairs set roles per transaction, and connection options avoid unnecessary privilege drops.
- Table-diff now records the base table and table filter in diff summaries, cleans up filtered materialized views automatically, and handles JSON/bytea/interval/array casting more defensively.

### Fixed
- CDC processing handles slot `confirmed_flush_lsn` fallbacks, stops once it reaches the caller’s high-water mark, and flushes metadata during/after streaming to avoid re-chasing new WAL.
- Table-repair validates diff-file schema/table metadata, enforces consistent column definitions across nodes, and closes connection pools reliably across all repair paths.

## [v1.4.0] - 2025-11-24

### Added
- REST API server secured with mutual TLS (configurable allowed CNs and optional CRL) that runs table-diff, table-rerun, table-repair, schema/repset/spock diffs, and Merkle tree init/build/update/diff/teardown as tracked tasks in a SQLite-backed store; runnable via `ace server` or `ace start --component api`.
- Client certificate authentication for PostgreSQL connections via the new `cert_auth` settings, allowing ACE to use user cert/key/CA pairs instead of passwords when talking to mTLS-secured nodes.
- `max_diff_rows` guardrail for table-diff runs (configurable in `ace.yaml` or API payloads) that stops comparisons once the threshold is hit and marks JSON/HTML reports when early termination occurs.

### Changed
- All CLI commands now expose short flag aliases and updated command reference docs; `ace start` also gained a `--component` selector to run just the scheduler, just the API server, or both.
- Release automation and packaging were reworked: GoReleaser now builds CGO-enabled Linux binaries (amd64/arm64) alongside non-CGO macOS/Windows artifacts, and a GitHub Actions workflow publishes tagged releases.

### Fixed
- Table filters now create deterministic, sanitised filtered view names per task to avoid collisions and invalid identifiers when `--table-filter` is used.

## [v1.3.5] - 2025-11-06

### Added
- Background task scheduler capable of running table-diff, schema-diff, and repset-diff jobs on a cron or fixed interval cadence, including run-on-start semantics and shared job definitions.
- Persistent SQLite-backed task store that records job metadata, execution status, and timing so schedules and operators have historical visibility.
- `default_cluster` config option plus supporting docs to eliminate the need to pass the cluster name on every CLI invocation once set.
- Official multi-stage Dockerfile that fetches a published ACE release tarball, installs it into a distroless base image, and wires up `/etc/ace/ace.yaml`.
- Comprehensive Go API and scheduling documentation covering the new automation flows.

## [v1.3.4] - 2025-10-22

### Fixed
- `ace --help`, `ace help …`, `ace config init`, and `ace cluster init` now skip configuration checks, so bootstrapping ACE no longer fails when `ace.yaml` is missing.
- All other commands still abort early with a clear error if a config file is not found, making misconfiguration obvious.

## [v1.3.3] - 2025-10-21

### Added
- Native support for PostgreSQL `pg_service.conf` files (including `ACE_PGSERVICEFILE` and `PGSERVICEFILE` overrides) so ACE can auto-discover cluster and node definitions without JSON stubs.

### Changed
- Service-file parsing merges base and per-node settings, validates required host/database attributes, and respects node filters before running diffs.
- Updated CLI options and documentation to explain the new service-file workflow.

## [v1.3.2] - 2025-10-18

### Changed
- Configuration discovery now walks `ACE_CONFIG`, the working directory, `$HOME/.config/ace/ace.yaml`, and `/etc/ace/ace.yaml`, dramatically simplifying shared installs.
- When no config file is present, ACE exits immediately with `config file 'ace.yaml' not found`, preventing confusing downstream failures.

## [v1.3.1] - 2025-10-17

### Added
- HTML diff report output (paired with JSON/CSV) that presents per-node summaries, highlights differing cells, and bundles metadata for sharing with stakeholders.

### Changed
- Table-diff runtime received major performance optimisations: per-task contexts, parallel leaf-hash recomputation, and more efficient goroutine usage cut latency for large tables.
- GoReleaser settings were updated so the new artifacts ship with every tagged release.

## [v1.3.0] - 2025-09-30

### Added
- Merkle tree subsystem (init/build/update/listen/table-diff/teardown) that maintains per-table rolling hashes to detect drift without rescanning entire relations.
- CDC listener now streams from PostgreSQL’s native `pgoutput` plugin for lower overhead Merkle updates.
- Automatic Merkle range split/merge plus transactional writes keep trees balanced and consistent as workloads fluctuate.
- Merkle objects moved into the dedicated `spock` schema and a `visualise.sh` helper script renders tree hierarchies for debugging.
- Additional docs and integration tests covering Merkle workflows and a key boundary-calculation fix for accurate diff windows.

## [v1.2.0] - 2025-07-23

### Added
- Centralised logger abstraction adopted across the CLI/server so every module emits consistent, leveled output.
- Shared helper functions consolidated into `pkg/common/utils.go`, reducing duplication and paving the way for future configuration improvements.
- GitHub Actions CI now runs the full integration test suite on every push and pull request.

### Changed
- Repository licensing and documentation were aligned with PostgreSQL License requirements: added LICENSE, propagated headers to all files, and reorganised long-form API docs.

## [v1.1.0] - 2025-07-16

### Added
- `schema-diff` for whole-schema comparisons (with optional per-table diffs) plus a DDL-only inspection mode.
- `repset-diff` to iterate table-diff across all tables in a replication set and aggregate the findings.
- `spock-diff` to validate Spock metadata, slots, and replication state across nodes.
- `table-rerun` to re-check only the rows captured in a previous diff report and confirm whether repairs succeeded.
- Diff reports now include commit timestamps and origin metadata for better provenance tracking.

## [v1.0.0] - 2025-06-30

### Added
- Initial ACE release featuring `table-diff` with rich tuning flags (block size, concurrency, compare units, table filters, and JSON/CSV output).
- `table-repair` workflow that consumes a diff report and applies repairs with safeguards such as dry-run, insert-only/upsert-only, and trigger controls.
- Baseline configuration workflow using `ace.yaml` plus cluster definitions, establishing the foundation for subsequent automation.
