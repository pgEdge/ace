# Changelog

All notable changes to ACE will be captured in this document. This project follows semantic versioning; the latest changes appear first.

## [v2.0.0] - 2026-04-21

### Added
- **Native PostgreSQL support.** `table-diff`, `table-repair`, and Merkle tree
  operations now work on vanilla PostgreSQL (14+) without the spock extension.
  ACE auto-detects whether spock is installed on each node and branches
  accordingly; `spock-diff` and `repset-diff` return a clear error when spock
  is not present.
- Native PG alternatives for replication origin and slot LSN queries using
  `pg_subscription` and `pg_replication_origin` catalog tables.
- `--against-origin` now works on native PG logical replication setups by
  resolving replication origin IDs to subscription names.
- Integration test suite for native PostgreSQL covering table-diff, table-repair
  (unidirectional, bidirectional, fix-nulls, dry-run), Merkle tree operations,
  and origin-tracked replication with repair.

### Changed
- ACE schema name in SQL templates is now quoted with `pgx.Identifier.Sanitize()`
  to prevent SQL breakage with non-simple schema names (e.g., mixed case,
  hyphens).
- `spock.xact_commit_timestamp_origin()` replaced with the standard PostgreSQL
  function `pg_xact_commit_timestamp_origin()` in the `--against-origin` filter.
  The two are functionally identical (spock's implementation is a thin wrapper
  around the same PG core function).
- Spock detection is now per-node and lazy, supporting mixed clusters where some
  nodes have spock and others do not.
- `SpockNodeNames` renamed to `NodeOriginNames` throughout the codebase to
  reflect dual-mode (spock + native PG) usage.

### Fixed
- Recovery-mode auto-selection (`autoSelectSourceOfTruth`) silently used native
  PG LSN queries on spock clusters because the connection pool was stored after
  `fetchLSNsForNode` returned instead of before.
- `aceSchema` template function used `config.Cfg` (not thread-safe) instead of
  `config.Get()`, risking data races during concurrent SIGHUP reloads.
- `repset-diff` child table-diff tasks did not inherit the parent's TaskStore,
  causing each to open its own SQLite connection.
- `isSpockAvailable` swallowed errors, causing callers to silently fall back to
  native PG mode on detection failures.
- Native PG subscription matching used LIKE substring patterns that could
  collide on similar node names (e.g., `n1` matching `n10`); now uses regex
  word boundaries.

## [v1.9.0] - 2026-04-17

### Added
- `--until` flag for `mtree` build/diff commands to bound operations by commit timestamp. 

### Changed
- Switched SQLite driver from cgo `mattn/go-sqlite3` to pure-Go `modernc.org/sqlite`. Enables fully static binaries on every platform and fixes a latent bug where darwin/windows builds silently dropped the sqlite driver at runtime.

### Fixed
- `table-diff` could OOM when a node became unresponsive mid-run. Workers had no way to stop and would grind through every remaining sub-range (each waiting ~60 s for context deadline), accumulating errors and log output. Added a circuit breaker that short-circuits all workers — including the initial hash phase — as soon as any node error is recorded. Backed by `atomic.Bool` to avoid mutex overhead on the common path.
- `--until` correctly handles frozen rows (NULL commit timestamps after freeze).
- Merkle tree row-hash and fetch-rows queries now quote identifiers via `pgx.Identifier.Sanitize()` instead of interpolating raw qualified names.
- Spock origin filter is validated as an integer via `strconv.Atoi` rather than string-escaped; non-numeric values are rejected.

### Security
- Go directive bumped 1.25.4 → 1.26.0; release builds use Go 1.26.2, resolving stdlib CVEs including CVE-2025-68121 (CRITICAL).
- Upgraded `moby/buildkit` v0.27.1 → v0.28.1 (CVE-2026-33747, CVE-2026-33748).
- Upgraded `go.opentelemetry.io/otel/sdk` + exporters to v1.43.0 (CVE-2026-39882, CVE-2026-39883).
- Upgraded `google.golang.org/grpc` v1.79.1 → v1.80.0 (CVE-2026-33186).
- Switching to distroless/static-debian12 removes libc6 and libssl3 from the image entirely, eliminating the class of CVEs reported against those packages (including CVE-2025-27587).
- GitHub Actions pinned to commit SHAs; `goreleaser` image pinned to v2.15.2.

## [v1.8.1] - 2026-04-10

### Changed
- Prevent overlapping scheduled job runs.
  The scheduler now uses singleton mode so a job that exceeds its
  `run_frequency` will not start a second concurrent instance.

### Fixed
- Fix connection pool leak when table validation fails.
  `RunChecks()` did not close its connection pool on error paths. Leaked
  connections accumulated across scheduled runs — one per failing table
  per tick — and could exhaust database connection limits over time.

- Apply max_connections cap to discovery pools.
  Repset-diff and schema-diff metadata queries now respect the configured
  `max_connections` limit instead of using the pgxpool default.

## [v1.8.0] - 2026-04-08

### Added
- `--max-connections` / `-M` flag for `table-diff`, `repset-diff`, and `schema-diff` to cap the number of database connections per node. Also configurable via `table_diff.max_connections` in `ace.yaml` and the HTTP API. Workers queue for connections when the pool is full rather than failing.

### Fixed
- Connection pool size for `table-diff` (and by extension `repset-diff` and `schema-diff`) was unbounded — defaulting to `max(4, NumCPU)` per node regardless of the concurrency factor. On high-core machines this could exhaust database connections. Pool size is now derived from the concurrency factor (`NumCPU × ConcurrencyFactor`, minimum 4).

### Changed
- Added missing integration tests to CI: repset-diff, schema-diff, Merkle numeric scale invariance, catastrophic single-node failure recovery, and timestamp comparison tests.

## [v1.7.2] - 2026-04-01

### Fixed
- Fix precision loss for large bigint and numeric primary keys during table repair. Previously it was possible that type conversions may cause the incorrect key to be used.

## [v1.7.1] - 2026-03-23

### Added
- End-of-run summary for `schema-diff` and `repset-diff` listing identical, skipped, differed, missing, and errored tables with error reasons.

### Changed
- `schema-diff` and `repset-diff` now query tables from all nodes and report tables not present on every node, instead of silently using only the first node's table list.  still compared across all nodes.
- `repset-diff` reports asymmetric repset membership when a table is in the repset on some nodes but not others.

### Fixed
- `schema-diff` and `repset-diff` silently excluded tables that failed during per-table comparison (e.g. missing primary key). Failed tables now appear in the summary with the error reason, and the task status is set to FAILED.

## [v1.7.0] - 2026-03-18

### Added
- `table-repair --preserve-origin` flag to preserve replication origin node ID, LSN, and per-row commit timestamps during repair operations. Repaired rows retain the original source node's origin metadata instead of being stamped with the local node's identity, preventing replication conflicts when a failed node rejoins the cluster. Upserts are grouped by (origin, timestamp) into separate transactions to satisfy PostgreSQL's per-transaction replication origin session constraint; deletes commit in a preceding transaction.
- Runtime configuration reload via SIGHUP for all long-running ACE modes. The scheduler waits for in-flight jobs to complete before swapping config; the API server reloads immediately, including mTLS security config (CRL and allowed CN list). Works for `ace start`, `ace server`, and `ace start --component=all`.
- End-of-run summary for `schema-diff` and `repset-diff` listing identical, skipped, differed, missing, and errored tables with error reasons.

### Changed
- CLI migrated from urfave/cli v2 to v3 for native interspersed flag support, allowing flags to be placed before or after subcommands.
- Config reads are now thread-safe and snapshotted per HTTP request, per Merkle-tree task, and per CDC replication stream to prevent mid-operation drift during concurrent SIGHUP reloads.
- mTLS certificate validator is swapped atomically on SIGHUP using `atomic.Pointer` for lock-free reads on the request path.
- `schema-diff` and `repset-diff` now query tables from all nodes and report tables not present on every node, instead of silently using only the first node's table list.  still compared across all nodes.
- `repset-diff` reports asymmetric repset membership when a table is in the repset on some nodes but not others.

### Fixed
- `repset-diff` was not working. Fixed and added tests.
- `schema-diff --skip-tables` now actually filters tables. Schema-qualified names (e.g. `myschema.mytable`) are also accepted; the schema prefix is validated against the target schema and stripped for matching.
- Replication origin LSN lookup rewritten to join through `spock.subscription` and `spock.node` instead of a broken LIKE pattern that never matched.
- `executeUpserts` no longer calls `resetReplicationOriginSession` before `tx.Commit()`, which was clearing the origin from WAL commit records.
- Unexpected scheduler exit in the SIGHUP reload loop is now handled gracefully.
- `schema-diff` and `repset-diff` silently excluded tables that failed during per-table comparison (e.g. missing primary key). Failed tables now appear in the summary with the error reason, and the task status is set to FAILED.

## [v1.6.0] 2026-02-25

### Changed
- To accommodate value comparison fix below, changed row hash algorithm and added algorithm versioning
    - If using Merkle Trees, it will automatically update to the new format when detected.
      Alternatively, you can reinitialize the trees.
- Batch concat_ws calls to support tales with 100+ columns
- Avoid extra memory usage when writing JSON table diff to output file

### Fixed
- Fixed OOM in recursive diff by limiting goroutine concurrency
- Fixed column comparisons with equal values but different scales (e.g., 3000.00 vs 3000.0).
- Fixed timestamp without timezone column value repair
- Close connections in table diff sooner

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
