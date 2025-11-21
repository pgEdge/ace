# Changelog

All notable changes to ACE will be captured in this document. The project follows semantic versioning; the latest changes appear first.

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

