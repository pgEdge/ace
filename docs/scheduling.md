# Scheduling ACE Runs

ACE can watch for drift on a schedule so you do not have to trigger every diff manually. There are two ways to automate checks:

1. **Ad-hoc runs from the CLI**: Add `--schedule` and `--every=<duration>` when invoking `table-diff`, `repset-diff`, or `schema-diff` to loop a single job until you cancel it.
2. **The background scheduler**: Define jobs in `ace.yaml`, then run `./ace start` to execute multiple jobs on a cadence (either by frequency or a cron expression).

## Ad-hoc Scheduling with CLI Flags

All diff commands accept the same pair of options:

- `--schedule` (no value): Switch the command from “run once” to “run on a timer”.
- `--every=<duration>`: Required when `--schedule` is set. Uses Go duration syntax (`5m`, `1h30m`, `12h`, etc.).

When you supply both flags, ACE performs an initial run immediately, waits for the duration, and then repeats until you press <kbd>Ctrl+C</kbd> (or otherwise terminate the process). The job inherits the rest of the CLI options (nodes, block-size, output format, and so on) for every run.

### Examples

```sh
# Compare a single table every 30 minutes
./ace table-diff --schedule --every=30m my-cluster public.orders

# Verify an entire replication set hourly, skipping a noisy table
./ace repset-diff --schedule --every=1h \
    --skip-tables=public.audit_log \
    acctg replication_set_1

# Validate a schema every night; "--ddl-only" cannot be combined with scheduling
./ace schema-diff --schedule --every=24h \
    --nodes=n1,n2 \
    my-cluster public
```

!!! note
    `schema-diff --schedule` works only when data comparisons are enabled. If you pass `--ddl-only`, the CLI will return an error because there is no table-level work to repeat.

## Running the Background Scheduler

For long-lived automation, define jobs in `ace.yaml` and let `./ace start` orchestrate them. The scheduler reads two top-level arrays:

- `schedule_jobs`: describes each job (what to run and with which arguments).
- `schedule_config`: pairs a job name with either a frequency or a cron expression and marks it enabled/disabled.

### Sample Configuration

```yaml
default_cluster: acctg

schedule_jobs:
  - name: nightly-orders
    type: table-diff            # table-diff, schema-diff, or repset-diff
    table_name: public.orders   # schema_name for schema-diff, repset_name for repset-diff
    args:
      dbname: acctg
      nodes: n1,n2
      block_size: 50000
      output: json

  - name: weekly-schema
    type: schema-diff
    schema_name: public
    cluster_name: reporting
    args:
      nodes: n1,n3

schedule_config:
  - job_name: nightly-orders
    run_frequency: 24h          # Go duration string (cannot be combined with crontab_schedule)
    enabled: true
  - job_name: weekly-schema
    crontab_schedule: "30 2 * * 1"   # standard cron syntax (minute hour day month weekday)
    enabled: true
```

Key points:

- `type` is optional when the target is unambiguous (for example, providing `table_name` implies `table-diff`).
- The `args` map mirrors CLI flags using snake_case keys (`block_size`, `concurrency_factor`, `compare_unit_size`, `override_block_size`, `table_filter`, etc.).
- Each entry in `schedule_config` must reference a job from `schedule_jobs`. Exactly one of `run_frequency` or `crontab_schedule` must be set, and `enabled` controls whether the scheduler registers the job.
- Jobs run once immediately and then follow the defined cadence. Failures are logged; ACE keeps the schedule alive unless you stop it.

### Starting the Scheduler

1. Ensure `ace.yaml` contains your `schedule_jobs` and `schedule_config` entries (you can start from the commented example in `internal/cli/default_config.yaml`).
2. Launch the scheduler:

   ```sh
   ./ace start
   ```

   Add `--debug` if you want verbose logging of every run.

3. Leave the process running (for example, under a supervisor or system service). Press <kbd>Ctrl+C</kbd> to stop it manually.

The scheduler reuses the same validation and guardrails as the on-demand commands, so jobs will bail out early if the underlying table or replication set cannot be diffed. Adjust the job definitions and retry.

For full details on defining jobs, see the `schedule_jobs` and `schedule_config` rows in [ACE Configuration](configuration.md#the-aceyaml-file).
