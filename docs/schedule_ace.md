# Scheduling ACE Diff Operations (Beta)

ACE supports automated scheduling of table-diff and repset-diff operations through configuration settings in `ace_config.py`. The job scheduler allows you to perform regular consistency checks without manual intervention.

Use properties in the `ACE Background Service Options` section of the `ace_config.py` file to specify general background service preferences:

```bash
** ACE Background Service Options **

LISTEN_ADDRESS = "0.0.0.0"
LISTEN_PORT = 5000

# Smallest interval that can be used for any ACE background service
MIN_RUN_FREQUENCY = timedelta(minutes=5)
```

* `LISTEN_ADDRESS` (default="0.0.0.0") is the network address ACE should bind to when started as a background process.
* `LISTEN_PORT` (default=5000) is the default port ACE should listen on when started as a background process.
* `MIN_RUN_FREQUENCY` (default=timedelta(minutes=5)) is the minimum interval between consecutive runs of a background job. This value can be set using any timedelta unit -- such as minutes, seconds, or hours. For example, if MIN_RUN_FREQUENCY is set to 5 minutes, then no job can be scheduled to run more frequently than once every 5 minutes.

Additionally, use properties in the following sections to define jobs and schedules for their execution.

## Scheduling a Job

[The `ace_config.py` file](../ace/installing_ace.md#configuring-ace-preferences-with-the-ace_configpy-file) (by default, located in `$PGEDGE_HOME/hub/scripts/`) contains information about jobs and their schedules in two .json-formatted sections; first, use the following property:value pairs in the `schedule_jobs` section to define jobs:

**Job Configuration Options**

Each job in `schedule_jobs` supports:

- `name` (required): Unique identifier for the job
- `cluster_name` (required): Name of the cluster
- `table_name` OR `repset_name` (required): Fully qualified table name or repset name
- `args` (optional): Dictionary of table-diff parameters
  - `max_cpu_ratio`: Maximum CPU usage ratio
  - `batch_size`: Batch size for processing
  - `block_rows`: Number of rows per block
  - `table_filter`: `SQL WHERE` clause used to filter rows for comparison
  - `nodes`: Nodes to include
  - `output`: Output format ["json", "csv", "html"]
  - `quiet`: Suppress output
  - `dbname`: Database name

**For Example**

```python
# Define the jobs
schedule_jobs = [
    {
        "name": "t1",
        "cluster_name": "my_cluster",
        "table_name": "public.users"
    },
    {
        "name": "t2",
        "cluster_name": "my_cluster",
        "table_name": "public.orders",
        "args": {
            "max_cpu_ratio": 0.7,
            "batch_size": 1000,
            "block_rows": 10000,
            "nodes": "all",
            "output": "json",
            "quiet": False,
            "dbname": "mydb"
        }
    }
]
```

Then, use the property:value pairs in the `schedule_config` section to define the schedule for each job:

**Schedule Configuration Options**

Each schedule in `schedule_config` supports:

- `job_name` (required): Name of the job to schedule (must match a job name)
- `crontab_schedule`: Cron-style schedule expression
  - **Cron Format**: `* * * * *` (minute hour day_of_month month day_of_week)
  - Examples:
    - `0 0 * * *`: Daily at midnight
    - `0 */4 * * *`: Every 4 hours
    - `0 0 * * 0`: Weekly on Sunday
- `run_frequency`: Alternative to crontab, using time units (e.g., "30s", "5m", "1h")
  - **Run Frequency Format**: `<number><unit>`
  - Units: "s" (seconds), "m" (minutes), "h" (hours)
  - Minimum: 5 minutes
  - Examples:
    - "30s": Every 30 seconds
    - "5m": Every 5 minutes
    - "1h": Every hour
- `enabled`: Whether the schedule is active (default: False)
- `rerun_after`: Time to wait before rerunning if differences found

**For Example**

```json
schedule_config = [
    {
        "job_name": "t1",
        "crontab_schedule": "0 0 * * *",    # Run at midnight
        "run_frequency": "30s",             # Alternative to crontab
        "enabled": True,
        "rerun_after": "1h"                # Rerun if diff found after 1 hour
    },
    {
        "job_name": "t2",
        "crontab_schedule": "0 */4 * * *",  # Every 4 hours
        "run_frequency": "5m",              # Alternative to crontab
        "enabled": True,
        "rerun_after": "30m"
    }
]
```

**Starting the Scheduler**

The scheduler starts automatically when ACE is started.

```bash
./pgedge ace start
```


**Best Practices**

1. **Resource Management**: 
   - Stagger schedules to avoid overlapping resource-intensive jobs
   - Set appropriate `max_cpu_ratio`, `block_rows`, and `batch_size` values based on the
     table size and expected load
2. **Frequency Selection**:
   - Use `crontab_schedule` for specific times
   - Use `run_frequency` for regular intervals


## Scheduling Auto-Repair Jobs (Beta)

The `auto-repair` module monitors and repairs INSERT-INSERT exceptions in tables containing data that has been detected to have diverged. It runs as a background process, periodically checking for inconsistencies and applying repairs based on configured settings.

To enable auto-repair, specify your auto-repair preferences in `ace_config.py`:

```json
auto_repair_config = {
    "enabled": False,
    "cluster_name": "eqn-t9da",
    "dbname": "demo",
    "poll_frequency": "10m",
    "repair_frequency": "15m"
}
```

**Configuration Options**

- `enabled`: Enable/disable auto-repair functionality (default: False)
- `cluster_name`: Name of the cluster to monitor
- `dbname`: Database name to monitor
- `poll_frequency`: How often the Spock exception log is polled to check for new exceptions.
- `repair_frequency`: How often to repair exceptions that have been detected.

**Time Intervals**

You can specify the time intervals for execution in either cron format or in a simple frequency format.  Both `poll_interval` and `status_update_interval` accept time strings in the following formats:

**Cron Format**: `* * * * *` (minute hour day_of_month month day_of_week); for example:
  - `0 0 * * *`: Daily at midnight
  - `0 */4 * * *`: Every 4 hours
  - `0 0 * * 0`: Weekly on Sunday

**Run Frequency Format**: `<number><unit>`; for example:
  - Units: "s" (seconds), "m" (minutes), "h" (hours)
  - Minimum: 5 minutes
  - Examples:
    - "30s": Every 30 seconds
    - "5m": Every 5 minutes
    - "1h": Every hour

Note: The minimum frequency allowed is 5 minutes. However, you can modify that time by editing the `MIN_RUN_FREQUENCY` variable in `ace_config.py`.

**Controlling the auto-repair Daemon**

The auto-repair daemon starts automatically when ACE is started.

```bash
./pgedge ace start
```


**Common Use Cases**

Auto-repair is a great candidate for handling use-cases that have a high probability of `INSERT`-`INSERT` conflicts.  For example, on bidding and reservation servers,  `INSERT`-`INSERT` conflicts are likely to arise across multiple nodes.

### Limitations and Considerations

- The auto-repair daemon is currently limited to handling `INSERT`-`INSERT` conflicts only.
