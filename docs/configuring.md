# Configuring ACE Preferences with the ace_config.py File

You can provide your preferences for ACE configuration options in the `ace_config.py` file (by default, created in `$PGEDGE_HOME/hub/scripts/`).  You can use the configuration file to specify:

* ACE operational preferences.
* Job and Schedule information for ACE jobs.
* ACE Auto Repair options
* SSL certificate details for API users.

!!! hint

    If you're already running the ACE process, and need to modify the `ace_config.py` file, use `Ctrl+C` to stop the process before making changes.

## Specifying ACE Operational Preferences

Use properties in the `Postgres options` section of the `ace_config.py` file to specify your timeout preferences:

```bash
# Postgres options
STATEMENT_TIMEOUT = 60000  # in milliseconds
CONNECTION_TIMEOUT = 10  # in seconds
```

* `STATEMENT_TIMEOUT` (default=60000) is equivalent to Postgres' `statement_timeout`. Aborts any query that takes more than the specified amount of time (in milliseconds). A value of 0 disables the timeout. Could be useful when running table-diff on very large tables to minimise performance impact on the database.
* `CONNECTION_TIMEOUT` (default=10): Equivalent to Postgres' `connect_timeout`. Maximum time to wait while connecting, in seconds. A value of 0 means that ACE will wait indefinitely to connect. 

Use properties in the `Default values for ACE table-diff` section to specify default values for the ACE table-diff command:

```bash
#  Default values for ACE table-diff
MAX_DIFF_ROWS = 10000
MIN_ALLOWED_BLOCK_SIZE = 1000
MAX_ALLOWED_BLOCK_SIZE = 100000
BLOCK_ROWS_DEFAULT = os.environ.get("ACE_BLOCK_ROWS", 10000)
MAX_CPU_RATIO_DEFAULT = os.environ.get("ACE_MAX_CPU_RATIO", 0.6)
BATCH_SIZE_DEFAULT = os.environ.get("ACE_BATCH_SIZE", 1)
MAX_BATCH_SIZE = 1000
```

* `MAX_DIFF_ROWS` (default=10000) is the number of differences after which table-diff will abort the run. A value of 10000 means that table-diff will abort if there are more than 10000 differences between the specified nodes.
* `MIN_ALLOWED_BLOCK_SIZE` (default=1000) is the smallest allowed block size during table-diff. 
* `MAX_ALLOWED_BLOCK_SIZE` (default=100000) is the largest allowed block size during table-diff.
* `BLOCK_ROWS_DEFAULT` (default=10000) is the default number of block rows to use if nothing is specified. It attempts to read the environment variable ACE_BLOCK_ROWS, and falls back to 10000 if it is not set. Can be overridden with the CLI option `--block-rows`.
* `MAX_CPU_RATIO_DEFAULT` (default=0.6) is the number of multiprocessing workers (a float value between 0 and 1) used during table-diff is given by NUM_CPUS * MAX_CPU_RATIO_DEFAULT. Can be overridden with the CLI option `--max-cpu-ratio`.
* `BATCH_SIZE_DEFAULT` (default=1) is the number of work items to process per worker. Can be overridden with the CLI option `--batch-size`.
* `MAX_BATCH_SIZE` (default=1000) is the largest batch size allowed during a table-diff.

Use properties in the `ACE Auto-repair Options` section to specify your preferences related to auto-repair:

```bash
auto_repair_config = {
    "enabled": False,
    "cluster_name": "eqn-t9da",
    "dbname": "demo",
    "poll_frequency": "10m",
    "repair_frequency": "15m",
}
```
* `enabled` is a boolean value that specifies that auto-repair should or should not be enabled; the default is `false`.
* `cluster_name` is the name of your cluster.
* `dbname` is the name of the Postgres database that auto-repair is monitoring.
* `poll_frequency` is the interval at which to poll the `exception_log` table and populate the `exception_status` and `exception_status_detail` tables.
* `repair_frequency` is the interval at which to repair exceptions.

### The ACE Configuration File (ace_config.py)

The following listing is a complete `ace_config.py` file, provided for reference only.  Please note that your configuration values will vary.

```
import os
from datetime import timedelta

"""

** ACE CLI and common configuration options **

"""

# ==============================================================================
# Postgres options
STATEMENT_TIMEOUT = 60000  # in milliseconds
CONNECTION_TIMEOUT = 10  # in seconds


#  Default values for ACE table-diff
MAX_DIFF_ROWS = 10000
MIN_ALLOWED_BLOCK_SIZE = 1000
MAX_ALLOWED_BLOCK_SIZE = 100000
BLOCK_ROWS_DEFAULT = os.environ.get("ACE_BLOCK_ROWS", 10000)
MAX_CPU_RATIO_DEFAULT = os.environ.get("ACE_MAX_CPU_RATIO", 0.6)
BATCH_SIZE_DEFAULT = os.environ.get("ACE_BATCH_SIZE", 1)
MAX_BATCH_SIZE = 1000


# Return codes for compare_checksums
BLOCK_OK = 0
MAX_DIFFS_EXCEEDED = 1
BLOCK_MISMATCH = 2
BLOCK_ERROR = 3

# The minimum version of Spock that supports the repair mode
SPOCK_REPAIR_MODE_MIN_VERSION = 4.0

# ==============================================================================

"""

** ACE Background Service Options **

"""

LISTEN_ADDRESS = "0.0.0.0"
LISTEN_PORT = 5000

# Smallest interval that can be used for any ACE background service
MIN_RUN_FREQUENCY = timedelta(minutes=5)

"""
Table-diff scheduling options
Specify a list of job definitions. Currently, only table-diff and repset-diff
jobs are supported.

A job definition must have the following fields:
- name: The name of the job
- cluster_name: The name of the cluster
- table_name: The name of the table
OR
- repset_name: The name of the repset for repset-diff

If finer control is needed, you could also specify additional arguments in the
args field.
args currently supports the following fields:
- max_cpu_ratio: The maximum number of CPUs to use for the job. Expressed as a
  float between 0 and 1.
- batch_size: The batch size to use for the job. How many blocks does a single
  job process at a time.
- block_rows: The maximum number of rows per block. How many rows does a
  single block contain. Each multiprocessing worker running in parallel will
  process this many rows at a time.
- nodes:  A list of node OIDs--if you'd like to run the job only on specific nodes.
- output: The output format to use for the job. Can be "json", "html" or "csv".
- quiet: Whether to suppress output.
- table_filter: A where clause to run table-diff only on a subset of rows. E.g.
  "id < 10000". Note: table_filter argument will be ignored for repset-diff.
- dbname: The database to use.
- skip_tables: A list of tables to skip during repset-diff.


NOTE: For best results, stagger the jobs by at least a few seconds. Do not run
overlapping jobs

Example:
schedule_jobs = [
    {
        "name": "t1",
        "cluster_name": "eqn-t9da",
        "table_name": "public.t1",
    },
    {
        "name": "t2",
        "cluster_name": "eqn-t9da",
        "table_name": "public.t2",
        "args": {
            "max_cpu_ratio": 0.7,
            "batch_size": 1000,
            "block_rows": 10000,
            "nodes": "all",
            "output": "json",
            "quiet": False,
            "dbname": "demo",
        },
    },
    {
        "name": "t3",
        "cluster_name": "eqn-t9da",
        "repset_name": "demo_repset",
        "args": {
            "max_cpu_ratio": 0.7,
            "batch_size": 1000,
            "block_rows": 10000,
            "nodes": "all",
            "output": "json",
            "quiet": False,
            "dbname": "demo",
            "skip_tables": ["public.test_table_1", "public.test_table_2"],
        },
    },
]
"""

schedule_jobs = []

"""
Specify a list of jobs and their crontab schedules or run_frequency as a string.
This list must reference job names from schedule_jobs above.
run_frequency can be string like "1 h", "5 min" or "30 s".
If the crontab_schedule is specified, run_frequency is ignored.
Minimum run_frequency is 5 minutes by default. Can be overriden by setting
MIN_RUN_FREQUENCY above.

Example:
schedule_config = [
    {
        "job_name": "t1",
        "crontab_schedule": "0 0 * * *",
        "run_frequency": "30s",
        "enabled": False,
    },
    {
        "job_name": "t2",
        "crontab_schedule": "0 0 * * *",
        "run_frequency": "5s",
        "enabled": False,
    },
    {
        "job_name": "t3",
        "crontab_schedule": "0 0 * * *",
        "run_frequency": "30s",
        "enabled": False,
    },
]
"""

schedule_config = []

"""

ACE Auto-repair Options

Auto-repair is a feature in ACE to automatically repair tables that are
detected to have diverged. Currently, auto-repair supports handling only
insert-insert exceptions. Handling other types of exception will need replication
information from Spock, which it either doesn't track or simply doesn't provide.
The detection happens by polling the spock.exception_status table. However, since
Spock does not automatically insert into the spock.exception_status or the
spock.exception_status_detail tables, ACE has to manually insert into them by
performing a MERGE INTO using all three tables.

How often the exception_status is populated is controlled by the poll_frequency
setting, and how often the repair happens is controlled by the repair_frequency
setting.

To enable auto-repair, set the following options:
- enabled: Whether to enable auto-repair.
- cluster_name: The name of the cluster.
- dbname: The name of the database.
- poll_frequency: The interval at which to poll the exception_log table and
  populate the exception_status and exception_status_detail tables.
- repair_frequency: The interval at which to repair exceptions.

auto_repair_config = {
    "enabled": False,
    "cluster_name": "eqn-t9da",
    "dbname": "demo",
    "poll_frequency": "10m",
    "repair_frequency": "15m",
}
"""

auto_repair_config = {}

"""
Cert-based auth options

Client-cert-based auth is a *required* option for using the ACE APIs. It can
optionally be used with the CLI modules as well.

"""
USE_CERT_AUTH = False
ACE_USER_CERT_FILE = ""
ACE_USER_KEY_FILE = ""
CA_CERT_FILE = ""

# Prior to version 42.0.0 of cryptography, the not_valid_before and not_valid_after
# fields in the certificate object returned a naive datetime object.
# These fields were deprecated starting with version 42.0.0.
# If the user has a pre- 42.0.0 version of cryptography, we need to support it
# correctly.
USE_NAIVE_DATETIME = False

DEBUG_MODE = False

# ==============================================================================

```