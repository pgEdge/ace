package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/config"
)

type scheduleSpec struct {
	frequency time.Duration
	cron      string
}

func BuildJobsFromConfig(cfg *config.Config) ([]Job, error) {
	if cfg == nil {
		return nil, fmt.Errorf("scheduler: configuration is not initialised")
	}

	jobDefs := make(map[string]config.JobDef, len(cfg.ScheduleJobs))
	for _, def := range cfg.ScheduleJobs {
		jobDefs[def.Name] = def
	}

	var jobs []Job
	for _, sched := range cfg.ScheduleConfig {
		if !sched.Enabled {
			continue
		}
		def, ok := jobDefs[sched.JobName]
		if !ok {
			return nil, fmt.Errorf("scheduler: job definition %q not found", sched.JobName)
		}
		spec, err := specFromConfig(sched)
		if err != nil {
			return nil, fmt.Errorf("scheduler: job %q: %w", def.Name, err)
		}
		job, err := buildJobFromDefinition(cfg, def, spec)
		if err != nil {
			return nil, fmt.Errorf("scheduler: job %q: %w", def.Name, err)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func specFromConfig(def config.SchedDef) (scheduleSpec, error) {
	var spec scheduleSpec

	if strings.TrimSpace(def.CrontabSchedule) != "" {
		spec.cron = def.CrontabSchedule
	}
	if strings.TrimSpace(def.RunFrequency) != "" {
		freq, err := ParseFrequency(def.RunFrequency)
		if err != nil {
			return scheduleSpec{}, err
		}
		spec.frequency = freq
	}

	if spec.cron == "" && spec.frequency == 0 {
		return scheduleSpec{}, fmt.Errorf("either run_frequency or crontab_schedule must be set")
	}
	if spec.cron != "" && spec.frequency > 0 {
		return scheduleSpec{}, fmt.Errorf("cannot set both run_frequency and crontab_schedule")
	}

	return spec, nil
}

func buildJobFromDefinition(cfg *config.Config, def config.JobDef, spec scheduleSpec) (Job, error) {
	jobType := strings.ToLower(strings.TrimSpace(def.Type))
	switch {
	case jobType == "table-diff" || (jobType == "" && def.TableName != ""):
		return buildTableDiffJob(cfg, def, spec)
	case jobType == "schema-diff" || (jobType == "" && def.SchemaName != ""):
		return buildSchemaDiffJob(cfg, def, spec)
	case jobType == "repset-diff" || (jobType == "" && def.RepsetName != ""):
		return buildRepsetDiffJob(cfg, def, spec)
	default:
		return Job{}, fmt.Errorf("unable to infer job type; set job.type explicitly")
	}
}

func buildTableDiffJob(cfg *config.Config, def config.JobDef, spec scheduleSpec) (Job, error) {
	if strings.TrimSpace(def.TableName) == "" {
		return Job{}, fmt.Errorf("table_name is required for table-diff jobs")
	}

	base := diff.NewTableDiffTask()
	base.ClusterName = selectCluster(cfg, def.ClusterName)
	base.QualifiedTableName = def.TableName
	base.DBName = stringArg(def.Args, "dbname")
	base.Nodes = stringArg(def.Args, "nodes")
	if v := intArg(def.Args, "block_size", 0); v > 0 {
		base.BlockSize = v
	}
	if v := float64Arg(def.Args, "concurrency_factor", 0); v > 0 {
		base.ConcurrencyFactor = v
	}
	if v := intArg(def.Args, "compare_unit_size", 0); v > 0 {
		base.CompareUnitSize = v
	}
	if v := intArg(def.Args, "max_diff_rows", 0); v > 0 {
		base.MaxDiffRows = int64(v)
	}
	if out := stringArg(def.Args, "output"); out != "" {
		base.Output = out
	}
	base.TableFilter = stringArg(def.Args, "table_filter")
	base.QuietMode = boolArg(def.Args, "quiet", base.QuietMode)
	base.OverrideBlockSize = boolArg(def.Args, "override_block_size", base.OverrideBlockSize)
	base.SkipDBUpdate = boolArg(def.Args, "skip_db_update", base.SkipDBUpdate)
	if path := stringArg(def.Args, "taskstore_path"); path != "" {
		base.TaskStorePath = path
	}

	if base.ConcurrencyFactor == 0.0 {
		if cfg.TableDiff.ConcurrencyFactor > 0 {
			base.ConcurrencyFactor = cfg.TableDiff.ConcurrencyFactor
		} else {
			base.ConcurrencyFactor = 0.5
		}
	}
	if base.BlockSize == 0 && cfg.TableDiff.DiffBlockSize > 0 {
		base.BlockSize = cfg.TableDiff.DiffBlockSize
	}
	if base.CompareUnitSize == 0 && cfg.TableDiff.CompareUnitSize > 0 {
		base.CompareUnitSize = cfg.TableDiff.CompareUnitSize
	}

	return Job{
		Name:       jobName(def, "table-diff", base.QualifiedTableName),
		Frequency:  spec.frequency,
		Cron:       spec.cron,
		RunOnStart: true,
		Task: func(ctx context.Context) error {
			runTask := base.CloneForSchedule(ctx)
			if err := runTask.Validate(); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
			if err := runTask.RunChecks(true); err != nil {
				return fmt.Errorf("checks failed: %w", err)
			}
			if err := runTask.ExecuteTask(); err != nil {
				return fmt.Errorf("execution failed: %w", err)
			}
			return nil
		},
	}, nil
}

func buildSchemaDiffJob(cfg *config.Config, def config.JobDef, spec scheduleSpec) (Job, error) {
	if strings.TrimSpace(def.SchemaName) == "" {
		return Job{}, fmt.Errorf("schema_name is required for schema-diff jobs")
	}

	base := diff.NewSchemaDiffTask()
	base.ClusterName = selectCluster(cfg, def.ClusterName)
	base.SchemaName = def.SchemaName
	base.DBName = stringArg(def.Args, "dbname")
	base.Nodes = stringArg(def.Args, "nodes")
	base.SkipTables = stringArg(def.Args, "skip_tables")
	base.SkipFile = stringArg(def.Args, "skip_file")
	base.Quiet = boolArg(def.Args, "quiet", base.Quiet)
	base.DDLOnly = boolArg(def.Args, "ddl_only", false)
	if base.DDLOnly {
		return Job{}, fmt.Errorf("schema-diff jobs with ddl_only=true cannot be scheduled")
	}
	if v := intArg(def.Args, "block_size", 0); v > 0 {
		base.BlockSize = v
	}
	if v := float64Arg(def.Args, "concurrency_factor", 0); v > 0 {
		base.ConcurrencyFactor = v
	}
	if v := intArg(def.Args, "compare_unit_size", 0); v > 0 {
		base.CompareUnitSize = v
	}
	if out := stringArg(def.Args, "output"); out != "" {
		base.Output = out
	}
	base.OverrideBlockSize = boolArg(def.Args, "override_block_size", base.OverrideBlockSize)
	base.SkipDBUpdate = boolArg(def.Args, "skip_db_update", base.SkipDBUpdate)
	base.TableFilter = stringArg(def.Args, "table_filter")
	if path := stringArg(def.Args, "taskstore_path"); path != "" {
		base.TaskStorePath = path
	}

	if base.ConcurrencyFactor == 0.0 {
		if cfg.TableDiff.ConcurrencyFactor > 0 {
			base.ConcurrencyFactor = cfg.TableDiff.ConcurrencyFactor
		} else {
			base.ConcurrencyFactor = 0.5
		}
	}
	if base.BlockSize == 0 && cfg.TableDiff.DiffBlockSize > 0 {
		base.BlockSize = cfg.TableDiff.DiffBlockSize
	}
	if base.CompareUnitSize == 0 && cfg.TableDiff.CompareUnitSize > 0 {
		base.CompareUnitSize = cfg.TableDiff.CompareUnitSize
	}

	return Job{
		Name:       jobName(def, "schema-diff", base.SchemaName),
		Frequency:  spec.frequency,
		Cron:       spec.cron,
		RunOnStart: true,
		Task: func(ctx context.Context) error {
			runTask := base.CloneForSchedule(ctx)

			if err := runTask.Validate(); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
			if err := runTask.RunChecks(true); err != nil {
				return fmt.Errorf("checks failed: %w", err)
			}
			if err := runTask.SchemaTableDiff(); err != nil {
				return fmt.Errorf("execution failed: %w", err)
			}
			return nil
		},
	}, nil
}

func buildRepsetDiffJob(cfg *config.Config, def config.JobDef, spec scheduleSpec) (Job, error) {
	if strings.TrimSpace(def.RepsetName) == "" {
		return Job{}, fmt.Errorf("repset_name is required for repset-diff jobs")
	}

	base := diff.NewRepsetDiffTask()
	base.ClusterName = selectCluster(cfg, def.ClusterName)
	base.RepsetName = def.RepsetName
	base.DBName = stringArg(def.Args, "dbname")
	base.Nodes = stringArg(def.Args, "nodes")
	base.SkipTables = stringArg(def.Args, "skip_tables")
	base.SkipFile = stringArg(def.Args, "skip_file")
	base.Quiet = boolArg(def.Args, "quiet", base.Quiet)
	if v := intArg(def.Args, "block_size", 0); v > 0 {
		base.BlockSize = v
	}
	if v := float64Arg(def.Args, "concurrency_factor", 0); v > 0 {
		base.ConcurrencyFactor = v
	}
	if v := intArg(def.Args, "compare_unit_size", 0); v > 0 {
		base.CompareUnitSize = v
	}
	if out := stringArg(def.Args, "output"); out != "" {
		base.Output = out
	}
	base.TableFilter = stringArg(def.Args, "table_filter")
	base.OverrideBlockSize = boolArg(def.Args, "override_block_size", base.OverrideBlockSize)
	base.SkipDBUpdate = boolArg(def.Args, "skip_db_update", base.SkipDBUpdate)
	if path := stringArg(def.Args, "taskstore_path"); path != "" {
		base.TaskStorePath = path
	}

	if base.ConcurrencyFactor == 0.0 {
		if cfg.TableDiff.ConcurrencyFactor > 0 {
			base.ConcurrencyFactor = cfg.TableDiff.ConcurrencyFactor
		} else {
			base.ConcurrencyFactor = 0.5
		}
	}
	if base.BlockSize == 0 && cfg.TableDiff.DiffBlockSize > 0 {
		base.BlockSize = cfg.TableDiff.DiffBlockSize
	}
	if base.CompareUnitSize == 0 && cfg.TableDiff.CompareUnitSize > 0 {
		base.CompareUnitSize = cfg.TableDiff.CompareUnitSize
	}

	return Job{
		Name:       jobName(def, "repset-diff", base.RepsetName),
		Frequency:  spec.frequency,
		Cron:       spec.cron,
		RunOnStart: true,
		Task: func(ctx context.Context) error {
			runTask := base.CloneForSchedule(ctx)
			if err := runTask.Validate(); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
			if err := runTask.RunChecks(true); err != nil {
				return fmt.Errorf("checks failed: %w", err)
			}
			if err := diff.RepsetDiff(runTask); err != nil {
				return fmt.Errorf("execution failed: %w", err)
			}
			return nil
		},
	}, nil
}

func selectCluster(cfg *config.Config, override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	return strings.TrimSpace(cfg.DefaultCluster)
}

func jobName(def config.JobDef, kind, target string) string {
	if strings.TrimSpace(def.Name) != "" {
		return def.Name
	}
	if target != "" {
		return fmt.Sprintf("%s:%s", kind, target)
	}
	return kind
}

func stringArg(args map[string]any, key string) string {
	if args == nil {
		return ""
	}
	if val, ok := args[key]; ok {
		switch v := val.(type) {
		case string:
			return v
		case fmt.Stringer:
			return v.String()
		default:
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func boolArg(args map[string]any, key string, defaultVal bool) bool {
	if args == nil {
		return defaultVal
	}
	if val, ok := args[key]; ok {
		switch v := val.(type) {
		case bool:
			return v
		case string:
			parsed, err := strconv.ParseBool(v)
			if err == nil {
				return parsed
			}
		case float64:
			return v != 0
		case int:
			return v != 0
		case int64:
			return v != 0
		}
	}
	return defaultVal
}

func float64Arg(args map[string]any, key string, defaultVal float64) float64 {
	if args == nil {
		return defaultVal
	}
	if val, ok := args[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				return parsed
			}
		}
	}
	return defaultVal
}

func intArg(args map[string]any, key string, defaultVal int) int {
	if args == nil {
		return defaultVal
	}
	if val, ok := args[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		case string:
			if parsed, err := strconv.Atoi(v); err == nil {
				return parsed
			}
		}
	}
	return defaultVal
}
