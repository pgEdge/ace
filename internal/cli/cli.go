// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2026, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package cli

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/pgedge/ace/internal/api/http"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/consistency/repair"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/internal/jobs"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/spf13/cobra"
)

//go:embed default_config.yaml
var defaultConfigYAML string

//go:embed default_pg_service.conf
var defaultPgServiceConf string

// ─── Flag helpers ──────────────────────────────────────────────────────────

func addCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("dbname", "d", "", "Name of the database")
	cmd.Flags().StringP("nodes", "n", "all", "Nodes to include in the diff (default: all)")
	cmd.Flags().BoolP("quiet", "q", false, "Whether to suppress output")
	cmd.Flags().BoolP("debug", "v", false, "Enable debug logging")
}

func addDiffFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("block-size", "b", "100000", "Number of rows per block")
	cmd.Flags().Float64P("concurrency-factor", "c", 0.5, "CPU ratio for concurrency (0.0–4.0, e.g. 0.5 uses half of available CPUs)")
	cmd.Flags().IntP("compare-unit-size", "u", 10000, "Max size of the smallest block to use when diffs are present")
	cmd.Flags().StringP("output", "o", "json", "Output format")
	cmd.Flags().BoolP("override-block-size", "B", false, "Override block size")
}

func addSkipFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("skip-tables", "T", "", "Comma-separated list of tables to skip")
	cmd.Flags().StringP("skip-file", "s", "", "Path to a file with a list of tables to skip")
}

func addScheduleFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("schedule", "S", false, "Schedule a job to run periodically")
	cmd.Flags().StringP("every", "e", "", "Time duration (e.g., 5m, 3h, etc.)")
}

func addConfigInitFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("path", "p", "ace.yaml", "Path to write the config file")
	cmd.Flags().BoolP("force", "x", false, "Overwrite the config file if it already exists")
	cmd.Flags().BoolP("stdout", "z", false, "Print the config to stdout instead of writing a file")
}

func addClusterInitFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("path", "p", "pg_service.conf", "Path to write the pg service file")
	cmd.Flags().BoolP("force", "x", false, "Overwrite the service file if it already exists")
	cmd.Flags().BoolP("stdout", "z", false, "Print the service file to stdout instead of writing a file")
}

func addTableRepairFlags(cmd *cobra.Command) {
	addCommonFlags(cmd)
	cmd.Flags().StringP("diff-file", "f", "", "Path to the diff file (required)")
	_ = cmd.MarkFlagRequired("diff-file")
	cmd.Flags().StringP("repair-plan", "p", "", "Path to the advanced repair plan (YAML/JSON); skips source-of-truth requirement")
	cmd.Flags().StringP("source-of-truth", "r", "", "Name of the node to be considered the source of truth")
	cmd.Flags().BoolP("dry-run", "y", false, "Show what would be done without executing")
	cmd.Flags().BoolP("generate-report", "g", false, "Generate a report of the repair operation")
	cmd.Flags().BoolP("insert-only", "i", false, "Only perform inserts, no updates or deletes")
	cmd.Flags().BoolP("upsert-only", "P", false, "Only perform upserts (insert or update), no deletes")
	cmd.Flags().BoolP("fire-triggers", "t", false, "Whether to fire triggers during repairs")
	cmd.Flags().BoolP("bidirectional", "Z", false, "Whether to perform repairs in both directions. Can be used only with the insert-only option")
	cmd.Flags().Bool("recovery-mode", false, "Enable recovery-mode repair using origin-only diffs")
	cmd.Flags().Bool("preserve-origin", false, "Preserve replication origin node ID and commit timestamp for repaired rows")
	cmd.Flags().BoolP("fix-nulls", "X", false, "Fill NULL columns on each node using non-NULL values from its peers (no source-of-truth required)")
}

func addMtreeBuildFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("block-size", "b", "10000", "Rows per leaf block")
	cmd.Flags().Float64P("max-cpu-ratio", "m", 0.5, "Max CPU for parallel operations")
	cmd.Flags().BoolP("override-block-size", "B", false, "Skip block size check, and potentially tolerate unsafe block sizes")
	cmd.Flags().BoolP("analyse", "a", false, "Run ANALYZE on the table")
	cmd.Flags().BoolP("recreate-objects", "R", false, "Drop and recreate Merkle tree objects")
	cmd.Flags().BoolP("write-ranges", "w", false, "Write block ranges to a JSON file")
	cmd.Flags().StringP("ranges-file", "k", "", "Path to a file with pre-computed ranges")
	addCommonFlags(cmd)
}

func addMtreeUpdateFlags(cmd *cobra.Command) {
	cmd.Flags().Float64P("max-cpu-ratio", "m", 0.5, "Max CPU for parallel operations")
	cmd.Flags().BoolP("rebalance", "l", false, "Rebalance the tree by merging small blocks")
	cmd.Flags().BoolP("skip-cdc", "U", false, "Skip CDC processing (only rehash dirty blocks)")
	addCommonFlags(cmd)
}

func addMtreeDiffFlags(cmd *cobra.Command) {
	cmd.Flags().Float64P("max-cpu-ratio", "m", 0.5, "Max CPU for parallel operations")
	cmd.Flags().StringP("output", "o", "json", "Output format")
	cmd.Flags().BoolP("skip-cdc", "U", false, "Skip CDC processing (only rehash dirty blocks and compare)")
	addCommonFlags(cmd)
}

// debugPreRunE sets up log level based on the --debug flag.
// It is used as PersistentPreRunE on group commands and as PreRunE
// on leaf commands that carry the debug flag.
func debugPreRunE(cmd *cobra.Command, _ []string) error {
	debug, _ := cmd.Flags().GetBool("debug")
	if debug {
		logger.SetLevel(log.DebugLevel)
	} else {
		logger.SetLevel(log.InfoLevel)
	}
	return nil
}

// ─── SetupCLI ──────────────────────────────────────────────────────────────

func SetupCLI() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ace",
		Short: "ACE - Active Consistency Engine",
		// Silence default usage/error output so we control it.
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// ── config ──────────────────────────────────────────────────────────

	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Manage ACE configuration files",
	}

	configInitCmd := &cobra.Command{
		Use:   "init",
		Short: "Create a default ace.yaml file",
		RunE:  ConfigInitCLI,
	}
	addConfigInitFlags(configInitCmd)

	configCmd.AddCommand(configInitCmd)

	// ── start ───────────────────────────────────────────────────────────

	startCmd := &cobra.Command{
		Use:    "start",
		Short:  "Start the ACE scheduler for configured jobs",
		PreRunE: debugPreRunE,
		RunE:   StartSchedulerCLI,
	}
	startCmd.Flags().BoolP("debug", "v", false, "Enable debug logging")
	startCmd.Flags().StringP("component", "C", "all", "Component to start: scheduler, api, or all")

	// ── server ──────────────────────────────────────────────────────────

	serverCmd := &cobra.Command{
		Use:    "server",
		Short:  "Run the ACE REST API server",
		PreRunE: debugPreRunE,
		RunE:   StartAPIServerCLI,
	}
	serverCmd.Flags().BoolP("debug", "v", false, "Enable debug logging")

	// ── cluster ─────────────────────────────────────────────────────────

	clusterCmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage ACE cluster service definitions",
	}

	clusterInitCmd := &cobra.Command{
		Use:   "init",
		Short: "Create a sample pg_service.conf file",
		RunE:  ClusterInitCLI,
	}
	addClusterInitFlags(clusterInitCmd)

	clusterCmd.AddCommand(clusterInitCmd)

	// ── table-diff ──────────────────────────────────────────────────────

	tableDiffCmd := &cobra.Command{
		Use:   "table-diff [cluster] <table>",
		Short: "Compare tables between PostgreSQL databases",
		Long: "A tool for comparing tables between PostgreSQL databases " +
			"and detecting data inconsistencies",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    TableDiffCLI,
	}
	addCommonFlags(tableDiffCmd)
	addDiffFlags(tableDiffCmd)
	tableDiffCmd.Flags().StringP("table-filter", "F", "", "Where clause expression to use while diffing tables")
	tableDiffCmd.Flags().String("against-origin", "", "Restrict diff to rows whose node_origin matches this Spock node id or name")
	tableDiffCmd.Flags().String("until", "", "Optional commit timestamp upper bound (RFC3339) for rows to include")
	tableDiffCmd.Flags().Bool("ensure-pgcrypto", false, "Ensure pgcrypto extension is installed on each node before diffing")
	addScheduleFlags(tableDiffCmd)

	// ── table-rerun ─────────────────────────────────────────────────────

	tableRerunCmd := &cobra.Command{
		Use:     "table-rerun [cluster]",
		Short:   "Re-run a diff from a file to check for persistent differences",
		Args:    cobra.MaximumNArgs(1),
		PreRunE: debugPreRunE,
		RunE:    TableRerunCLI,
	}
	addCommonFlags(tableRerunCmd)
	tableRerunCmd.Flags().StringP("diff-file", "f", "", "Path to the diff file to rerun from (required)")
	_ = tableRerunCmd.MarkFlagRequired("diff-file")

	// ── table-repair ────────────────────────────────────────────────────

	tableRepairCmd := &cobra.Command{
		Use:     "table-repair [cluster] <table>",
		Short:   "Repair table inconsistencies based on a diff file",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    TableRepairCLI,
	}
	addTableRepairFlags(tableRepairCmd)

	// ── spock-diff ──────────────────────────────────────────────────────

	spockDiffCmd := &cobra.Command{
		Use:     "spock-diff [cluster]",
		Short:   "Compare spock metadata across cluster nodes",
		Args:    cobra.MaximumNArgs(1),
		PreRunE: debugPreRunE,
		RunE:    SpockDiffCLI,
	}
	addCommonFlags(spockDiffCmd)
	spockDiffCmd.Flags().StringP("output", "o", "json", "Output format")

	// ── schema-diff ─────────────────────────────────────────────────────

	schemaDiffCmd := &cobra.Command{
		Use:     "schema-diff [cluster] <schema>",
		Short:   "Compare schemas across cluster nodes",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    SchemaDiffCLI,
	}
	addCommonFlags(schemaDiffCmd)
	addDiffFlags(schemaDiffCmd)
	addSkipFlags(schemaDiffCmd)
	schemaDiffCmd.Flags().BoolP("ddl-only", "L", false, "Compare only schema objects (tables, functions, etc.), not table data")
	addScheduleFlags(schemaDiffCmd)

	// ── repset-diff ─────────────────────────────────────────────────────

	repsetDiffCmd := &cobra.Command{
		Use:     "repset-diff [cluster] <repset>",
		Short:   "Compare replication sets across cluster nodes",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    RepsetDiffCLI,
	}
	addCommonFlags(repsetDiffCmd)
	addDiffFlags(repsetDiffCmd)
	addSkipFlags(repsetDiffCmd)
	addScheduleFlags(repsetDiffCmd)

	// ── mtree ───────────────────────────────────────────────────────────

	mtreeCmd := &cobra.Command{
		Use:   "mtree",
		Short: "Merkle tree operations",
	}

	mtreeInitCmd := &cobra.Command{
		Use:     "init [cluster]",
		Short:   "Initialise Merkle tree replication for a cluster",
		Args:    cobra.MaximumNArgs(1),
		PreRunE: debugPreRunE,
		RunE:    MtreeInitCLI,
	}
	addCommonFlags(mtreeInitCmd)

	mtreeListenCmd := &cobra.Command{
		Use:     "listen [cluster]",
		Short:   "Listen for changes and update Merkle trees",
		Args:    cobra.MaximumNArgs(1),
		PreRunE: debugPreRunE,
		RunE:    MtreeListenCLI,
	}
	addCommonFlags(mtreeListenCmd)

	mtreeTeardownCmd := &cobra.Command{
		Use:     "teardown [cluster]",
		Short:   "Teardown Merkle tree replication for a cluster",
		Args:    cobra.MaximumNArgs(1),
		PreRunE: debugPreRunE,
		RunE:    MtreeTeardownCLI,
	}
	addCommonFlags(mtreeTeardownCmd)

	mtreeTeardownTableCmd := &cobra.Command{
		Use:     "teardown-table [cluster] <table>",
		Short:   "Teardown Merkle tree objects for a specific table",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    MtreeTeardownTableCLI,
	}
	addCommonFlags(mtreeTeardownTableCmd)

	mtreeBuildCmd := &cobra.Command{
		Use:     "build [cluster] <table>",
		Short:   "Build a new Merkle tree for a table",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    MtreeBuildCLI,
	}
	addMtreeBuildFlags(mtreeBuildCmd)

	mtreeUpdateCmd := &cobra.Command{
		Use:     "update [cluster] <table>",
		Short:   "Update an existing Merkle tree for a table",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    MtreeUpdateCLI,
	}
	addMtreeUpdateFlags(mtreeUpdateCmd)

	mtreeTableDiffCmd := &cobra.Command{
		Use:     "table-diff [cluster] <table>",
		Short:   "Use Merkle Trees for performing table-diff",
		Args:    cobra.RangeArgs(1, 2),
		PreRunE: debugPreRunE,
		RunE:    MtreeDiffCLI,
	}
	addMtreeDiffFlags(mtreeTableDiffCmd)

	mtreeCmd.AddCommand(
		mtreeInitCmd,
		mtreeListenCmd,
		mtreeTeardownCmd,
		mtreeTeardownTableCmd,
		mtreeBuildCmd,
		mtreeUpdateCmd,
		mtreeTableDiffCmd,
	)

	// ── assemble root ───────────────────────────────────────────────────

	rootCmd.AddCommand(
		configCmd,
		startCmd,
		serverCmd,
		clusterCmd,
		tableDiffCmd,
		tableRerunCmd,
		tableRepairCmd,
		spockDiffCmd,
		schemaDiffCmd,
		repsetDiffCmd,
		mtreeCmd,
	)

	return rootCmd
}

// ─── Template helpers ──────────────────────────────────────────────────────

func initTemplateFile(cmd *cobra.Command, content string, defaultPath string, label string, perm os.FileMode) error {
	outputPath, _ := cmd.Flags().GetString("path")
	if outputPath == "" {
		outputPath = defaultPath
	}

	stdout, _ := cmd.Flags().GetBool("stdout")
	if stdout || outputPath == "-" {
		fmt.Println(content)
		return nil
	}

	force, _ := cmd.Flags().GetBool("force")
	if !force {
		if _, err := os.Stat(outputPath); err == nil {
			return fmt.Errorf("%s already exists at %s (use --force to overwrite)", label, outputPath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("unable to verify existing %s at %s: %w", label, outputPath, err)
		}
	}

	dir := filepath.Dir(outputPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	if err := os.WriteFile(outputPath, []byte(content), perm); err != nil {
		return fmt.Errorf("failed to write %s to %s: %w", label, outputPath, err)
	}

	fmt.Printf("Wrote %s to %s\n", label, outputPath)
	return nil
}

func ConfigInitCLI(cmd *cobra.Command, _ []string) error {
	return initTemplateFile(cmd, defaultConfigYAML, "ace.yaml", "config file", 0o644)
}

func ClusterInitCLI(cmd *cobra.Command, _ []string) error {
	return initTemplateFile(cmd, defaultPgServiceConf, "pg_service.conf", "pg service file", 0o600)
}

// ─── Cluster arg resolution ────────────────────────────────────────────────

func resolveClusterArg(cmdName, missingUsage, argsUsage string, required int, args []string) (string, []string, error) {
	if len(args) < required {
		if required == 1 {
			return "", nil, fmt.Errorf("missing required argument for %s: needs %s", cmdName, missingUsage)
		}
		return "", nil, fmt.Errorf("missing required arguments for %s: needs %s", cmdName, missingUsage)
	}

	if len(args) == required {
		cluster := config.DefaultCluster()
		if cluster == "" {
			return "", nil, fmt.Errorf("cluster name is required: specify one explicitly or set default_cluster in ace.yaml")
		}
		return cluster, args, nil
	}

	if len(args) == required+1 {
		cluster := strings.TrimSpace(args[0])
		if cluster == "" {
			return "", nil, fmt.Errorf("cluster name is required: specify one explicitly or set default_cluster in ace.yaml")
		}
		return cluster, args[1:], nil
	}

	return "", nil, fmt.Errorf("unexpected arguments for %s (usage: %s)", cmdName, argsUsage)
}

// ─── Command implementations ───────────────────────────────────────────────

func TableDiffCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	blockSizeStr, _ := cmd.Flags().GetString("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	concurrencyFactor, _ := cmd.Flags().GetFloat64("concurrency-factor")
	compareUnitSize, _ := cmd.Flags().GetInt("compare-unit-size")
	output, _ := cmd.Flags().GetString("output")
	nodes, _ := cmd.Flags().GetString("nodes")
	ensurePgcrypto, _ := cmd.Flags().GetBool("ensure-pgcrypto")
	scheduleEnabled, _ := cmd.Flags().GetBool("schedule")
	scheduleEvery, _ := cmd.Flags().GetString("every")
	tableFilter, _ := cmd.Flags().GetString("table-filter")
	againstOrigin, _ := cmd.Flags().GetString("against-origin")
	until, _ := cmd.Flags().GetString("until")
	quietMode, _ := cmd.Flags().GetBool("quiet")
	overrideBlockSize, _ := cmd.Flags().GetBool("override-block-size")

	task := diff.NewTableDiffTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = dbname
	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = concurrencyFactor
	task.CompareUnitSize = compareUnitSize
	task.Output = strings.ToLower(output)
	task.Nodes = nodes
	task.EnsurePgcrypto = ensurePgcrypto
	task.TableFilter = tableFilter
	task.AgainstOrigin = againstOrigin
	task.Until = until
	task.QuietMode = quietMode
	task.OverrideBlockSize = overrideBlockSize
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if !scheduleEnabled {
		if err := task.RunChecks(true); err != nil {
			return fmt.Errorf("checks failed: %w", err)
		}
		if err := task.ExecuteTask(); err != nil {
			return fmt.Errorf("error during comparison: %w", err)
		}
		return nil
	}

	freq, err := scheduler.ParseFrequency(scheduleEvery)
	if err != nil {
		return err
	}

	job := scheduler.Job{
		Name:       fmt.Sprintf("table-diff:%s", task.QualifiedTableName),
		Frequency:  freq,
		RunOnStart: true,
		Task: func(runCtx context.Context) error {
			runTask := task.CloneForSchedule(runCtx)
			if err := runTask.Validate(); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
			if err := runTask.RunChecks(true); err != nil {
				return fmt.Errorf("checks failed: %w", err)
			}
			if err := runTask.ExecuteTask(); err != nil {
				return fmt.Errorf("error during comparison: %w", err)
			}
			return nil
		},
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return scheduler.RunSingleJob(runCtx, job)
}

func MtreeInitCLI(cmd *cobra.Command, args []string) error {
	clusterName, _, err := resolveClusterArg("mtree init", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.Mode = "init"
	task.Ctx = context.Background()

	if err := task.MtreeInit(); err != nil {
		return fmt.Errorf("error during mtree init: %w", err)
	}

	return nil
}

func MtreeListenCLI(cmd *cobra.Command, args []string) error {
	clusterName, _, err := resolveClusterArg("mtree listen", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.Mode = "listen"
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Shutdown signal received, stopping listeners...")
		cancel()
	}()

	var wg sync.WaitGroup
	clusterNodes := task.GetClusterNodes()
	wg.Add(len(clusterNodes))

	for _, nodeInfo := range clusterNodes {
		go func(ni map[string]any) {
			defer wg.Done()
			cdc.ListenForChanges(appCtx, ni)
		}(nodeInfo)
	}

	logger.Info("CDC listeners started. Press Ctrl+C to stop.")
	wg.Wait()
	logger.Info("All CDC listeners stopped.")

	return nil
}

func MtreeTeardownCLI(cmd *cobra.Command, args []string) error {
	clusterName, _, err := resolveClusterArg("mtree teardown", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.Mode = "teardown"
	task.Ctx = context.Background()

	if err := task.MtreeTeardown(); err != nil {
		return fmt.Errorf("error during mtree teardown: %w", err)
	}

	return nil
}

func MtreeTeardownTableCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("mtree teardown-table", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.Mode = "teardown-table"
	task.Ctx = context.Background()

	if err := task.MtreeTeardownTable(); err != nil {
		return fmt.Errorf("error during mtree table teardown: %w", err)
	}

	return nil
}

func MtreeBuildCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("mtree build", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	blockSizeStr, _ := cmd.Flags().GetString("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")
	maxCpuRatio, _ := cmd.Flags().GetFloat64("max-cpu-ratio")
	overrideBlockSize, _ := cmd.Flags().GetBool("override-block-size")
	analyse, _ := cmd.Flags().GetBool("analyse")
	recreateObjects, _ := cmd.Flags().GetBool("recreate-objects")
	writeRanges, _ := cmd.Flags().GetBool("write-ranges")
	rangesFile, _ := cmd.Flags().GetString("ranges-file")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.BlockSize = int(blockSizeInt)
	task.MaxCpuRatio = maxCpuRatio
	task.OverrideBlockSize = overrideBlockSize
	task.Analyse = analyse
	task.RecreateObjects = recreateObjects
	task.WriteRanges = writeRanges
	task.RangesFile = rangesFile
	task.Mode = "build"
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.RunChecks(true); err != nil {
		return fmt.Errorf("checks failed: %w", err)
	}

	if err := task.BuildMtree(); err != nil {
		return fmt.Errorf("error during merkle tree build: %w", err)
	}

	return nil
}

func MtreeUpdateCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("mtree update", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")
	maxCpuRatio, _ := cmd.Flags().GetFloat64("max-cpu-ratio")
	rebalance, _ := cmd.Flags().GetBool("rebalance")
	skipCDC, _ := cmd.Flags().GetBool("skip-cdc")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.MaxCpuRatio = maxCpuRatio
	task.Rebalance = rebalance
	task.NoCDC = skipCDC
	task.Mode = "update"
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.RunChecks(true /* skipValidation */); err != nil {
		return fmt.Errorf("checks failed: %w", err)
	}
	if err := task.UpdateMtree(true /* skipAllChecks */); err != nil {
		return fmt.Errorf("error during merkle tree update: %w", err)
	}

	return nil
}

func MtreeDiffCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("mtree diff", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")
	maxCpuRatio, _ := cmd.Flags().GetFloat64("max-cpu-ratio")
	output, _ := cmd.Flags().GetString("output")
	skipCDC, _ := cmd.Flags().GetBool("skip-cdc")

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.MaxCpuRatio = maxCpuRatio
	task.Output = output
	task.NoCDC = skipCDC
	task.Mode = "diff"
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.RunChecks(true); err != nil {
		return fmt.Errorf("checks failed: %w", err)
	}

	if err := task.DiffMtree(); err != nil {
		return fmt.Errorf("error during merkle tree diff: %w", err)
	}

	return nil
}

func TableRerunCLI(cmd *cobra.Command, args []string) error {
	clusterName, _, err := resolveClusterArg("table-rerun", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}

	diffFile, _ := cmd.Flags().GetString("diff-file")
	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	quietMode, _ := cmd.Flags().GetBool("quiet")

	task := diff.NewTableDiffTask()
	task.TaskID = uuid.NewString()
	task.Mode = "rerun"
	task.ClusterName = clusterName
	task.DiffFilePath = diffFile
	task.DBName = dbname
	task.Nodes = nodes
	task.QuietMode = quietMode
	task.Ctx = context.Background()

	if err := task.ExecuteTask(); err != nil {
		return fmt.Errorf("error during table-rerun: %w", err)
	}

	return nil
}

func TableRepairCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("table-repair", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	diffFile, _ := cmd.Flags().GetString("diff-file")
	repairPlan, _ := cmd.Flags().GetString("repair-plan")
	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	sourceOfTruth, _ := cmd.Flags().GetString("source-of-truth")
	quietMode, _ := cmd.Flags().GetBool("quiet")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	insertOnly, _ := cmd.Flags().GetBool("insert-only")
	upsertOnly, _ := cmd.Flags().GetBool("upsert-only")
	fireTriggers, _ := cmd.Flags().GetBool("fire-triggers")
	fixNulls, _ := cmd.Flags().GetBool("fix-nulls")
	bidirectional, _ := cmd.Flags().GetBool("bidirectional")
	generateReport, _ := cmd.Flags().GetBool("generate-report")
	recoveryMode, _ := cmd.Flags().GetBool("recovery-mode")
	preserveOrigin, _ := cmd.Flags().GetBool("preserve-origin")

	task := repair.NewTableRepairTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DiffFilePath = diffFile
	task.RepairPlanPath = repairPlan
	task.DBName = dbname
	task.Nodes = nodes
	task.SourceOfTruth = sourceOfTruth
	task.QuietMode = quietMode
	task.Ctx = context.Background()

	task.DryRun = dryRun
	task.InsertOnly = insertOnly
	task.UpsertOnly = upsertOnly
	task.FireTriggers = fireTriggers
	task.FixNulls = fixNulls
	task.Bidirectional = bidirectional
	task.GenerateReport = generateReport
	task.RecoveryMode = recoveryMode
	task.PreserveOrigin = preserveOrigin

	if err := task.ValidateAndPrepare(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.Run(true); err != nil {
		return fmt.Errorf("error during table repair: %w", err)
	}

	return nil
}

func SpockDiffCLI(cmd *cobra.Command, args []string) error {
	clusterName, _, err := resolveClusterArg("spock-diff", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	output, _ := cmd.Flags().GetString("output")

	task := diff.NewSpockDiffTask()
	task.ClusterName = clusterName
	task.DBName = dbname
	task.Nodes = nodes
	task.Output = output
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.RunChecks(true); err != nil {
		return fmt.Errorf("checks failed: %w", err)
	}

	if err := task.ExecuteTask(); err != nil {
		return fmt.Errorf("error during spock diff: %w", err)
	}
	return nil
}

func SchemaDiffCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("schema-diff", "<schema>", "[cluster] <schema>", 1, args)
	if err != nil {
		return err
	}

	blockSizeStr, _ := cmd.Flags().GetString("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	skipTables, _ := cmd.Flags().GetString("skip-tables")
	scheduleEnabled, _ := cmd.Flags().GetBool("schedule")
	scheduleEvery, _ := cmd.Flags().GetString("every")
	skipFile, _ := cmd.Flags().GetString("skip-file")
	quiet, _ := cmd.Flags().GetBool("quiet")
	ddlOnly, _ := cmd.Flags().GetBool("ddl-only")
	concurrencyFactor, _ := cmd.Flags().GetFloat64("concurrency-factor")
	compareUnitSize, _ := cmd.Flags().GetInt("compare-unit-size")
	output, _ := cmd.Flags().GetString("output")
	overrideBlockSize, _ := cmd.Flags().GetBool("override-block-size")

	task := diff.NewSchemaDiffTask()
	task.ClusterName = clusterName
	task.SchemaName = positional[0]
	task.DBName = dbname
	task.Nodes = nodes
	task.SkipTables = skipTables
	task.SkipFile = skipFile
	task.Quiet = quiet
	task.DDLOnly = ddlOnly
	task.Ctx = context.Background()

	if scheduleEnabled && task.DDLOnly {
		return fmt.Errorf("scheduling is only supported when --ddl-only is false")
	}

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = concurrencyFactor
	task.CompareUnitSize = compareUnitSize
	task.Output = output
	task.OverrideBlockSize = overrideBlockSize

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if !scheduleEnabled {
		if err := task.RunChecks(true); err != nil {
			return fmt.Errorf("checks failed: %w", err)
		}
		if err := task.SchemaTableDiff(); err != nil {
			return fmt.Errorf("error during schema diff: %w", err)
		}
		return nil
	}

	freq, err := scheduler.ParseFrequency(scheduleEvery)
	if err != nil {
		return err
	}

	job := scheduler.Job{
		Name:       fmt.Sprintf("schema-diff:%s", task.SchemaName),
		Frequency:  freq,
		RunOnStart: true,
		Task: func(runCtx context.Context) error {
			runTask := task.CloneForSchedule(runCtx)
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
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return scheduler.RunSingleJob(runCtx, job)
}

func RepsetDiffCLI(cmd *cobra.Command, args []string) error {
	clusterName, positional, err := resolveClusterArg("repset-diff", "<repset>", "[cluster] <repset>", 1, args)
	if err != nil {
		return err
	}

	blockSizeStr, _ := cmd.Flags().GetString("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	scheduleEnabled, _ := cmd.Flags().GetBool("schedule")
	scheduleEvery, _ := cmd.Flags().GetString("every")
	dbname, _ := cmd.Flags().GetString("dbname")
	nodes, _ := cmd.Flags().GetString("nodes")
	skipTables, _ := cmd.Flags().GetString("skip-tables")
	skipFile, _ := cmd.Flags().GetString("skip-file")
	quiet, _ := cmd.Flags().GetBool("quiet")
	concurrencyFactor, _ := cmd.Flags().GetFloat64("concurrency-factor")
	compareUnitSize, _ := cmd.Flags().GetInt("compare-unit-size")
	output, _ := cmd.Flags().GetString("output")
	overrideBlockSize, _ := cmd.Flags().GetBool("override-block-size")

	task := diff.NewRepsetDiffTask()
	task.ClusterName = clusterName
	task.RepsetName = positional[0]
	task.DBName = dbname
	task.Nodes = nodes
	task.SkipTables = skipTables
	task.SkipFile = skipFile
	task.Quiet = quiet
	task.Ctx = context.Background()

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = concurrencyFactor
	task.CompareUnitSize = compareUnitSize
	task.Output = output
	task.OverrideBlockSize = overrideBlockSize

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if !scheduleEnabled {
		if err := task.RunChecks(true); err != nil {
			return fmt.Errorf("checks failed: %w", err)
		}
		if err := diff.RepsetDiff(task); err != nil {
			return fmt.Errorf("error during repset diff: %w", err)
		}
		return nil
	}

	freq, err := scheduler.ParseFrequency(scheduleEvery)
	if err != nil {
		return err
	}

	job := scheduler.Job{
		Name:       fmt.Sprintf("repset-diff:%s", task.RepsetName),
		Frequency:  freq,
		RunOnStart: true,
		Task: func(runCtx context.Context) error {
			runTask := task.CloneForSchedule(runCtx)
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
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return scheduler.RunSingleJob(runCtx, job)
}

func StartSchedulerCLI(cmd *cobra.Command, _ []string) error {
	if config.Cfg == nil {
		return fmt.Errorf("configuration not loaded; run inside a directory with ace.yaml or set ACE_CONFIG")
	}

	component, _ := cmd.Flags().GetString("component")
	component = strings.ToLower(strings.TrimSpace(component))
	runScheduler := false
	runAPI := false
	switch component {
	case "", "all":
		runScheduler = true
		runAPI = true
	case "scheduler":
		runScheduler = true
	case "api":
		runAPI = true
	default:
		return fmt.Errorf("invalid component %q (expected scheduler, api, or all)", component)
	}

	jobs, err := scheduler.BuildJobsFromConfig(config.Cfg)
	if err != nil {
		return err
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	type runner struct {
		name string
		run  func(context.Context) error
	}

	var runners []runner

	if runScheduler {
		if len(jobs) == 0 {
			logger.Info("scheduler: no enabled jobs found in configuration")
		} else {
			for _, job := range jobs {
				logger.Info("scheduler: registering job %s", job.Name)
			}
			runners = append(runners, runner{
				name: "scheduler",
				run: func(ctx context.Context) error {
					return scheduler.RunJobs(ctx, jobs)
				},
			})
		}
	}

	if runAPI {
		if ok, apiErr := canStartAPIServer(config.Cfg); ok {
			apiServer, err := server.New(config.Cfg)
			if err != nil {
				return fmt.Errorf("api server init failed: %w", err)
			}
			runners = append(runners, runner{
				name: "api-server",
				run: func(ctx context.Context) error {
					return apiServer.Run(ctx)
				},
			})
		} else if component == "api" {
			return fmt.Errorf("api server requested but cannot start: %w", apiErr)
		} else {
			logger.Info("api server not started: %v", apiErr)
		}
	}

	if len(runners) == 0 {
		return nil
	}

	errCh := make(chan error, len(runners))
	for _, r := range runners {
		go func(r runner) {
			errCh <- r.run(runCtx)
		}(r)
	}

	for i := 0; i < len(runners); i++ {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
			stop()
			return err
		}
	}

	return nil
}

func StartAPIServerCLI(_ *cobra.Command, _ []string) error {
	if config.Cfg == nil {
		return fmt.Errorf("configuration not loaded; run inside a directory with ace.yaml or set ACE_CONFIG")
	}

	if ok, err := canStartAPIServer(config.Cfg); !ok {
		return err
	}

	apiServer, err := server.New(config.Cfg)
	if err != nil {
		return err
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return apiServer.Run(runCtx)
}

func canStartAPIServer(cfg *config.Config) (bool, error) {
	if cfg == nil {
		return false, fmt.Errorf("configuration not loaded")
	}
	serverCfg := cfg.Server
	if strings.TrimSpace(serverCfg.TLSCertFile) == "" {
		return false, fmt.Errorf("server.tls_cert_file is not configured")
	}
	if strings.TrimSpace(serverCfg.TLSKeyFile) == "" {
		return false, fmt.Errorf("server.tls_key_file is not configured")
	}
	if strings.TrimSpace(cfg.CertAuth.CACertFile) == "" {
		return false, fmt.Errorf("cert_auth.ca_cert_file is not configured")
	}
	return true, nil
}
