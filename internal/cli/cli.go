// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/pgedge/ace/internal/cdc"
	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/urfave/cli/v2"
)

func SetupCLI() *cli.App {
	commonFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "dbname",
			Aliases: []string{"d"},
			Usage:   "Name of the database",
			Value:   "",
		},
		&cli.StringFlag{
			Name:    "nodes",
			Aliases: []string{"n"},
			Usage:   "Nodes to include in the diff (default: all)",
			Value:   "all",
		},
		&cli.BoolFlag{
			Name:  "quiet",
			Usage: "Whether to suppress output",
			Value: false,
		},
		&cli.BoolFlag{
			Name:    "debug",
			Aliases: []string{"v"},
			Usage:   "Enable debug logging",
			Value:   false,
		},
	}

	diffFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "block-size",
			Aliases: []string{"b"},
			Usage:   "Number of rows per block",
			Value:   "100000",
		},
		&cli.IntFlag{
			Name:    "concurrency-factor",
			Aliases: []string{"c"},
			Usage:   "Concurrency factor",
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "compare-unit-size",
			Aliases: []string{"s"},
			Usage:   "Max size of the smallest block to use when diffs are present",
			Value:   10000,
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "Output format",
			Value:   "json",
		},
		&cli.BoolFlag{
			Name:  "override-block-size",
			Usage: "Override block size",
			Value: false,
		},
	}

	rerunOnlyFlags := []cli.Flag{
		&cli.StringFlag{
			Name:     "diff-file",
			Usage:    "Path to the diff file to rerun from (required)",
			Required: true,
		},
	}

	skipFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "skip-tables",
			Usage: "Comma-separated list of tables to skip",
		},
		&cli.StringFlag{
			Name:  "skip-file",
			Usage: "Path to a file with a list of tables to skip",
		},
	}

	tableDiffFlags := append(commonFlags, diffFlags...)
	tableDiffFlags = append(tableDiffFlags, &cli.StringFlag{
		Name:  "table-filter",
		Usage: "Where clause expression to use while diffing tables",
		Value: "",
	})

	tableRerunFlags := append(commonFlags, rerunOnlyFlags...)

	tableRepairFlags := []cli.Flag{
		&cli.StringFlag{
			Name:     "diff-file",
			Aliases:  []string{"f"},
			Usage:    "Path to the diff file (required)",
			Required: true,
		},
		&cli.StringFlag{
			Name:    "source-of-truth",
			Aliases: []string{"s"},
			Usage:   "Name of the node to be considered the source of truth",
		},
		&cli.BoolFlag{
			Name:  "dry-run",
			Usage: "Show what would be done without executing",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "generate-report",
			Usage: "Generate a report of the repair operation",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "insert-only",
			Usage: "Only perform inserts, no updates or deletes",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "upsert-only",
			Usage: "Only perform upserts (insert or update), no deletes",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "fire-triggers",
			Usage: "Whether to fire triggers during repairs",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "bidirectional",
			Usage: "Whether to perform repairs in both directions. Can be used only with the insert-only option",
			Value: false,
		},
	}

	tableRepairFlags = append(commonFlags, tableRepairFlags...)

	spockDiffFlags := append(commonFlags, &cli.StringFlag{
		Name:    "output",
		Aliases: []string{"o"},
		Usage:   "Output format",
		Value:   "json",
	})

	repsetDiffFlags := append(commonFlags, diffFlags...)
	repsetDiffFlags = append(repsetDiffFlags, skipFlags...)

	schemaDiffFlags := append(commonFlags, diffFlags...)
	schemaDiffFlags = append(schemaDiffFlags, skipFlags...)
	schemaDiffFlags = append(schemaDiffFlags, &cli.BoolFlag{
		Name:  "ddl-only",
		Usage: "Compare only schema objects (tables, functions, etc.), not table data",
		Value: false,
	})

	mtreeBuildFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "block-size",
			Aliases: []string{"b"},
			Usage:   "Rows per leaf block",
			Value:   "10000",
		},
		&cli.Float64Flag{
			Name:  "max-cpu-ratio",
			Usage: "Max CPU for parallel operations",
			Value: 0.5,
		},
		&cli.BoolFlag{
			Name:  "override-block-size",
			Usage: "Skip block size check, and potentially tolerate unsafe block sizes",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "analyse",
			Usage: "Run ANALYZE on the table",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "recreate-objects",
			Usage: "Drop and recreate Merkle tree objects",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "write-ranges",
			Usage: "Write block ranges to a JSON file",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "ranges-file",
			Usage: "Path to a file with pre-computed ranges",
		},
	}
	mtreeBuildFlags = append(mtreeBuildFlags, commonFlags...)

	mtreeUpdateFlags := []cli.Flag{
		&cli.Float64Flag{
			Name:  "max-cpu-ratio",
			Usage: "Max CPU for parallel operations",
			Value: 0.5,
		},
		&cli.BoolFlag{
			Name:  "rebalance",
			Usage: "Rebalance the tree by merging small blocks",
			Value: false,
		},
	}
	mtreeUpdateFlags = append(mtreeUpdateFlags, commonFlags...)

	mtreeDiffFlags := []cli.Flag{
		&cli.Float64Flag{
			Name:  "max-cpu-ratio",
			Usage: "Max CPU for parallel operations",
			Value: 0.5,
		},
		&cli.IntFlag{
			Name:  "batch-size",
			Usage: "Number of ranges to process in a batch",
			Value: 100,
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "Output format",
			Value:   "json",
		},
		&cli.BoolFlag{
			Name:    "skip-update",
			Aliases: []string{"s"},
			Usage:   "Skip updating the Merkle tree",
			Value:   false,
		},
	}
	mtreeDiffFlags = append(mtreeDiffFlags, commonFlags...)

	app := &cli.App{
		Name:  "ace",
		Usage: "ACE - Active Consistency Engine",
		Commands: []*cli.Command{
			{
				Name:      "table-diff",
				Usage:     "Compare tables between PostgreSQL databases",
				ArgsUsage: "<cluster> <table>",
				Description: "A tool for comparing tables between PostgreSQL databases " +
					"and detecting data inconsistencies",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return fmt.Errorf("missing required arguments for table-diff: needs <cluster> and <table>")
					}
					return TableDiffCLI(ctx)
				},
				Flags: tableDiffFlags,
				Before: func(ctx *cli.Context) error {
					if ctx.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return nil
				},
			},
			{
				Name:      "table-rerun",
				Usage:     "Re-run a diff from a file to check for persistent differences",
				ArgsUsage: "<cluster>",
				Flags:     tableRerunFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return fmt.Errorf("missing required argument for table-rerun: needs <cluster>")
					}
					return TableRerunCLI(ctx)
				},
				Before: func(ctx *cli.Context) error {
					if ctx.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return nil
				},
			},
			{
				Name:      "table-repair",
				Usage:     "Repair table inconsistencies based on a diff file",
				ArgsUsage: "<cluster> <table>",
				Flags:     tableRepairFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return fmt.Errorf("missing required arguments for table-repair: needs <cluster> and <table>")
					}
					return TableRepairCLI(ctx)
				},
				Before: func(ctx *cli.Context) error {
					if ctx.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return nil
				},
			},
			{
				Name:      "spock-diff",
				Usage:     "Compare spock metadata across cluster nodes",
				ArgsUsage: "<cluster>",
				Flags:     spockDiffFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 1 {
						return fmt.Errorf("missing required argument for spock-diff: needs <cluster>")
					}
					return SpockDiffCLI(ctx)
				},
				Before: func(ctx *cli.Context) error {
					if ctx.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return nil
				},
			},
			{
				Name:      "schema-diff",
				Usage:     "Compare schemas across cluster nodes",
				ArgsUsage: "<cluster> <schema>",
				Flags:     schemaDiffFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return fmt.Errorf("missing required arguments for schema-diff: needs <cluster> and <schema>")
					}
					return SchemaDiffCLI(ctx)
				},
				Before: func(ctx *cli.Context) error {
					if ctx.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return nil
				},
			},
			{
				Name:      "repset-diff",
				Usage:     "Compare replication sets across cluster nodes",
				ArgsUsage: "<cluster> <repset>",
				Flags:     repsetDiffFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return fmt.Errorf("missing required arguments for repset-diff: needs <cluster> and <repset>")
					}
					return RepsetDiffCLI(ctx)
				},
				Before: func(ctx *cli.Context) error {
					if ctx.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return nil
				},
			},
			{
				Name:  "mtree",
				Usage: "Merkle tree operations",
				Subcommands: []*cli.Command{
					{
						Name:      "init",
						Usage:     "Initialise Merkle tree replication for a cluster",
						ArgsUsage: "<cluster>",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 1 {
								return fmt.Errorf("missing required argument for mtree init: needs <cluster>")
							}
							return MtreeInitCLI(ctx)
						},
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
					{
						Name:      "listen",
						Usage:     "Listen for changes and update Merkle trees",
						ArgsUsage: "<cluster>",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 1 {
								return fmt.Errorf("missing required argument for mtree listen: needs <cluster>")
							}
							return MtreeListenCLI(ctx)
						},
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
					{
						Name:      "teardown",
						Usage:     "Teardown Merkle tree replication for a cluster",
						ArgsUsage: "<cluster>",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 1 {
								return fmt.Errorf("missing required argument for mtree teardown: needs <cluster>")
							}
							return MtreeTeardownCLI(ctx)
						},
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
					{
						Name:      "teardown-table",
						Usage:     "Teardown Merkle tree objects for a specific table",
						ArgsUsage: "<cluster> <table>",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 2 {
								return fmt.Errorf("missing required arguments for mtree teardown-table: needs <cluster> and <table>")
							}
							return MtreeTeardownTableCLI(ctx)
						},
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
					{
						Name:      "build",
						Usage:     "Build a new Merkle tree for a table",
						ArgsUsage: "<cluster> <table>",
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 2 {
								return fmt.Errorf("missing required arguments for mtree build: needs <cluster> and <table>")
							}
							return MtreeBuildCLI(ctx)
						},
						Flags: mtreeBuildFlags,
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
					{
						Name:      "update",
						Usage:     "Update an existing Merkle tree for a table",
						ArgsUsage: "<cluster> <table>",
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 2 {
								return fmt.Errorf("missing required arguments for mtree update: needs <cluster> and <table>")
							}
							return MtreeUpdateCLI(ctx)
						},
						Flags: mtreeUpdateFlags,
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
					{
						Name:      "table-diff",
						Usage:     "Use Merkle Trees for performing table-diff",
						ArgsUsage: "<cluster> <table>",
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() < 2 {
								return fmt.Errorf("missing required arguments for mtree diff: needs <cluster> and <table>")
							}
							return MtreeDiffCLI(ctx)
						},
						Flags: mtreeDiffFlags,
						Before: func(ctx *cli.Context) error {
							if ctx.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return nil
						},
					},
				},
			},
		},
	}

	return app
}

func TableDiffCLI(ctx *cli.Context) error {
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := core.NewTableDiffTask()
	task.ClusterName = ctx.Args().Get(0)
	task.QualifiedTableName = ctx.Args().Get(1)
	task.DBName = ctx.String("dbname")
	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Int("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = ctx.String("output")
	task.Nodes = ctx.String("nodes")
	task.TableFilter = ctx.String("table-filter")
	task.QuietMode = ctx.Bool("quiet")
	task.OverrideBlockSize = ctx.Bool("override-block-size")
	task.Ctx = context.Background()

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.RunChecks(true); err != nil {
		return fmt.Errorf("checks failed: %w", err)
	}

	if err := task.ExecuteTask(); err != nil {
		return fmt.Errorf("error during comparison: %w", err)
	}

	return nil
}

func MtreeInitCLI(ctx *cli.Context) error {
	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.Mode = "init"
	task.Ctx = context.Background()

	if err := task.MtreeInit(); err != nil {
		return fmt.Errorf("error during mtree init: %w", err)
	}

	return nil
}

func MtreeListenCLI(ctx *cli.Context) error {
	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
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

func MtreeTeardownCLI(ctx *cli.Context) error {
	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.Mode = "teardown"
	task.Ctx = context.Background()

	if err := task.MtreeTeardown(); err != nil {
		return fmt.Errorf("error during mtree teardown: %w", err)
	}

	return nil
}

func MtreeTeardownTableCLI(ctx *cli.Context) error {
	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.QualifiedTableName = ctx.Args().Get(1)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.Mode = "teardown-table"
	task.Ctx = context.Background()

	if err := task.MtreeTeardownTable(); err != nil {
		return fmt.Errorf("error during mtree table teardown: %w", err)
	}

	return nil
}

func MtreeBuildCLI(ctx *cli.Context) error {
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.QualifiedTableName = ctx.Args().Get(1)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.BlockSize = int(blockSizeInt)
	task.MaxCpuRatio = ctx.Float64("max-cpu-ratio")
	task.OverrideBlockSize = ctx.Bool("override-block-size")
	task.Analyse = ctx.Bool("analyse")
	task.RecreateObjects = ctx.Bool("recreate-objects")
	task.WriteRanges = ctx.Bool("write-ranges")
	task.RangesFile = ctx.String("ranges-file")
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

func MtreeUpdateCLI(ctx *cli.Context) error {
	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.QualifiedTableName = ctx.Args().Get(1)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.MaxCpuRatio = ctx.Float64("max-cpu-ratio")
	task.Rebalance = ctx.Bool("rebalance")
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

func MtreeDiffCLI(ctx *cli.Context) error {
	task := core.NewMerkleTreeTask()
	task.ClusterName = ctx.Args().Get(0)
	task.QualifiedTableName = ctx.Args().Get(1)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.MaxCpuRatio = ctx.Float64("max-cpu-ratio")
	task.BatchSize = ctx.Int("batch-size")
	task.Output = ctx.String("output")
	task.NoCDC = ctx.Bool("skip-update")
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

func TableRerunCLI(ctx *cli.Context) error {
	task := core.NewTableDiffTask()
	task.TaskID = uuid.NewString()
	task.Mode = "rerun"
	task.ClusterName = ctx.Args().Get(0)
	task.DiffFilePath = ctx.String("diff-file")
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.Ctx = context.Background()

	if err := task.ExecuteRerunTask(); err != nil {
		return fmt.Errorf("error during table-rerun: %w", err)
	}

	return nil
}

func TableRepairCLI(ctx *cli.Context) error {
	task := core.NewTableRepairTask()
	task.ClusterName = ctx.Args().Get(0)
	task.QualifiedTableName = ctx.Args().Get(1)
	task.DiffFilePath = ctx.String("diff-file")
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.SourceOfTruth = ctx.String("source-of-truth")
	task.QuietMode = ctx.Bool("quiet")
	task.Ctx = context.Background()

	task.DryRun = ctx.Bool("dry-run")
	task.InsertOnly = ctx.Bool("insert-only")
	task.UpsertOnly = ctx.Bool("upsert-only")
	task.FireTriggers = ctx.Bool("fire-triggers")
	task.FixNulls = ctx.Bool("fix-nulls")
	task.Bidirectional = ctx.Bool("bidirectional")
	task.GenerateReport = ctx.Bool("generate-report")

	if err := task.ValidateAndPrepare(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.Run(true); err != nil {
		return fmt.Errorf("error during table repair: %w", err)
	}

	return nil
}

func SpockDiffCLI(ctx *cli.Context) error {
	task := core.NewSpockDiffTask()
	task.ClusterName = ctx.Args().Get(0)
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.Output = ctx.String("output")
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

func SchemaDiffCLI(ctx *cli.Context) error {
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := &core.SchemaDiffCmd{
		ClusterName: ctx.Args().Get(0),
		SchemaName:  ctx.Args().Get(1),
		DBName:      ctx.String("dbname"),
		Nodes:       ctx.String("nodes"),
		SkipTables:  ctx.String("skip-tables"),
		SkipFile:    ctx.String("skip-file"),
		Quiet:       ctx.Bool("quiet"),
		DDLOnly:     ctx.Bool("ddl-only"),
		Ctx:         context.Background(),
	}

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Int("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = ctx.String("output")
	task.OverrideBlockSize = ctx.Bool("override-block-size")

	if err := task.SchemaTableDiff(); err != nil {
		return fmt.Errorf("error during schema diff: %w", err)
	}
	return nil
}

func RepsetDiffCLI(ctx *cli.Context) error {
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := &core.RepsetDiffCmd{
		ClusterName: ctx.Args().Get(0),
		RepsetName:  ctx.Args().Get(1),
		DBName:      ctx.String("dbname"),
		Nodes:       ctx.String("nodes"),
		SkipTables:  ctx.String("skip-tables"),
		SkipFile:    ctx.String("skip-file"),
		Quiet:       ctx.Bool("quiet"),
		Ctx:         context.Background(),
	}

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Int("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = ctx.String("output")
	task.OverrideBlockSize = ctx.Bool("override-block-size")

	if err := core.RepsetDiff(task); err != nil {
		return fmt.Errorf("error during repset diff: %w", err)
	}
	return nil
}
