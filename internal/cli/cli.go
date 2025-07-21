/////////////////////////////////////////////////////////////////////////////
//
// ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the pgEdge Community License:
//      https://www.pgedge.com/communitylicense
//
/////////////////////////////////////////////////////////////////////////////

package cli

import (
	"fmt"
	"strconv"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/internal/logger"
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

func TableRerunCLI(ctx *cli.Context) error {
	task := core.NewTableDiffTask()
	task.TaskID = uuid.NewString()
	task.Mode = "rerun"
	task.ClusterName = ctx.Args().Get(0)
	task.DiffFilePath = ctx.String("diff-file")
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")

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
	}

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Int("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = ctx.String("output")
	task.OverrideBlockSize = ctx.Bool("override-block-size")

	if err := core.SchemaDiff(task); err != nil {
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
