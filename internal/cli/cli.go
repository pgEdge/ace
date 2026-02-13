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
	"github.com/urfave/cli/v2"
)

//go:embed default_config.yaml
var defaultConfigYAML string

//go:embed default_pg_service.conf
var defaultPgServiceConf string

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
			Name:    "quiet",
			Aliases: []string{"q"},
			Usage:   "Whether to suppress output",
			Value:   false,
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
		&cli.Float64Flag{
			Name:    "concurrency-factor",
			Aliases: []string{"c"},
			Usage:   "CPU ratio for concurrency (0.0–4.0, e.g. 0.5 uses half of available CPUs)",
			Value:   0.5,
		},
		&cli.IntFlag{
			Name:    "compare-unit-size",
			Aliases: []string{"u"},
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
			Name:    "override-block-size",
			Aliases: []string{"B"},
			Usage:   "Override block size",
			Value:   false,
		},
	}

	rerunOnlyFlags := []cli.Flag{
		&cli.StringFlag{
			Name:     "diff-file",
			Aliases:  []string{"f"},
			Usage:    "Path to the diff file to rerun from (required)",
			Required: true,
		},
	}

	skipFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "skip-tables",
			Aliases: []string{"T"},
			Usage:   "Comma-separated list of tables to skip",
		},
		&cli.StringFlag{
			Name:    "skip-file",
			Aliases: []string{"s"},
			Usage:   "Path to a file with a list of tables to skip",
		},
	}

	tableDiffFlags := append(commonFlags, diffFlags...)
	tableDiffFlags = append(tableDiffFlags,
		&cli.StringFlag{
			Name:    "table-filter",
			Aliases: []string{"F"},
			Usage:   "Where clause expression to use while diffing tables",
			Value:   "",
		},
		&cli.StringFlag{
			Name:  "against-origin",
			Usage: "Restrict diff to rows whose node_origin matches this Spock node id or name",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "until",
			Usage: "Optional commit timestamp upper bound (RFC3339) for rows to include",
			Value: "",
		},
		&cli.BoolFlag{
			Name:  "ensure-pgcrypto",
			Usage: "Ensure pgcrypto extension is installed on each node before diffing",
			Value: false,
		},
		&cli.BoolFlag{
			Name:    "schedule",
			Aliases: []string{"S"},
			Usage:   "Schedule a table-diff job to run periodically",
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "every",
			Aliases: []string{"e"},
			Usage:   "Time duration (e.g., 5m, 3h, etc.)",
			Value:   "",
		},
	)

	tableRerunFlags := append(commonFlags, rerunOnlyFlags...)

	tableRepairFlags := []cli.Flag{
		&cli.StringFlag{
			Name:     "diff-file",
			Aliases:  []string{"f"},
			Usage:    "Path to the diff file (required)",
			Required: true,
		},
		&cli.StringFlag{
			Name:    "repair-plan",
			Aliases: []string{"p"},
			Usage:   "Path to the advanced repair plan (YAML/JSON); skips source-of-truth requirement",
		},
		&cli.StringFlag{
			Name:    "source-of-truth",
			Aliases: []string{"r"},
			Usage:   "Name of the node to be considered the source of truth",
		},
		&cli.BoolFlag{
			Name:    "dry-run",
			Aliases: []string{"y"},
			Usage:   "Show what would be done without executing",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "generate-report",
			Aliases: []string{"g"},
			Usage:   "Generate a report of the repair operation",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "insert-only",
			Aliases: []string{"i"},
			Usage:   "Only perform inserts, no updates or deletes",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "upsert-only",
			Aliases: []string{"P"},
			Usage:   "Only perform upserts (insert or update), no deletes",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "fire-triggers",
			Aliases: []string{"t"},
			Usage:   "Whether to fire triggers during repairs",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "bidirectional",
			Aliases: []string{"Z"},
			Usage:   "Whether to perform repairs in both directions. Can be used only with the insert-only option",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:  "recovery-mode",
			Usage: "Enable recovery-mode repair using origin-only diffs",
			Value: false,
		},
		&cli.BoolFlag{
			Name:    "fix-nulls",
			Aliases: []string{"X"},
			Usage:   "Fill NULL columns on each node using non-NULL values from its peers (no source-of-truth required)",
			Value:   false,
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
	repsetDiffFlags = append(repsetDiffFlags,
		&cli.BoolFlag{
			Name:    "schedule",
			Aliases: []string{"S"},
			Usage:   "Schedule a repset-diff job to run periodically",
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "every",
			Aliases: []string{"e"},
			Usage:   "Time duration (e.g., 5m, 3h, etc.)",
			Value:   "",
		},
	)

	schemaDiffFlags := append(commonFlags, diffFlags...)
	schemaDiffFlags = append(schemaDiffFlags, skipFlags...)
	schemaDiffFlags = append(schemaDiffFlags, &cli.BoolFlag{
		Name:    "ddl-only",
		Aliases: []string{"L"},
		Usage:   "Compare only schema objects (tables, functions, etc.), not table data",
		Value:   false,
	},
		&cli.BoolFlag{
			Name:    "schedule",
			Aliases: []string{"S"},
			Usage:   "Schedule a schema-diff job to run periodically",
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "every",
			Aliases: []string{"e"},
			Usage:   "Time duration (e.g., 5m, 3h, etc.)",
			Value:   "",
		},
	)

	mtreeBuildFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "block-size",
			Aliases: []string{"b"},
			Usage:   "Rows per leaf block",
			Value:   "10000",
		},
		&cli.Float64Flag{
			Name:    "max-cpu-ratio",
			Aliases: []string{"m"},
			Usage:   "Max CPU for parallel operations",
			Value:   0.5,
		},
		&cli.BoolFlag{
			Name:    "override-block-size",
			Aliases: []string{"B"},
			Usage:   "Skip block size check, and potentially tolerate unsafe block sizes",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "analyse",
			Aliases: []string{"a"},
			Usage:   "Run ANALYZE on the table",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "recreate-objects",
			Aliases: []string{"R"},
			Usage:   "Drop and recreate Merkle tree objects",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "write-ranges",
			Aliases: []string{"w"},
			Usage:   "Write block ranges to a JSON file",
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "ranges-file",
			Aliases: []string{"k"},
			Usage:   "Path to a file with pre-computed ranges",
		},
	}
	mtreeBuildFlags = append(mtreeBuildFlags, commonFlags...)

	mtreeUpdateFlags := []cli.Flag{
		&cli.Float64Flag{
			Name:    "max-cpu-ratio",
			Aliases: []string{"m"},
			Usage:   "Max CPU for parallel operations",
			Value:   0.5,
		},
		&cli.BoolFlag{
			Name:    "rebalance",
			Aliases: []string{"l"},
			Usage:   "Rebalance the tree by merging small blocks",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "skip-cdc",
			Aliases: []string{"U"},
			Usage:   "Skip CDC processing (only rehash dirty blocks)",
			Value:   false,
		},
	}
	mtreeUpdateFlags = append(mtreeUpdateFlags, commonFlags...)

	mtreeDiffFlags := []cli.Flag{
		&cli.Float64Flag{
			Name:    "max-cpu-ratio",
			Aliases: []string{"m"},
			Usage:   "Max CPU for parallel operations",
			Value:   0.5,
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "Output format",
			Value:   "json",
		},
		&cli.BoolFlag{
			Name:    "skip-cdc",
			Aliases: []string{"U"},
			Usage:   "Skip CDC processing (only rehash dirty blocks and compare)",
			Value:   false,
		},
	}
	mtreeDiffFlags = append(mtreeDiffFlags, commonFlags...)

	configInitFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "path",
			Aliases: []string{"p"},
			Usage:   "Path to write the config file",
			Value:   "ace.yaml",
		},
		&cli.BoolFlag{
			Name:    "force",
			Aliases: []string{"x"},
			Usage:   "Overwrite the config file if it already exists",
		},
		&cli.BoolFlag{
			Name:    "stdout",
			Aliases: []string{"z"},
			Usage:   "Print the config to stdout instead of writing a file",
		},
	}

	clusterInitFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "path",
			Aliases: []string{"p"},
			Usage:   "Path to write the pg service file",
			Value:   "pg_service.conf",
		},
		&cli.BoolFlag{
			Name:    "force",
			Aliases: []string{"x"},
			Usage:   "Overwrite the service file if it already exists",
		},
		&cli.BoolFlag{
			Name:    "stdout",
			Aliases: []string{"z"},
			Usage:   "Print the service file to stdout instead of writing a file",
		},
	}

	app := &cli.App{
		Name:  "ace",
		Usage: "ACE - Active Consistency Engine",
		Commands: []*cli.Command{
			{
				Name:  "config",
				Usage: "Manage ACE configuration files",
				Subcommands: []*cli.Command{
					{
						Name:   "init",
						Usage:  "Create a default ace.yaml file",
						Flags:  configInitFlags,
						Action: ConfigInitCLI,
					},
				},
			},
			{
				Name:  "start",
				Usage: "Start the ACE scheduler for configured jobs",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "debug",
						Aliases: []string{"v"},
						Usage:   "Enable debug logging",
					},
					&cli.StringFlag{
						Name:    "component",
						Aliases: []string{"C"},
						Usage:   "Component to start: scheduler, api, or all",
						Value:   "all",
					},
				},
				Action: StartSchedulerCLI,
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
				Name:  "server",
				Usage: "Run the ACE REST API server",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "debug",
						Aliases: []string{"v"},
						Usage:   "Enable debug logging",
					},
				},
				Action: StartAPIServerCLI,
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
				Name:  "cluster",
				Usage: "Manage ACE cluster service definitions",
				Subcommands: []*cli.Command{
					{
						Name:   "init",
						Usage:  "Create a sample pg_service.conf file",
						Flags:  clusterInitFlags,
						Action: ClusterInitCLI,
					},
				},
			},
			{
				Name:      "table-diff",
				Usage:     "Compare tables between PostgreSQL databases",
				ArgsUsage: "[cluster] <table>",
				Description: "A tool for comparing tables between PostgreSQL databases " +
					"and detecting data inconsistencies",
				Action: func(ctx *cli.Context) error {
					argsLen := ctx.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for table-diff: needs <table>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for table-diff (usage: [cluster] <table>)")
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
				ArgsUsage: "[cluster]",
				Flags:     tableRerunFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() > 1 {
						return fmt.Errorf("unexpected arguments for table-rerun (usage: [cluster])")
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
				ArgsUsage: "[cluster] <table>",
				Flags:     tableRepairFlags,
				Action: func(ctx *cli.Context) error {
					argsLen := ctx.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for table-repair: needs <table>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for table-repair (usage: [cluster] <table>)")
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
				ArgsUsage: "[cluster]",
				Flags:     spockDiffFlags,
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() > 1 {
						return fmt.Errorf("unexpected arguments for spock-diff (usage: [cluster])")
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
				ArgsUsage: "[cluster] <schema>",
				Flags:     schemaDiffFlags,
				Action: func(ctx *cli.Context) error {
					argsLen := ctx.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for schema-diff: needs <schema>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for schema-diff (usage: [cluster] <schema>)")
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
				ArgsUsage: "[cluster] <repset>",
				Flags:     repsetDiffFlags,
				Action: func(ctx *cli.Context) error {
					argsLen := ctx.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for repset-diff: needs <repset>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for repset-diff (usage: [cluster] <repset>)")
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
						ArgsUsage: "[cluster]",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() > 1 {
								return fmt.Errorf("unexpected arguments for mtree init (usage: [cluster])")
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
						ArgsUsage: "[cluster]",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() > 1 {
								return fmt.Errorf("unexpected arguments for mtree listen (usage: [cluster])")
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
						ArgsUsage: "[cluster]",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							if ctx.Args().Len() > 1 {
								return fmt.Errorf("unexpected arguments for mtree teardown (usage: [cluster])")
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
						ArgsUsage: "[cluster] <table>",
						Flags:     commonFlags,
						Action: func(ctx *cli.Context) error {
							argsLen := ctx.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree teardown-table: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree teardown-table (usage: [cluster] <table>)")
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
						ArgsUsage: "[cluster] <table>",
						Action: func(ctx *cli.Context) error {
							argsLen := ctx.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree build: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree build (usage: [cluster] <table>)")
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
						ArgsUsage: "[cluster] <table>",
						Action: func(ctx *cli.Context) error {
							argsLen := ctx.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree update: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree update (usage: [cluster] <table>)")
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
						ArgsUsage: "[cluster] <table>",
						Action: func(ctx *cli.Context) error {
							argsLen := ctx.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree diff: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree diff (usage: [cluster] <table>)")
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

func initTemplateFile(ctx *cli.Context, content string, defaultPath string, label string, perm os.FileMode) error {
	outputPath := ctx.String("path")
	if outputPath == "" {
		outputPath = defaultPath
	}

	if ctx.Bool("stdout") || outputPath == "-" {
		fmt.Println(content)
		return nil
	}

	if !ctx.Bool("force") {
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

func ConfigInitCLI(ctx *cli.Context) error {
	return initTemplateFile(ctx, defaultConfigYAML, "ace.yaml", "config file", 0o644)
}

func ClusterInitCLI(ctx *cli.Context) error {
	return initTemplateFile(ctx, defaultPgServiceConf, "pg_service.conf", "pg service file", 0o600)
}

func resolveClusterArg(cmd, missingUsage, argsUsage string, required int, args []string) (string, []string, error) {
	if len(args) < required {
		if required == 1 {
			return "", nil, fmt.Errorf("missing required argument for %s: needs %s", cmd, missingUsage)
		}
		return "", nil, fmt.Errorf("missing required arguments for %s: needs %s", cmd, missingUsage)
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

	return "", nil, fmt.Errorf("unexpected arguments for %s (usage: %s)", cmd, argsUsage)
}

func TableDiffCLI(ctx *cli.Context) error {
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := diff.NewTableDiffTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = ctx.String("dbname")
	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Float64("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = strings.ToLower(ctx.String("output"))
	task.Nodes = ctx.String("nodes")
	task.EnsurePgcrypto = ctx.Bool("ensure-pgcrypto")
	scheduleEnabled := ctx.Bool("schedule")
	scheduleEvery := ctx.String("every")
	task.TableFilter = ctx.String("table-filter")
	task.AgainstOrigin = ctx.String("against-origin")
	task.Until = ctx.String("until")
	task.QuietMode = ctx.Bool("quiet")
	task.OverrideBlockSize = ctx.Bool("override-block-size")
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

func MtreeInitCLI(ctx *cli.Context) error {
	args := ctx.Args().Slice()
	clusterName, _, err := resolveClusterArg("mtree init", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
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
	args := ctx.Args().Slice()
	clusterName, _, err := resolveClusterArg("mtree listen", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
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
	args := ctx.Args().Slice()
	clusterName, _, err := resolveClusterArg("mtree teardown", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
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
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree teardown-table", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
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
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree build", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
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
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree update", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.MaxCpuRatio = ctx.Float64("max-cpu-ratio")
	task.Rebalance = ctx.Bool("rebalance")
	task.NoCDC = ctx.Bool("skip-cdc")
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
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree diff", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.MaxCpuRatio = ctx.Float64("max-cpu-ratio")
	task.Output = ctx.String("output")
	task.NoCDC = ctx.Bool("skip-cdc")
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
	args := ctx.Args().Slice()
	clusterName, _, err := resolveClusterArg("table-rerun", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := diff.NewTableDiffTask()
	task.TaskID = uuid.NewString()
	task.Mode = "rerun"
	task.ClusterName = clusterName
	task.DiffFilePath = ctx.String("diff-file")
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.QuietMode = ctx.Bool("quiet")
	task.Ctx = context.Background()

	if err := task.ExecuteTask(); err != nil {
		return fmt.Errorf("error during table-rerun: %w", err)
	}

	return nil
}

func TableRepairCLI(ctx *cli.Context) error {
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("table-repair", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := repair.NewTableRepairTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DiffFilePath = ctx.String("diff-file")
	task.RepairPlanPath = ctx.String("repair-plan")
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
	task.RecoveryMode = ctx.Bool("recovery-mode")

	if err := task.ValidateAndPrepare(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.Run(true); err != nil {
		return fmt.Errorf("error during table repair: %w", err)
	}

	return nil
}

func SpockDiffCLI(ctx *cli.Context) error {
	args := ctx.Args().Slice()
	clusterName, _, err := resolveClusterArg("spock-diff", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := diff.NewSpockDiffTask()
	task.ClusterName = clusterName
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
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("schema-diff", "<schema>", "[cluster] <schema>", 1, args)
	if err != nil {
		return err
	}
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := diff.NewSchemaDiffTask()
	task.ClusterName = clusterName
	task.SchemaName = positional[0]
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.SkipTables = ctx.String("skip-tables")
	scheduleEnabled := ctx.Bool("schedule")
	scheduleEvery := ctx.String("every")
	task.SkipFile = ctx.String("skip-file")
	task.Quiet = ctx.Bool("quiet")
	task.DDLOnly = ctx.Bool("ddl-only")
	task.Ctx = context.Background()

	if scheduleEnabled && task.DDLOnly {
		return fmt.Errorf("scheduling is only supported when --ddl-only is false")
	}

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Float64("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = ctx.String("output")
	task.OverrideBlockSize = ctx.Bool("override-block-size")

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

func RepsetDiffCLI(ctx *cli.Context) error {
	args := ctx.Args().Slice()
	clusterName, positional, err := resolveClusterArg("repset-diff", "<repset>", "[cluster] <repset>", 1, args)
	if err != nil {
		return err
	}
	blockSizeStr := ctx.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	scheduleEnabled := ctx.Bool("schedule")
	scheduleEvery := ctx.String("every")

	task := diff.NewRepsetDiffTask()
	task.ClusterName = clusterName
	task.RepsetName = positional[0]
	task.DBName = ctx.String("dbname")
	task.Nodes = ctx.String("nodes")
	task.SkipTables = ctx.String("skip-tables")
	task.SkipFile = ctx.String("skip-file")
	task.Quiet = ctx.Bool("quiet")
	task.Ctx = context.Background()

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = ctx.Float64("concurrency-factor")
	task.CompareUnitSize = ctx.Int("compare-unit-size")
	task.Output = ctx.String("output")
	task.OverrideBlockSize = ctx.Bool("override-block-size")

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

func StartSchedulerCLI(ctx *cli.Context) error {
	if config.Cfg == nil {
		return fmt.Errorf("configuration not loaded; run inside a directory with ace.yaml or set ACE_CONFIG")
	}

	component := strings.ToLower(strings.TrimSpace(ctx.String("component")))
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

func StartAPIServerCLI(ctx *cli.Context) error {
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
