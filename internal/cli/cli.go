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
	"github.com/urfave/cli/v3"
)

//go:embed default_config.yaml
var defaultConfigYAML string

//go:embed default_pg_service.conf
var defaultPgServiceConf string

func SetupCLI() *cli.Command {
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
		&cli.IntFlag{
			Name:    "max-connections",
			Aliases: []string{"M"},
			Usage:   "Maximum number of database connections per node (default: derived from concurrency factor)",
			Value:   0,
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
			Name:    "preserve-origin",
			Usage:   "Preserve replication origin node ID and commit timestamp for repaired rows",
			Value:   false,
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

	app := &cli.Command{
		Name:  "ace",
		Usage: "ACE - Active Consistency Engine",
		Commands: []*cli.Command{
			{
				Name:  "config",
				Usage: "Manage ACE configuration files",
				Commands: []*cli.Command{
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
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
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
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:  "cluster",
				Usage: "Manage ACE cluster service definitions",
				Commands: []*cli.Command{
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
				Action: func(_ context.Context, cmd *cli.Command) error {
					argsLen := cmd.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for table-diff: needs <table>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for table-diff (usage: [cluster] <table>)")
					}
					return TableDiffCLI(cmd)
				},
				Flags: tableDiffFlags,
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:      "table-rerun",
				Usage:     "Re-run a diff from a file to check for persistent differences",
				ArgsUsage: "[cluster]",
				Flags:     tableRerunFlags,
				Action: func(_ context.Context, cmd *cli.Command) error {
					if cmd.Args().Len() > 1 {
						return fmt.Errorf("unexpected arguments for table-rerun (usage: [cluster])")
					}
					return TableRerunCLI(cmd)
				},
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:      "table-repair",
				Usage:     "Repair table inconsistencies based on a diff file",
				ArgsUsage: "[cluster] <table>",
				Flags:     tableRepairFlags,
				Action: func(_ context.Context, cmd *cli.Command) error {
					argsLen := cmd.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for table-repair: needs <table>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for table-repair (usage: [cluster] <table>)")
					}
					return TableRepairCLI(cmd)
				},
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:      "spock-diff",
				Usage:     "Compare spock metadata across cluster nodes",
				ArgsUsage: "[cluster]",
				Flags:     spockDiffFlags,
				Action: func(_ context.Context, cmd *cli.Command) error {
					if cmd.Args().Len() > 1 {
						return fmt.Errorf("unexpected arguments for spock-diff (usage: [cluster])")
					}
					return SpockDiffCLI(cmd)
				},
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:      "schema-diff",
				Usage:     "Compare schemas across cluster nodes",
				ArgsUsage: "[cluster] <schema>",
				Flags:     schemaDiffFlags,
				Action: func(_ context.Context, cmd *cli.Command) error {
					argsLen := cmd.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for schema-diff: needs <schema>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for schema-diff (usage: [cluster] <schema>)")
					}
					return SchemaDiffCLI(cmd)
				},
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:      "repset-diff",
				Usage:     "Compare replication sets across cluster nodes",
				ArgsUsage: "[cluster] <repset>",
				Flags:     repsetDiffFlags,
				Action: func(_ context.Context, cmd *cli.Command) error {
					argsLen := cmd.Args().Len()
					if argsLen == 0 {
						return fmt.Errorf("missing required argument for repset-diff: needs <repset>")
					}
					if argsLen > 2 {
						return fmt.Errorf("unexpected arguments for repset-diff (usage: [cluster] <repset>)")
					}
					return RepsetDiffCLI(cmd)
				},
				Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
					if cmd.Bool("debug") {
						logger.SetLevel(log.DebugLevel)
					} else {
						logger.SetLevel(log.InfoLevel)
					}
					return ctx, nil
				},
			},
			{
				Name:  "mtree",
				Usage: "Merkle tree operations",
				Commands: []*cli.Command{
					{
						Name:      "init",
						Usage:     "Initialise Merkle tree replication for a cluster",
						ArgsUsage: "[cluster]",
						Flags:     commonFlags,
						Action: func(_ context.Context, cmd *cli.Command) error {
							if cmd.Args().Len() > 1 {
								return fmt.Errorf("unexpected arguments for mtree init (usage: [cluster])")
							}
							return MtreeInitCLI(cmd)
						},
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
					{
						Name:      "listen",
						Usage:     "Listen for changes and update Merkle trees",
						ArgsUsage: "[cluster]",
						Flags:     commonFlags,
						Action: func(_ context.Context, cmd *cli.Command) error {
							if cmd.Args().Len() > 1 {
								return fmt.Errorf("unexpected arguments for mtree listen (usage: [cluster])")
							}
							return MtreeListenCLI(cmd)
						},
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
					{
						Name:      "teardown",
						Usage:     "Teardown Merkle tree replication for a cluster",
						ArgsUsage: "[cluster]",
						Flags:     commonFlags,
						Action: func(_ context.Context, cmd *cli.Command) error {
							if cmd.Args().Len() > 1 {
								return fmt.Errorf("unexpected arguments for mtree teardown (usage: [cluster])")
							}
							return MtreeTeardownCLI(cmd)
						},
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
					{
						Name:      "teardown-table",
						Usage:     "Teardown Merkle tree objects for a specific table",
						ArgsUsage: "[cluster] <table>",
						Flags:     commonFlags,
						Action: func(_ context.Context, cmd *cli.Command) error {
							argsLen := cmd.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree teardown-table: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree teardown-table (usage: [cluster] <table>)")
							}
							return MtreeTeardownTableCLI(cmd)
						},
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
					{
						Name:      "build",
						Usage:     "Build a new Merkle tree for a table",
						ArgsUsage: "[cluster] <table>",
						Action: func(_ context.Context, cmd *cli.Command) error {
							argsLen := cmd.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree build: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree build (usage: [cluster] <table>)")
							}
							return MtreeBuildCLI(cmd)
						},
						Flags: mtreeBuildFlags,
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
					{
						Name:      "update",
						Usage:     "Update an existing Merkle tree for a table",
						ArgsUsage: "[cluster] <table>",
						Action: func(_ context.Context, cmd *cli.Command) error {
							argsLen := cmd.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree update: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree update (usage: [cluster] <table>)")
							}
							return MtreeUpdateCLI(cmd)
						},
						Flags: mtreeUpdateFlags,
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
					{
						Name:      "table-diff",
						Usage:     "Use Merkle Trees for performing table-diff",
						ArgsUsage: "[cluster] <table>",
						Action: func(_ context.Context, cmd *cli.Command) error {
							argsLen := cmd.Args().Len()
							if argsLen == 0 {
								return fmt.Errorf("missing required argument for mtree diff: needs <table>")
							}
							if argsLen > 2 {
								return fmt.Errorf("unexpected arguments for mtree diff (usage: [cluster] <table>)")
							}
							return MtreeDiffCLI(cmd)
						},
						Flags: mtreeDiffFlags,
						Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
							if cmd.Bool("debug") {
								logger.SetLevel(log.DebugLevel)
							} else {
								logger.SetLevel(log.InfoLevel)
							}
							return ctx, nil
						},
					},
				},
			},
		},
	}

	return app
}

func initTemplateFile(cmd *cli.Command, content string, defaultPath string, label string, perm os.FileMode) error {
	outputPath := cmd.String("path")
	if outputPath == "" {
		outputPath = defaultPath
	}

	if cmd.Bool("stdout") || outputPath == "-" {
		fmt.Println(content)
		return nil
	}

	if !cmd.Bool("force") {
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

func ConfigInitCLI(_ context.Context, cmd *cli.Command) error {
	return initTemplateFile(cmd, defaultConfigYAML, "ace.yaml", "config file", 0o644)
}

func ClusterInitCLI(_ context.Context, cmd *cli.Command) error {
	return initTemplateFile(cmd, defaultPgServiceConf, "pg_service.conf", "pg service file", 0o600)
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

func TableDiffCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}

	blockSizeStr := cmd.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := diff.NewTableDiffTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = cmd.String("dbname")
	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = cmd.Float64("concurrency-factor")
	task.MaxConnections = cmd.Int("max-connections")
	task.CompareUnitSize = cmd.Int("compare-unit-size")
	task.Output = strings.ToLower(cmd.String("output"))
	task.Nodes = cmd.String("nodes")
	task.EnsurePgcrypto = cmd.Bool("ensure-pgcrypto")
	scheduleEnabled := cmd.Bool("schedule")
	scheduleEvery := cmd.String("every")
	task.TableFilter = cmd.String("table-filter")
	task.AgainstOrigin = cmd.String("against-origin")
	task.Until = cmd.String("until")
	task.QuietMode = cmd.Bool("quiet")
	task.OverrideBlockSize = cmd.Bool("override-block-size")
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

func MtreeInitCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, _, err := resolveClusterArg("mtree init", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.Mode = "init"
	task.Ctx = context.Background()

	if err := task.MtreeInit(); err != nil {
		return fmt.Errorf("error during mtree init: %w", err)
	}

	return nil
}

func MtreeListenCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, _, err := resolveClusterArg("mtree listen", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
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

func MtreeTeardownCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, _, err := resolveClusterArg("mtree teardown", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.Mode = "teardown"
	task.Ctx = context.Background()

	if err := task.MtreeTeardown(); err != nil {
		return fmt.Errorf("error during mtree teardown: %w", err)
	}

	return nil
}

func MtreeTeardownTableCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree teardown-table", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.Mode = "teardown-table"
	task.Ctx = context.Background()

	if err := task.MtreeTeardownTable(); err != nil {
		return fmt.Errorf("error during mtree table teardown: %w", err)
	}

	return nil
}

func MtreeBuildCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree build", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	blockSizeStr := cmd.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.BlockSize = int(blockSizeInt)
	task.MaxCpuRatio = cmd.Float64("max-cpu-ratio")
	task.OverrideBlockSize = cmd.Bool("override-block-size")
	task.Analyse = cmd.Bool("analyse")
	task.RecreateObjects = cmd.Bool("recreate-objects")
	task.WriteRanges = cmd.Bool("write-ranges")
	task.RangesFile = cmd.String("ranges-file")
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

func MtreeUpdateCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree update", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.MaxCpuRatio = cmd.Float64("max-cpu-ratio")
	task.Rebalance = cmd.Bool("rebalance")
	task.NoCDC = cmd.Bool("skip-cdc")
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

func MtreeDiffCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("mtree diff", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.MaxCpuRatio = cmd.Float64("max-cpu-ratio")
	task.Output = cmd.String("output")
	task.NoCDC = cmd.Bool("skip-cdc")
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

func TableRerunCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, _, err := resolveClusterArg("table-rerun", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := diff.NewTableDiffTask()
	task.TaskID = uuid.NewString()
	task.Mode = "rerun"
	task.ClusterName = clusterName
	task.DiffFilePath = cmd.String("diff-file")
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.QuietMode = cmd.Bool("quiet")
	task.Ctx = context.Background()

	if err := task.ExecuteTask(); err != nil {
		return fmt.Errorf("error during table-rerun: %w", err)
	}

	return nil
}

func TableRepairCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("table-repair", "<table>", "[cluster] <table>", 1, args)
	if err != nil {
		return err
	}
	task := repair.NewTableRepairTask()
	task.ClusterName = clusterName
	task.QualifiedTableName = positional[0]
	task.DiffFilePath = cmd.String("diff-file")
	task.RepairPlanPath = cmd.String("repair-plan")
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.SourceOfTruth = cmd.String("source-of-truth")
	task.QuietMode = cmd.Bool("quiet")
	task.Ctx = context.Background()

	task.DryRun = cmd.Bool("dry-run")
	task.InsertOnly = cmd.Bool("insert-only")
	task.UpsertOnly = cmd.Bool("upsert-only")
	task.FireTriggers = cmd.Bool("fire-triggers")
	task.FixNulls = cmd.Bool("fix-nulls")
	task.Bidirectional = cmd.Bool("bidirectional")
	task.GenerateReport = cmd.Bool("generate-report")
	task.RecoveryMode = cmd.Bool("recovery-mode")
	task.PreserveOrigin = cmd.Bool("preserve-origin")

	if err := task.ValidateAndPrepare(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := task.Run(true); err != nil {
		return fmt.Errorf("error during table repair: %w", err)
	}

	return nil
}

func SpockDiffCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, _, err := resolveClusterArg("spock-diff", "", "[cluster]", 0, args)
	if err != nil {
		return err
	}
	task := diff.NewSpockDiffTask()
	task.ClusterName = clusterName
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.Output = cmd.String("output")
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

func SchemaDiffCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("schema-diff", "<schema>", "[cluster] <schema>", 1, args)
	if err != nil {
		return err
	}
	blockSizeStr := cmd.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	task := diff.NewSchemaDiffTask()
	task.ClusterName = clusterName
	task.SchemaName = positional[0]
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.SkipTables = cmd.String("skip-tables")
	scheduleEnabled := cmd.Bool("schedule")
	scheduleEvery := cmd.String("every")
	task.SkipFile = cmd.String("skip-file")
	task.Quiet = cmd.Bool("quiet")
	task.DDLOnly = cmd.Bool("ddl-only")
	task.Ctx = context.Background()

	if scheduleEnabled && task.DDLOnly {
		return fmt.Errorf("scheduling is only supported when --ddl-only is false")
	}

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = cmd.Float64("concurrency-factor")
	task.MaxConnections = cmd.Int("max-connections")
	task.CompareUnitSize = cmd.Int("compare-unit-size")
	task.Output = cmd.String("output")
	task.OverrideBlockSize = cmd.Bool("override-block-size")

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

func RepsetDiffCLI(cmd *cli.Command) error {
	args := cmd.Args().Slice()
	clusterName, positional, err := resolveClusterArg("repset-diff", "<repset>", "[cluster] <repset>", 1, args)
	if err != nil {
		return err
	}
	blockSizeStr := cmd.String("block-size")
	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %w", blockSizeStr, err)
	}

	scheduleEnabled := cmd.Bool("schedule")
	scheduleEvery := cmd.String("every")

	task := diff.NewRepsetDiffTask()
	task.ClusterName = clusterName
	task.RepsetName = positional[0]
	task.DBName = cmd.String("dbname")
	task.Nodes = cmd.String("nodes")
	task.SkipTables = cmd.String("skip-tables")
	task.SkipFile = cmd.String("skip-file")
	task.Quiet = cmd.Bool("quiet")
	task.Ctx = context.Background()

	task.BlockSize = int(blockSizeInt)
	task.ConcurrencyFactor = cmd.Float64("concurrency-factor")
	task.MaxConnections = cmd.Int("max-connections")
	task.CompareUnitSize = cmd.Int("compare-unit-size")
	task.Output = cmd.String("output")
	task.OverrideBlockSize = cmd.Bool("override-block-size")

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

func StartSchedulerCLI(_ context.Context, cmd *cli.Command) error {
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("configuration not loaded; run inside a directory with ace.yaml or set ACE_CONFIG")
	}

	component := strings.ToLower(strings.TrimSpace(cmd.String("component")))
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

	// runCtx is canceled on SIGINT or SIGTERM – triggers a full shutdown.
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// sighupCh receives SIGHUP signals for in-place config reload.
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	defer signal.Stop(sighupCh)

	// Start the API server once.  It does not need to restart on reload because
	// it handles on-demand requests rather than reading scheduled job config.
	var apiServer *server.APIServer
	if runAPI {
		if ok, apiErr := canStartAPIServer(cfg); ok {
			var err error
			apiServer, err = server.New(cfg)
			if err != nil {
				return fmt.Errorf("api server init failed: %w", err)
			}
			go func() {
				if err := apiServer.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("api server error: %v", err)
					stop()
				}
			}()
		} else if component == "api" {
			return fmt.Errorf("api server requested but cannot start: %w", apiErr)
		} else {
			logger.Info("api server not started: %v", apiErr)
		}
	}

	if !runScheduler {
		// API-only mode: reload config on SIGHUP, wait for shutdown.
		go runConfigReloadLoop(runCtx, sighupCh, apiServer)
		<-runCtx.Done()
		return nil
	}

	// schedulerReloadLoop runs the scheduler and restarts it on each valid
	// SIGHUP.  It returns only when runCtx is canceled (SIGINT/SIGTERM).
	return schedulerReloadLoop(runCtx, sighupCh, apiServer)
}

// schedulerReloadLoop is the heart of the SIGHUP feature.
//
// Design:
//  1. Build jobs from the current config and start the gocron scheduler.
//  2. Block waiting for either a shutdown signal (runCtx.Done) or SIGHUP.
//  3. On SIGHUP:
//     a. Load and validate the new config (parse YAML + dry-run job build).
//     b. If invalid: log the error and keep the current scheduler running.
//     c. If valid: cancel the per-iteration scheduler context, which causes
//        gocron.Shutdown() to drain all in-flight jobs before returning.
//        Then atomically swap in the new config and loop back to step 1.
//  4. On SIGINT/SIGTERM: drain in-flight jobs via schedCancel and return.
func schedulerReloadLoop(
	runCtx context.Context,
	sighupCh <-chan os.Signal,
	apiServer *server.APIServer,
) error {
	for {
		currentCfg := config.Get()

		jobs, err := scheduler.BuildJobsFromConfig(currentCfg)
		if err != nil {
			return err
		}

		if len(jobs) == 0 {
			logger.Info("scheduler: no enabled jobs found in configuration")
		} else {
			for _, job := range jobs {
				logger.Info("scheduler: registering job %s", job.Name)
			}
		}

		// Per-iteration context: canceled to trigger graceful scheduler drain.
		schedCtx, schedCancel := context.WithCancel(runCtx)
		schedDone := make(chan error, 1)

		go func() {
			if len(jobs) == 0 {
				// No jobs: park until the context is canceled.
				<-schedCtx.Done()
				schedDone <- nil
				return
			}
			schedDone <- scheduler.RunJobs(schedCtx, jobs)
		}()

		// Inner loop: handle repeated SIGHUPs without restarting for invalid configs.
		reloaded := false
		for !reloaded {
			select {
			case <-runCtx.Done():
				// SIGINT / SIGTERM: drain in-flight ops and exit.
				schedCancel()
				if err := <-schedDone; err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
				return nil

			case err := <-schedDone:
				// The scheduler exited on its own – without being told to via
				// schedCancel.  This is unexpected (RunJobs normally blocks until
				// its context is canceled).  Treat a real error as fatal; a nil
				// or Canceled result means it exited cleanly and we just stop.
				schedCancel()
				if err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
				return nil

			case <-sighupCh:
				logger.Info("scheduler: received SIGHUP – reloading configuration")

				newCfg, loadErr := config.Reload()
				if loadErr != nil {
					logger.Error("scheduler: config reload failed (keeping current config): %v", loadErr)
					continue // wait for next signal
				}

				// Validate the new config by doing a dry-run job build.
				if _, buildErr := scheduler.BuildJobsFromConfig(newCfg); buildErr != nil {
					logger.Error("scheduler: new config rejected (keeping current config): %v", buildErr)
					continue // wait for next signal
				}

				// New config is valid.  Drain in-flight jobs, then swap.
				logger.Info("scheduler: new configuration valid – draining in-flight operations")
				schedCancel()
				if err := <-schedDone; err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("scheduler: error while draining for reload: %v", err)
					// The scheduler exited with a real error; propagate it.
					return err
				}

				// Atomic config swap.
				config.Set(newCfg)
				if apiServer != nil {
					if err := apiServer.ReloadSecurityConfig(newCfg); err != nil {
						logger.Warn("scheduler: security config reload failed (mTLS config unchanged): %v", err)
					}
				}
				logger.Info("scheduler: configuration reloaded successfully")
				reloaded = true // break inner loop → outer loop restarts scheduler
			}
		}
		// schedCancel is always called before reaching here (either inside the
		// SIGTERM branch, which returns, or inside the SIGHUP branch above).
		schedCancel()
	}
}

func StartAPIServerCLI(_ context.Context, cmd *cli.Command) error {
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("configuration not loaded; run inside a directory with ace.yaml or set ACE_CONFIG")
	}

	if ok, err := canStartAPIServer(cfg); !ok {
		return err
	}

	apiServer, err := server.New(cfg)
	if err != nil {
		return err
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	defer signal.Stop(sighupCh)
	go runConfigReloadLoop(runCtx, sighupCh, apiServer)

	return apiServer.Run(runCtx)
}

// runConfigReloadLoop reloads config on each SIGHUP until the channel
// is closed or ctx is canceled.  Used by API-only and standalone-API code paths.
func runConfigReloadLoop(ctx context.Context, ch <-chan os.Signal, apiServer *server.APIServer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			logger.Info("api: received SIGHUP – reloading configuration")
			newCfg, err := config.Reload()
			if err != nil {
				logger.Error("api: config reload failed (keeping current config): %v", err)
				continue
			}
			if ok, valErr := canStartAPIServer(newCfg); !ok {
				logger.Error("api: new config rejected (keeping current config): %v", valErr)
				continue
			}
			config.Set(newCfg)
			if apiServer != nil {
				if err := apiServer.ReloadSecurityConfig(newCfg); err != nil {
					logger.Warn("api: security config reload failed (mTLS config unchanged): %v", err)
				}
			}
			logger.Info("api: configuration reloaded successfully")
		}
	}
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
