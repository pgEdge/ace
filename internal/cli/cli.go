package cli

import (
	"fmt"
	"os"
	"strconv"

	"github.com/pgedge/ace/internal/core"
	"github.com/urfave/cli/v2"
)

func SetupCLI() *cli.App {
	app := &cli.App{
		Name:  "ace",
		Usage: "Advanced Command-line Executor for database operations",
		Commands: []*cli.Command{
			{
				Name:      "table-diff",
				Usage:     "Compare tables between PostgreSQL databases",
				ArgsUsage: "<cluster> <table>",
				Description: "A tool for comparing tables between PostgreSQL databases " +
					"and detecting data inconsistencies",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "dbname",
						Usage: "Name of the database",
						Value: "",
					},
					&cli.StringFlag{
						Name:  "block-size",
						Usage: "Number of rows per block",
						Value: "100000",
					},
					&cli.Float64Flag{
						Name:  "max-cpu-ratio",
						Usage: "Maximum CPU usage ratio",
						Value: 0.8,
					},
					&cli.StringFlag{
						Name:  "output",
						Usage: "Output format",
						Value: "json",
					},
					&cli.StringFlag{
						Name:  "nodes",
						Usage: "Nodes to include in the diff",
						Value: "all",
					},
					&cli.StringFlag{
						Name:  "batch-size",
						Usage: "Size of each batch",
						Value: "10",
					},
					&cli.StringFlag{
						Name:  "table-filter",
						Usage: "Filter expression for tables",
						Value: "",
					},
					&cli.BoolFlag{
						Name:  "quiet",
						Usage: "Whether to suppress output",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "override-block-size",
						Usage: "Override block size",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "debug",
						Usage: "Enable debug logging",
						Value: false,
					},
				},
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return fmt.Errorf("missing required arguments: needs <cluster> and <table>")
					}
					return TableDiffCLI(ctx)
				},
			},
		},
	}

	return app
}

func TableDiffCLI(ctx *cli.Context) error {
	clusterName := ctx.Args().Get(0)
	tableName := ctx.Args().Get(1)
	dbName := ctx.String("dbname")
	blockSizeStr := ctx.String("block-size")
	debugMode := ctx.Bool("debug")
	maxCPURatio := ctx.Float64("max-cpu-ratio")
	outputFormat := ctx.String("output")
	nodesParam := ctx.String("nodes")
	batchSizeStr := ctx.String("batch-size")
	tableFilter := ctx.String("table-filter")
	quietMode := ctx.Bool("quiet")
	overrideBlockSize := ctx.Bool("override-block-size")

	blockSizeInt, err := strconv.ParseInt(blockSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block size '%s': %v", blockSizeStr, err)
	}

	batchSizeInt, err := strconv.ParseInt(batchSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid batch size '%s': %v", batchSizeStr, err)
	}

	task := core.NewTableDiffTask()
	task.ClusterName = clusterName
	task.TableName = tableName
	task.DBName = dbName
	task.BlockSize = int(blockSizeInt)
	task.MaxCPURatio = maxCPURatio
	task.Output = outputFormat
	task.Nodes = nodesParam
	task.BatchSize = int(batchSizeInt)
	task.TableFilter = tableFilter
	task.QuietMode = quietMode
	task.OverrideBlockSize = overrideBlockSize

	if err := task.Validate(); err != nil {
		return fmt.Errorf("validation failed: %v", err)
	}

	if err := task.RunChecks(true); err != nil {
		return fmt.Errorf("checks failed: %v", err)
	}

	if debugMode {
		os.Args = append(os.Args, "--debug")
	}

	if err := task.ExecuteTask(); err != nil {
		return fmt.Errorf("error during comparison: %v", err)
	}

	fmt.Println("Table diff completed")
	return nil
}
