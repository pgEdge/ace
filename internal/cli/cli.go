package cli

import (
	"fmt"
	"strconv"

	"github.com/pgedge/ace/internal/core"
	"github.com/urfave/cli/v2"
)

func SetupCLI() *cli.App {
	td_flags := []cli.Flag{
		&cli.StringFlag{
			Name:    "dbname",
			Aliases: []string{"d"},
			Usage:   "Name of the database",
			Value:   "",
		},
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
			Value:   2,
		},
		&cli.IntFlag{
			Name:    "compare-unit-size",
			Aliases: []string{"s"},
			Usage:   "Compare unit size",
			Value:   10000,
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "Output format",
			Value:   "json",
		},
		&cli.StringFlag{
			Name:    "nodes",
			Aliases: []string{"n"},
			Usage:   "Nodes to include in the diff",
			Value:   "all",
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
	}
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
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() < 2 {
						return fmt.Errorf("missing required arguments: needs <cluster> and <table>")
					}
					return TableDiffCLI(ctx)
				},
				Flags: td_flags,
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
	compareUnitSize := ctx.Int("compare-unit-size")
	debugMode := ctx.Bool("debug")
	concurrencyFactor := ctx.Int("concurrency-factor")
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
	task.ConcurrencyFactor = concurrencyFactor
	task.CompareUnitSize = compareUnitSize
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

	if err := task.ExecuteTask(debugMode); err != nil {
		return fmt.Errorf("error during comparison: %v", err)
	}

	fmt.Println("Table diff completed")
	return nil
}
