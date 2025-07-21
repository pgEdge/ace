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

package main

import (
	"os"
	"path/filepath"

	"github.com/pgedge/ace/internal/cli"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

func main() {
	cfgPath := "ace.yaml"
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		execPath, err := os.Executable()
		if err != nil {
			logger.Fatal("unable to determine executable path: %v", err)
		}
		root := filepath.Dir(filepath.Dir(execPath))
		cfgPath = filepath.Join(root, "ace.yaml")
	}
	if err := config.Init(cfgPath); err != nil {
		logger.Fatal("loading config (%s): %v", cfgPath, err)
	}

	app := cli.SetupCLI()
	err := app.Run(os.Args)
	if err != nil {
		logger.Error("%v", err)
	}
}
