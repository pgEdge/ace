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

package main

import (
	"os"
	"path/filepath"

	"github.com/pgedge/ace/internal/cli"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

func main() {
	var cfgPath string
	potentialPaths := []string{}

	// This is the order of precedence for finding the config file.
	// 1. env var (ACE_CONFIG)
	// 2. current dir
	// 3. $HOME/.config/pgedge/ace/
	// 4. /etc/pgedge/ace/
	if envPath := os.Getenv("ACE_CONFIG"); envPath != "" {
		potentialPaths = append(potentialPaths, envPath)
	}

	potentialPaths = append(potentialPaths, "ace.yaml")
	if home, err := os.UserHomeDir(); err == nil {
		p := filepath.Join(home, ".config", "pgedge", "ace", "ace.yaml")
		potentialPaths = append(potentialPaths, p)
	}

	potentialPaths = append(potentialPaths, "/etc/pgedge/ace/ace.yaml")

	for _, p := range potentialPaths {
		if _, err := os.Stat(p); err == nil {
			cfgPath = p
			break
		}
	}

	if cfgPath == "" {
		logger.Fatal("config file 'ace.yaml' not found")
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
