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
	"strings"

	"github.com/pgedge/ace/internal/cli"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

func main() {
	var cfgPath string
	if !shouldSkipConfig(os.Args[1:]) {
		potentialPaths := []string{}

		// This is the order of precedence for finding the config file.
		// 1. env var (ACE_CONFIG)
		// 2. current dir
		// 3. $HOME/.config/ace/
		// 4. /etc/ace/
		if envPath := os.Getenv("ACE_CONFIG"); envPath != "" {
			potentialPaths = append(potentialPaths, envPath)
		}

		potentialPaths = append(potentialPaths, "ace.yaml")
		if home, err := os.UserHomeDir(); err == nil {
			p := filepath.Join(home, ".config", "ace", "ace.yaml")
			potentialPaths = append(potentialPaths, p)
		}

		potentialPaths = append(potentialPaths, "/etc/ace/ace.yaml")

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
	}

	app := cli.SetupCLI()
	err := app.Run(os.Args)
	if err != nil {
		logger.Error("%v", err)
	}
}

func shouldSkipConfig(args []string) bool {
	if len(args) == 0 {
		return true
	}

	for _, arg := range args {
		if arg == "--help" || arg == "-h" || arg == "help" {
			return true
		}
	}

	var commandPath []string
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		commandPath = append(commandPath, arg)
		if len(commandPath) >= 2 {
			break
		}
	}

	if len(commandPath) == 0 {
		return true
	}

	switch commandPath[0] {
	case "config":
		if len(commandPath) == 1 || commandPath[1] == "init" {
			return true
		}
	case "cluster":
		if len(commandPath) == 1 || commandPath[1] == "init" {
			return true
		}
	}

	return false
}
