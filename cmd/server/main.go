package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/pgedge/ace/internal/cli"
	"github.com/pgedge/ace/pkg/config"
)

func main() {
	cfgPath := "ace.yaml"
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		execPath, err := os.Executable()
		if err != nil {
			log.Fatalf("unable to determine executable path: %v", err)
		}
		root := filepath.Dir(filepath.Dir(execPath))
		cfgPath = filepath.Join(root, "ace.yaml")
	}
	if err := config.Init(cfgPath); err != nil {
		log.Fatalf("loading config (%s): %v", cfgPath, err)
	}

	app := cli.SetupCLI()
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}
