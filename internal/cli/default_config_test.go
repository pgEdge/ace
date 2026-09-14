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
	"os"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/pgedge/ace/pkg/config"
)

// sampleConfigPath is the repo-root reference copy that the README and
// docs/configuration.md link to. It must stay byte-identical to the template
// embedded in ConfigInitCLI, otherwise the documented defaults are not the
// defaults users actually get from `ace config init`.
const sampleConfigPath = "../../ace.sample.yaml"

func TestSampleConfigMatchesEmbeddedTemplate(t *testing.T) {
	sample, err := os.ReadFile(sampleConfigPath)
	if err != nil {
		t.Fatalf("read %s: %v", sampleConfigPath, err)
	}

	if string(sample) != defaultConfigYAML {
		t.Errorf(
			"%s has drifted from internal/cli/default_config.yaml.\n"+
				"`ace config init` writes the embedded template, so the two must "+
				"match or the documented defaults are wrong.\n"+
				"Run: cp internal/cli/default_config.yaml ace.sample.yaml",
			sampleConfigPath,
		)
	}
}

// TestDefaultConfigTemplateParses guards against shipping a template that
// fails to load, which would break `ace config init` followed by any command.
func TestDefaultConfigTemplateParses(t *testing.T) {
	var cfg config.Config
	if err := yaml.Unmarshal([]byte(defaultConfigYAML), &cfg); err != nil {
		t.Fatalf("default_config.yaml does not parse into config.Config: %v", err)
	}

	// The template must agree with the concurrency-factor default advertised by
	// the CLI flag; disagreement is the drift that shipped a documented "1".
	if got, want := cfg.TableDiff.ConcurrencyFactor, 0.5; got != want {
		t.Errorf("table_diff.concurrency_factor = %v, want %v (matches the --concurrency-factor flag default)", got, want)
	}
}
