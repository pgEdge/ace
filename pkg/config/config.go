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

package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres  PostgresConfig `yaml:"postgres"`
	TableDiff DiffConfig     `yaml:"table_diff"`
	MTree     MTreeConfig    `yaml:"mtree"`
	Server    ServerConfig   `yaml:"server"`

	ScheduleJobs   []JobDef   `yaml:"schedule_jobs"`
	ScheduleConfig []SchedDef `yaml:"schedule_config"`

	AutoRepair AutoRepairConfig `yaml:"auto_repair_config"`
	CertAuth   CertAuthConfig   `yaml:"cert_auth"`

	DebugMode bool `yaml:"debug_mode"`
}

type PostgresConfig struct {
	StatementTimeout  int `yaml:"statement_timeout"`  // ms
	ConnectionTimeout int `yaml:"connection_timeout"` // s
}

type DiffConfig struct {
	ConcurrencyFactor int     `yaml:"concurrency_factor"`
	MaxDiffRows       int64   `yaml:"max_diff_rows"`
	MinBlockSize      int     `yaml:"min_diff_block_size"`
	MaxBlockSize      int     `yaml:"max_diff_block_size"`
	DiffBlockSize     int     `yaml:"diff_block_size"`
	MaxCPURatio       float64 `yaml:"max_cpu_ratio"`
	BatchSize         int     `yaml:"diff_batch_size"`
	MaxBatchSize      int     `yaml:"max_diff_batch_size"`
	CompareUnitSize   int     `yaml:"compare_unit_size"`
}

type MTreeConfig struct {
	CDC struct {
		SlotName        string `yaml:"slot_name"`
		PublicationName string `yaml:"publication_name"`
	} `yaml:"cdc"`
	Diff struct {
		MinBlockSize int `yaml:"min_block_size"`
		BlockSize    int `yaml:"block_size"`
		MaxBlockSize int `yaml:"max_block_size"`
	} `yaml:"diff"`
}

type ServerConfig struct {
	ListenAddress string `yaml:"listen_address"`
	ListenPort    int    `yaml:"listen_port"`
}

type JobDef struct {
	Name        string                 `yaml:"name"`
	ClusterName string                 `yaml:"cluster_name,omitempty"`
	TableName   string                 `yaml:"table_name,omitempty"`
	RepsetName  string                 `yaml:"repset_name,omitempty"`
	Args        map[string]interface{} `yaml:"args,omitempty"`
}

type SchedDef struct {
	JobName         string `yaml:"job_name"`
	CrontabSchedule string `yaml:"crontab_schedule,omitempty"`
	RunFrequency    string `yaml:"run_frequency,omitempty"`
	Enabled         bool   `yaml:"enabled"`
}

type AutoRepairConfig struct {
	Enabled         bool   `yaml:"enabled"`
	ClusterName     string `yaml:"cluster_name"`
	DBName          string `yaml:"dbname"`
	PollFrequency   string `yaml:"poll_frequency"`
	RepairFrequency string `yaml:"repair_frequency"`
}

type CertAuthConfig struct {
	UseCertAuth      bool   `yaml:"use_cert_auth"`
	UserCertFile     string `yaml:"user_cert_file"`
	UserKeyFile      string `yaml:"user_key_file"`
	CACertFile       string `yaml:"ca_cert_file"`
	UseNaiveDatetime bool   `yaml:"use_naive_datetime"`
}

// Cfg holds the loaded config for the whole app.
var Cfg *Config

// Load reads and parses path into a Config.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Init loads the config and assigns it to the package variable.
func Init(path string) error {
	c, err := Load(path)
	if err != nil {
		return err
	}
	Cfg = c
	return nil
}
