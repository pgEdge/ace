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

package config

import (
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type Config struct {
	DefaultCluster string         `yaml:"default_cluster"`
	Postgres       PostgresConfig `yaml:"postgres"`
	TableDiff      DiffConfig     `yaml:"table_diff"`
	MTree          MTreeConfig    `yaml:"mtree"`
	Server         ServerConfig   `yaml:"server"`

	ScheduleJobs   []JobDef   `yaml:"schedule_jobs"`
	ScheduleConfig []SchedDef `yaml:"schedule_config"`

	AutoRepair AutoRepairConfig `yaml:"auto_repair_config"`
	CertAuth   CertAuthConfig   `yaml:"cert_auth"`

	DebugMode bool `yaml:"debug_mode"`
}

type PostgresConfig struct {
	StatementTimeout      int    `yaml:"statement_timeout"`  // ms
	ConnectionTimeout     int    `yaml:"connection_timeout"` // s
	ApplicationName       string `yaml:"application_name"`
	TCPKeepalivesIdle     *int   `yaml:"tcp_keepalives_idle"`     // s
	TCPKeepalivesInterval *int   `yaml:"tcp_keepalives_interval"` // s
	TCPKeepalivesCount    *int   `yaml:"tcp_keepalives_count"`
}

type DiffConfig struct {
	ConcurrencyFactor float64 `yaml:"concurrency_factor"`
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
		SlotName             string `yaml:"slot_name"`
		PublicationName      string `yaml:"publication_name"`
		CDCProcessingTimeout int    `yaml:"cdc_processing_timeout"`
		CDCMetadataFlushSec  int    `yaml:"cdc_metadata_flush_seconds"`
	} `yaml:"cdc"`
	Schema string `yaml:"schema"`
	Diff   struct {
		MinBlockSize int `yaml:"min_block_size"`
		BlockSize    int `yaml:"block_size"`
		MaxBlockSize int `yaml:"max_block_size"`
	} `yaml:"diff"`
}

type ServerConfig struct {
	ListenAddress string   `yaml:"listen_address"`
	ListenPort    int      `yaml:"listen_port"`
	TLSCertFile   string   `yaml:"tls_cert_file"`
	TLSKeyFile    string   `yaml:"tls_key_file"`
	ClientCRLFile string   `yaml:"client_crl_file"`
	AllowedCNs    []string `yaml:"allowed_common_names"`
	TaskStorePath string   `yaml:"taskstore_path"`
}

type JobDef struct {
	Name        string         `yaml:"name"`
	Type        string         `yaml:"type,omitempty"`
	ClusterName string         `yaml:"cluster_name,omitempty"`
	TableName   string         `yaml:"table_name,omitempty"`
	SchemaName  string         `yaml:"schema_name,omitempty"`
	RepsetName  string         `yaml:"repset_name,omitempty"`
	Args        map[string]any `yaml:"args,omitempty"`
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
	UseCertAuth     bool   `yaml:"use_cert_auth"`
	ACEUserCertFile string `yaml:"ace_user_cert_file"`
	ACEUserKeyFile  string `yaml:"ace_user_key_file"`
	CACertFile      string `yaml:"ca_cert_file"`
}

// Cfg holds the loaded config for the whole app.
// Reads may use Cfg directly; concurrent reloads must go through Set/Get.
var Cfg *Config

// CfgPath is the path from which the current config was loaded.
// It is set by Init and used by the SIGHUP reload handler.
var CfgPath string

// cfgMu protects Cfg and CfgPath during live reloads.
var cfgMu sync.RWMutex

// Load reads the YAML file at the given path and unmarshals it into a Config.
// It performs no mutation of package-level state; callers must call Set to apply
// the loaded configuration. It returns the parsed Config or an error if the
// file cannot be read or the YAML cannot be decoded.
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

// Init loads the configuration from the file at path and sets the package-level
// Cfg and CfgPath under a write lock to enable safe concurrent access.
// It returns any error encountered while loading or parsing the configuration.
func Init(path string) error {
	c, err := Load(path)
	if err != nil {
		return err
	}
	cfgMu.Lock()
	Cfg = c
	CfgPath = path
	cfgMu.Unlock()
	return nil
}

// Reload loads a new Config from path and returns it for validation.
// The caller is responsible for calling Set to apply the new config.
// Reload attempts to parse the configuration file at the given path without applying it.
// If path is empty, Reload uses the most recently loaded configuration path (CfgPath) while holding a read lock.
// It does not modify the active package configuration (Cfg).
// It returns the parsed *Config on success, or an error if loading or parsing fails.
func Reload(path string) (*Config, error) {
	if path == "" {
		cfgMu.RLock()
		path = CfgPath
		cfgMu.RUnlock()
	}
	return Load(path)
}

// Set replaces the package's active configuration with c atomically.
// Passing nil sets the active configuration to nil.
func Set(c *Config) {
	cfgMu.Lock()
	Cfg = c
	cfgMu.Unlock()
}

// Get returns the active configuration under a read lock.
// Prefer Get() over reading Cfg directly in code that runs concurrently with
// Get returns the currently active Config instance and is safe for concurrent use.
func Get() *Config {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	return Cfg
}

// DefaultCluster returns the trimmed default cluster name from the loaded config.
// If no configuration is loaded, it returns the empty string.
func DefaultCluster() string {
	c := Get()
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.DefaultCluster)
}
