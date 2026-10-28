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
	"fmt"
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
	// MaxConnections is the number of connections that one table-diff task
	// opens to each node. A value set here is a strict limit for the whole
	// task.
	MaxConnections ConnectionLimits `yaml:"max_connections"`

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
	// MaxParallelWorkersPerGather is sent as a session setting on every ACE
	// connection. ACE already runs its hash queries from several client-side
	// workers, so unset (or 0) stops Postgres from adding parallel workers
	// on top of that. A negative value leaves the server's setting alone.
	MaxParallelWorkersPerGather *int `yaml:"max_parallel_workers_per_gather"`
}

type DiffConfig struct {
	ConcurrencyFactor float64 `yaml:"concurrency_factor"`
	MaxDiffRows       int64   `yaml:"max_diff_rows"`
	MaxHTMLRows       int64   `yaml:"max_html_rows"`
	MinBlockSize      int     `yaml:"min_diff_block_size"`
	MaxBlockSize      int     `yaml:"max_diff_block_size"`
	DiffBlockSize     int     `yaml:"diff_block_size"`
	MaxCPURatio       float64 `yaml:"max_cpu_ratio"`
	BatchSize         int     `yaml:"diff_batch_size"`
	MaxBatchSize      int     `yaml:"max_diff_batch_size"`
	CompareUnitSize   int     `yaml:"compare_unit_size"`
	// MaxConnections is the old name of max_connections.default. It is used
	// only when max_connections.default is not set.
	MaxConnections int `yaml:"max_connections"`
}

// DefaultMaxConnections is the number of connections per node when neither
// the command nor the configuration file sets one.
const DefaultMaxConnections = 4

// ConnectionLimits is the max_connections section of the configuration
// file. A value of 0 means "not set".
type ConnectionLimits struct {
	// Default applies to every node that Nodes does not list.
	Default int `yaml:"default"`
	// Nodes maps "<cluster>.<node>" to the value for that node.
	Nodes map[string]int `yaml:"nodes"`
}

// UnmarshalYAML also accepts a single number, "max_connections: 8", as the
// default value.
func (l *ConnectionLimits) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&l.Default)
	}
	type plain ConnectionLimits
	return node.Decode((*plain)(l))
}

// ConnectionsFor returns the number of connections per node that the file
// sets for node of cluster: the value for "<cluster>.<node>", then
// max_connections.default, then table_diff.max_connections, then
// DefaultMaxConnections. set is false only when the file sets none of them
// and the result is DefaultMaxConnections. c can be nil.
func (c *Config) ConnectionsFor(cluster, node string) (n int, set bool) {
	if c == nil {
		return DefaultMaxConnections, false
	}
	if n := c.MaxConnections.Nodes[cluster+"."+node]; n > 0 {
		return n, true
	}
	if n := c.MaxConnections.Default; n > 0 {
		return n, true
	}
	if n := c.TableDiff.MaxConnections; n > 0 {
		return n, true
	}
	return DefaultMaxConnections, false
}

// CheckConnectionLimits returns an error if a number of connections in the
// file is negative. c can be nil.
func (c *Config) CheckConnectionLimits() error {
	if c == nil {
		return nil
	}
	if c.MaxConnections.Default < 0 {
		return fmt.Errorf("max_connections.default must be >= 0, got %d", c.MaxConnections.Default)
	}
	if c.TableDiff.MaxConnections < 0 {
		return fmt.Errorf("table_diff.max_connections must be >= 0, got %d", c.TableDiff.MaxConnections)
	}
	for name, n := range c.MaxConnections.Nodes {
		if n < 0 {
			return fmt.Errorf("max_connections.nodes.%s must be >= 0, got %d", name, n)
		}
	}
	return nil
}

type MTreeConfig struct {
	CDC struct {
		SlotName             string `yaml:"slot_name"`
		PublicationName      string `yaml:"publication_name"`
		CDCProcessingTimeout int    `yaml:"cdc_processing_timeout"`
		CDCMetadataFlushSec  int    `yaml:"cdc_metadata_flush_seconds"`
		CDCFlushBatchSize    int    `yaml:"cdc_flush_batch_size"`
		// AdaptiveDrainFraction and AdaptiveDrainMinChanges tune when a
		// bounded CDC drain escalates a flooded table's UPDATE tracking from
		// per-change dirty-marking to one mark-all-dirty + bulk rehash
		// (inserts/deletes are always tracked per-change: their per-block
		// counters drive block split/merge maintenance). Threshold per
		// table = max(min_changes, fraction * table_row_estimate), counting
		// UPDATEs only. 0 means "use default" (0.01 / 1000); a negative
		// fraction disables escalation entirely.
		AdaptiveDrainFraction   float64 `yaml:"adaptive_drain_fraction"`
		AdaptiveDrainMinChanges int     `yaml:"adaptive_drain_min_changes"`
	} `yaml:"cdc"`
	Schema string `yaml:"schema"`
	// MaxConnections caps the connection pool per node for mtree build,
	// update and table-diff. The max_connections section of the file does
	// not apply to these commands. One connection holds the tree's
	// transaction and the rest run hash queries, so this also caps the busy
	// backends per node. 0 = derive the worker count from max_cpu_ratio
	// alone.
	MaxConnections int `yaml:"max_connections"`
	Diff           struct {
		MinBlockSize int   `yaml:"min_block_size"`
		BlockSize    int   `yaml:"block_size"`
		MaxBlockSize int   `yaml:"max_block_size"`
		MaxDiffRows  int64 `yaml:"max_diff_rows"`
		MaxHTMLRows  int64 `yaml:"max_html_rows"`
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
// Use Get() for any code that runs concurrently with SIGHUP reload; bare
// reads of Cfg are only safe during single-threaded startup.
var Cfg *Config

// CfgPath is the path from which the current config was loaded.
// It is set by Init and used by the SIGHUP reload handler.
var CfgPath string

// cfgMu protects Cfg and CfgPath during live reloads.
var cfgMu sync.RWMutex

// Load reads and parses path into a Config without applying it.
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

// Init loads the config from path and assigns it to the package variables.
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

// Reload re-reads the configuration file and returns the parsed result
// without replacing the active config.
func Reload() (*Config, error) {
	cfgMu.RLock()
	path := CfgPath
	cfgMu.RUnlock()
	return Load(path)
}

// Set atomically replaces the active configuration.
func Set(c *Config) {
	cfgMu.Lock()
	Cfg = c
	cfgMu.Unlock()
}

// Get returns the active configuration under a read lock.
// Prefer Get() over reading Cfg directly in code that runs concurrently with
// SIGHUP reload (e.g. the scheduler loop).
func Get() *Config {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	return Cfg
}

// DefaultCluster returns the trimmed default cluster name from the loaded config.
func DefaultCluster() string {
	c := Get()
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.DefaultCluster)
}
