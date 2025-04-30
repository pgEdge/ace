package types

import (
	"time"
)

type ClusterConfig struct {
	JSONVersion string `json:"json_version"`
	ClusterName string `json:"cluster_name"`
	LogLevel    string `json:"log_level"`
	UpdateDate  string `json:"update_date"`
	PGEdge      struct {
		PGVersion int         `json:"pg_version"`
		AutoStart string      `json:"auto_start"`
		Spock     SpockConfig `json:"spock"`
		Databases []Database  `json:"databases"`
	} `json:"pgedge"`
	NodeGroups []NodeGroup `json:"node_groups"`
}

type SpockConfig struct {
	SpockVersion string `json:"spock_version"`
	AutoDDL      string `json:"auto_ddl"`
}

type Database struct {
	DBName     string `json:"db_name"`
	DBUser     string `json:"db_user"`
	DBPassword string `json:"db_password"`
}

type NodeGroup struct {
	SSH struct {
		OSUser     string `json:"os_user"`
		PrivateKey string `json:"private_key"`
	} `json:"ssh"`
	Name      string `json:"name"`
	IsActive  string `json:"is_active"`
	PublicIP  string `json:"public_ip"`
	PrivateIP string `json:"private_ip"`
	Port      string `json:"port"`
	Path      string `json:"path"`
}

type Task struct {
	ClusterName string
	TaskID      string
	TaskType    string
	TaskStatus  string
	TaskContext string
	StartedAt   time.Time
	FinishedAt  time.Time
	TimeTaken   float64
}

type DerivedFields struct {
	ClusterNodes     []map[string]any
	Schema           string
	Table            string
	Key              []string
	Cols             []string
	ConnParams       []string
	Database         Database
	NodeList         []string
	HostMap          map[string]string
	TableList        []string
	ColTypes         map[string]map[string]string
	SimplePrimaryKey bool
}
