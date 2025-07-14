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

// DiffOutput holds the structured diff data.
type DiffOutput struct {
	NodeDiffs map[string]DiffByNodePair `json:"diffs"` // Key: "nodeA/nodeB" (sorted names)
	Summary   DiffSummary               `json:"summary"`
}

// DiffByNodePair holds the differing rows for a pair of nodes.
// The keys in the DiffOutput.Diffs map will be "nodeX/nodeY",
// and the Node1/Node2 fields here will store rows corresponding to nodeX and nodeY respectively.
type DiffByNodePair struct {
	Rows map[string][]map[string]any `json:"rows"` // Keyed by actual node name e.g. "n1", "n2"
}

// DiffSummary provides metadata about the diff operation.
type DiffSummary struct {
	Schema                string         `json:"schema"`
	Table                 string         `json:"table"`
	Nodes                 []string       `json:"nodes"`
	BlockSize             int            `json:"block_size"`
	CompareUnitSize       int            `json:"compare_unit_size"`
	ConcurrencyFactor     int            `json:"concurrency_factor"`
	StartTime             string         `json:"start_time"`
	EndTime               string         `json:"end_time"`
	TimeTaken             string         `json:"time_taken"`
	DiffRowsCount         map[string]int `json:"diff_rows_count"`    // Key: "nodeA/nodeB", Value: count of differing rows
	TotalRowsChecked      int64          `json:"total_rows_checked"` // Estimated
	InitialRangesCount    int            `json:"initial_ranges_count"`
	MismatchedRangesCount int            `json:"mismatched_ranges_count"`
}

// NodePairDiff is a more detailed breakdown of differences for a single pair, often used internally during comparison.
type NodePairDiff struct {
	Node1OnlyRows []map[string]any
	Node2OnlyRows []map[string]any
	ModifiedRows  []struct {
		Pkey      string
		Node1Data map[string]any
		Node2Data map[string]any
	}
}

// SpockSubscription holds information about a spock subscription.
type SpockSubscription struct {
	SubName         string   `json:"sub_name"`
	SubEnabled      bool     `json:"sub_enabled"`
	ReplicationSets []string `json:"replication_sets"`
}

// SpockDiffOutput represents the result of a spock-diff operation.
type SpockDiffOutput struct {
	SpockConfigs map[string]any           `json:"spock_config"`
	Diffs        map[string]SpockPairDiff `json:"diffs"`
}

// SpockPairDiff represents the diff result for a pair of nodes.
type SpockPairDiff struct {
	Mismatch bool            `json:"mismatch"`
	Message  string          `json:"message"`
	Details  SpockDiffDetail `json:"details,omitempty"`
}

// SpockDiffDetail holds the detailed differences between two nodes' spock configurations.
type SpockDiffDetail struct {
	Subscriptions   SubscriptionDiff   `json:"subscriptions"`
	ReplicationSets ReplicationSetDiff `json:"replication_sets"`
}

// SubscriptionDiff highlights differences in subscriptions between two nodes.
type SubscriptionDiff struct {
	MissingOnNode1 []string           `json:"missing_on_node1"`
	MissingOnNode2 []string           `json:"missing_on_node2"`
	Different      []SubscriptionPair `json:"different"`
}

// SubscriptionPair shows a subscription that exists on both nodes but has different properties.
type SubscriptionPair struct {
	Name  string            `json:"name"`
	Node1 SpockSubscription `json:"node1"`
	Node2 SpockSubscription `json:"node2"`
}

// RepSetInfo holds information about a replication set.
type RepSetInfo struct {
	SetName string   `json:"set_name"`
	Tables  []string `json:"tables"`
}

// ReplicationSetDiff highlights differences in replication sets.
type ReplicationSetDiff struct {
	TablePlacementDiffs []TableRepSetDiff `json:"table_placement_differences"`
}

// TableRepSetDiff details which replication set a table belongs to on each node if they differ.
type TableRepSetDiff struct {
	TableName   string `json:"table_name"`
	Node1RepSet string `json:"node1_repset"`
	Node2RepSet string `json:"node2_repset"`
}

// ReplicationSetPair shows a replication set that exists on both nodes but has different tables.
type ReplicationSetPair struct {
	Name  string     `json:"name"`
	Node1 RepSetInfo `json:"node1"`
	Node2 RepSetInfo `json:"node2"`
}
