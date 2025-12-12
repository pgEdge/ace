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

package types

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	DBName      string `json:"db_name"`
	DBUser      string `json:"db_user"`
	DBPassword  string `json:"db_password"`
	SSLMode     string `json:"ssl_mode"`
	SSLCert     string `json:"ssl_cert"`
	SSLKey      string `json:"ssl_key"`
	SSLRootCert string `json:"ssl_root_cert"`
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
	PKeyTypes        map[string]string
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
	Rows map[string][]OrderedMap `json:"rows"` // Keyed by actual node name e.g. "n1", "n2"
}

// DiffSummary provides metadata about the diff operation.
type DiffSummary struct {
	Schema                string         `json:"schema"`
	Table                 string         `json:"table"`
	TableFilter           string         `json:"table_filter,omitempty"`
	Nodes                 []string       `json:"nodes"`
	BlockSize             int            `json:"block_size"`
	CompareUnitSize       int            `json:"compare_unit_size"`
	ConcurrencyFactor     int            `json:"concurrency_factor"`
	MaxDiffRows           int64          `json:"max_diff_rows"`
	StartTime             string         `json:"start_time"`
	EndTime               string         `json:"end_time"`
	TimeTaken             string         `json:"time_taken"`
	DiffRowsCount         map[string]int `json:"diff_rows_count"` // Key: "nodeA/nodeB", Value: count of differing rows
	DiffRowLimitReached   bool           `json:"diff_row_limit_reached"`
	TotalRowsChecked      int64          `json:"total_rows_checked"` // Estimated
	InitialRangesCount    int            `json:"initial_ranges_count"`
	MismatchedRangesCount int            `json:"mismatched_ranges_count"`
	PrimaryKey            []string       `json:"primary_key"`
	OnlyOrigin            string         `json:"only_origin,omitempty"`
	Until                 string         `json:"until,omitempty"`
}

type KVPair struct {
	Key   string
	Value any
}

type OrderedMap []KVPair

func (om OrderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("{")
	for i, kv := range om {
		if i != 0 {
			buf.WriteString(",")
		}

		key, err := json.Marshal(kv.Key)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteString(":")

		val, err := json.Marshal(kv.Value)
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

func (om *OrderedMap) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return fmt.Errorf("expected '{' at start of JSON object, got %v", t)
	}

	var kvs []KVPair
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := t.(string)
		if !ok {
			return fmt.Errorf("expected string key, got %v", t)
		}

		var value any
		if err := dec.Decode(&value); err != nil {
			return err
		}
		kvs = append(kvs, KVPair{Key: key, Value: value})
	}

	t, err = dec.Token()
	if err != nil {
		return err
	}
	if d, ok := t.(json.Delim); !ok || d != '}' {
		return fmt.Errorf("expected '}' at end of JSON object, got %v", t)
	}

	*om = kvs
	return nil
}

func (om OrderedMap) Get(key string) (any, bool) {
	for _, kv := range om {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return nil, false
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

// ColumnType holds the name and data type of a column.
type ColumnType struct {
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
}

// UserPrivileges describes the privileges a user has.
type UserPrivileges struct {
	TableSelect            bool `db:"table_select"`
	TableCreate            bool `db:"table_create"`
	TableInsert            bool `db:"table_insert"`
	TableUpdate            bool `db:"table_update"`
	TableDelete            bool `db:"table_delete"`
	ColumnsSelect          bool `db:"columns_select"`
	TableConstraintsSelect bool `db:"table_constraints_select"`
	KeyColumnUsageSelect   bool `db:"key_column_usage_select"`
}

// SpockNodeAndSubInfo contains information about a spock node and its subscription.
type SpockNodeAndSubInfo struct {
	NodeID             int64    `db:"node_id"`
	NodeName           string   `db:"node_name"`
	Location           string   `db:"location"`
	Country            string   `db:"country"`
	SubID              int64    `db:"sub_id"`
	SubName            string   `db:"sub_name"`
	SubEnabled         bool     `db:"sub_enabled"`
	SubReplicationSets []string `db:"sub_replication_sets"`
}

// SpockRepSetInfo contains information about a replication set.
type SpockRepSetInfo struct {
	SetName string   `db:"set_name"`
	RelName []string `db:"relname"`
}

// PkeyColumnType holds the name and data type of a primary key column.
// Note: The query for this does not have aliases, so scanning relies on column order.
type PkeyColumnType struct {
	ColumnName string
	DataType   string
}

// BlockRange represents a block in the Merkle tree.
// The RangeStart and RangeEnd fields are of type any because they can be
// a single value or a composite type.
type BlockRange struct {
	NodePosition int64 `db:"node_position"`
	RangeStart   []any `db:"range_start"`
	RangeEnd     []any `db:"range_end"`
}

type BlockRangeWithCount struct {
	NodePosition int64 `db:"node_position"`
	RangeStart   []any `db:"range_start"`
	RangeEnd     []any `db:"range_end"`
	Count        int64 `db:"count"`
}

// RootNode represents the root of the Merkle tree.
type RootNode struct {
	NodePosition int64  `db:"node_position"`
	NodeHash     []byte `db:"node_hash"`
}

// NodeChild represents a child of a node in the Merkle tree.
type NodeChild struct {
	NodeLevel    int    `db:"node_level"`
	NodePosition int64  `db:"node_position"`
	NodeHash     []byte `db:"node_hash"`
}

// LeafRange represents the start and end of a leaf's range.
type LeafRange struct {
	RangeStart []any `db:"range_start"`
	RangeEnd   []any `db:"range_end"`
}

// BlockCountComposite contains the row count for a block in a table with a composite primary key.
type BlockCountComposite struct {
	NodePosition int64 `db:"node_position"`
	RangeStart   any   `db:"range_start"`
	RangeEnd     any   `db:"range_end"`
	Count        int64 `db:"cnt"`
}

// BlockCountSimple contains the row count for a block in a table with a simple primary key.
type BlockCountSimple struct {
	NodePosition int64 `db:"node_position"`
	RangeStart   any   `db:"range_start"`
	RangeEnd     any   `db:"range_end"`
	Count        int64 `db:"count"`
}

// MtreeMetadata represents a row in the ace_mtree_metadata table.
type MtreeMetadata struct {
	SchemaName  string    `db:"schema_name"`
	TableName   string    `db:"table_name"`
	TotalRows   int64     `db:"total_rows"`
	BlockSize   int       `db:"block_size"`
	NumBlocks   int       `db:"num_blocks"`
	IsComposite bool      `db:"is_composite"`
	LastUpdated time.Time `db:"last_updated"`
}

// PkeyOffset represents a range of primary key values.
type PkeyOffset struct {
	RangeStart []any
	RangeEnd   []any
}
