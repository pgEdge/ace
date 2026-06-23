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

package diff

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
)

// SpockNodeConfig aggregates all spock configuration for a single node.
type SpockNodeConfig struct {
	NodeName      string                    `json:"node_name"`
	Subscriptions []types.SpockSubscription `json:"subscriptions"`
	RepSetInfo    []types.SpockRepSetInfo   `json:"rep_set_info"`
	Hints         []string                  `json:"hints"`
}

// SpockDiffTask defines the task for comparing spock metadata across nodes.
type SpockDiffTask struct {
	types.Task
	types.DerivedFields
	DBName string
	Nodes  string
	Output string

	ClientRole   string
	InvokeMethod string
	Pools        map[string]*pgxpool.Pool

	DiffResult   *types.SpockDiffOutput
	DiffFilePath string
	SkipDBUpdate bool

	TaskStore     *taskstore.Store
	TaskStorePath string

	Ctx context.Context
}

// Implement ClusterConfigProvider interface for SpockDiffTask
func (t *SpockDiffTask) GetClusterName() string        { return t.ClusterName }
func (t *SpockDiffTask) GetDBName() string             { return t.DBName }
func (t *SpockDiffTask) SetDBName(name string)         { t.DBName = name }
func (t *SpockDiffTask) GetNodes() string              { return t.Nodes }
func (t *SpockDiffTask) GetNodeList() []string         { return t.NodeList }
func (t *SpockDiffTask) SetNodeList(nl []string)       { t.NodeList = nl }
func (t *SpockDiffTask) SetDatabase(db types.Database) { t.Database = db }
func (t *SpockDiffTask) GetClusterNodes() []map[string]any {
	return t.ClusterNodes
}
func (t *SpockDiffTask) SetClusterNodes(cn []map[string]any) { t.ClusterNodes = cn }

func NewSpockDiffTask() *SpockDiffTask {
	return &SpockDiffTask{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskType:   taskstore.TaskTypeSpockDiff,
			TaskStatus: taskstore.StatusPending,
		},
		DerivedFields: types.DerivedFields{
			HostMap: make(map[string]string),
		},
		InvokeMethod: "cli",
		Pools:        make(map[string]*pgxpool.Pool),
		DiffResult: &types.SpockDiffOutput{
			SpockConfigs: make(map[string]any),
			Diffs:        make(map[string]types.SpockPairDiff),
		},
	}
}

func (t *SpockDiffTask) connOpts() auth.ConnectionOptions {
	return auth.ConnectionOptions{
		Role:           t.ClientRole,
		DropPrivileges: true,
	}
}

func (t *SpockDiffTask) Validate() error {
	if t.ClusterName == "" {
		return fmt.Errorf("cluster_name is a required argument")
	}

	nodeList, err := utils.ParseNodes(t.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. E.g., nodes=\"n1,n2\". Error: %w", err)
	}
	t.NodeList = nodeList

	if t.Nodes != "all" && len(nodeList) < 2 {
		return fmt.Errorf("spock-diff needs at least two nodes to compare")
	}

	err = utils.ReadClusterInfo(t)
	if err != nil {
		return fmt.Errorf("error loading cluster information: %w", err)
	}

	logger.Info("Cluster %s exists", t.ClusterName)

	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		if len(nodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !utils.Contains(nodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeMap)
		utils.ApplyDatabaseCredentials(combinedMap, t.Database)
		clusterNodes = append(clusterNodes, combinedMap)
	}

	if t.Nodes != "all" && len(nodeList) > 1 {
		for _, n := range nodeList {
			found := false
			for _, node := range clusterNodes {
				if name, ok := node["Name"].(string); ok && name == n {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("specified nodename %s not present in cluster", n)
			}
		}
	} else if len(nodeList) == 0 {
		t.NodeList = []string{}
		for _, node := range clusterNodes {
			t.NodeList = append(t.NodeList, node["Name"].(string))
		}
	}

	t.ClusterNodes = clusterNodes

	return nil
}

func (t *SpockDiffTask) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	hostMap := make(map[string]string)

	for _, nodeInfo := range t.ClusterNodes {
		hostname, _ := nodeInfo["Name"].(string)
		hostIP, _ := nodeInfo["PublicIP"].(string)

		port, ok := nodeInfo["Port"].(string)
		if !ok {
			port = "5432"
		}

		if !utils.Contains(t.NodeList, hostname) {
			continue
		}

		conn, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", hostname, err)
		}
		defer conn.Close()

		hostMap[hostIP+":"+port] = hostname
	}

	logger.Info("Connections successful to nodes in cluster")
	t.HostMap = hostMap
	return nil
}

func (t *SpockDiffTask) ExecuteTask() (err error) {
	startTime := time.Now()

	if strings.TrimSpace(t.TaskID) == "" {
		t.TaskID = uuid.NewString()
	}
	if t.Task.TaskType == "" {
		t.Task.TaskType = taskstore.TaskTypeSpockDiff
	}
	t.Task.StartedAt = startTime
	t.Task.TaskStatus = taskstore.StatusRunning
	t.Task.ClusterName = t.ClusterName

	var recorder *taskstore.Recorder
	if !t.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(t.TaskStore, t.TaskStorePath)
		if recErr != nil {
			logger.Warn("spock-diff: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if t.TaskStore == nil && rec.Store() != nil {
				t.TaskStore = rec.Store()
			}

			ctx := map[string]any{
				"nodes":  t.NodeList,
				"output": t.Output,
			}

			record := taskstore.Record{
				TaskID:      t.TaskID,
				TaskType:    taskstore.TaskTypeSpockDiff,
				Status:      taskstore.StatusRunning,
				ClusterName: t.ClusterName,
				StartedAt:   startTime,
				TaskContext: ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("spock-diff: unable to write initial task status (%v)", err)
			}
		}
	}

	defer func() {
		finishedAt := time.Now()
		t.Task.FinishedAt = finishedAt
		t.Task.TimeTaken = finishedAt.Sub(startTime).Seconds()

		status := taskstore.StatusFailed
		if err == nil {
			status = taskstore.StatusCompleted
		}
		t.Task.TaskStatus = status

		if recorder != nil && recorder.Created() {
			diffPairs := 0
			if t.DiffResult != nil {
				diffPairs = len(t.DiffResult.Diffs)
			}
			ctx := map[string]any{
				"nodes":      t.NodeList,
				"diff_pairs": diffPairs,
			}
			if t.DiffFilePath != "" {
				ctx["diff_file"] = t.DiffFilePath
			}
			if err != nil {
				ctx["error"] = err.Error()
			}

			updateErr := recorder.Update(taskstore.Record{
				TaskID:       t.TaskID,
				Status:       status,
				DiffFilePath: t.DiffFilePath,
				FinishedAt:   finishedAt,
				TimeTaken:    t.Task.TimeTaken,
				TaskContext:  ctx,
			})
			if updateErr != nil {
				logger.Warn("spock-diff: unable to update task status (%v)", updateErr)
			}
		}

		if recorder != nil && recorder.OwnsStore() {
			storePtr := recorder.Store()
			if closeErr := recorder.Close(); closeErr != nil {
				logger.Warn("spock-diff: failed to close task store (%v)", closeErr)
			}
			if storePtr != nil && t.TaskStore == storePtr {
				t.TaskStore = nil
			}
		}
	}()

	t.DiffFilePath = ""

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		pool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", name, err)
		}
		pools[name] = pool
		defer pool.Close()
	}
	t.Pools = pools

	// Check that spock is installed on every selected node
	for name, pool := range t.Pools {
		spockInstalled, err := queries.CheckSpockInstalled(t.Ctx, pool)
		if err != nil {
			return fmt.Errorf("failed to check for spock extension on node %s: %w", name, err)
		}
		if !spockInstalled {
			return fmt.Errorf("spock-diff requires the spock extension, which is not installed on node %s", name)
		}
	}

	allNodeConfigs := make(map[string]SpockNodeConfig)

	var nodeNames []string
	for _, nodeInfo := range t.ClusterNodes {
		nodeNames = append(nodeNames, nodeInfo["Name"].(string))
	}
	sort.Strings(nodeNames)

	for _, nodeName := range nodeNames {
		pool := pools[nodeName]
		config := SpockNodeConfig{NodeName: nodeName, Hints: []string{}}

		logger.Debug("Fetching Spock config for node: %s", nodeName)

		// Fetch node and subscription info
		nodeInfos, err := queries.GetSpockNodeAndSubInfo(t.Ctx, pool)
		if err != nil {
			return fmt.Errorf("querying spock.node and spock.subscription on node %s failed: %w", nodeName, err)
		}

		if len(nodeInfos) > 0 {
			config.NodeName = nodeInfos[0].NodeName
			for _, ni := range nodeInfos {
				sub := types.SpockSubscription{}
				if ni.SubName != "" {
					sub.SubName = ni.SubName
					sub.ProviderNode = ni.SubOriginName
					sub.SubEnabled = ni.SubEnabled
					sub.ReplicationSets = ni.SubReplicationSets
					if ni.SubOriginName == "" {
						hint := fmt.Sprintf("Subscription '%s' has an unresolved origin node; its reciprocal peer cannot be determined and it may be reported below as a missing subscription.", sub.SubName)
						if !utils.Contains(config.Hints, hint) {
							config.Hints = append(config.Hints, hint)
						}
					}
					if len(ni.SubReplicationSets) == 0 {
						hint := fmt.Sprintf("Subscription '%s' has no replication sets.", sub.SubName)
						if !utils.Contains(config.Hints, hint) {
							config.Hints = append(config.Hints, hint)
						}
					}
				}
				config.Subscriptions = append(config.Subscriptions, sub)
			}
		} else {
			config.Hints = append(config.Hints, "Hint: No subscriptions have been created on this node.")
		}

		// Fetch replication set info
		repRows, err := queries.GetSpockRepSetInfo(t.Ctx, pool)
		if err != nil {
			return fmt.Errorf("querying spock.tables on node %s failed: %w", nodeName, err)
		}

		config.RepSetInfo = repRows

		var tablesInRepSets []string
		for _, rs := range repRows {
			if rs.SetName != "" {
				tablesInRepSets = append(tablesInRepSets, rs.RelName...)
			}
		}
		if len(repRows) > 0 && len(tablesInRepSets) == 0 {
			config.Hints = append(config.Hints, "Hint: Tables not in replication set might not have primary keys, or you need to run repset-add-table.")
		}

		allNodeConfigs[nodeName] = config
	}

	// Pretty print configs
	for _, nodeName := range nodeNames {
		config := allNodeConfigs[nodeName]
		fmt.Printf("\n===== Spock Config: %s =====\n", nodeName)
		if len(config.Subscriptions) > 0 {
			fmt.Println("  Subscriptions:")
			for _, sub := range config.Subscriptions {
				if sub.SubName != "" {
					fmt.Printf("    - Name: %s (Enabled: %t)\n", sub.SubName, sub.SubEnabled)
					fmt.Printf("      Replication Sets: %v\n", sub.ReplicationSets)
				}
			}
		} else {
			fmt.Println("  No subscriptions found.")
		}

		if len(config.RepSetInfo) > 0 {
			fmt.Println("  Replication Sets:")
			for _, rs := range config.RepSetInfo {
				if rs.SetName != "" {
					fmt.Printf("    - %s:\n", rs.SetName)
					for _, table := range rs.RelName {
						fmt.Printf("      - %s\n", table)
					}
				}
			}
		} else {
			fmt.Println("  No replication sets found.")
		}

		if len(config.Hints) > 0 {
			fmt.Println("  Hints:")
			for _, hint := range config.Hints {
				fmt.Printf("    - %s\n", hint)
			}
		}
	}

	t.DiffResult.SpockConfigs = make(map[string]any, len(allNodeConfigs))
	for k, v := range allNodeConfigs {
		t.DiffResult.SpockConfigs[k] = v
	}

	fmt.Println("\n===== Spock Diff =====")

	for i := 0; i < len(nodeNames); i++ {
		for j := i + 1; j < len(nodeNames); j++ {
			refNodeName := nodeNames[i]
			compareNodeName := nodeNames[j]
			refConfig := allNodeConfigs[refNodeName]
			compareConfig := allNodeConfigs[compareNodeName]

			pairKey := fmt.Sprintf("%s/%s", refNodeName, compareNodeName)

			fmt.Printf("\nComparing %s vs %s:\n", refNodeName, compareNodeName)

			// Perform detailed diff
			diff := compareSpockConfigs(refConfig, compareConfig)

			if !diff.Mismatch {
				diff.Message = fmt.Sprintf("Replication rules are the same for %s and %s", refNodeName, compareNodeName)
				fmt.Printf("%s No differences found.\n", utils.CheckMark)
			} else {
				diff.Message = fmt.Sprintf("Difference in Replication Rules between %s and %s", refNodeName, compareNodeName)
				fmt.Printf("%s Differences found:\n", utils.CrossMark)
				printDiffDetails(diff.Details, refNodeName, compareNodeName)
			}
			t.DiffResult.Diffs[pairKey] = diff
		}
	}

	fmt.Println()

	endTime := time.Now()

	if len(t.DiffResult.Diffs) > 0 {
		outputFileName := fmt.Sprintf("spock_diffs-%s.json",
			time.Now().Format("20060102150405"),
		)

		jsonData, err := json.MarshalIndent(t.DiffResult, "", "  ")
		if err != nil {
			logger.Info("ERROR marshalling diff output to JSON: %v", err)
			return fmt.Errorf("failed to marshal diffs: %w", err)
		}

		if err = os.WriteFile(outputFileName, jsonData, 0644); err != nil {
			logger.Info("ERROR writing diff output to file %s: %v", outputFileName, err)
			return fmt.Errorf("failed to write diffs file: %w", err)
		}
		logger.Info("Diff report written to %s", outputFileName)
		t.DiffFilePath = outputFileName
	} else {
		t.DiffFilePath = ""
	}

	logger.Info("Spock diff completed in %.3f seconds", endTime.Sub(startTime).Seconds())

	return nil
}

func compareSpockConfigs(c1, c2 SpockNodeConfig) types.SpockPairDiff {
	diff := types.SpockPairDiff{
		Mismatch: false,
		Details:  types.SpockDiffDetail{},
	}

	// Compare Subscriptions
	subDiff := compareSubscriptions(c1, c2)
	diff.Details.Subscriptions = subDiff

	// Compare Replication Sets
	repSetDiff := compareReplicationSets(c1, c2)
	diff.Details.ReplicationSets = repSetDiff

	if len(subDiff.MissingOnNode1) > 0 || len(subDiff.MissingOnNode2) > 0 || len(subDiff.Different) > 0 ||
		len(repSetDiff.TablePlacementDiffs) > 0 {
		diff.Mismatch = true
	}

	return diff
}

func compareSubscriptions(c1, c2 SpockNodeConfig) types.SubscriptionDiff {
	diff := types.SubscriptionDiff{}
	n1Name := c1.NodeName
	n2Name := c2.NodeName

	// A healthy pair requires n1 to subscribe from n2 and n2 from n1. Match on the
	// provider node identity, not the subscription name (which users may override).
	subsFromOnN1 := subscriptionsByProvider(c1.Subscriptions)
	subsFromOnN2 := subscriptionsByProvider(c2.Subscriptions)

	s1, n1SubsFromN2 := subsFromOnN1[n2Name] // subscription on n1 receiving from n2
	s2, n2SubsFromN1 := subsFromOnN2[n1Name] // subscription on n2 receiving from n1

	if !n1SubsFromN2 {
		diff.MissingOnNode1 = append(diff.MissingOnNode1, n2Name)
	}
	if !n2SubsFromN1 {
		diff.MissingOnNode2 = append(diff.MissingOnNode2, n1Name)
	}

	if n1SubsFromN2 && n2SubsFromN1 {
		// Compare order-insensitively without mutating the originals: these slices
		// are shared with the SpockConfigs JSON output, which keeps DB order.
		sets1 := append([]string(nil), s1.ReplicationSets...)
		sets2 := append([]string(nil), s2.ReplicationSets...)
		sort.Strings(sets1)
		sort.Strings(sets2)

		// Both directions exist; their properties should match (names aside).
		if s1.SubEnabled != s2.SubEnabled || !reflect.DeepEqual(sets1, sets2) {
			diff.Different = append(diff.Different, types.SubscriptionPair{
				Name:  fmt.Sprintf("reciprocal subscriptions between %s and %s", n1Name, n2Name),
				Node1: s1, // subscription on n1 (from n2)
				Node2: s2, // subscription on n2 (from n1)
			})
		}
	}

	return diff
}

// subscriptionsByProvider indexes subscriptions by the node they replicate from.
func subscriptionsByProvider(subs []types.SpockSubscription) map[string]types.SpockSubscription {
	byProvider := make(map[string]types.SpockSubscription, len(subs))
	for _, s := range subs {
		if s.ProviderNode != "" {
			byProvider[s.ProviderNode] = s
		}
	}
	return byProvider
}

func compareReplicationSets(c1, c2 SpockNodeConfig) types.ReplicationSetDiff {
	diff := types.ReplicationSetDiff{}

	// Map tables to their replication sets for each node
	tablesToRepSetN1 := make(map[string]string)
	for _, rs := range c1.RepSetInfo {
		if rs.SetName != "" {
			for _, table := range rs.RelName {
				tablesToRepSetN1[table] = rs.SetName
			}
		}
	}
	tablesToRepSetN2 := make(map[string]string)
	for _, rs := range c2.RepSetInfo {
		if rs.SetName != "" {
			for _, table := range rs.RelName {
				tablesToRepSetN2[table] = rs.SetName
			}
		}
	}

	// Find all unique tables across both nodes
	allTables := make(map[string]bool)
	for table := range tablesToRepSetN1 {
		allTables[table] = true
	}
	for table := range tablesToRepSetN2 {
		allTables[table] = true
	}

	for table := range allTables {
		repSet1, ok1 := tablesToRepSetN1[table]
		if !ok1 {
			repSet1 = "Not in any repset"
		}
		repSet2, ok2 := tablesToRepSetN2[table]
		if !ok2 {
			repSet2 = "Not in any repset"
		}

		if repSet1 != repSet2 {
			diff.TablePlacementDiffs = append(diff.TablePlacementDiffs, types.TableRepSetDiff{
				TableName:   table,
				Node1RepSet: repSet1,
				Node2RepSet: repSet2,
			})
		}
	}
	return diff
}

func printDiffDetails(details types.SpockDiffDetail, node1, node2 string) {
	if len(details.Subscriptions.MissingOnNode1) > 0 {
		fmt.Printf("    %s is missing a subscription receiving from: %v\n", node1, details.Subscriptions.MissingOnNode1)
	}
	if len(details.Subscriptions.MissingOnNode2) > 0 {
		fmt.Printf("    %s is missing a subscription receiving from: %v\n", node2, details.Subscriptions.MissingOnNode2)
	}
	if len(details.Subscriptions.Different) > 0 {
		fmt.Println("    Reciprocal subscriptions with different properties:")
		for _, d := range details.Subscriptions.Different {
			fmt.Printf("      - Mismatch in settings for reciprocal subscriptions between %s and %s:\n", node1, node2)
			fmt.Printf("        - On %s (subscription '%s', from %s): Enabled: %t, Repsets: %v\n", node1, d.Node1.SubName, d.Node1.ProviderNode, d.Node1.SubEnabled, d.Node1.ReplicationSets)
			fmt.Printf("        - On %s (subscription '%s', from %s): Enabled: %t, Repsets: %v\n", node2, d.Node2.SubName, d.Node2.ProviderNode, d.Node2.SubEnabled, d.Node2.ReplicationSets)
		}
	}

	if len(details.ReplicationSets.TablePlacementDiffs) > 0 {
		fmt.Println("    Table placement in replication sets differs:")
		for _, d := range details.ReplicationSets.TablePlacementDiffs {
			fmt.Printf("      - Table '%s':\n", d.TableName)
			fmt.Printf("        - on %s: in repset '%s'\n", node1, d.Node1RepSet)
			fmt.Printf("        - on %s: in repset '%s'\n", node2, d.Node2RepSet)
		}
	}
}
