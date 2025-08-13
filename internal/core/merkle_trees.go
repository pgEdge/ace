// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the pgEdge Community License:
//
//	https://www.pgedge.com/communitylicense
//
// ///////////////////////////////////////////////////////////////////////////
package core

import (
	"context"
	"fmt"
	"maps"
	"math"
	"os"
	"strings"
	"time"

	"runtime"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type MerkleTreeTask struct {
	types.Task
	types.DerivedFields

	QualifiedTableName string
	DBName             string
	Nodes              string

	Analyse           bool
	Rebalance         bool
	RecreateObjects   bool
	BlockSize         int
	MaxCpuRatio       float64
	BatchSize         int
	Output            string
	QuietMode         bool
	RangesFile        string
	WriteRanges       bool
	OverrideBlockSize bool
	Mode              string // Placeholder for build, update, rebalance
}

func (m *MerkleTreeTask) GetClusterName() string              { return m.ClusterName }
func (m *MerkleTreeTask) GetDBName() string                   { return m.DBName }
func (m *MerkleTreeTask) SetDBName(name string)               { m.DBName = name }
func (m *MerkleTreeTask) GetNodes() string                    { return m.Nodes }
func (m *MerkleTreeTask) GetNodeList() []string               { return m.NodeList }
func (m *MerkleTreeTask) SetNodeList(nl []string)             { m.NodeList = nl }
func (m *MerkleTreeTask) SetDatabase(db types.Database)       { m.Database = db }
func (m *MerkleTreeTask) GetClusterNodes() []map[string]any   { return m.ClusterNodes }
func (m *MerkleTreeTask) SetClusterNodes(cn []map[string]any) { m.ClusterNodes = cn }

func NewMerkleTreeTask() *MerkleTreeTask {
	return &MerkleTreeTask{
		Task: types.Task{
			TaskID:    uuid.NewString(),
			StartedAt: time.Now(),
		},
		DerivedFields: types.DerivedFields{
			ColTypes: make(map[string]map[string]string),
		},
	}
}

func (m *MerkleTreeTask) Validate() error {
	if m.ClusterName == "" {
		return fmt.Errorf("cluster_name is a required argument")
	}

	if m.BlockSize != 0 && !m.OverrideBlockSize {
		if m.BlockSize > config.Cfg.MTree.MaxBlockSize {
			return fmt.Errorf("block size should be <= %d", config.Cfg.MTree.MaxBlockSize)
		}
		if m.BlockSize < config.Cfg.MTree.MinBlockSize {
			return fmt.Errorf("block size should be >= %d", config.Cfg.MTree.MinBlockSize)
		}
	}

	if m.MaxCpuRatio > 1.0 || m.MaxCpuRatio < 0.0 {
		return fmt.Errorf("invalid value range for max_cpu_ratio")
	}

	if m.RangesFile != "" {
		if _, err := os.Stat(m.RangesFile); os.IsNotExist(err) {
			return fmt.Errorf("file %s does not exist", m.RangesFile)
		}
		// TODO: Add parsing and validation for ranges file content
	}

	nodeList, err := utils.ParseNodes(m.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. Error: %w", err)
	}
	m.SetNodeList(nodeList)

	if len(m.GetNodeList()) > 3 {
		return fmt.Errorf("mtree-diff currently supports up to a three-way table comparison")
	}

	err = utils.ReadClusterInfo(m)
	if err != nil {
		return fmt.Errorf("error loading cluster information: %w", err)
	}

	logger.Info("Cluster %s exists", m.ClusterName)

	parts := strings.Split(m.QualifiedTableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("tableName %s must be of form 'schema.table_name'", m.QualifiedTableName)
	}
	schema, table := parts[0], parts[1]

	// Sanitise inputs here
	if err := queries.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := queries.SanitiseIdentifier(table); err != nil {
		return err
	}

	var clusterNodes []map[string]any
	for _, nodeMap := range m.ClusterNodes {
		if len(nodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !utils.Contains(nodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)

		maps.Copy(combinedMap, nodeMap)

		combinedMap["DBName"] = m.Database.DBName
		combinedMap["DBUser"] = m.Database.DBUser
		combinedMap["DBPassword"] = m.Database.DBPassword

		clusterNodes = append(clusterNodes, combinedMap)
	}

	if m.Nodes != "all" && len(nodeList) > 1 {
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
		m.NodeList = []string{}
		for _, node := range clusterNodes {
			m.NodeList = append(m.NodeList, node["Name"].(string))
		}
	}

	m.Schema = schema
	m.Table = table
	m.ClusterNodes = clusterNodes

	return nil
}

func (m *MerkleTreeTask) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := m.Validate(); err != nil {
			return err
		}
	}
	var localCols, localKey []string

	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		if _, err := pool.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS pgcrypto;"); err != nil {
			return fmt.Errorf("failed to ensure pgcrypto is installed on %s: %w", nodeInfo["Name"], err)
		}

		currentColsSlice, err := queries.GetColumns(context.Background(), pool, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("failed to get columns on node %s: %w", nodeInfo["Name"], err)
		}
		currentKeySlice, err := queries.GetPrimaryKey(context.Background(), pool, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("failed to get primary key on node %s: %w", nodeInfo["Name"], err)
		}

		if len(currentColsSlice) == 0 {
			return fmt.Errorf("table '%s' not found on %s, or the current user does not have adequate privileges", m.QualifiedTableName, nodeInfo["Name"])
		}
		if len(currentKeySlice) == 0 {
			return fmt.Errorf("no primary key found for '%s'", m.QualifiedTableName)
		}

		if localCols == nil && localKey == nil {
			localCols, localKey = currentColsSlice, currentKeySlice
		}

		if strings.Join(currentColsSlice, ",") != strings.Join(localCols, ",") || strings.Join(currentKeySlice, ",") != strings.Join(localKey, ",") {
			return fmt.Errorf("table schemas or primary keys do not match between nodes")
		}
	}

	m.Cols = localCols
	m.Key = localKey
	m.SimplePrimaryKey = len(m.Key) == 1

	fmt.Println("Connections successful to all nodes in cluster.")
	fmt.Printf("Table %s is comparable across nodes.\n", m.QualifiedTableName)
	return nil
}

func (m *MerkleTreeTask) BuildMtree() error {
	var blockRanges []types.BlockRange
	var numBlocks int
	fmt.Println("Getting row estimates from all nodes...")
	var maxRows int64
	var refNode map[string]any
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("could not connect to node %s to get row estimate: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		count, err := queries.GetRowCountEstimate(context.Background(), pool, m.Schema, m.Table)
		if err != nil {
			fmt.Printf("Warning: Could not get row estimate from node %s: %v\n", nodeInfo["Name"], err)
			continue
		}

		if count > maxRows {
			maxRows = count
			refNode = nodeInfo
		}
	}

	if refNode == nil {
		return fmt.Errorf("could not determine a reference node; failed to get row estimates from all nodes")
	}
	fmt.Printf("Using node %s as the reference for defining block ranges.\n", refNode["Name"])

	if len(blockRanges) == 0 {
		fmt.Printf("Calculating block ranges for ~%d rows...\n", maxRows)

		refPool, err := auth.GetClusterNodeConnection(refNode, "")
		if err != nil {
			return fmt.Errorf("could not connect to reference node %s: %w", refNode["Name"], err)
		}

		sampleMethod, samplePercent := computeSamplingParameters(maxRows)
		fmt.Printf("Using %s with sample percent %.2f\n", sampleMethod, samplePercent)

		numBlocks = int(math.Ceil(float64(maxRows) / float64(m.BlockSize)))
		if numBlocks == 0 && maxRows > 0 {
			numBlocks = 1
		}

		keyColumns := m.Key

		offsetsQuery, err := queries.GeneratePkeyOffsetsQuery(m.Schema, m.Table, keyColumns, sampleMethod, samplePercent, numBlocks)
		if err != nil {
			return fmt.Errorf("failed to generate pkey offsets query: %w", err)
		}

		rows, err := refPool.Query(context.Background(), offsetsQuery)
		if err != nil {
			return fmt.Errorf("failed to execute pkey offsets query on node %s: %w", refNode["Name"], err)
		}
		defer rows.Close()

		numKeyCols := len(keyColumns)
		for i := 0; rows.Next(); i++ {
			dest := make([]any, 2*numKeyCols)
			destPtrs := make([]any, 2*numKeyCols)
			for j := range dest {
				destPtrs[j] = &dest[j]
			}

			if err := rows.Scan(destPtrs...); err != nil {
				return fmt.Errorf("failed to scan pkey offset row: %w", err)
			}

			startVals := make([]any, numKeyCols)
			endVals := make([]any, numKeyCols)

			for k := 0; k < numKeyCols; k++ {
				startVals[k] = dest[k]
				endVals[k] = dest[numKeyCols+k]
			}
			blockRanges = append(blockRanges, types.BlockRange{NodePosition: int64(i), RangeStart: startVals, RangeEnd: endVals})
		}
		if rows.Err() != nil {
			refPool.Close()
			return fmt.Errorf("error iterating over pkey offset rows: %w", rows.Err())
		}
		refPool.Close()
	}

	for _, nodeInfo := range m.ClusterNodes {
		fmt.Printf("\nProcessing node: %s\n", nodeInfo["Name"])
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("failed to connect to node %s for mtree build: %w", nodeInfo["Name"], err)
		}

		fmt.Println("  - Creating Merkle Tree objects...")
		err = m.createMtreeObjects(pool, maxRows, numBlocks)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to create mtree objects on node %s: %w", nodeInfo["Name"], err)
		}

		fmt.Println("  - Inserting block ranges...")
		err = m.insertBlockRanges(pool, blockRanges)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to insert block ranges on node %s: %w", nodeInfo["Name"], err)
		}

		fmt.Println("  - Computing leaf hashes...")
		err = m.computeLeafHashes(pool, blockRanges)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to compute leaf hashes on node %s: %w", nodeInfo["Name"], err)
		}

		fmt.Println("  - Building parent nodes...")
		err = m.buildParentNodes(pool)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to build parent nodes on node %s: %w", nodeInfo["Name"], err)
		}

		fmt.Printf("Merkle tree built successfully on %s\n", nodeInfo["Name"])
		pool.Close()
	}

	return nil
}

func (m *MerkleTreeTask) UpdateMtree(skipAllChecks bool) error {
	if !skipAllChecks {
		if err := m.RunChecks(true); err != nil {
			return err
		}
	}

	var blockSize int
	var foundBlockSize bool
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("error getting connection pool for node %s: %w", nodeInfo["Name"], err)
		}

		blockSize, err = queries.GetBlockSizeFromMetadata(context.Background(), pool, m.Schema, m.Table)
		if err != nil {
			pool.Close()
			return fmt.Errorf("error getting block size from metadata on node %s: %w", nodeInfo["Name"], err)
		}

		pool.Close()
		foundBlockSize = true
	}

	if !foundBlockSize {
		return fmt.Errorf("could not determine block size from any node")
	}
	m.BlockSize = blockSize

	for _, nodeInfo := range m.ClusterNodes {
		fmt.Printf("\nUpdating Merkle tree on node: %s\n", nodeInfo["Name"])
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("error getting connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		conn, err := pool.Acquire(context.Background())
		if err != nil {
			return fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()
		var compositeTypeName string

		if !m.SimplePrimaryKey {
			compositeTypeName = fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)
			dt, err := conn.Conn().LoadType(context.Background(), compositeTypeName)
			if err != nil {
				return fmt.Errorf("failed to load composite type %s: %w", compositeTypeName, err)
			}
			conn.Conn().TypeMap().RegisterType(dt)
		}

		tx, err := conn.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback(context.Background())

		mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)

		// Ensure the first block's start reflects the table's current minimum key.
		// This handles inserts that introduce new keys smaller than the existing first range_start.
		if !m.SimplePrimaryKey {
			minVals, err := queries.GetMinValCompositeTx(context.Background(), tx, m.Schema, m.Table, m.Key)
			if err != nil {
				return fmt.Errorf("failed to fetch composite min key: %w", err)
			}
			if minVals != nil {
				if err := queries.UpdateBlockRangeStartCompositeTx(context.Background(), tx, mtreeTableName, minVals, 0); err != nil {
					return fmt.Errorf("failed to update first block start (composite): %w", err)
				}
			}
		} else {
			minVal, err := queries.GetMinValSimpleTx(context.Background(), tx, m.Schema, m.Table, m.Key[0])
			if err != nil {
				return fmt.Errorf("failed to fetch simple min key: %w", err)
			}
			if minVal != nil {
				if err := queries.UpdateBlockRangeStartTx(context.Background(), tx, mtreeTableName, minVal, 0); err != nil {
					return fmt.Errorf("failed to update first block start (simple): %w", err)
				}
			}
		}

		blocksToUpdate, err := queries.GetDirtyAndNewBlocksTx(context.Background(), tx, mtreeTableName, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return fmt.Errorf("error getting dirty blocks on node %s: %w", nodeInfo["Name"], err)
		}

		if len(blocksToUpdate) == 0 {
			fmt.Printf("No updates needed for %s\n", nodeInfo["Name"])
			tx.Commit(context.Background())
			continue
		}

		splitThreshold := m.BlockSize / 2
		var blockPositionsToSplit []int64
		for _, b := range blocksToUpdate {
			blockPositionsToSplit = append(blockPositionsToSplit, b.NodePosition)
		}

		blocksToSplit, err := queries.FindBlocksToSplitTx(context.Background(), tx, mtreeTableName, splitThreshold, blockPositionsToSplit, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTableName, err)
		}

		var modifiedPositions []int64

		if len(blocksToSplit) > 0 {
			fmt.Printf("Found %d blocks that may need splitting\n", len(blocksToSplit))
			modified, err := m.splitBlocks(tx, blocksToSplit)
			if err != nil {
				return err
			}
			modifiedPositions = append(modifiedPositions, modified...)
		}

		if m.Rebalance {
			mergeThreshold := 0.25
			blocksToMerge, err := queries.FindBlocksToMergeTx(context.Background(), tx, mtreeTableName, !m.SimplePrimaryKey, m.Schema, m.Table, m.Key, mergeThreshold, blockPositionsToSplit)
			if err != nil {
				return fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTableName, err)
			}
			if len(blocksToMerge) > 0 {
				fmt.Printf("Found %d blocks that may need merging\n", len(blocksToMerge))
				merged, err := m.mergeBlocks(tx, blocksToMerge)
				if err != nil {
					return err
				}
				modifiedPositions = append(modifiedPositions, merged...)
			}
		}

		blocksToUpdate, err = queries.GetDirtyAndNewBlocksTx(context.Background(), tx, mtreeTableName, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return err
		}

		if len(blocksToUpdate) == 0 {
			fmt.Printf("No updates needed for %s after rebalancing\n", nodeInfo["Name"])
			tx.Commit(context.Background())
			continue
		}
		fmt.Printf("Found %d blocks to update\n", len(blocksToUpdate))

		var affectedPositions []int64
		for _, block := range blocksToUpdate {
			affectedPositions = append(affectedPositions, block.NodePosition)
		}

		if len(affectedPositions) > 0 {
			// Recompute hashes for all affected blocks (newly split, merged, or dirty)
			p := mpb.New(mpb.WithOutput(os.Stderr))
			bar := p.AddBar(int64(len(blocksToUpdate)),
				mpb.BarRemoveOnComplete(),
				mpb.PrependDecorators(
					decor.Name("Recomputing leaf hashes:"),
					decor.CountersNoUnit(" %d / %d"),
				),
				mpb.AppendDecorators(
					decor.Elapsed(decor.ET_STYLE_GO),
					decor.Name(" | "),
					decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
				),
			)

			for _, block := range blocksToUpdate {
				leafHash, err := queries.ComputeLeafHashesTx(context.Background(), tx, m.Schema, m.Table, m.Cols, m.SimplePrimaryKey, m.Key, block.RangeStart, block.RangeEnd)
				if err != nil {
					return fmt.Errorf("failed to recompute hash for block %d: %w", block.NodePosition, err)
				}
				if _, err := queries.UpdateLeafHashesTx(context.Background(), tx, mtreeTableName, leafHash, block.NodePosition); err != nil {
					return fmt.Errorf("failed to update leaf hash for block %d: %w", block.NodePosition, err)
				}
				bar.Increment()
			}
			p.Wait()

			// Rebuild parent nodes since leaf hashes have changed
			fmt.Println("Rebuilding parent nodes")
			if err := m.buildParentNodes(tx); err != nil {
				return err
			}

			fmt.Println("Clearing dirty flags for affected blocks")
			err = queries.ClearDirtyFlagsTx(context.Background(), tx, mtreeTableName, affectedPositions)
			if err != nil {
				return err
			}
		}

		if err := tx.Commit(context.Background()); err != nil {
			return fmt.Errorf("error committing transaction on node %s: %w", nodeInfo["Name"], err)
		}
		fmt.Printf("Successfully updated %d blocks on %s\n", len(affectedPositions), nodeInfo["Name"])
	}

	return nil
}

func (m *MerkleTreeTask) splitBlocks(tx pgx.Tx, blocksToSplit []types.BlockRange) ([]int64, error) {
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	isComposite := !m.SimplePrimaryKey
	ctx := context.Background()
	var modifiedPositions []int64

	if err := queries.DeleteParentNodesTx(ctx, tx, mtreeTableName); err != nil {
		return nil, fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	currentBlocks := blocksToSplit
	i := 0
	for i < len(currentBlocks) {
		blk := currentBlocks[i]
		pos := blk.NodePosition
		start := blk.RangeStart
		end := blk.RangeEnd
		originallyUnbounded := len(end) == 0 || allNil(end)

		// Handle last block with null end
		if originallyUnbounded && i == len(currentBlocks)-1 {
			var maxVal []any
			var err error
			if isComposite {
				maxVal, err = queries.GetMaxValCompositeTx(ctx, tx, m.Schema, m.Table, m.Key, start)
			} else {
				var simpleMaxVal any
				simpleMaxVal, err = queries.GetMaxValSimpleTx(ctx, tx, m.Schema, m.Table, m.Key[0], start[0])
				if err == nil && simpleMaxVal != nil {
					maxVal = []any{simpleMaxVal}
				}
			}

			if err == nil && maxVal != nil {
				if isComposite {
					err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, maxVal, pos)
				} else {
					err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, maxVal[0], pos)
				}
				if err != nil {
					return nil, err
				}
				end = maxVal
				currentBlocks[i].RangeEnd = end // Update in slice
			}
		}

		// Get actual row count for the block
		count, err := queries.GetBlockRowCount(ctx, tx, m.Schema, m.Table, m.Key, isComposite, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to get block row count for block %d: %w", pos, err)
		}

		if count < int64(float64(m.BlockSize)*1.5) {
			i++
			continue
		}

		// Find split point at midpoint
		midpoint := count / 2
		var splitPoint []any
		if isComposite {
			splitPoint, err = queries.GetSplitPointCompositeTx(ctx, tx, m.Schema, m.Table, m.Key, start, end, midpoint)
		} else {
			pkeyType, pkeyTypeErr := queries.GetPkeyTypeTx(ctx, tx, m.Schema, m.Table, m.Key[0])
			if pkeyTypeErr != nil {
				return nil, pkeyTypeErr
			}
			var sp any
			sp, err = queries.GetSplitPointSimpleTx(ctx, tx, m.Schema, m.Table, m.Key[0], pkeyType, start[0], valueOrNil(end), midpoint)
			if err == nil && sp != nil {
				splitPoint = []any{sp}
			}
		}

		if err != nil || splitPoint == nil {
			i++
			continue
		}

		// Insert new block at the end (next available position)
		newPos, err := queries.GetMaxNodePositionTx(ctx, tx, mtreeTableName)
		if err != nil {
			return nil, err
		}

		if isComposite {
			var endVals []any
			if originallyUnbounded {
				endVals = nil
			} else {
				endVals = end
			}
			err = queries.InsertCompositeBlockRangesTx(ctx, tx, mtreeTableName, newPos, splitPoint, endVals)
		} else {
			var endVal any
			if originallyUnbounded {
				endVal = nil
			} else {
				endVal = valueOrNil(end)
			}
			err = queries.InsertBlockRangesTx(ctx, tx, mtreeTableName, newPos, splitPoint[0], endVal)
		}
		if err != nil {
			return nil, err
		}

		// Update original block's end
		if isComposite {
			err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, splitPoint, pos)
		} else {
			err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, splitPoint[0], pos)
		}
		if err != nil {
			return nil, err
		}

		modifiedPositions = append(modifiedPositions, pos, newPos)

		// Modify current block and insert new block for potential further splits
		currentBlocks[i].RangeEnd = splitPoint
		newBlock := types.BlockRange{NodePosition: newPos, RangeStart: splitPoint, RangeEnd: end}

		// Insert new block into the slice for processing
		currentBlocks = append(currentBlocks[:i+1], append([]types.BlockRange{newBlock}, currentBlocks[i+1:]...)...)

		i++
	}

	return modifiedPositions, nil
}

func valueOrNil(end []any) interface{} {
	if len(end) == 0 || end[0] == nil {
		return nil
	}
	return end[0]
}

func allNil(vals []any) bool {
	if len(vals) == 0 {
		return true
	}
	for _, v := range vals {
		if v != nil {
			return false
		}
	}
	return true
}

func (m *MerkleTreeTask) mergeBlocks(tx pgx.Tx, blocksToMerge []types.BlockRange) ([]int64, error) {
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	isComposite := !m.SimplePrimaryKey
	ctx := context.Background()
	var modifiedPositions []int64
	const tempOffset = 1_000_000

	if err := queries.DeleteParentNodesTx(ctx, tx, mtreeTableName); err != nil {
		return nil, fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	currentBlocks := blocksToMerge
	i := 0
	for i < len(currentBlocks) {
		blk := currentBlocks[i]
		pos := blk.NodePosition
		start := blk.RangeStart
		end := blk.RangeEnd

		// Handle last block with null end
		if (len(end) == 0 || end[0] == nil) && i == len(currentBlocks)-1 {
			var maxVal []any
			var err error
			if isComposite {
				maxVal, err = queries.GetMaxValCompositeTx(ctx, tx, m.Schema, m.Table, m.Key, start)
			} else {
				var simpleMaxVal any
				simpleMaxVal, err = queries.GetMaxValSimpleTx(ctx, tx, m.Schema, m.Table, m.Key[0], start[0])
				if err == nil && simpleMaxVal != nil {
					maxVal = []any{simpleMaxVal}
				}
			}

			if err == nil && maxVal != nil {
				if isComposite {
					err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, maxVal, pos)
				} else {
					err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, maxVal[0], pos)
				}
				if err != nil {
					return nil, err
				}
				end = maxVal
				currentBlocks[i].RangeEnd = end
			}
		}
		count, err := queries.GetBlockRowCount(ctx, tx, m.Schema, m.Table, m.Key, isComposite, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to get block row count for block %d: %w", pos, err)
		}

		if count >= int64(float64(m.BlockSize)*0.25) {
			i++
			continue
		}

		// Try to merge with the previous block
		if pos > 0 {
			prevBlock, err := queries.GetBlockWithCountTx(ctx, tx, mtreeTableName, m.Schema, m.Table, m.Key, isComposite, pos-1)
			if err != nil {
				return nil, err
			}
			if prevBlock != nil && (count+prevBlock.Count) <= int64(m.BlockSize*2) {
				// Move blocks to temp positions
				if err := queries.UpdateNodePositionTx(ctx, tx, mtreeTableName, pos-1, pos-1+tempOffset); err != nil {
					return nil, err
				}
				if err := queries.UpdateNodePositionTx(ctx, tx, mtreeTableName, pos, pos+tempOffset); err != nil {
					return nil, err
				}
				// Merge with previous block
				if isComposite {
					err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, end, pos-1+tempOffset)
				} else {
					err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, valueOrNil(end), pos-1+tempOffset)
				}
				if err != nil {
					return nil, err
				}

				// Delete current block
				if err := queries.DeleteBlockTx(ctx, tx, mtreeTableName, pos+tempOffset); err != nil {
					return nil, err
				}
				// Move remaining blocks back
				if err := queries.UpdateNodePositionsSequentialTx(ctx, tx, mtreeTableName, pos); err != nil {
					return nil, err
				}
				if err := queries.UpdateNodePositionTx(ctx, tx, mtreeTableName, pos-1+tempOffset, pos-1); err != nil {
					return nil, err
				}

				modifiedPositions = append(modifiedPositions, pos-1)

				// Remove merged block from slice
				currentBlocks = append(currentBlocks[:i], currentBlocks[i+1:]...)
				for j := i; j < len(currentBlocks); j++ {
					currentBlocks[j].NodePosition--
				}
				continue
			}
		}

		// Try to merge with the next block
		if i+1 < len(currentBlocks) {
			nextBlock, err := queries.GetBlockWithCountTx(ctx, tx, mtreeTableName, m.Schema, m.Table, m.Key, isComposite, pos+1)
			if err != nil {
				return nil, err
			}
			if nextBlock != nil && (count+nextBlock.Count) <= int64(m.BlockSize*2) {
				// Move blocks to temp positions
				if err := queries.UpdateNodePositionTx(ctx, tx, mtreeTableName, pos, pos+tempOffset); err != nil {
					return nil, err
				}
				if err := queries.UpdateNodePositionTx(ctx, tx, mtreeTableName, pos+1, pos+1+tempOffset); err != nil {
					return nil, err
				}
				// Merge with next block
				if isComposite {
					err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, nextBlock.RangeEnd, pos+tempOffset)
				} else {
					err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, valueOrNil(nextBlock.RangeEnd), pos+tempOffset)
				}
				if err != nil {
					return nil, err
				}
				// Delete next block
				if err := queries.DeleteBlockTx(ctx, tx, mtreeTableName, pos+1+tempOffset); err != nil {
					return nil, err
				}
				// Move remaining blocks back
				if err := queries.UpdateNodePositionsSequentialTx(ctx, tx, mtreeTableName, pos+1); err != nil {
					return nil, err
				}
				if err := queries.UpdateNodePositionTx(ctx, tx, mtreeTableName, pos+tempOffset, pos); err != nil {
					return nil, err
				}
				modifiedPositions = append(modifiedPositions, pos)

				currentBlocks = append(currentBlocks[:i+1], currentBlocks[i+2:]...)
				for j := i + 1; j < len(currentBlocks); j++ {
					currentBlocks[j].NodePosition--
				}
				continue
			}
		}
		i++
	}

	return modifiedPositions, nil
}

func (m *MerkleTreeTask) buildParentNodes(conn queries.DBTX) error {
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)

	var err error
	if tx, ok := conn.(pgx.Tx); ok {
		err = queries.DeleteParentNodesTx(context.Background(), tx, mtreeTableName)
	} else if pool, ok := conn.(*pgxpool.Pool); ok {
		err = queries.DeleteParentNodes(context.Background(), pool, mtreeTableName)
	} else {
		return fmt.Errorf("unsupported connection type for DeleteParentNodes")
	}

	if err != nil {
		return err
	}

	level := 0
	for {
		var count int
		var buildErr error
		if tx, ok := conn.(pgx.Tx); ok {
			count, buildErr = queries.BuildParentNodesTx(context.Background(), tx, mtreeTableName, level)
		} else if pool, ok := conn.(*pgxpool.Pool); ok {
			count, buildErr = queries.BuildParentNodes(context.Background(), pool, mtreeTableName, level)
		} else {
			return fmt.Errorf("unsupported connection type for BuildParentNodes")
		}

		if buildErr != nil {
			return fmt.Errorf("failed to build parent nodes at level %d: %w", level, buildErr)
		}
		if count <= 1 {
			break
		}
		level++
	}

	return nil
}

type LeafHashResult struct {
	BlockID int64
	Hash    []byte
	Err     error
}

func (m *MerkleTreeTask) computeLeafHashes(pool *pgxpool.Pool, ranges []types.BlockRange) error {

	numWorkers := int(float64(runtime.NumCPU()) * m.MaxCpuRatio)
	if numWorkers < 1 {
		numWorkers = 1
	}
	jobs := make(chan types.BlockRange, len(ranges))
	results := make(chan LeafHashResult, len(ranges))

	p := mpb.New(mpb.WithOutput(os.Stderr))
	bar := p.AddBar(int64(len(ranges)),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Computing leaf hashes:", decor.WC{W: 25}),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(" | "),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
		),
	)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go m.leafHashWorker(&wg, jobs, results, pool, bar)
	}

	for _, r := range ranges {
		jobs <- r
	}
	close(jobs)

	wg.Wait()
	close(results)

	p.Wait()

	leafHashes := make(map[int64][]byte)
	for result := range results {
		if result.Err != nil {
			return result.Err
		}
		leafHashes[result.BlockID] = result.Hash
	}

	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)

	batch := &pgx.Batch{}
	for blockID, hash := range leafHashes {
		_, err := queries.UpdateLeafHashes(context.Background(), pool, mtreeTableName, hash, blockID)
		if err != nil {
			return err
		}
	}

	tx, err := pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	br := tx.SendBatch(context.Background(), batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to execute batch update for leaf hashes: %w", err)
		}
	}

	if err := br.Close(); err != nil {
		return fmt.Errorf("failed to close batch: %w", err)
	}

	return tx.Commit(context.Background())
}

func (m *MerkleTreeTask) leafHashWorker(wg *sync.WaitGroup, jobs <-chan types.BlockRange, results chan<- LeafHashResult, pool *pgxpool.Pool, bar *mpb.Bar) {
	defer wg.Done()

	for block := range jobs {
		leafHash, err := queries.ComputeLeafHashes(context.Background(), pool, m.Schema, m.Table, m.SimplePrimaryKey, m.Key, block.RangeStart, block.RangeEnd)
		if err != nil {
			results <- LeafHashResult{BlockID: block.NodePosition, Err: fmt.Errorf("failed to compute hash for block %d: %w", block.NodePosition, err)}
			bar.Increment()
			continue
		}
		results <- LeafHashResult{BlockID: block.NodePosition, Hash: leafHash}
		bar.Increment()
	}
}

// func (m *MerkleTreeTask) buildWhereClause(block types.BlockRange) (string, error) {
// 	var whereConditions []string
// 	keyColumns := m.Key

// 	if m.SimplePrimaryKey {
// 		if block.RangeStart[0] != nil {
// 			whereConditions = append(whereConditions, fmt.Sprintf("%s >= %v", pgx.Identifier{keyColumns[0]}.Sanitize(), block.RangeStart[0]))
// 		}
// 		if block.RangeEnd[0] != nil {
// 			whereConditions = append(whereConditions, fmt.Sprintf("%s <= %v", pgx.Identifier{keyColumns[0]}.Sanitize(), block.RangeEnd[0]))
// 		}
// 	} else {
// 		pkCols := make([]string, len(keyColumns))
// 		for i, c := range keyColumns {
// 			pkCols[i] = pgx.Identifier{c}.Sanitize()
// 		}
// 		pkTuple := fmt.Sprintf("(%s)", strings.Join(pkCols, ", "))

// 		if len(block.RangeStart) > 0 && block.RangeStart[0] != nil {
// 			startVals := make([]string, len(block.RangeStart))
// 			for i, v := range block.RangeStart {
// 				startVals[i] = fmt.Sprintf("'%v'", v)
// 			}
// 			whereConditions = append(whereConditions, fmt.Sprintf("%s >= (%s)", pkTuple, strings.Join(startVals, ", ")))
// 		}
// 		if len(block.RangeEnd) > 0 && block.RangeEnd[0] != nil {
// 			endVals := make([]string, len(block.RangeEnd))
// 			for i, v := range block.RangeEnd {
// 				endVals[i] = fmt.Sprintf("'%v'", v)
// 			}
// 			whereConditions = append(whereConditions, fmt.Sprintf("%s <= (%s)", pkTuple, strings.Join(endVals, ", ")))
// 		}
// 	}

// 	if len(whereConditions) == 0 {
// 		return "TRUE", nil
// 	}
// 	return strings.Join(whereConditions, " AND "), nil
// }

func (m *MerkleTreeTask) insertBlockRanges(pool *pgxpool.Pool, ranges []types.BlockRange) error {
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	mtreeTableIdent := pgx.Identifier{mtreeTableName}

	if m.SimplePrimaryKey {
		if err := queries.InsertBlockRangesBatchSimple(context.Background(), pool, mtreeTableIdent.Sanitize(), ranges); err != nil {
			return err
		}
	} else {
		if err := queries.InsertBlockRangesBatchComposite(context.Background(), pool, mtreeTableIdent.Sanitize(), ranges, len(m.Key)); err != nil {
			return err
		}
	}

	return nil
}

func (m *MerkleTreeTask) createMtreeObjects(pool *pgxpool.Pool, totalRows int64, numBlocks int) error {
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	err = queries.CreateXORFunction(context.Background(), pool)
	if err != nil {
		return fmt.Errorf("failed to create xor function: %w", err)
	}

	err = queries.CreateMetadataTable(context.Background(), pool)
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}

	err = queries.UpdateMetadata(context.Background(), pool, m.Schema, m.Table, totalRows, m.BlockSize, numBlocks, !m.SimplePrimaryKey)
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	err = queries.DropMtreeTable(context.Background(), pool, mtreeTableName)
	if err != nil {
		return fmt.Errorf("failed to render drop mtree table sql: %w", err)
	}

	if m.SimplePrimaryKey {
		pkeyType, err := queries.GetPkeyType(context.Background(), pool, m.Schema, m.Table, m.Key[0])
		if err != nil {
			return err
		}
		err = queries.CreateSimpleMtreeTable(context.Background(), pool, mtreeTableName, pkeyType)
		if err != nil {
			return fmt.Errorf("failed to render create simple mtree table sql: %w", err)
		}
	} else {
		keyTypeColumns := make([]string, len(m.Key))
		for i, col := range m.Key {
			colType, err := queries.GetPkeyType(context.Background(), pool, m.Schema, m.Table, col)
			if err != nil {
				return err
			}
			keyTypeColumns[i] = fmt.Sprintf("%s %s", pgx.Identifier{col}.Sanitize(), colType)
		}

		compositeTypeName := fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)

		err = queries.DropCompositeType(context.Background(), pool, compositeTypeName)
		if err != nil {
			return fmt.Errorf("failed to render drop composite type sql: %w", err)
		}

		err = queries.CreateCompositeType(context.Background(), pool, compositeTypeName, strings.Join(keyTypeColumns, ", "))
		if err != nil {
			return fmt.Errorf("failed to render create composite type sql: %w", err)
		}

		err = queries.CreateCompositeMtreeTable(context.Background(), pool, mtreeTableName, compositeTypeName)
		if err != nil {
			return fmt.Errorf("failed to render create composite mtree table sql: %w", err)
		}
	}
	err = m.buildParentNodes(pool)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func computeSamplingParameters(rowCount int64) (string, float64) {
	sampleMethod := "BERNOULLI"
	samplePercent := 100.0

	if rowCount <= 10000 {
		return sampleMethod, samplePercent
	}
	if rowCount <= 100000 {
		samplePercent = 10
	} else if rowCount <= 1000000 {
		samplePercent = 1
	} else if rowCount <= 100000000 {
		sampleMethod = "SYSTEM"
		samplePercent = 0.1
	} else {
		sampleMethod = "SYSTEM"
		samplePercent = 0.01
	}
	return sampleMethod, samplePercent
}
