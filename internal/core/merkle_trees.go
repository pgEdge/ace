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
	"errors"
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/internal/cdc"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

const tableAlreadyInPublicationError = "42710"

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
	Mode              string
}

func (m *MerkleTreeTask) MtreeInit() error {
	if err := m.validateInit(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	cfg := config.Cfg.MTree.CDC

	for _, nodeInfo := range m.ClusterNodes {
		logger.Info("Initialising Merkle tree objects on node: %s", nodeInfo["Name"])
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		err = queries.CreateXORFunction(context.Background(), pool)
		if err != nil {
			return fmt.Errorf("failed to create xor function: %w", err)
		}

		err = queries.CreateCDCMetadataTable(context.Background(), pool)
		if err != nil {
			return fmt.Errorf("failed to create cdc metadata table: %w", err)
		}

		lsn, err := cdc.SetupCDC(nodeInfo)
		if err != nil {
			return fmt.Errorf("failed to setup replication: %w", err)
		}

		err = queries.UpdateCDCMetadata(context.Background(), pool, cfg.PublicationName, cfg.SlotName, lsn.String(), []string{})
		if err != nil {
			return fmt.Errorf("failed to update cdc metadata: %w", err)
		}

		logger.Info("Merkle tree objects initialised on node: %s", nodeInfo["Name"])
	}
	return nil
}

func (m *MerkleTreeTask) MtreeTeardown() error {
	if err := m.validateInit(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		err = queries.DropPublication(context.Background(), pool, "ace_mtree_pub")
		if err != nil {
			return fmt.Errorf("failed to drop publication: %w", err)
		}
		logger.Info("Publication dropped on node: %s", nodeInfo["Name"])

		err = queries.DropReplicationSlot(context.Background(), pool, "ace_mtree_slot")
		if err != nil {
			return fmt.Errorf("failed to drop replication slot: %w", err)
		}
		logger.Info("Replication slot dropped on node: %s", nodeInfo["Name"])
		err = queries.DropCDCMetadataTable(context.Background(), pool)
		if err != nil {
			return fmt.Errorf("failed to drop cdc metadata table: %w", err)
		}
		logger.Info("CDC metadata table dropped on node: %s", nodeInfo["Name"])
	}
	return nil
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

func (m *MerkleTreeTask) validateInit() error {
	return m.validateCommon()
}

func (m *MerkleTreeTask) validateCommon() error {
	if m.ClusterName == "" {
		return fmt.Errorf("cluster_name is a required argument")
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
		combinedMap["Host"] = nodeMap["Host"]

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

	m.ClusterNodes = clusterNodes
	return nil
}

func (m *MerkleTreeTask) Validate() error {
	if err := m.validateCommon(); err != nil {
		return err
	}
	if m.Mode == "listen" {
		return nil
	}
	cfg := config.Cfg.MTree.Diff

	if m.BlockSize != 0 && !m.OverrideBlockSize {
		if m.BlockSize > cfg.MaxBlockSize {
			return fmt.Errorf("block size should be <= %d", cfg.MaxBlockSize)
		}
		if m.BlockSize < cfg.MinBlockSize {
			return fmt.Errorf("block size should be >= %d", cfg.MinBlockSize)
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
	m.Schema = schema
	m.Table = table

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
	cfg := config.Cfg.MTree.CDC

	logger.Info("Getting row estimates from all nodes...")
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
			logger.Warn("Warning: Could not get row estimate from node %s: %v", nodeInfo["Name"], err)
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
	logger.Info("Using node %s as the reference for defining block ranges.", refNode["Name"])

	if len(blockRanges) == 0 {
		logger.Info("Calculating block ranges for ~%d rows...", maxRows)

		refPool, err := auth.GetClusterNodeConnection(refNode, "")
		if err != nil {
			return fmt.Errorf("could not connect to reference node %s: %w", refNode["Name"], err)
		}

		sampleMethod, samplePercent := computeSamplingParameters(maxRows)
		logger.Info("Using %s with sample percent %.2f", sampleMethod, samplePercent)

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
		logger.Info("Processing node: %s", nodeInfo["Name"])
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("failed to connect to node %s for mtree build: %w", nodeInfo["Name"], err)
		}

		publicationName := cfg.PublicationName
		err = queries.AlterPublicationAddTable(context.Background(), pool, publicationName, m.QualifiedTableName)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == tableAlreadyInPublicationError {
				logger.Info("Table %s is already in publication %s on node %s", m.QualifiedTableName, publicationName, nodeInfo["Name"])
			} else {
				pool.Close()
				return fmt.Errorf("failed to add table to publication on node %s: %w", nodeInfo["Name"], err)
			}
		} else {
			logger.Info("Added table %s to publication %s on node %s", m.QualifiedTableName, publicationName, nodeInfo["Name"])
		}

		slotName, startLSN, tables, err := queries.GetCDCMetadata(context.Background(), pool, publicationName)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to get cdc metadata on node %s: %w", nodeInfo["Name"], err)
		}

		var tableExists bool
		for _, table := range tables {
			if table == m.QualifiedTableName {
				tableExists = true
				break
			}
		}

		if !tableExists {
			tables = append(tables, m.QualifiedTableName)
		}

		err = queries.UpdateCDCMetadata(context.Background(), pool, publicationName, slotName, startLSN, tables)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to update cdc metadata on node %s: %w", nodeInfo["Name"], err)
		}
		logger.Info("Updated CDC metadata for table %s on node %s", m.QualifiedTableName, nodeInfo["Name"])

		logger.Info("Creating Merkle Tree objects on %s...", nodeInfo["Name"])
		err = m.createMtreeObjects(pool, maxRows, numBlocks)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to create mtree objects on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Inserting block ranges on %s...", nodeInfo["Name"])
		err = m.insertBlockRanges(pool, blockRanges)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to insert block ranges on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Computing leaf hashes on %s...", nodeInfo["Name"])
		err = m.computeLeafHashes(pool, blockRanges)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to compute leaf hashes on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Building parent nodes on %s...", nodeInfo["Name"])
		err = m.buildParentNodes(pool)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to build parent nodes on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Merkle tree built successfully on %s", nodeInfo["Name"])
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

	for _, nodeInfo := range m.ClusterNodes {
		cdc.UpdateFromCDC(nodeInfo)
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

		if len(blocksToSplit) > 0 {
			fmt.Printf("Found %d blocks that may need splitting\n", len(blocksToSplit))
			_, err := m.splitBlocks(tx, blocksToSplit)
			if err != nil {
				return err
			}
		}

		if m.Rebalance {
			if _, err := m.performMerges(tx); err != nil {
				return err
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

	compositeTypeName := fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)

	currentBlocks := make([]types.BlockRange, len(blocksToSplit))
	copy(currentBlocks, blocksToSplit)

	if err := queries.DeleteParentNodesTx(ctx, tx, mtreeTableName); err != nil {
		return nil, fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	for _, blk := range currentBlocks {
		pos := blk.NodePosition
		start := blk.RangeStart
		end := blk.RangeEnd
		originallyUnbounded := len(end) == 0 || allNil(end)

		if originallyUnbounded {
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
				end = maxVal
			}
		}

		count, err := queries.GetBlockRowCount(ctx, tx, m.Schema, m.Table, m.Key, isComposite, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to get block row count for block %d: %w", pos, err)
		}

		if count < int64(m.BlockSize*2) {
			continue
		}

		splitPoints, err := queries.GetBulkSplitPointsTx(ctx, tx, m.Schema, m.Table, m.Key, isComposite, start, end, m.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to get bulk split points for block %d: %w", pos, err)
		}

		if len(splitPoints) > 0 {
			lastSplitPoint := splitPoints[len(splitPoints)-1]
			sliverCount, err := queries.GetBlockRowCount(ctx, tx, m.Schema, m.Table, m.Key, isComposite, lastSplitPoint, end)
			if err != nil {
				return nil, fmt.Errorf("failed to get row count for sliver block: %w", err)
			}

			if sliverCount < int64(float64(m.BlockSize)*0.25) {
				splitPoints = splitPoints[:len(splitPoints)-1]
			}
		}

		if len(splitPoints) == 0 {
			continue
		}

		for _, sp := range splitPoints {
			if isComposite {
				err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, compositeTypeName, sp, pos)
			} else {
				err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, sp[0], pos)
			}
			if err != nil {
				return nil, err
			}

			newPos, err := queries.GetMaxNodePositionTx(ctx, tx, mtreeTableName)
			if err != nil {
				return nil, err
			}
			if isComposite {
				err = queries.InsertCompositeBlockRangesTx(ctx, tx, mtreeTableName, newPos, sp, nil)
			} else {
				err = queries.InsertBlockRangesTx(ctx, tx, mtreeTableName, newPos, sp[0], nil)
			}
			if err != nil {
				return nil, err
			}
			modifiedPositions = append(modifiedPositions, newPos)
			pos = newPos
		}

		if originallyUnbounded {
			if isComposite {
				err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, compositeTypeName, nil, pos)
			} else {
				err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, nil, pos)
			}
		} else {
			if isComposite {
				err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, compositeTypeName, end, pos)
			} else {
				err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, end[0], pos)
			}
		}
		if err != nil {
			return nil, err
		}
		modifiedPositions = append(modifiedPositions, pos)
	}

	return modifiedPositions, nil
}

func (m *MerkleTreeTask) performMerges(tx pgx.Tx) ([]int64, error) {
	var allModifiedPositions []int64
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)

	for {
		blocksToMerge, err := queries.FindBlocksToMergeTx(context.Background(), tx, mtreeTableName, m.SimplePrimaryKey, m.Schema, m.Table, m.Key, 0.25, []int64{})
		if err != nil {
			return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTableName, err)
		}

		if len(blocksToMerge) == 0 {
			break
		}
		fmt.Printf("Found %d blocks that may need merging\n", len(blocksToMerge))

		modified, err := m.mergeBlocks(tx, blocksToMerge)
		if err != nil {
			return nil, err
		}

		if len(modified) == 0 {
			// No useful merges could be made in this pass, so we're done.
			break
		}
		allModifiedPositions = append(allModifiedPositions, modified...)

		// Reset positions after each pass to ensure pos+1 logic works correctly in the next iteration.
		if err := queries.ResetPositionsByStartTx(context.Background(), tx, mtreeTableName, m.Key, !m.SimplePrimaryKey); err != nil {
			return nil, fmt.Errorf("failed to reset positions after merges: %w", err)
		}
	}

	return allModifiedPositions, nil
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

	compositeTypeName := fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)

	if err := queries.DeleteParentNodesTx(ctx, tx, mtreeTableName); err != nil {
		return nil, fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	mergedPositions := make(map[int64]bool)

	for _, blk := range blocksToMerge {
		pos := blk.NodePosition

		if mergedPositions[pos] {
			continue
		}

		currentBlock, err := queries.GetBlockWithCountTx(ctx, tx, mtreeTableName, m.Schema, m.Table, m.Key, isComposite, pos)
		if err != nil {
			return nil, fmt.Errorf("failed to get current block %d with count: %w", pos, err)
		}
		if currentBlock == nil {
			continue
		}

		// Attempt to merge with the next block
		nextBlock, err := queries.GetBlockWithCountTx(ctx, tx, mtreeTableName, m.Schema, m.Table, m.Key, isComposite, pos+1)
		if err != nil {
			return nil, fmt.Errorf("failed to get next block for %d: %w", pos, err)
		}

		if nextBlock != nil && (currentBlock.Count+nextBlock.Count < int64(float64(m.BlockSize)*1.5)) {
			if isComposite {
				err = queries.UpdateBlockRangeEndCompositeTx(ctx, tx, mtreeTableName, compositeTypeName, nextBlock.RangeEnd, currentBlock.NodePosition)
			} else {
				err = queries.UpdateBlockRangeEndTx(ctx, tx, mtreeTableName, valueOrNil(nextBlock.RangeEnd), currentBlock.NodePosition)
			}
			if err != nil {
				return nil, err
			}

			if err := queries.DeleteBlockTx(ctx, tx, mtreeTableName, nextBlock.NodePosition); err != nil {
				return nil, err
			}
			modifiedPositions = append(modifiedPositions, currentBlock.NodePosition)
			mergedPositions[nextBlock.NodePosition] = true
		}
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
