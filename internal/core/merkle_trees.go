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
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/helpers"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
)

const (
	MinMtreeBlockSize = 1000
	MaxMtreeBlockSize = 1000000
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

type BlockRange struct {
	ID    int
	Start []any
	End   []any
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

	if !m.OverrideBlockSize {
		if m.BlockSize > MaxMtreeBlockSize {
			return fmt.Errorf("block size should be <= %d", MaxMtreeBlockSize)
		}
		if m.BlockSize < MinMtreeBlockSize {
			return fmt.Errorf("block size should be >= %d", MinMtreeBlockSize)
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
	if err := helpers.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := helpers.SanitiseIdentifier(table); err != nil {
		return err
	}

	/*
		We've not eliminated our dependence on the cluster json file just yet.
		So, for convenience, we'll append the chosen db credentials to the
		cluster nodes.
	*/
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

		currentColsSlice, err := utils.GetColumns(pool, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("failed to get columns on node %s: %w", nodeInfo["Name"], err)
		}
		currentKeySlice, err := utils.GetPrimaryKey(pool, m.Schema, m.Table)
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
	var blockRanges []BlockRange

	fmt.Println("Getting row estimates from all nodes...")
	var maxRows int64
	var refNode map[string]any
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
		if err != nil {
			return fmt.Errorf("could not connect to node %s to get row estimate: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		q := queries.NewQuerier(pool)
		countPtr, err := q.EstimateRowCount(context.Background(), pgtype.Name{String: m.Schema, Status: pgtype.Present}, pgtype.Name{String: m.Table, Status: pgtype.Present})
		if err != nil {
			fmt.Printf("Warning: Could not get row estimate from node %s: %v\n", nodeInfo["Name"], err)
			continue
		}
		if countPtr == nil {
			fmt.Printf("Warning: Row estimate from node %s was nil\n", nodeInfo["Name"])
			continue
		}
		count := int64(*countPtr)

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

		numBlocks := int(math.Ceil(float64(maxRows) / float64(m.BlockSize)))
		if numBlocks == 0 && maxRows > 0 {
			numBlocks = 1
		}

		keyColumns := m.Key

		offsetsQuery, err := helpers.GeneratePkeyOffsetsQuery(m.Schema, m.Table, keyColumns, sampleMethod, samplePercent, numBlocks)
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
			dest := make([]interface{}, 2*numKeyCols)
			destPtrs := make([]interface{}, 2*numKeyCols)
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
			blockRanges = append(blockRanges, BlockRange{ID: i, Start: startVals, End: endVals})
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
		err = m.createMtreeObjects(pool)
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
		err = m.computeLeafHashes(pool, blockRanges, nodeInfo)
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

func (m *MerkleTreeTask) buildParentNodes(pool *pgxpool.Pool) error {
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	mtreeTableIdent := pgx.Identifier{mtreeTableName}

	tx, err := pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	deleteSQL, err := helpers.RenderSQL(helpers.SQLTemplates.DeleteParentNodes, map[string]string{
		"MtreeTable": mtreeTableIdent.Sanitize(),
	})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(context.Background(), deleteSQL); err != nil {
		return fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	buildSQL, err := helpers.RenderSQL(helpers.SQLTemplates.BuildParentNodes, map[string]string{
		"MtreeTable": mtreeTableIdent.Sanitize(),
	})
	if err != nil {
		return err
	}

	level := 0
	for {
		var count int
		err := tx.QueryRow(context.Background(), buildSQL, level).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to build parent nodes at level %d: %w", level, err)
		}
		if count <= 1 {
			break
		}
		level++
	}

	return tx.Commit(context.Background())
}

type LeafHashResult struct {
	BlockID int
	Hash    []byte
	Err     error
}

func (m *MerkleTreeTask) computeLeafHashes(pool *pgxpool.Pool, ranges []BlockRange, nodeInfo map[string]any) error {

	numWorkers := int(float64(runtime.NumCPU()) * m.MaxCpuRatio)
	if numWorkers < 1 {
		numWorkers = 1
	}
	jobs := make(chan BlockRange, len(ranges))
	results := make(chan LeafHashResult, len(ranges))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go m.leafHashWorker(i, &wg, jobs, results, pool)
	}

	for _, r := range ranges {
		jobs <- r
	}
	close(jobs)

	wg.Wait()
	close(results)

	leafHashes := make(map[int][]byte)
	for result := range results {
		if result.Err != nil {
			return result.Err
		}
		leafHashes[result.BlockID] = result.Hash
	}

	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	updateSQL, err := helpers.RenderSQL(helpers.SQLTemplates.UpdateLeafHashes, map[string]string{
		"MtreeTable": pgx.Identifier{mtreeTableName}.Sanitize(),
	})
	if err != nil {
		return err
	}

	batch := &pgx.Batch{}
	for blockID, hash := range leafHashes {
		batch.Queue(updateSQL, hash, blockID)
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

func (m *MerkleTreeTask) leafHashWorker(id int, wg *sync.WaitGroup, jobs <-chan BlockRange, results chan<- LeafHashResult, pool *pgxpool.Pool) {
	defer wg.Done()

	for block := range jobs {
		whereClause, err := m.buildWhereClause(block)
		if err != nil {
			results <- LeafHashResult{BlockID: block.ID, Err: err}
			continue
		}

		computeSQL, err := helpers.RenderSQL(helpers.SQLTemplates.ComputeLeafHashes, map[string]string{
			"SchemaIdent": pgx.Identifier{m.Schema}.Sanitize(),
			"TableIdent":  pgx.Identifier{m.Table}.Sanitize(),
			"WhereClause": whereClause,
			"Columns":     strings.Join(m.Cols, ", "),
			"Key":         strings.Join(m.Key, ", "),
		})
		if err != nil {
			results <- LeafHashResult{BlockID: block.ID, Err: fmt.Errorf("failed to render compute leaf hashes sql: %w", err)}
			continue
		}

		var leafHash []byte
		err = pool.QueryRow(context.Background(), computeSQL).Scan(&leafHash)
		if err != nil {
			results <- LeafHashResult{BlockID: block.ID, Err: fmt.Errorf("failed to compute hash for block %d: %w", block.ID, err)}
			continue
		}
		results <- LeafHashResult{BlockID: block.ID, Hash: leafHash}
	}
}

func (m *MerkleTreeTask) buildWhereClause(block BlockRange) (string, error) {
	var whereConditions []string
	keyColumns := m.Key

	if m.SimplePrimaryKey {
		if block.Start[0] != nil {
			whereConditions = append(whereConditions, fmt.Sprintf("%s >= %v", pgx.Identifier{keyColumns[0]}.Sanitize(), block.Start[0]))
		}
		if block.End[0] != nil {
			whereConditions = append(whereConditions, fmt.Sprintf("%s <= %v", pgx.Identifier{keyColumns[0]}.Sanitize(), block.End[0]))
		}
	} else {
		pkCols := make([]string, len(keyColumns))
		for i, c := range keyColumns {
			pkCols[i] = pgx.Identifier{c}.Sanitize()
		}
		pkTuple := fmt.Sprintf("(%s)", strings.Join(pkCols, ", "))

		if block.Start != nil && len(block.Start) > 0 && block.Start[0] != nil {
			startVals := make([]string, len(block.Start))
			for i, v := range block.Start {
				startVals[i] = fmt.Sprintf("'%v'", v)
			}
			whereConditions = append(whereConditions, fmt.Sprintf("%s >= (%s)", pkTuple, strings.Join(startVals, ", ")))
		}
		if block.End != nil && len(block.End) > 0 && block.End[0] != nil {
			endVals := make([]string, len(block.End))
			for i, v := range block.End {
				endVals[i] = fmt.Sprintf("'%v'", v)
			}
			whereConditions = append(whereConditions, fmt.Sprintf("%s <= (%s)", pkTuple, strings.Join(endVals, ", ")))
		}
	}

	if len(whereConditions) == 0 {
		return "TRUE", nil
	}
	return strings.Join(whereConditions, " AND "), nil
}

func (m *MerkleTreeTask) insertBlockRanges(pool *pgxpool.Pool, ranges []BlockRange) error {
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	mtreeTableIdent := pgx.Identifier{mtreeTableName}

	tx, err := pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	batch := &pgx.Batch{}

	if m.SimplePrimaryKey {
		sql, err := helpers.RenderSQL(helpers.SQLTemplates.InsertBlockRanges, map[string]string{
			"MtreeTable": mtreeTableIdent.Sanitize(),
		})
		if err != nil {
			return err
		}
		for _, r := range ranges {
			batch.Queue(sql, r.ID, r.Start[0], r.End[0])
		}
	} else {
		startPlaceholders := make([]string, len(m.Key))
		endPlaceholders := make([]string, len(m.Key))
		for i := 0; i < len(m.Key); i++ {
			startPlaceholders[i] = fmt.Sprintf("$%d", i+2)
			endPlaceholders[i] = fmt.Sprintf("$%d", i+2+len(m.Key))
		}

		sql, err := helpers.RenderSQL(helpers.SQLTemplates.InsertCompositeBlockRanges, map[string]string{
			"MtreeTable":       mtreeTableIdent.Sanitize(),
			"StartTupleValues": strings.Join(startPlaceholders, ","),
			"EndTupleValues":   strings.Join(endPlaceholders, ","),
		})
		if err != nil {
			return err
		}

		for _, r := range ranges {
			args := []any{r.ID}
			args = append(args, r.Start...)
			args = append(args, r.End...)
			batch.Queue(sql, args...)
		}
	}

	br := tx.SendBatch(context.Background(), batch)
	// We will close the batch explicitly once we're done with it (either on error
	// or before committing). Avoid deferring the close to ensure it happens
	// before the transaction commit which requires a free connection.
	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			_ = br.Close()
			return fmt.Errorf("error executing batch insert for block ranges: %w", err)
		}
	}

	if err := br.Close(); err != nil {
		return fmt.Errorf("failed to close batch: %w", err)
	}

	return tx.Commit(context.Background())
}

func (m *MerkleTreeTask) createMtreeObjects(pool *pgxpool.Pool) error {
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	// TODO: Check if these objects exist before creating them to be more idempotent.
	if _, err := tx.Exec(context.Background(), helpers.SQLTemplates.CreateXORFunction.Root.String()); err != nil {
		return fmt.Errorf("failed to create xor function: %w", err)
	}

	if _, err := tx.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS ace_mtree_metadata (schema_name text, table_name text, total_rows bigint, block_size int, num_blocks int, is_composite boolean NOT NULL DEFAULT false, last_updated timestamptz, PRIMARY KEY (schema_name, table_name))"); err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}

	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", pgx.Identifier{mtreeTableName}.Sanitize())
	if _, err := tx.Exec(context.Background(), dropTableSQL); err != nil {
		return fmt.Errorf("failed to drop existing mtree table: %w", err)
	}

	if m.SimplePrimaryKey {
		pkeyType, err := m.getPkeyType(pool, m.Key[0])
		if err != nil {
			return err
		}
		createSQL, err := helpers.RenderSQL(helpers.SQLTemplates.CreateSimpleMtreeTable, map[string]string{
			"MtreeTable": pgx.Identifier{mtreeTableName}.Sanitize(),
			"PkeyType":   pkeyType,
		})
		if err != nil {
			return fmt.Errorf("failed to render create simple mtree table sql: %w", err)
		}
		if _, err := tx.Exec(context.Background(), createSQL); err != nil {
			return fmt.Errorf("failed to create simple mtree table: %w", err)
		}
	} else {
		keyTypeColumns := make([]string, len(m.Key))
		for i, col := range m.Key {
			colType, err := m.getPkeyType(pool, col)
			if err != nil {
				return err
			}
			keyTypeColumns[i] = fmt.Sprintf("%s %s", pgx.Identifier{col}.Sanitize(), colType)
		}

		compositeTypeName := fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)
		dropTypeSQL := fmt.Sprintf("DROP TYPE IF EXISTS %s CASCADE;", pgx.Identifier{compositeTypeName}.Sanitize())
		if _, err := tx.Exec(context.Background(), dropTypeSQL); err != nil {
			return fmt.Errorf("failed to drop composite type: %w", err)
		}

		createTypeSQL := fmt.Sprintf("CREATE TYPE %s AS (%s);", pgx.Identifier{compositeTypeName}.Sanitize(), strings.Join(keyTypeColumns, ", "))
		if _, err := tx.Exec(context.Background(), createTypeSQL); err != nil {
			return fmt.Errorf("failed to create composite type: %w", err)
		}

		createSQL, err := helpers.RenderSQL(helpers.SQLTemplates.CreateCompositeMtreeTable, map[string]string{
			"MtreeTable":  pgx.Identifier{mtreeTableName}.Sanitize(),
			"SchemaIdent": pgx.Identifier{m.Schema}.Sanitize(),
			"TableIdent":  pgx.Identifier{m.Table}.Sanitize(),
		})
		if err != nil {
			return fmt.Errorf("failed to render create composite mtree table sql: %w", err)
		}
		if _, err := tx.Exec(context.Background(), createSQL); err != nil {
			return fmt.Errorf("failed to create composite mtree table: %w", err)
		}
	}

	return tx.Commit(context.Background())
}

func (m *MerkleTreeTask) getPkeyType(pool *pgxpool.Pool, colName string) (string, error) {
	var pkeyType string
	err := pool.QueryRow(context.Background(), helpers.SQLTemplates.GetPkeyType.Root.String(), m.Schema, m.Table, colName).Scan(&pkeyType)
	if err != nil {
		return "", fmt.Errorf("failed to get pkey type for column %s: %w", colName, err)
	}
	return pkeyType, nil
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
