package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// makeRow builds a single OrderedMap row with an id and nCols text columns,
// each ~valLen bytes, simulating JSONB-heavy table data.
func makeRow(id int, nCols int, valLen int) types.OrderedMap {
	row := types.OrderedMap{{Key: "id", Value: id}}
	payload := make([]byte, valLen)
	for i := range payload {
		payload[i] = 'x'
	}
	s := string(payload)
	for c := 0; c < nCols; c++ {
		row = append(row, types.KVPair{Key: fmt.Sprintf("col_%d", c), Value: s})
	}
	return row
}

// buildLargeDiff creates a DiffOutput with the given number of node-pair
// blocks, rows per block, columns per row, and bytes per column value.
func buildLargeDiff(nPairs, rowsPerNode, cols, valLen int) types.DiffOutput {
	diffs := make(map[string]types.DiffByNodePair, nPairs)
	for p := 0; p < nPairs; p++ {
		n1 := fmt.Sprintf("n%d", p*2+1)
		n2 := fmt.Sprintf("n%d", p*2+2)
		key := n1 + "/" + n2

		rows1 := make([]types.OrderedMap, rowsPerNode)
		rows2 := make([]types.OrderedMap, rowsPerNode)
		for i := 0; i < rowsPerNode; i++ {
			rows1[i] = makeRow(i, cols, valLen)
			rows2[i] = makeRow(i, cols, valLen)
		}
		diffs[key] = types.DiffByNodePair{
			Rows: map[string][]types.OrderedMap{
				n1: rows1,
				n2: rows2,
			},
		}
	}

	return types.DiffOutput{
		NodeDiffs: diffs,
		Summary: types.DiffSummary{
			Schema:     "public",
			Table:      "large_test",
			Nodes:      []string{"n1", "n2"},
			BlockSize:  1000,
			PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{
				"n1/n2": rowsPerNode,
			},
		},
	}
}

func currentHeapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

// TestStreamDiffJSON_LargeOutput verifies that streamDiffJSON produces valid
// JSON and that peak heap growth stays well below the serialised JSON size.
// It also measures the old json.NewEncoder.Encode approach for comparison.
func TestStreamDiffJSON_LargeOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large-memory streaming test in short mode")
	}

	const (
		nPairs      = 10
		rowsPerNode = 5000
		cols        = 5
		valLen      = 200
	)

	dir := t.TempDir()

	// --- old approach: json.NewEncoder.Encode (buffers everything) ---
	diff := buildLargeDiff(nPairs, rowsPerNode, cols, valLen)
	baselineOld := currentHeapMB()
	t.Logf("[old] baseline heap: %.1f MB", baselineOld)

	oldPath := filepath.Join(dir, "old.json")
	startOld := time.Now()
	f, err := os.Create(oldPath)
	require.NoError(t, err)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(diff)
	require.NoError(t, err)
	f.Close()
	elapsedOld := time.Since(startOld)

	peakOld := currentHeapMB()
	growthOld := peakOld - baselineOld
	fiOld, _ := os.Stat(oldPath)
	t.Logf("[old] heap after Encode: %.1f MB (growth: %.1f MB), time: %v", peakOld, growthOld, elapsedOld)
	t.Logf("[old] output file size: %.1f MB", float64(fiOld.Size())/(1024*1024))

	// Free the old output so it doesn't skew the streaming measurement.
	diff = types.DiffOutput{}
	runtime.GC()

	// --- new approach: streamDiffJSON (row-at-a-time, in-memory Rows) ---
	diff = buildLargeDiff(nPairs, rowsPerNode, cols, valLen)
	baselineNew := currentHeapMB()
	t.Logf("[new] baseline heap: %.1f MB", baselineNew)

	newPath := filepath.Join(dir, "new.json")
	startNew := time.Now()
	err = streamDiffJSON(newPath, diff, nil)
	require.NoError(t, err)
	elapsedNew := time.Since(startNew)

	peakNew := currentHeapMB()
	growthNew := peakNew - baselineNew
	fiNew, _ := os.Stat(newPath)
	t.Logf("[new] heap after streamDiffJSON: %.1f MB (growth: %.1f MB), time: %v", peakNew, growthNew, elapsedNew)
	t.Logf("[new] output file size: %.1f MB", float64(fiNew.Size())/(1024*1024))

	// Free before sink test.
	diff = types.DiffOutput{}
	runtime.GC()

	// --- sink approach: accumulate via DiffRowSinks with small spill threshold ---
	sinkThreshold := 500 // low threshold to exercise spill path
	sinks := make(DiffSinks)
	sinkDiff := types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Schema:     "public",
			Table:      "large_test",
			PrimaryKey: []string{"id"},
			DiffRowsCount: make(map[string]int),
		},
	}

	// Simulate what table_diff does: append rows one at a time into sinks.
	// Time includes both accumulation (with spilling) and writing.
	startSinkAccum := time.Now()
	for p := 0; p < nPairs; p++ {
		n1 := fmt.Sprintf("n%d", p*2+1)
		n2 := fmt.Sprintf("n%d", p*2+2)
		pairKey := n1 + "/" + n2
		sinkDiff.NodeDiffs[pairKey] = types.DiffByNodePair{
			Rows: make(map[string][]types.OrderedMap),
		}
		sink1 := sinks.GetSink(pairKey, n1, sinkThreshold)
		sink2 := sinks.GetSink(pairKey, n2, sinkThreshold)
		for i := 0; i < rowsPerNode; i++ {
			row := makeRow(i, cols, valLen)
			require.NoError(t, sink1.Append(row))
			require.NoError(t, sink2.Append(row))
		}
	}
	elapsedSinkAccum := time.Since(startSinkAccum)

	baselineSink := currentHeapMB()
	t.Logf("[sink] baseline heap after accumulation: %.1f MB, accumulation time: %v", baselineSink, elapsedSinkAccum)

	sinkPath := filepath.Join(dir, "sink.json")
	startSinkWrite := time.Now()
	err = streamDiffJSON(sinkPath, sinkDiff, sinks)
	require.NoError(t, err)
	elapsedSinkWrite := time.Since(startSinkWrite)
	sinks.CloseAll()

	peakSink := currentHeapMB()
	growthSink := peakSink - baselineSink
	fiSink, _ := os.Stat(sinkPath)
	t.Logf("[sink] heap after streamDiffJSON: %.1f MB (growth: %.1f MB), write time: %v", peakSink, growthSink, elapsedSinkWrite)
	t.Logf("[sink] output file size: %.1f MB", float64(fiSink.Size())/(1024*1024))

	t.Logf("--- summary ---")
	t.Logf("old Encode     heap growth: %+6.1f MB   time: %v", growthOld, elapsedOld)
	t.Logf("new stream     heap growth: %+6.1f MB   time: %v", growthNew, elapsedNew)
	t.Logf("sink accum+write           %+6.1f MB   time: %v (accum: %v, write: %v)",
		growthSink, elapsedSinkAccum+elapsedSinkWrite, elapsedSinkAccum, elapsedSinkWrite)

	// The streaming approach should use significantly less additional memory.
	require.Less(t, growthNew, 50.0,
		"streaming writer heap growth should be well under 50 MB; got %.1f MB", growthNew)

	// Validate the sink output is legal JSON and round-trips correctly.
	data, err := os.ReadFile(sinkPath)
	require.NoError(t, err)

	var parsed types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &parsed), "output must be valid JSON")

	require.Len(t, parsed.NodeDiffs, nPairs)
	for _, pair := range parsed.NodeDiffs {
		for _, rows := range pair.Rows {
			require.Len(t, rows, rowsPerNode)
		}
	}
	require.Equal(t, "public", parsed.Summary.Schema)
	require.Equal(t, "large_test", parsed.Summary.Table)
}

// TestStreamDiffJSON_Empty verifies empty diffs produce valid JSON.
func TestStreamDiffJSON_Empty(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "empty.json")

	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{},
		Summary: types.DiffSummary{
			Schema: "public",
			Table:  "empty_test",
		},
	}

	require.NoError(t, streamDiffJSON(outPath, diff, nil))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	var parsed types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.Empty(t, parsed.NodeDiffs)
	require.Equal(t, "public", parsed.Summary.Schema)
}

// TestStreamDiffJSON_WithSinks verifies the full pipeline: rows accumulated
// in DiffRowSinks (with a small spill threshold) produce valid, sorted JSON.
func TestStreamDiffJSON_WithSinks(t *testing.T) {
	const (
		spillThreshold = 3 // very small to force spilling
		totalRows      = 20
	)

	// Build sinks with rows in reverse PK order to verify sorting.
	sinks := make(DiffSinks)
	sink1 := sinks.GetSink("n1/n2", "n1", spillThreshold)
	sink2 := sinks.GetSink("n1/n2", "n2", spillThreshold)
	for i := totalRows - 1; i >= 0; i-- {
		row := types.OrderedMap{
			{Key: "id", Value: float64(i)},
			{Key: "val", Value: fmt.Sprintf("data_%d", i)},
		}
		require.NoError(t, sink1.Append(row))
		require.NoError(t, sink2.Append(row))
	}
	defer sinks.CloseAll()

	require.NotNil(t, sink1.spillFile, "sink1 should have spilled")
	require.NotNil(t, sink2.spillFile, "sink2 should have spilled")

	// DiffResult has the pair entry (for NodeDiffs key iteration) but empty Rows.
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{
			"n1/n2": {Rows: make(map[string][]types.OrderedMap)},
		},
		Summary: types.DiffSummary{
			Schema:     "public",
			Table:      "sink_test",
			PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": totalRows},
		},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "sink_test.json")
	require.NoError(t, streamDiffJSON(outPath, diff, sinks))

	// Parse and validate.
	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	var parsed types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &parsed), "output must be valid JSON")

	require.Len(t, parsed.NodeDiffs, 1)
	pair := parsed.NodeDiffs["n1/n2"]
	require.Len(t, pair.Rows["n1"], totalRows)
	require.Len(t, pair.Rows["n2"], totalRows)

	// Verify rows are sorted by PK ascending (not reverse insertion order).
	for _, nodeKey := range []string{"n1", "n2"} {
		rows := pair.Rows[nodeKey]
		for i, row := range rows {
			require.Equal(t, float64(i), row[0].Value,
				"node %s row %d should have id %d (sorted)", nodeKey, i, i)
		}
	}
}
