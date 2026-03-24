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

package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/pgedge/ace/pkg/types"
)

// DiffRowSink accumulates diff rows during table-diff / mtree-diff, spilling
// to a temporary NDJSON file on disk when the in-memory row count exceeds a
// configurable threshold (default 10 000 rows).
//
// Architecture overview:
//
//   - During diffing, concurrent goroutines discover mismatched rows and call
//     Append() under the task's diffMutex. Rows are buffered in memory; when
//     the buffer reaches the threshold the entire buffer is flushed to a temp
//     NDJSON (newline-delimited JSON) file and the buffer is reset.
//
//   - One sink exists per (nodePair, nodeName) combination. For a typical
//     2-node cluster that is 2 sinks. Memory is bounded to roughly
//     threshold × row_size × number_of_sinks regardless of total diff count.
//
//   - At report time, WriteDiffReport calls SortedIterate on each sink
//     sequentially. This loads one sink's rows back into memory, sorts by
//     primary key, streams the sorted rows to the JSON output file, then
//     releases them before moving to the next sink. Peak memory during the
//     write phase is therefore one sink's worth of rows.
//
//   - Downstream consumers (repair, rerun) read from the written JSON file,
//     not from the sinks, so they are unaffected by this change.
//
// DiffRowSink is NOT safe for concurrent use — callers must synchronize
// externally (e.g. via diffMutex).
type DiffRowSink struct {
	rows      []types.OrderedMap
	count     int // total rows (memory + spilled)
	threshold int // flush to disk when len(rows) reaches this

	spillFile *os.File
	spilled   int // rows written to spillFile
}

const defaultSpillThreshold = 10_000

// NewDiffRowSink creates a sink that keeps up to threshold rows in memory.
// If threshold <= 0, defaultSpillThreshold is used.
func NewDiffRowSink(threshold int) *DiffRowSink {
	if threshold <= 0 {
		threshold = defaultSpillThreshold
	}
	return &DiffRowSink{
		threshold: threshold,
	}
}

// Append adds a row to the sink. If the in-memory buffer is full, it is
// flushed to a temporary spill file on disk.
func (s *DiffRowSink) Append(row types.OrderedMap) error {
	s.rows = append(s.rows, row)
	s.count++

	if len(s.rows) >= s.threshold {
		return s.flush()
	}
	return nil
}

// Len returns the total number of rows (in-memory + spilled).
func (s *DiffRowSink) Len() int {
	return s.count
}

// Iterate calls fn for every row in insertion order: spilled rows first,
// then in-memory rows.
func (s *DiffRowSink) Iterate(fn func(types.OrderedMap) error) error {
	if err := s.iterateSpillFile(fn); err != nil {
		return err
	}
	for _, row := range s.rows {
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

// SortedIterate loads all rows (spill + memory) into a temporary slice,
// sorts by primary key, calls fn for each row, then releases the slice.
func (s *DiffRowSink) SortedIterate(pk []string, fn func(types.OrderedMap) error) error {
	all := make([]types.OrderedMap, 0, s.count)

	if err := s.iterateSpillFile(func(row types.OrderedMap) error {
		all = append(all, row)
		return nil
	}); err != nil {
		return err
	}
	all = append(all, s.rows...)

	sort.SliceStable(all, func(i, j int) bool {
		pkI := getPKValues(all[i], pk)
		pkJ := getPKValues(all[j], pk)
		return comparePKValues(pkI, pkJ) < 0
	})

	for _, row := range all {
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

// Close removes the spill file (if any) and releases in-memory rows.
func (s *DiffRowSink) Close() {
	s.rows = nil
	if s.spillFile != nil {
		name := s.spillFile.Name()
		s.spillFile.Close()
		os.Remove(name)
		s.spillFile = nil
	}
}

// flush writes the in-memory buffer to the spill file and resets it.
func (s *DiffRowSink) flush() error {
	if s.spillFile == nil {
		f, err := os.CreateTemp("", "ace-diff-spill-*.ndjson")
		if err != nil {
			return fmt.Errorf("failed to create diff spill file: %w", err)
		}
		s.spillFile = f
	}

	enc := json.NewEncoder(s.spillFile)
	for _, row := range s.rows {
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("failed to write to diff spill file: %w", err)
		}
	}
	s.spilled += len(s.rows)
	s.rows = s.rows[:0]
	return nil
}

// iterateSpillFile reads the spill file from the beginning and calls fn
// for each row. If no spill file exists, this is a no-op.
func (s *DiffRowSink) iterateSpillFile(fn func(types.OrderedMap) error) error {
	if s.spillFile == nil || s.spilled == 0 {
		return nil
	}

	if _, err := s.spillFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek diff spill file: %w", err)
	}

	scanner := bufio.NewScanner(s.spillFile)
	scanner.Buffer(make([]byte, 0, 256*1024), 10*1024*1024)
	for scanner.Scan() {
		var row types.OrderedMap
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return fmt.Errorf("failed to decode spill row: %w", err)
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// DiffSinks maps pairKey -> nodeName -> *DiffRowSink. It is held on the task
// struct (TableDiffTask.diffSinks, MerkleTreeTask.diffSinks) during a diff run
// and passed to WriteDiffReport for streaming the sorted JSON output.
type DiffSinks map[string]map[string]*DiffRowSink

// GetSink returns the sink for the given pair and node, creating it if needed.
func (ds DiffSinks) GetSink(pairKey, nodeName string, threshold int) *DiffRowSink {
	nodeMap, ok := ds[pairKey]
	if !ok {
		nodeMap = make(map[string]*DiffRowSink)
		ds[pairKey] = nodeMap
	}
	sink, ok := nodeMap[nodeName]
	if !ok {
		sink = NewDiffRowSink(threshold)
		nodeMap[nodeName] = sink
	}
	return sink
}

// getSink returns the sink for the given pair and node, or nil if not present.
func (ds DiffSinks) getSink(pairKey, nodeName string) *DiffRowSink {
	if ds == nil {
		return nil
	}
	if nodeMap, ok := ds[pairKey]; ok {
		return nodeMap[nodeName]
	}
	return nil
}

// CloseAll closes and removes all sinks.
func (ds DiffSinks) CloseAll() {
	for _, nodeMap := range ds {
		for _, sink := range nodeMap {
			sink.Close()
		}
	}
}
