package common

import (
	"os"
	"testing"

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

func makeTestRow(id int) types.OrderedMap {
	return types.OrderedMap{
		{Key: "id", Value: float64(id)},
		{Key: "name", Value: "row"},
	}
}

func TestDiffRowSink_NoSpill(t *testing.T) {
	s := NewDiffRowSink(100)
	defer s.Close()

	for i := 0; i < 50; i++ {
		require.NoError(t, s.Append(makeTestRow(i)))
	}

	require.Equal(t, 50, s.Len())
	require.Nil(t, s.spillFile, "should not spill when below threshold")

	var collected []types.OrderedMap
	require.NoError(t, s.Iterate(func(row types.OrderedMap) error {
		collected = append(collected, row)
		return nil
	}))
	require.Len(t, collected, 50)
	// Verify insertion order.
	for i, row := range collected {
		require.Equal(t, float64(i), row[0].Value)
	}
}

func TestDiffRowSink_Spill(t *testing.T) {
	s := NewDiffRowSink(10) // very low threshold
	defer s.Close()

	for i := 0; i < 35; i++ {
		require.NoError(t, s.Append(makeTestRow(i)))
	}

	require.Equal(t, 35, s.Len())
	require.NotNil(t, s.spillFile, "should have spilled to disk")
	require.Equal(t, 30, s.spilled, "3 full batches of 10 should be spilled")
	require.Len(t, s.rows, 5, "5 rows should remain in memory")

	// Iterate should return all 35 rows in insertion order.
	var collected []types.OrderedMap
	require.NoError(t, s.Iterate(func(row types.OrderedMap) error {
		collected = append(collected, row)
		return nil
	}))
	require.Len(t, collected, 35)
	for i, row := range collected {
		require.Equal(t, float64(i), row[0].Value)
	}
}

func TestDiffRowSink_SortedIterate(t *testing.T) {
	s := NewDiffRowSink(5)
	defer s.Close()

	// Insert rows in reverse order to verify sorting.
	for i := 19; i >= 0; i-- {
		require.NoError(t, s.Append(makeTestRow(i)))
	}

	require.Equal(t, 20, s.Len())
	require.NotNil(t, s.spillFile)

	var ids []float64
	require.NoError(t, s.SortedIterate([]string{"id"}, func(row types.OrderedMap) error {
		ids = append(ids, row[0].Value.(float64))
		return nil
	}))

	require.Len(t, ids, 20)
	for i, id := range ids {
		require.Equal(t, float64(i), id, "row %d should have id %d", i, i)
	}
}

func TestDiffRowSink_Close_RemovesFile(t *testing.T) {
	s := NewDiffRowSink(5)

	for i := 0; i < 10; i++ {
		require.NoError(t, s.Append(makeTestRow(i)))
	}
	require.NotNil(t, s.spillFile)
	path := s.spillFile.Name()

	s.Close()

	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "spill file should be removed after Close")
}

func TestDiffSinks_GetSink(t *testing.T) {
	ds := make(DiffSinks)

	s1 := ds.GetSink("n1/n2", "n1", 100)
	s2 := ds.GetSink("n1/n2", "n2", 100)
	s3 := ds.GetSink("n1/n2", "n1", 100) // same as s1

	require.Same(t, s1, s3, "should return the same sink for same pair+node")
	require.NotSame(t, s1, s2, "different nodes should get different sinks")
}

func TestDiffSinks_CloseAll(t *testing.T) {
	ds := make(DiffSinks)
	s1 := ds.GetSink("n1/n2", "n1", 5)
	s2 := ds.GetSink("n1/n2", "n2", 5)

	// Force spill on both.
	for i := 0; i < 10; i++ {
		require.NoError(t, s1.Append(makeTestRow(i)))
		require.NoError(t, s2.Append(makeTestRow(i)))
	}

	path1 := s1.spillFile.Name()
	path2 := s2.spillFile.Name()

	ds.CloseAll()

	_, err := os.Stat(path1)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(path2)
	require.True(t, os.IsNotExist(err))
}
