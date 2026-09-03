package repair

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func placementTask() *TableRepairTask {
	t := NewTableRepairTask()
	t.Schema = "s"
	t.Table = "parent"
	t.Key = []string{"id"}
	t.heapLeaves = map[string]map[string]bool{
		"n1": {"s.parent": true, "s.child_heap": true},
		"n2": {"s.parent": true, "s.child_heap": true},
	}
	t.rowPlacement = map[string]map[string]string{
		"n1": {"1": "s.parent", "12": "s.child_heap", "50": "s.child_only_on_n1"},
		"n2": {"1": "s.parent"},
	}
	return t
}

func TestTargetRelation_PlainTableIsParent(t *testing.T) {
	task := NewTableRepairTask()
	task.Schema, task.Table = "s", "t"
	assert.Equal(t, pgx.Identifier{"s", "t"}, task.targetRelation("n1", "1"))
}

func TestTargetRelation_RowPresentOnTargetUsesItsOwnRelation(t *testing.T) {
	task := placementTask()
	assert.Equal(t, pgx.Identifier{"s", "parent"}, task.targetRelation("n2", "1"))
	assert.Equal(t, pgx.Identifier{"s", "child_heap"}, task.targetRelation("n1", "12"))
}

func TestTargetRelation_MissingRowUsesSameNamedHeapOnTarget(t *testing.T) {
	task := placementTask()
	// id 12 is missing on n2; on n1 it lives in child_heap, which n2 also has.
	assert.Equal(t, pgx.Identifier{"s", "child_heap"}, task.targetRelation("n2", "12"))
}

func TestTargetRelation_MissingRowFallsBackToParent(t *testing.T) {
	task := placementTask()
	// id 50 lives in a child that n2 does not have.
	assert.Equal(t, pgx.Identifier{"s", "parent"}, task.targetRelation("n2", "50"))
	// unknown everywhere
	assert.Equal(t, pgx.Identifier{"s", "parent"}, task.targetRelation("n2", "999"))
}

func TestGroupByTargetRelation_NodeWithoutForeignRelationsKeepsParentWithoutOnly(t *testing.T) {
	task := placementTask()
	delete(task.heapLeaves, "n3")
	groups := task.groupByTargetRelation("n3", map[string]map[string]any{"12": {"id": 12}})
	require.Len(t, groups, 1)
	g := groups[`"s"."parent"`]
	assert.False(t, g.Only)
	assert.Len(t, g.Rows, 1)
}

func TestGroupByTargetRelation(t *testing.T) {
	task := placementTask()
	rows := map[string]map[string]any{
		"1":  {"id": 1},
		"12": {"id": 12},
		"50": {"id": 50},
	}
	groups := task.groupByTargetRelation("n2", rows)
	require.Len(t, groups, 2)
	assert.Len(t, groups[`"s"."parent"`].Rows, 2)
	assert.Len(t, groups[`"s"."child_heap"`].Rows, 1)
	assert.True(t, groups[`"s"."parent"`].Only)
}

func TestBuildRowPlacement_ReadsStorageRelationMetadata(t *testing.T) {
	diffs := types.DiffOutput{NodeDiffs: map[string]types.DiffByNodePair{
		"n1/n2": {Rows: map[string][]types.OrderedMap{
			"n1": {
				{{Key: "id", Value: 12}, {Key: "val", Value: "x"}, {Key: "_spock_metadata_", Value: map[string]any{"storage_relation": "s.child_heap"}}},
			},
			"n2": {
				{{Key: "id", Value: 1}, {Key: "val", Value: "y"}, {Key: "_spock_metadata_", Value: map[string]any{"storage_relation": "s.parent"}}},
				{{Key: "id", Value: 2}, {Key: "val", Value: "z"}}, // no metadata: ignored
			},
		}},
	}}
	got, err := buildRowPlacement(diffs, []string{"id"})
	require.NoError(t, err)
	assert.Equal(t, "s.child_heap", got["n1"]["12"])
	assert.Equal(t, "s.parent", got["n2"]["1"])
	_, has := got["n2"]["2"]
	assert.False(t, has)
}
