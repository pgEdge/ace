package queries

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func leaves(names ...string) []RelationInfo {
	out := make([]RelationInfo, 0, len(names))
	for _, n := range names {
		out = append(out, RelationInfo{Schema: "s", Name: n, RelKind: "r"})
	}
	return out
}

func TestPlainTableSource_FromClause(t *testing.T) {
	src := PlainTableSource("public", "users")
	assert.False(t, src.IsUnion())
	assert.Equal(t, `"public"."users"`, src.FromClause(""))
	assert.Equal(t, `"public"."users" AS _tbl_`, src.FromClause("_tbl_"))
	assert.Equal(t, `"public"."users" TABLESAMPLE BERNOULLI(10)`, src.FromClauseSampled("", "TABLESAMPLE BERNOULLI(10)"))
}

func TestUnionTableSource_SingleBranch(t *testing.T) {
	src, err := UnionTableSource("s", "parent", leaves("parent"), []string{"id", "val"}, 10)
	require.NoError(t, err)
	assert.True(t, src.IsUnion())
	assert.Equal(t,
		`(SELECT "id", "val", xmin, tableoid FROM ONLY "s"."parent") AS _src_`,
		src.FromClause(""))
}

func TestUnionTableSource_TwoBranchesUsesOnlyAndAlias(t *testing.T) {
	src, err := UnionTableSource("s", "parent", leaves("parent", "child_heap"), []string{"id", "val"}, 10)
	require.NoError(t, err)
	got := src.FromClause("t")
	assert.Equal(t,
		`((SELECT "id", "val", xmin, tableoid FROM ONLY "s"."parent" UNION ALL SELECT "id", "val", xmin, tableoid FROM ONLY "s"."child_heap")) AS t`,
		got)
}

func TestUnionTableSource_Sampled(t *testing.T) {
	src, err := UnionTableSource("s", "parent", leaves("parent", "child_heap"), []string{"id"}, 10)
	require.NoError(t, err)
	got := src.FromClauseSampled("", "TABLESAMPLE BERNOULLI(5.5)")
	assert.Equal(t, 2, strings.Count(got, `TABLESAMPLE BERNOULLI(5.5)`))
	assert.Contains(t, got, `FROM ONLY "s"."parent" TABLESAMPLE BERNOULLI(5.5)`)
	assert.Contains(t, got, `FROM ONLY "s"."child_heap" TABLESAMPLE BERNOULLI(5.5)`)
}

func TestUnionTableSource_BalancedTreeDepth(t *testing.T) {
	// 8 branches. A left-deep chain would open 8 parens before the first
	// SELECT; a balanced tree opens 4 (3 levels plus the outer wrapper).
	src, err := UnionTableSource("s", "p", leaves("a", "b", "c", "d", "e", "f", "g", "h"), []string{"id"}, 100)
	require.NoError(t, err)
	got := src.FromClause("")
	assert.Equal(t, 7, strings.Count(got, "UNION ALL"))
	assert.True(t, strings.HasPrefix(got, "((((SELECT"), "expected 4 opening parens, got %s", got)
	assert.NotContains(t, got, "(((((SELECT")
}

func TestUnionTableSource_BranchCap(t *testing.T) {
	_, err := UnionTableSource("s", "p", leaves("a", "b", "c"), []string{"id"}, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "3 heap relations")
	assert.Contains(t, err.Error(), "max_inheritance_branches")
}

func TestUnionTableSource_RequiresColumns(t *testing.T) {
	_, err := UnionTableSource("s", "p", leaves("a"), nil, 10)
	require.Error(t, err)
}

func TestUnionTableSource_RequiresLeaves(t *testing.T) {
	_, err := UnionTableSource("s", "p", nil, []string{"id"}, 10)
	require.Error(t, err)
}
