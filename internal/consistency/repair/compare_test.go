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

package repair

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/pgedge/ace/internal/consistency/repair/plan/parser"
	"github.com/stretchr/testify/require"
)

// goCompare decides comparisons in Go, for tests without a server: a
// timestamp with time zone is parsed, an integer type is read as an
// integer, and any other type is compared as text.
func goCompare(cmps []textComparison) ([]bool, error) {
	out := make([]bool, len(cmps))
	for i, c := range cmps {
		var order int
		switch {
		case c.leftType == "timestamp with time zone":
			l, err := time.Parse(time.RFC3339Nano, c.left)
			if err != nil {
				return nil, err
			}
			r, err := time.Parse(time.RFC3339Nano, c.right)
			if err != nil {
				return nil, err
			}
			order = l.Compare(r)
		case strings.Contains(c.leftType, "int"):
			l, ok := new(big.Int).SetString(c.left, 10)
			if !ok {
				return nil, fmt.Errorf("invalid input syntax for type %s: %q", c.leftType, c.left)
			}
			r, ok := new(big.Int).SetString(c.right, 10)
			if !ok {
				return nil, fmt.Errorf("invalid input syntax for type %s: %q", c.rightType, c.right)
			}
			order = l.Cmp(r)
		default:
			order = strings.Compare(c.left, c.right)
		}
		switch c.op {
		case "=":
			out[i] = order == 0
		case "<>":
			out[i] = order != 0
		case "<":
			out[i] = order < 0
		case "<=":
			out[i] = order <= 0
		case ">":
			out[i] = order > 0
		case ">=":
			out[i] = order >= 0
		default:
			return nil, fmt.Errorf("unknown operator %s", c.op)
		}
	}
	return out, nil
}

// The comparer sends a comparison once, reads a literal with the type of
// the column on the other side, and refuses a comparison that it did not
// compute or whose column it does not know.
func TestComparer(t *testing.T) {
	var runs [][]textComparison
	cmp := newComparer(map[string]string{"id": "bigint"}, func(cmps []textComparison) ([]bool, error) {
		runs = append(runs, append([]textComparison(nil), cmps...))
		return goCompare(cmps)
	})
	lt := parser.Comparison{Op: "<", Left: "9", LeftColumn: "id", Right: "10"}
	require.NoError(t, cmp.add(lt))
	require.NoError(t, cmp.add(lt))
	require.NoError(t, cmp.flush())
	require.Len(t, runs, 1)
	require.Equal(t, []textComparison{{op: "<", left: "9", right: "10", leftType: "bigint", rightType: "bigint"}}, runs[0])

	ok, err := cmp.compare(lt)
	require.NoError(t, err)
	require.True(t, ok, "9 < 10 as integers")

	// A flush with nothing queued does not run a query.
	require.NoError(t, cmp.flush())
	require.Len(t, runs, 1)

	_, err = cmp.compare(parser.Comparison{Op: ">", Left: "9", LeftColumn: "id", Right: "10"})
	require.ErrorContains(t, err, "not computed")
	require.ErrorContains(t, cmp.add(parser.Comparison{Op: "=", Left: "a", LeftColumn: "nope", Right: "b"}), "not a column")

	// commit_ts of the spock metadata is a timestamp.
	ts, err := cmp.typed(parser.Comparison{Op: "<", Left: "x", LeftColumn: "commit_ts", Right: "y", RightColumn: "commit_ts"})
	require.NoError(t, err)
	require.Equal(t, "timestamp with time zone", ts.leftType)
}

// A pair whose nodes repair could not connect to still gets a comparer: a
// plan without comparisons runs, and a comparison fails with a clear error.
// The comparer takes the second node of the pair when only it is connected.
func TestNewComparerWithoutNode(t *testing.T) {
	task := NewTableRepairTask()
	task.HostMap = map[string]string{"h1:5432": "n1", "h2:5432": "n2"}
	task.castTypes = map[string]map[string]string{"h2:5432": {"id": "bigint"}}
	p := &diffPair{name: "n1/n2", node1: "n1", node2: "n2"}
	c := parser.Comparison{Op: "<", Left: "1", LeftColumn: "id", Right: "2"}

	cmp := task.newComparer(p)
	require.NoError(t, cmp.flush())
	require.ErrorContains(t, cmp.add(c), "no node of the pair is connected")

	var used string
	task.compareValues = func(node string, cmps []textComparison) ([]bool, error) {
		used = node
		return goCompare(cmps)
	}
	cmp = task.newComparer(p)
	require.NoError(t, cmp.add(c))
	require.NoError(t, cmp.flush())
	require.Equal(t, "n2", used, "n1 has no types, so n2 compares")
}
