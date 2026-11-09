package mtree

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/keydiff"
	"github.com/pgedge/ace/pkg/types"
)

func uuidBytes(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// mtreeTaskWithDiffState returns a task wired just enough to exercise the diff
// accumulator (appendDiffs) without a database.
func mtreeTaskWithDiffState(maxDiffRows int64) *MerkleTreeTask {
	m := &MerkleTreeTask{MaxDiffRows: maxDiffRows}
	m.Key = []string{"id"}
	m.Cols = []string{"id", "val"}
	m.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary:   types.DiffSummary{DiffRowsCount: make(map[string]int)},
	}
	m.diffRowCounts = make(map[string]int64)
	return m
}

// node1OnlyRows builds n rows with unique primary keys, all absent from the peer.
func node1OnlyRows(n int) []types.OrderedMap {
	rows := make([]types.OrderedMap, n)
	for i := range rows {
		rows[i] = types.OrderedMap{{Key: "id", Value: fmt.Sprintf("%d", i)}, {Key: "val", Value: "x"}}
	}
	return rows
}

// A diverged table stops collecting at max_diff_rows and marks the report truncated.
func TestMtreeDiffEnforcesMaxDiffRows(t *testing.T) {
	const cap = 5
	m := mtreeTaskWithDiffState(cap)
	work := CompareRangesWorkItem{
		Node1: map[string]any{"Name": "n1"},
		Node2: map[string]any{"Name": "n2"},
	}

	if err := m.appendDiffs("n1/n2", work, keydiff.Result{Only1: node1OnlyRows(20)}); err != nil {
		t.Fatalf("appendDiffs returned error: %v", err)
	}

	if got := len(m.DiffResult.NodeDiffs["n1/n2"].Rows["n1"]); got != cap {
		t.Errorf("collected %d rows for n1, want the cap of %d", got, cap)
	}
	if !m.DiffResult.Summary.DiffRowLimitReached {
		t.Errorf("expected DiffRowLimitReached=true after exceeding the cap")
	}
}

// A pair with exactly max_diff_rows diffs collects them all and IS marked
// truncated, matching table-diff: reaching the cap is a report-size bound, so we
// warn that additional differences may exist even at the exact boundary.
func TestMtreeDiffExactCapMarksTruncated(t *testing.T) {
	const cap = 5
	m := mtreeTaskWithDiffState(cap)
	work := CompareRangesWorkItem{
		Node1: map[string]any{"Name": "n1"},
		Node2: map[string]any{"Name": "n2"},
	}

	if err := m.appendDiffs("n1/n2", work, keydiff.Result{Only1: node1OnlyRows(cap)}); err != nil {
		t.Fatalf("appendDiffs returned error: %v", err)
	}

	if got := len(m.DiffResult.NodeDiffs["n1/n2"].Rows["n1"]); got != cap {
		t.Errorf("collected %d rows for n1, want all %d", got, cap)
	}
	if !m.DiffResult.Summary.DiffRowLimitReached {
		t.Errorf("expected DiffRowLimitReached=true when diffs exactly equal the cap")
	}
}

// With no cap configured, every differing row is collected and nothing is flagged truncated.
func TestMtreeDiffNoLimitCollectsAll(t *testing.T) {
	m := mtreeTaskWithDiffState(0)
	work := CompareRangesWorkItem{
		Node1: map[string]any{"Name": "n1"},
		Node2: map[string]any{"Name": "n2"},
	}

	if err := m.appendDiffs("n1/n2", work, keydiff.Result{Only1: node1OnlyRows(20)}); err != nil {
		t.Fatalf("appendDiffs returned error: %v", err)
	}

	if got := len(m.DiffResult.NodeDiffs["n1/n2"].Rows["n1"]); got != 20 {
		t.Errorf("collected %d rows for n1, want all 20", got)
	}
	if m.DiffResult.Summary.DiffRowLimitReached {
		t.Errorf("did not expect DiffRowLimitReached with no cap configured")
	}
}

// The ranges come in key order, the open-start range first, and a range
// that starts where the one before it ends leaves out its lower bound, so
// that a row on a shared bound is compared once.
func TestPkeyBatchesOrderAndSharedBounds(t *testing.T) {
	m := &MerkleTreeTask{}
	m.Key = []string{"id"}
	m.SimplePrimaryKey = true
	b := func(v int64) []any { return []any{v} }
	ranges := []types.LeafRange{
		{RangeStart: nil, RangeEnd: b(10)},
		{RangeStart: b(10), RangeEnd: b(20)},
		{RangeStart: b(30), RangeEnd: b(40)},
		{RangeStart: b(40), RangeEnd: nil},
	}
	got := m.pkeyBatchesFromRanges(ranges)
	type want struct {
		start, end any
		exclusive  bool
	}
	first := func(v []any) any {
		if len(v) == 0 {
			return nil
		}
		return v[0]
	}
	wants := []want{
		{nil, int64(10), false},
		{int64(10), int64(20), true},
		{int64(30), int64(40), false},
		{int64(40), nil, true},
	}
	if len(got) != len(wants) {
		t.Fatalf("got %d ranges, want %d: %+v", len(got), len(wants), got)
	}
	for i, w := range wants {
		if first(got[i].start) != w.start || first(got[i].end) != w.end || got[i].startExclusive != w.exclusive {
			t.Errorf("range %d = [%v, %v] exclusive=%v, want [%v, %v] exclusive=%v",
				i, first(got[i].start), first(got[i].end), got[i].startExclusive, w.start, w.end, w.exclusive)
		}
	}
}

// A negative max_diff_rows is rejected before any work runs.
func TestMtreeDiffRejectsNegativeMaxDiffRows(t *testing.T) {
	m := &MerkleTreeTask{MaxDiffRows: -1, SkipDBUpdate: true}
	m.Ctx = context.Background()

	err := m.DiffMtree()
	if err == nil || !strings.Contains(err.Error(), "max_diff_rows must be >= 0") {
		t.Fatalf("expected max_diff_rows validation error, got %v", err)
	}
}

// A lost work item means the pair has no result. The summary has to say so,
// otherwise a diff count of zero reads as a match.
func TestIncompletePairsReportsFailedWorkItems(t *testing.T) {
	m := &MerkleTreeTask{}

	if got := m.incompletePairs(); got != nil {
		t.Errorf("expected no incomplete pairs on a clean task, got %v", got)
	}

	m.recordPairCompareErr("n2/n3")
	m.recordPairCompareErr("n1/n2")
	m.recordPairCompareErr("n1/n2")

	got := m.incompletePairs()
	want := []string{"n1/n2", "n2/n3"}
	if len(got) != len(want) {
		t.Fatalf("incompletePairs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("incompletePairs() = %v, want %v (stable, sorted)", got, want)
		}
	}
}

// uuid and bytea bounds must be sorted like uuid_cmp() and byteacmp(), not by
// their printed Go form. 0x02 against 0x11 is the pair the fmt fallback got
// backwards: as text, "[17 ..." comes before "[2 ...".
func TestCompareBoundariesOrdersUUIDs(t *testing.T) {
	m := &MerkleTreeTask{}
	m.Key = []string{"id"}

	if got := m.compareBoundaries([]byte{0x02}, []byte{0x11}); got != -1 {
		t.Errorf("compareBoundaries(bytea 0x02, 0x11) = %d, want -1", got)
	}
	if got := m.compareBoundaries(false, true); got != -1 {
		t.Errorf("compareBoundaries(false, true) = %d, want -1", got)
	}
	if got := m.compareBoundaries(true, false); got != 1 {
		t.Errorf("compareBoundaries(true, false) = %d, want 1", got)
	}

	small, large := uuidBytes(0x02), uuidBytes(0x11)

	if got := m.compareBoundaries(small, large); got != -1 {
		t.Errorf("compareBoundaries(0x02.., 0x11..) = %d, want -1", got)
	}
	if got := m.compareBoundaries(large, small); got != 1 {
		t.Errorf("compareBoundaries(0x11.., 0x02..) = %d, want 1", got)
	}
	if got := m.compareBoundaries(large, large); got != 0 {
		t.Errorf("compareBoundaries of equal bounds = %d, want 0", got)
	}
}

// NaN is a legal float primary key, and Postgres sorts it above every number
// while Go's cmp.Compare puts it below. Getting this backwards inverts the
// bounds around it.
func TestComparePkeyValuesNaNSortsLast(t *testing.T) {
	nan := math.NaN()
	if got, ok := comparePkeyValues(nan, 1.0); !ok || got != 1 {
		t.Errorf("comparePkeyValues(NaN, 1.0) = %d, %v; want 1, true", got, ok)
	}
	if got, ok := comparePkeyValues(1.0, nan); !ok || got != -1 {
		t.Errorf("comparePkeyValues(1.0, NaN) = %d, %v; want -1, true", got, ok)
	}
	if got, ok := comparePkeyValues(nan, nan); !ok || got != 0 {
		t.Errorf("comparePkeyValues(NaN, NaN) = %d, %v; want 0, true", got, ok)
	}
}

// Postgres holds -0.0 = 0.0, so both must get one row identity. Two identities
// would split a single row into a phantom pair of differences.
func TestPkeyIdentityNegativeZero(t *testing.T) {
	pos, ok := pkeyIdentity(0.0)
	if !ok {
		t.Fatalf("pkeyIdentity(0.0) refused")
	}
	neg, ok := pkeyIdentity(math.Copysign(0, -1))
	if !ok {
		t.Fatalf("pkeyIdentity(-0.0) refused")
	}
	if pos != neg {
		t.Errorf("0.0 renders as %q and -0.0 as %q; Postgres treats them as equal", pos, neg)
	}
}

// pkeyKind, comparePkeyValues and pkeyIdentity are three independent switches
// over the same list of kinds. Nothing in the language keeps them in step, so
// adding a kind to pkeyKindOf without teaching the other two would otherwise
// surface as a sort that falls back to Go rendering, or a row identity that
// refuses mid-run. Every kind needs a sample value here.
func TestPkeyKindsAreFullySupported(t *testing.T) {
	samples := map[pkeyKind][2]any{
		pkeyInt:    {int64(1), int64(2)},
		pkeyUint:   {uint64(1), uint64(2)},
		pkeyFloat:  {1.0, 2.0},
		pkeyBool:   {false, true},
		pkeyString: {"a", "b"},
		pkeyTime:   {time.Unix(0, 0).UTC(), time.Unix(1, 0).UTC()},
		pkeyUUID:   {[16]byte{0x02}, [16]byte{0x0a}},
		pkeyBytes:  {[]byte{0x02}, []byte{0x0a}},
	}

	// Walk the kind range rather than the map, so a newly added constant fails
	// here instead of being quietly skipped.
	for kind := pkeyUnsupported + 1; kind < pkeyKindEnd; kind++ {
		pair, found := samples[kind]
		if !found {
			t.Errorf("kind %d has no sample value; add one when adding a kind", kind)
			continue
		}
		lo, hi := pair[0], pair[1]

		if got := pkeyKindOf(lo); got != kind {
			t.Errorf("pkeyKindOf(%#v) = %d, want %d", lo, got, kind)
		}

		// comparePkeyValues must order the kind, and order it the right way.
		c, ok := comparePkeyValues(lo, hi)
		if !ok {
			t.Errorf("comparePkeyValues refuses kind %d, which pkeyKindOf accepts", kind)
		} else if c >= 0 {
			t.Errorf("comparePkeyValues(%#v, %#v) = %d, want < 0", lo, hi, c)
		}

		// pkeyIdentity must render the kind, and render the two distinctly.
		idLo, ok := pkeyIdentity(lo)
		if !ok {
			t.Errorf("pkeyIdentity refuses kind %d, which pkeyKindOf accepts", kind)
			continue
		}
		idHi, ok := pkeyIdentity(hi)
		if !ok {
			t.Errorf("pkeyIdentity refuses kind %d, which pkeyKindOf accepts", kind)
			continue
		}
		if idLo == idHi {
			t.Errorf("kind %d renders %#v and %#v both as %q; identities must be injective",
				kind, lo, hi, idLo)
		}
	}
}

// Both sides of a comparison come from the same column, but a schema mismatch
// between nodes could still hand the comparator two different Go types. Every
// branch asserts on val2, so this must return ok=false rather than panic.
func TestComparePkeyValuesRejectsMixedKinds(t *testing.T) {
	for _, pair := range [][2]any{
		{"a", int64(1)},
		{int64(1), "a"},
		{[16]byte{0x01}, []byte{0x01}},
		{1.0, int64(1)},
		{time.Unix(0, 0), "1970-01-01"},
	} {
		if _, ok := comparePkeyValues(pair[0], pair[1]); ok {
			t.Errorf("comparePkeyValues(%T, %T) claimed a valid ordering across kinds",
				pair[0], pair[1])
		}
	}
}

// Bounds are de-duplicated by this key before the ranges are rebuilt, so two
// different cut points sharing a key drops one of them and can leave rows
// uncompared. uuids are the case fmt gets wrong: %v renders both of these
// through the same []byte formatting.
func TestBoundaryKeyDistinguishesBounds(t *testing.T) {
	m := &MerkleTreeTask{}

	distinct := [][]any{
		{"a b", "c"},
		{"a", "b c"},
		{"a|b", "c"},
		{"a", "b|c"},
		{[16]byte{0x02}, "x"},
		{[16]byte{0x0a}, "x"},
		{nil, "x"},
		{"", "x"},
		{"n", "x"},
		{"v", "x"},
	}

	seen := make(map[string][]any)
	for _, b := range distinct {
		key := m.boundaryKey(b)
		if prev, clash := seen[key]; clash {
			t.Errorf("bounds %#v and %#v share key %s", prev, b, key)
			continue
		}
		seen[key] = b
	}

	// Equal bounds must still share a key, or de-duplication stops working.
	if m.boundaryKey([]any{"a", 1}) != m.boundaryKey([]any{"a", 1}) {
		t.Error("the same bound produced two different keys")
	}
	// A scalar bound and the one-element slice holding it are the same bound.
	if m.boundaryKey("a") != m.boundaryKey([]any{"a"}) {
		t.Error("a scalar bound and its one-element slice disagree")
	}
}

// TestHashVersionError checks which stored hash versions a writer of leaf
// hashes accepts. A newer version is always refused: this build must not
// recompute leaves of a tree that a newer ACE wrote. An older version is
// accepted only by mtree update, which recomputes every leaf and the version
// in one transaction; any other writer would mix two algorithms in one tree.
func TestHashVersionError(t *testing.T) {
	m := &MerkleTreeTask{}
	m.Schema = "public"
	m.Table = "t"
	cur := queries.CurrentHashVersion

	tests := []struct {
		version    int
		allowOlder bool
		wantErr    []string // nil: no error
	}{
		{cur, false, nil},
		{cur, true, nil},
		{cur - 1, true, nil},
		{cur - 1, false, []string{"public.t", "n1", fmt.Sprintf("hash version %d, older", cur-1), "mtree update"}},
		{cur + 1, true, []string{"public.t", "n1", fmt.Sprintf("hash version %d,", cur+1), "mtree build"}},
		{cur + 1, false, []string{"public.t", "n1", fmt.Sprintf("hash version %d,", cur+1), "mtree build"}},
	}
	for _, tt := range tests {
		err := m.hashVersionError(tt.version, "n1", tt.allowOlder)
		if tt.wantErr == nil {
			if err != nil {
				t.Errorf("hashVersionError(%d, allowOlder=%v) = %v, want nil", tt.version, tt.allowOlder, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("hashVersionError(%d, allowOlder=%v) = nil, want an error", tt.version, tt.allowOlder)
			continue
		}
		for _, want := range tt.wantErr {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("hashVersionError(%d, allowOlder=%v) = %q, want it to mention %q", tt.version, tt.allowOlder, err, want)
			}
		}
	}
}

// A ranges file must give the first block position 0, or the first block gets
// a lower bound. renumberBlockRanges uses the order of the blocks in the file.
func TestRenumberBlockRanges(t *testing.T) {
	tests := []struct {
		name        string
		positions   []int64
		wantChanged bool
	}{
		{"already numbered", []int64{0, 1, 2}, false},
		{"starts at 1", []int64{1, 2, 3}, true},
		{"gap", []int64{0, 2, 3}, true},
		{"missing positions", []int64{0, 0, 0}, true},
		{"empty", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := make([]types.BlockRange, len(tt.positions))
			for i, p := range tt.positions {
				ranges[i] = types.BlockRange{NodePosition: p, RangeStart: []any{int64(i * 10)}}
			}
			if got := renumberBlockRanges(ranges); got != tt.wantChanged {
				t.Errorf("changed = %v, want %v", got, tt.wantChanged)
			}
			for i, r := range ranges {
				if r.NodePosition != int64(i) {
					t.Errorf("block %d has position %d", i, r.NodePosition)
				}
				if r.RangeStart[0] != int64(i*10) {
					t.Errorf("block %d moved: start %v", i, r.RangeStart[0])
				}
			}
		})
	}
}
