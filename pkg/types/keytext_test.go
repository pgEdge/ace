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

package types

import (
	"encoding/json"
	"reflect"
	"testing"
)

// A key value in a ranges file is an array of strings. A number or a boolean
// is read as its literal text, and an empty array or an array of nulls is an
// open bound. A value of another JSON kind, such as the array of 16 numbers
// of a uuid, is refused.
func TestKeyTextUnmarshalJSON(t *testing.T) {
	cases := []struct {
		in   string
		want KeyText
		err  bool
	}{
		{`["12", "a b"]`, KeyText{"12", "a b"}, false},
		{`[12345678901234567890, 1.50, true]`, KeyText{"12345678901234567890", "1.50", "true"}, false},
		{`null`, nil, false},
		{`[]`, nil, false},
		{`[null, null]`, nil, false},
		{`[1, null]`, nil, true},
		{`[[1]]`, nil, true},
		{`[[17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17]]`, nil, true},
		{`[{"Microseconds": 1, "Days": 0, "Months": 0, "Valid": true}]`, nil, true},
	}
	for _, c := range cases {
		var got KeyText
		err := json.Unmarshal([]byte(c.in), &got)
		if (err != nil) != c.err {
			t.Errorf("%s: error %v, want error %v", c.in, err, c.err)
			continue
		}
		if !c.err && !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %#v, want %#v", c.in, got, c.want)
		}
	}

	var br BlockRange
	if err := json.Unmarshal([]byte(`{"NodePosition": 3, "RangeStart": [10], "RangeEnd": null}`), &br); err != nil {
		t.Fatal(err)
	}
	if br.NodePosition != 3 || !reflect.DeepEqual(br.RangeStart, KeyText{"10"}) || br.RangeEnd != nil {
		t.Errorf("block range: %+v", br)
	}
}
