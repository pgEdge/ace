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

import "testing"

func TestComparePKComponentIsATotalOrder(t *testing.T) {
	vals := []string{"1", "1.0", "2", "9", "10", "1a", "10a", "9a", "", "NaN", "Inf", "-1", "abc", "a"}
	sign := func(x int) int {
		switch {
		case x < 0:
			return -1
		case x > 0:
			return 1
		}
		return 0
	}
	for _, a := range vals {
		for _, b := range vals {
			if sign(comparePKComponent(a, b)) != -sign(comparePKComponent(b, a)) {
				t.Errorf("not antisymmetric: %q, %q", a, b)
			}
			for _, c := range vals {
				if comparePKComponent(a, b) < 0 && comparePKComponent(b, c) < 0 && comparePKComponent(a, c) >= 0 {
					t.Errorf("not transitive: %q < %q < %q but not %q < %q", a, b, c, a, c)
				}
			}
		}
	}
}
