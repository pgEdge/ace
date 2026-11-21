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

package queries

import "testing"

func libcDefault(collate string) collationDef {
	return collationDef{name: "pg_catalog.\"default\"", isDefault: true, provider: "c", collate: collate, deterministic: true}
}

func icuDefault(collate, locale string) collationDef {
	return collationDef{name: "pg_catalog.\"default\"", isDefault: true, provider: "i", collate: collate, locale: locale, deterministic: true}
}

// Collations that order values in the same way must give the same text, or
// mtree refuses to run on nodes that differ only in how the catalog spells
// the locale.
func TestCollationOrderTextSameOrder(t *testing.T) {
	cases := []struct {
		name string
		a, b collationDef
	}{
		{"libc code set spelling", libcDefault("en_US.UTF-8"), libcDefault("en_US.utf8")},
		{"libc code set in upper case", libcDefault("en_US.UTF8"), libcDefault("en_US.utf8")},
		{"libc code set of digits", libcDefault("de_DE.ISO-8859-1"), libcDefault("de_DE.iso88591")},
		{"libc code set with a modifier", libcDefault("de_DE.UTF-8@euro"), libcDefault("de_DE.utf8@euro")},
		{"libc POSIX is C", libcDefault("POSIX"), libcDefault("C")},
		{"ICU database with another LC_COLLATE", icuDefault("en_US.UTF-8", "en-US"), icuDefault("C.UTF-8", "en-US")},
		{"builtin database with another LC_COLLATE",
			collationDef{isDefault: true, provider: "b", collate: "C", locale: "C.UTF-8", deterministic: true},
			collationDef{isDefault: true, provider: "b", collate: "en_US.utf8", locale: "C.UTF-8", deterministic: true}},
		{"named ICU collation ignores collcollate",
			collationDef{name: "public.k", provider: "i", collate: "und", locale: "und", deterministic: true},
			collationDef{name: "public.k", provider: "i", locale: "und", deterministic: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if a, b := tc.a.orderText(), tc.b.orderText(); a != b {
				t.Errorf("same order, other texts: %q and %q", a, b)
			}
		})
	}
}

// Collations that can order values in other ways must give other texts.
func TestCollationOrderTextOtherOrder(t *testing.T) {
	cases := []struct {
		name string
		a, b collationDef
	}{
		{"libc locale", libcDefault("en_US.utf8"), libcDefault("de_DE.utf8")},
		{"libc C and C.UTF-8", libcDefault("C"), libcDefault("C.UTF-8")},
		{"libc and ICU", libcDefault("en_US.utf8"), icuDefault("en_US.utf8", "en-US")},
		{"ICU locale", icuDefault("C", "und"), icuDefault("C", "und-u-kn")},
		{"ICU rules",
			collationDef{isDefault: true, provider: "i", locale: "und", deterministic: true},
			collationDef{isDefault: true, provider: "i", locale: "und", rules: "&b<a", deterministic: true}},
		{"determinism",
			collationDef{name: "public.k", provider: "i", locale: "und", deterministic: true},
			collationDef{name: "public.k", provider: "i", locale: "und", deterministic: false}},
		{"name of a named collation",
			collationDef{name: "public.k", provider: "i", locale: "und", deterministic: true},
			collationDef{name: "public.m", provider: "i", locale: "und", deterministic: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if a, b := tc.a.orderText(), tc.b.orderText(); a == b {
				t.Errorf("other orders, same text %q", a)
			}
		})
	}
}

func TestCanonicalLibcLocale(t *testing.T) {
	for in, want := range map[string]string{
		"C":                "C",
		"POSIX":            "C",
		"C.UTF-8":          "C.utf8",
		"en_US":            "en_US",
		"en_US.UTF-8":      "en_US.utf8",
		"de_DE.ISO-8859-1": "de_DE.iso88591",
		"ja_JP.eucJP":      "ja_JP.eucjp",
		"de_DE.UTF-8@euro": "de_DE.utf8@euro",
		"sr_RS@latin":      "sr_RS@latin",
	} {
		if got := canonicalLibcLocale(in); got != want {
			t.Errorf("canonicalLibcLocale(%q) = %q, want %q", in, got, want)
		}
	}
}
