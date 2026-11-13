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

package slicer

import (
	"math"
	"strings"
	"testing"
)

// TestParseMethod checks the names of the ways to cut, in any case and
// with spaces, and that an empty name means auto.
func TestParseMethod(t *testing.T) {
	for in, want := range map[string]string{
		"":         MethodAuto,
		"  ":       MethodAuto,
		"auto":     MethodAuto,
		"Cutter":   MethodCutter,
		" sample ": MethodSample,
	} {
		got, err := ParseMethod(in)
		if err != nil || got != want {
			t.Errorf("ParseMethod(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ParseMethod("histogram"); err == nil {
		t.Error("ParseMethod(histogram) must fail")
	}
}

// TestNotAllVisible checks the estimate of pages that are not
// all-visible from the visibility map and from the rows changed since
// ANALYZE.
func TestNotAllVisible(t *testing.T) {
	cases := []struct {
		name     string
		s        Signals
		want     float64
		fromMods bool
	}{
		{"never vacuumed", Signals{Pages: 0}, 1, false},
		{"after vacuum", Signals{Pages: 1000, AllVisiblePages: 1000}, 0, false},
		{"visibility map", Signals{Pages: 1000, AllVisiblePages: 600, ModsSinceAnalyze: 100}, 0.4, false},
		{"changes since analyze", Signals{Pages: 1000, AllVisiblePages: 1000, ModsSinceAnalyze: 180}, 0.36, true},
		{"many changes", Signals{Pages: 1000, AllVisiblePages: 1000, ModsSinceAnalyze: 5000}, 1, true},
		{"stale relallvisible", Signals{Pages: 1000, AllVisiblePages: 1200}, 0, false},
	}
	for _, c := range cases {
		got, fromMods := c.s.NotAllVisible()
		if math.Abs(got-c.want) > 1e-9 || fromMods != c.fromMods {
			t.Errorf("%s: got %v, %v; want %v, %v", c.name, got, fromMods, c.want, c.fromMods)
		}
	}
}

// corr returns a pointer to a correlation value.
func corr(v float64) *float64 { return &v }

// TestDecide checks the rule of the auto mode and the explicit ways to
// cut on catalog values of different tables.
func TestDecide(t *testing.T) {
	cfg := &Config{Schema: "s", Table: "t", Key: []string{"id"}, BlockSize: 1000}
	big := 1e7
	// 40% of the pages are not all-visible.
	busy := Signals{Pages: 100000, AllVisiblePages: 60000, Tuples: big}
	vacuumed := Signals{Pages: 100000, AllVisiblePages: 99000, Tuples: big}

	cases := []struct {
		name      string
		filter    string
		requested string
		s         Signals
		method    string
		slow      bool
		advice    string
	}{
		{"random key, busy table", "", MethodAuto, withCorr(busy, 0.01), MethodSample, false, ""},
		{"negative correlation is used as absolute", "", MethodAuto, withCorr(busy, 0.5), MethodCutter, true, "VACUUM"},
		{"sequential key, busy table", "", MethodAuto, withCorr(busy, 0.99), MethodCutter, true, "VACUUM"},
		{"random key, vacuumed table", "", MethodAuto, withCorr(vacuumed, 0.01), MethodCutter, false, ""},
		{"no statistics", "", MethodAuto, busy, MethodCutter, true, "ANALYZE"},
		{"never analyzed", "", MethodAuto, Signals{Tuples: -1}, MethodCutter, false, "ANALYZE"},
		{"statistics but no row count", "", MethodAuto, withTuples(withCorr(busy, 0.01), -1), MethodCutter, false, "ANALYZE"},
		{"small table without statistics", "", MethodAuto, Signals{Pages: 10, Tuples: 50}, MethodCutter, false, ""},
		{"small table", "", MethodAuto, withTuples(withCorr(busy, 0.01), 99_999), MethodCutter, false, ""},
		{"table filter", "id > 5", MethodAuto, withCorr(busy, 0.01), MethodCutter, true, "VACUUM"},
		{"partitioned", "", MethodAuto, Signals{Partitioned: true, Tuples: -1}, MethodCutter, false, ""},
		{"user cutter", "", MethodCutter, withCorr(busy, 0.01), MethodCutter, true, "VACUUM"},
		{"user sample", "", MethodSample, withCorr(vacuumed, 0.9), MethodSample, false, ""},
		{"edge of correlation", "", MethodAuto, withCorr(busy, 0.1), MethodCutter, true, "VACUUM"},
		{"few pages not all-visible", "", MethodAuto, withCorr(Signals{Pages: 100, AllVisiblePages: 96, Tuples: big}, 0), MethodCutter, false, ""},
	}
	for _, c := range cases {
		cc := *cfg
		cc.Filter = c.filter
		got, err := decide(&cc, c.requested, &c.s)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got.Method != c.method || got.SlowAnchor != c.slow || !strings.Contains(got.Advice, c.advice) {
			t.Errorf("%s: got %s slow=%v advice=%q (%s); want %s slow=%v advice with %q",
				c.name, got.Method, got.SlowAnchor, got.Advice, got.Reason, c.method, c.slow, c.advice)
		}
		if got.Requested != c.requested || got.Reason == "" {
			t.Errorf("%s: requested %q, reason %q", c.name, got.Requested, got.Reason)
		}
		if got.Method == MethodSample && got.SamplePercent != 1 {
			t.Errorf("%s: sample percent %v, want 1", c.name, got.SamplePercent)
		}
	}
}

// withCorr returns s with the absolute value of c as the correlation.
func withCorr(s Signals, c float64) Signals {
	s.Correlation = corr(math.Abs(c))
	return s
}

// withTuples returns s with n rows.
func withTuples(s Signals, n float64) Signals {
	s.Tuples = n
	return s
}

// TestDecideRefusesSample checks that an explicit sample fails with a row
// filter and on a partitioned table.
func TestDecideRefusesSample(t *testing.T) {
	cfg := &Config{Schema: "s", Table: "t", Key: []string{"id"}, BlockSize: 1000, Filter: "id > 5"}
	if _, err := decide(cfg, MethodSample, &Signals{Pages: 10}); err == nil {
		t.Error("the sample with a table filter must fail")
	}
	cfg.Filter = ""
	if _, err := decide(cfg, MethodSample, &Signals{Partitioned: true}); err == nil {
		t.Error("the sample of a partitioned table must fail")
	}
}

// TestChoiceMessages checks the log lines and the advice that a choice
// gives for the sample and for the cutter.
func TestChoiceMessages(t *testing.T) {
	cfg := &Config{Schema: "s", Table: "t", Key: []string{"id"}, BlockSize: 1000}
	s := Signals{Pages: 100000, AllVisiblePages: 36000, Tuples: 1e7, Correlation: corr(0.02)}
	c, err := decide(cfg, MethodAuto, &s)
	if err != nil {
		t.Fatal(err)
	}
	d := c.Describe("n1")
	for _, want := range []string{"slicer sample (auto) on node n1", "|correlation| 0.02", "not all-visible 64%", "1% of the heap pages", "about 1000 pages"} {
		if !strings.Contains(d, want) {
			t.Errorf("Describe() = %q, want %q in it", d, want)
		}
	}
	adv := c.TimeoutAdvice()
	for _, want := range []string{"slicer auto chose the sample", "--slicer=cutter", "VACUUM"} {
		if !strings.Contains(adv, want) {
			t.Errorf("TimeoutAdvice() = %q, want %q in it", adv, want)
		}
	}

	cut, err := decide(cfg, MethodCutter, &s)
	if err != nil {
		t.Fatal(err)
	}
	if d := cut.Describe("n1"); !strings.Contains(d, "add up to about 64% to the hashing time") {
		t.Errorf("Describe() = %q, want the cost of the cut", d)
	}
	if cut.TimeoutAdvice() != "" {
		t.Error("the cutter must give no timeout advice")
	}
	if a := cut.SlowCutAdvice(0.7); !strings.Contains(a, "70%") || !strings.Contains(a, "--slicer=sample") {
		t.Errorf("SlowCutAdvice() = %q", a)
	}
	if a := cut.SlowCutAdvice(0.7); !strings.Contains(a, "VACUUM") {
		t.Errorf("SlowCutAdvice() must advise VACUUM for many pages that are not all-visible: %q", a)
	}
	s.Correlation = corr(0.9)
	if a := cut.SlowCutAdvice(0.7); strings.Contains(a, "--slicer=sample") {
		t.Errorf("SlowCutAdvice() must not suggest the sample for a key in heap order: %q", a)
	}
	vacuumed := &Choice{Method: MethodCutter, cutShare: 0.1, Signals: &Signals{Correlation: corr(0.02)}}
	if a := vacuumed.SlowCutAdvice(0.7); !strings.Contains(a, "70%") || strings.Contains(a, "VACUUM") ||
		strings.Contains(a, "all-visible") || !strings.Contains(a, "--slicer=sample") {
		t.Errorf("SlowCutAdvice() on a vacuumed table = %q", a)
	}
	vacuumed.Signals.Correlation = corr(0.9)
	if a := vacuumed.SlowCutAdvice(0.7); strings.Contains(a, "VACUUM") || strings.Contains(a, "--slicer=sample") {
		t.Errorf("SlowCutAdvice() with no advice to give = %q", a)
	}
	fcfg := *cfg
	fcfg.Filter = "id > 5"
	s.Correlation = corr(0.02)
	filtered, err := decide(&fcfg, MethodAuto, &s)
	if err != nil {
		t.Fatal(err)
	}
	if a := filtered.SlowCutAdvice(0.7); strings.Contains(a, "--slicer=sample") || !strings.Contains(a, "VACUUM") {
		t.Errorf("SlowCutAdvice() must not suggest the sample with a row filter: %q", a)
	}
}

// TestSampleBlocksSQL checks the sample rate for a block size and the text
// of the sample query.
func TestSampleBlocksSQL(t *testing.T) {
	if p := blockSamplePercent(1000); p != 1 {
		t.Errorf("blockSamplePercent(1000) = %v, want 1", p)
	}
	if p := blockSamplePercent(10); p != 100 {
		t.Errorf("blockSamplePercent(10) = %v, want 100", p)
	}

	cfg := &Config{Schema: "s", Table: "t", Key: []string{"a", "b"}, BlockSize: 1000}
	sql := sampleBlocksSQL(cfg, 1)
	for _, want := range []string{
		`TABLESAMPLE SYSTEM (1) REPEATABLE (1)`,
		`SELECT "a" AS _ace_k0, "b" AS _ace_k1, (ctid::text::point)[0] AS _ace_blk`,
		`row_number() OVER _ace_w`,
		`WINDOW _ace_w AS (ORDER BY _ace_k0, _ace_k1)`,
		`count(*) OVER ()`,
		`WHERE _ace_rn % 10 = 0 OR _ace_rn = _ace_n`,
		`lag(_ace_blk) OVER _ace_w = _ace_blk`,
		`ORDER BY _ace_rn`,
		`FROM "s"."t"`,
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("sample SQL has no %q:\n%s", want, sql)
		}
	}
	if sql := sampleBlocksSQL(cfg, 100); strings.Contains(sql, "TABLESAMPLE") {
		t.Errorf("a full read must not sample:\n%s", sql)
	}
}

// TestCheckSample checks when the auto mode replaces the sample with the
// cutter after the sample is read, and that an explicit sample is kept.
func TestCheckSample(t *testing.T) {
	cfg := Config{Schema: "s", Table: "t", Key: []string{"id"}, BlockSize: 1000}
	signals := func() *Signals {
		return &Signals{Pages: 100000, AllVisiblePages: 36000, Tuples: 1e7, Correlation: corr(0.03)}
	}
	random := &Sample{keys: 40000, samePage: 0.01}
	clustered := &Sample{keys: 40000, samePage: 0.98}

	c, err := decide(&cfg, MethodAuto, signals())
	if err != nil || c.Method != MethodSample {
		t.Fatalf("decide: %v, %+v", err, c)
	}
	if sw, w := c.CheckSample(cfg, random); sw || w != "" || c.Method != MethodSample {
		t.Errorf("random heap: switched=%v warning=%q method=%s", sw, w, c.Method)
	}
	if c.SamePage == nil || *c.SamePage != 0.01 {
		t.Errorf("SamePage = %v", c.SamePage)
	}

	c, err = decide(&cfg, MethodAuto, signals())
	if err != nil {
		t.Fatal(err)
	}
	sw, w := c.CheckSample(cfg, clustered)
	if !sw || c.Method != MethodCutter || !strings.Contains(w, "98%") || !strings.Contains(w, "cutter is used instead") {
		t.Errorf("clustered heap, auto: switched=%v method=%s warning=%q", sw, c.Method, w)
	}
	if c.SamplePercent != 0 || !c.SlowAnchor || c.cutShare == 0 {
		t.Errorf("after the switch the choice must describe the cutter: %+v", c)
	}
	if strings.Contains(c.SlowCutAdvice(0.7), "--slicer=sample") {
		t.Error("a clustered sample must not suggest the sample")
	}
	if d := c.Describe("n1"); !strings.Contains(d, "slicer cutter (auto)") || !strings.Contains(d, "98%") {
		t.Errorf("Describe() = %q", d)
	}

	c, err = decide(&cfg, MethodSample, signals())
	if err != nil {
		t.Fatal(err)
	}
	sw, w = c.CheckSample(cfg, clustered)
	if sw || c.Method != MethodSample || !strings.Contains(w, "--slicer=cutter") {
		t.Errorf("clustered heap, explicit sample: switched=%v method=%s warning=%q", sw, c.Method, w)
	}

	// A sample with few keys says nothing about the heap order, but its
	// bounds are too few: auto takes the cutter, an explicit sample is kept.
	c, err = decide(&cfg, MethodAuto, signals())
	if err != nil {
		t.Fatal(err)
	}
	if sw, w := c.CheckSample(cfg, &Sample{keys: 50, samePage: 0.98}); !sw || w != "" ||
		c.Method != MethodCutter || c.Reason != "the sample has only 50 keys" || c.SamePage != nil {
		t.Errorf("a small sample, auto: switched=%v warning=%q %+v", sw, w, c)
	}
	c, err = decide(&cfg, MethodSample, signals())
	if err != nil {
		t.Fatal(err)
	}
	if sw, w := c.CheckSample(cfg, &Sample{}); sw || w != "" || c.Method != MethodSample {
		t.Errorf("an empty sample, explicit sample: switched=%v warning=%q method=%s", sw, w, c.Method)
	}
}

// TestSampleFailed checks that the auto mode takes the cutter when the
// sample query fails, and that an explicit sample is kept.
func TestSampleFailed(t *testing.T) {
	cfg := Config{Schema: "s", Table: "t", Key: []string{"id"}, BlockSize: 1000}
	signals := &Signals{Pages: 100000, AllVisiblePages: 36000, Tuples: 1e7, Correlation: corr(0.03)}

	c, err := decide(&cfg, MethodAuto, signals)
	if err != nil || c.Method != MethodSample {
		t.Fatalf("decide: %v, %+v", err, c)
	}
	if !c.SampleFailed(cfg) || c.Method != MethodCutter || c.SamplePercent != 0 || c.Reason != "the sample query failed" {
		t.Errorf("auto must take the cutter: %+v", c)
	}
	if c.TimeoutAdvice() != "" {
		t.Error("the cutter needs no advice about the sample")
	}

	c, err = decide(&cfg, MethodSample, signals)
	if err != nil {
		t.Fatal(err)
	}
	if c.SampleFailed(cfg) || c.Method != MethodSample {
		t.Errorf("an explicit sample must be kept: %+v", c)
	}
}
