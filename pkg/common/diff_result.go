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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
)

// DiffResultFormatVersion is the version of the layout of a diff result
// directory: the manifest, the pair directories and the block files.
const DiffResultFormatVersion = 1

// DiffResultManifestName is the name of the manifest in a result directory.
// The manifest is written last, so a directory with a manifest is complete.
const DiffResultManifestName = "manifest.json"

// The kinds of an entry of a block file. Node 1 and node 2 are the nodes of
// the pair, as the manifest names them.
const (
	DiffKindNode1Only = "node1_only"
	DiffKindNode2Only = "node2_only"
	DiffKindChanged   = "changed"
)

// DiffResultColumn is one column of the compared table.
type DiffResultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DiffResultPair describes the entries of one node pair.
type DiffResultPair struct {
	// Pair is the name of the pair in the diff summary, "node1/node2".
	Pair  string `json:"pair"`
	Node1 string `json:"node1"`
	Node2 string `json:"node2"`
	// Dir is the directory of the block files of the pair, relative to the
	// result directory.
	Dir string `json:"dir"`
	// Blocks is the number of block files.
	Blocks    int   `json:"blocks"`
	Node1Only int64 `json:"node1_only"`
	Node2Only int64 `json:"node2_only"`
	Changed   int64 `json:"changed"`
}

// Total returns the number of entries of the pair.
func (p DiffResultPair) Total() int64 {
	return p.Node1Only + p.Node2Only + p.Changed
}

// DiffResultManifest describes a result directory.
type DiffResultManifest struct {
	FormatVersion int `json:"format_version"`
	// RowFormatVersion is the format of the row values, as in the JSON
	// report (types.DiffFormatVersion).
	RowFormatVersion int `json:"row_format_version"`
	// HashVersion is the version of the row hash that gave the row hashes
	// of the entries.
	HashVersion int                `json:"hash_version"`
	Schema      string             `json:"schema"`
	Table       string             `json:"table"`
	PrimaryKey  []string           `json:"primary_key"`
	Columns     []DiffResultColumn `json:"columns"`
	Nodes       []string           `json:"nodes"`
	// Pairs lists the pairs that have entries, sorted by name.
	Pairs []DiffResultPair `json:"pairs"`
	// IntegerKey is true when the primary key is one column and the key of
	// every entry is a whole number.
	IntegerKey bool              `json:"integer_key"`
	Summary    types.DiffSummary `json:"summary"`
}

// DiffEntryLine is one line of a block file: one differing row of a pair.
// Row1 and Hash1 are set for an entry on node 1, Row2 and Hash2 for an entry
// on node 2. The rows hold the same values as the rows of the JSON report.
type DiffEntryLine struct {
	Kind  string          `json:"kind"`
	Hash1 string          `json:"hash1,omitempty"`
	Hash2 string          `json:"hash2,omitempty"`
	Row1  json.RawMessage `json:"row1,omitempty"`
	Row2  json.RawMessage `json:"row2,omitempty"`
}

// DiffResultBase returns the path, without an extension, of the result of a
// diff of schema.table that starts at now: the name of the result directory
// and the base name of the JSON and HTML reports.
func DiffResultBase(schema, table string, now time.Time) string {
	return fmt.Sprintf("%s_%s_diffs-%s",
		strings.ReplaceAll(schema, ".", "_"),
		strings.ReplaceAll(table, ".", "_"),
		now.Format("20060102150405"),
	)
}

// DiffResultWriter writes the result directory of one diff run. The
// directory is created under a temporary name when the first entry comes,
// and gets its final name only in Finish. Blocks can be written from
// several goroutines at the same time; each block has one writer.
type DiffResultWriter struct {
	base   string
	keyLen int
	key    string

	mu       sync.Mutex
	tmp      string
	pairs    map[string]*DiffResultPair
	dirs     map[string]bool
	intKey   bool
	finished bool
}

// NewDiffResultWriter returns a writer for the result of a diff of
// schema.table with the primary key key. Nothing is created on disk yet.
func NewDiffResultWriter(schema, table string, key []string) *DiffResultWriter {
	w := &DiffResultWriter{
		base:   DiffResultBase(schema, table, time.Now()),
		keyLen: len(key),
		pairs:  make(map[string]*DiffResultPair),
		dirs:   make(map[string]bool),
		intKey: len(key) == 1,
	}
	if len(key) == 1 {
		w.key = key[0]
	}
	return w
}

// Base returns the path of the result directory, which is also the base
// name of the JSON and HTML reports.
func (w *DiffResultWriter) Base() string {
	return w.base
}

var unsafeDirChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// pairLocked returns the state of a pair, and makes its directory when the
// pair gets its first entry. All blocks of a pair must name the nodes in the
// same order: the rows of a block are stored as node 1 and node 2 of the
// pair. The caller holds mu.
func (w *DiffResultWriter) pairLocked(pairKey, node1, node2 string) (*DiffResultPair, error) {
	if p, ok := w.pairs[pairKey]; ok {
		if p.Node1 != node1 || p.Node2 != node2 {
			return nil, fmt.Errorf("block of pair %s has nodes %s and %s, but the pair has nodes %s and %s",
				pairKey, node1, node2, p.Node1, p.Node2)
		}
		return p, nil
	}
	if w.tmp == "" {
		tmp, err := os.MkdirTemp(filepath.Dir(w.base), filepath.Base(w.base)+".tmp-")
		if err != nil {
			return nil, fmt.Errorf("create the diff result directory: %w", err)
		}
		if err := os.Chmod(tmp, SecureDirMode); err != nil {
			os.RemoveAll(tmp)
			return nil, fmt.Errorf("restrict permissions on %s: %w", tmp, err)
		}
		w.tmp = tmp
	}
	dir := unsafeDirChars.ReplaceAllString(node1+"_"+node2, "_")
	for i := 2; w.dirs[dir]; i++ {
		dir = fmt.Sprintf("%s-%d", unsafeDirChars.ReplaceAllString(node1+"_"+node2, "_"), i)
	}
	if err := os.Mkdir(filepath.Join(w.tmp, dir), SecureDirMode); err != nil {
		return nil, fmt.Errorf("create the diff result directory of %s: %w", pairKey, err)
	}
	w.dirs[dir] = true
	p := &DiffResultPair{Pair: pairKey, Node1: node1, Node2: node2, Dir: dir}
	w.pairs[pairKey] = p
	return p, nil
}

// DiffBlockName returns the name of the block file of block seq in part
// part. The numbers are padded with zeros, so the names sort in block order.
func DiffBlockName(part, seq int) string {
	return fmt.Sprintf("%05d-%09d.jsonl", part, seq)
}

// OpenBlock returns the writer of one compared block of a pair. The file is
// created only when the first entry is added. pairKey is the name of the
// pair in the diff summary; node1 and node2 are the nodes that the entries
// call node 1 and node 2.
func (w *DiffResultWriter) OpenBlock(pairKey, node1, node2 string, part, seq int) *DiffBlockWriter {
	return &DiffBlockWriter{w: w, pairKey: pairKey, node1: node1, node2: node2, part: part, seq: seq,
		name: DiffBlockName(part, seq)}
}

// DiffBlockWriter writes the entries of one block. It is not safe for use
// from several goroutines.
type DiffBlockWriter struct {
	w            *DiffResultWriter
	pairKey      string
	node1, node2 string
	part, seq    int
	name         string
	f            *os.File
	buf          *bufio.Writer
	line         bytes.Buffer
	node1Only    int64
	node2Only    int64
	changed      int64
	intKey       bool
	closed       bool
}

func (b *DiffBlockWriter) open() error {
	// Larger numbers would give names that do not sort in block order.
	if b.part < 0 || b.part > 99999 || b.seq < 0 || b.seq > 999999999 {
		return fmt.Errorf("block number %d-%d is out of range for a block file name", b.part, b.seq)
	}
	b.w.mu.Lock()
	defer b.w.mu.Unlock()
	if b.w.finished {
		return errors.New("the diff result is already finished")
	}
	p, err := b.w.pairLocked(b.pairKey, b.node1, b.node2)
	if err != nil {
		return err
	}
	path := filepath.Join(b.w.tmp, p.Dir, b.name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, SecureFileMode)
	if err != nil {
		return fmt.Errorf("create the block file %s: %w", path, err)
	}
	if err := f.Chmod(SecureFileMode); err != nil {
		f.Close()
		return fmt.Errorf("restrict permissions on %s: %w", path, err)
	}
	b.f = f
	b.buf = bufio.NewWriterSize(f, 256*1024)
	b.intKey = b.w.intKey
	return nil
}

// Add writes one entry. kind is one of the DiffKind constants; row1 and
// hash1 belong to node 1, row2 and hash2 to node 2, and are empty for a row
// that the node does not have.
func (b *DiffBlockWriter) Add(kind string, row1, row2 types.OrderedMap, hash1, hash2 []byte) error {
	if b.closed {
		return errors.New("the block file is already closed")
	}
	switch kind {
	case DiffKindNode1Only, DiffKindNode2Only, DiffKindChanged:
	default:
		return fmt.Errorf("unknown kind of a diff entry %q", kind)
	}
	if b.f == nil {
		if err := b.open(); err != nil {
			return err
		}
	}
	l := &b.line
	l.Reset()
	l.WriteString(`{"kind":`)
	writeJSONString(l, kind)
	if len(hash1) > 0 {
		l.WriteString(`,"hash1":"`)
		l.WriteString(hex.EncodeToString(hash1))
		l.WriteByte('"')
	}
	if len(hash2) > 0 {
		l.WriteString(`,"hash2":"`)
		l.WriteString(hex.EncodeToString(hash2))
		l.WriteByte('"')
	}
	for _, r := range []struct {
		name string
		row  types.OrderedMap
	}{{"row1", row1}, {"row2", row2}} {
		if r.row == nil {
			continue
		}
		data, err := r.row.MarshalJSON()
		if err != nil {
			return fmt.Errorf("encode a row of %s: %w", b.pairKey, err)
		}
		l.WriteString(`,"` + r.name + `":`)
		l.Write(data)
	}
	l.WriteString("}\n")
	if _, err := b.buf.Write(l.Bytes()); err != nil {
		return fmt.Errorf("write the block file %s: %w", b.name, err)
	}

	switch kind {
	case DiffKindNode1Only:
		b.node1Only++
	case DiffKindNode2Only:
		b.node2Only++
	default:
		b.changed++
	}
	if b.intKey {
		row := row1
		if row == nil {
			row = row2
		}
		b.intKey = isIntegerKey(row, b.w.key)
	}
	return nil
}

var integerLiteralRe = regexp.MustCompile(`^-?[0-9]+$`)

// isIntegerKey tells whether the value of the key column of row is a whole
// number in its JSON form.
func isIntegerKey(row types.OrderedMap, key string) bool {
	v, ok := row.Get(key)
	if !ok {
		return false
	}
	data, err := json.Marshal(v)
	return err == nil && integerLiteralRe.Match(data)
}

func writeJSONString(buf *bytes.Buffer, s string) {
	data, _ := json.Marshal(s)
	buf.Write(data)
}

// Count returns the number of entries written.
func (b *DiffBlockWriter) Count() int64 {
	return b.node1Only + b.node2Only + b.changed
}

// Close flushes and closes the block file, and adds its entries to the
// counts of the pair. A block with no entries has no file.
func (b *DiffBlockWriter) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.f == nil {
		return nil
	}
	err := b.buf.Flush()
	if cerr := b.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("write the block file %s: %w", b.name, err)
	}
	b.w.mu.Lock()
	defer b.w.mu.Unlock()
	p := b.w.pairs[b.pairKey]
	p.Blocks++
	p.Node1Only += b.node1Only
	p.Node2Only += b.node2Only
	p.Changed += b.changed
	if !b.intKey {
		b.w.intKey = false
	}
	return nil
}

// HasEntries tells whether any block got an entry.
func (w *DiffResultWriter) HasEntries() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tmp != ""
}

// Abort removes everything the writer has created. It does nothing after a
// successful Finish.
func (w *DiffResultWriter) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return
	}
	w.finished = true
	if w.tmp != "" {
		if err := os.RemoveAll(w.tmp); err != nil {
			logger.Warn("failed to remove %s: %v", w.tmp, err)
		}
	}
}

// DiffReportPaths are the files of a finished diff result.
type DiffReportPaths struct {
	Dir  string
	JSON string
	HTML string
}

// DiffReportOptions control the reports written at the end of a diff run.
type DiffReportOptions struct {
	// Format is the output format of the run; "html" adds the HTML report.
	Format string
	// MaxHTMLRows and MaxHTMLValueLength limit the HTML report; a value of 0
	// or less means the default.
	MaxHTMLRows        int64
	MaxHTMLValueLength int64
}

// Finish completes the result: it writes the manifest and the JSON report,
// and gives the directory and the report their final names. The writer fills
// in the format versions, the pairs and the integer key flag of m. With no
// entries it writes nothing and returns empty paths. Every block must be
// closed before Finish. On an error nothing is left on disk.
//
// When o.Format is "html", the HTML report is written last. The result is
// complete at that point, so a failure of the HTML report is only a
// warning: the result stays, and the report can be built again from it.
func (w *DiffResultWriter) Finish(ctx context.Context, m DiffResultManifest, o DiffReportOptions) (DiffReportPaths, error) {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return DiffReportPaths{}, errors.New("the diff result is already finished")
	}
	w.finished = true
	tmp := w.tmp
	m.FormatVersion = DiffResultFormatVersion
	m.RowFormatVersion = types.DiffFormatVersion
	m.IntegerKey = w.intKey && w.keyLen == 1
	m.Pairs = m.Pairs[:0]
	for _, p := range w.pairs {
		if p.Total() > 0 {
			m.Pairs = append(m.Pairs, *p)
		}
	}
	w.mu.Unlock()
	sort.Slice(m.Pairs, func(i, j int) bool { return m.Pairs[i].Pair < m.Pairs[j].Pair })

	if tmp == "" || len(m.Pairs) == 0 {
		if tmp != "" {
			os.RemoveAll(tmp)
		}
		logger.Info("%s TABLES MATCH", CheckMark)
		return DiffReportPaths{}, nil
	}

	paths, err := w.finish(ctx, tmp, m)
	if err != nil {
		os.RemoveAll(tmp)
		return DiffReportPaths{}, err
	}

	logger.Warn("%s TABLES DO NOT MATCH", CrossMark)
	for _, p := range m.Pairs {
		logger.Warn("Found %d differences between %s", p.Total(), p.Pair)
	}
	logger.Info("Diff result written to %s", paths.Dir)
	logger.Info("Diff report written to %s", paths.JSON)

	if strings.EqualFold(o.Format, "html") {
		r := &StoredDiff{Dir: paths.Dir, Manifest: m}
		htmlPath, err := WriteHTMLDiffReport(ctx, r, o.MaxHTMLRows, o.MaxHTMLValueLength)
		if err != nil {
			logger.Warn("the HTML report was not written: %v; the diff result %s is complete, "+
				"build the report with: ace diff-report --diff-file %s", err, paths.Dir, paths.Dir)
			return paths, nil
		}
		paths.HTML = htmlPath
		logger.Info("HTML diff report written to %s", htmlPath)
	}
	return paths, nil
}

// reportTmpName is the name of the JSON report inside the result directory
// while it is written.
const reportTmpName = "report.json.tmp"

func (w *DiffResultWriter) finish(ctx context.Context, tmp string, m DiffResultManifest) (DiffReportPaths, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return DiffReportPaths{}, fmt.Errorf("encode the diff result manifest: %w", err)
	}
	if err := WriteFileSecure(filepath.Join(tmp, DiffResultManifestName), append(data, '\n')); err != nil {
		return DiffReportPaths{}, fmt.Errorf("write the diff result manifest: %w", err)
	}

	// Every temporary file is inside the temporary directory, so one
	// RemoveAll cleans up after an error.
	if err := writeDiffJSON(ctx, &StoredDiff{Dir: tmp, Manifest: m}, filepath.Join(tmp, reportTmpName)); err != nil {
		return DiffReportPaths{}, err
	}
	// A run that was stopped while the report was written publishes
	// nothing.
	if err := ctx.Err(); err != nil {
		return DiffReportPaths{}, err
	}

	// Two runs on the same table in the same second get the same name, also
	// in two processes. The later run gets a number at the end. A rename
	// does not replace a directory that has files, so when two processes
	// take the same free name at the same time, one of them gets an error
	// and takes the next name.
	base := w.base
	for i := 2; ; i++ {
		if !pathExists(w.base) && !pathExists(w.base+".json") {
			err := os.Rename(tmp, w.base)
			if err == nil {
				break
			}
			if !pathExists(w.base) {
				return DiffReportPaths{}, fmt.Errorf("rename the diff result directory to %s: %w", w.base, err)
			}
		}
		if i > 1000 {
			return DiffReportPaths{}, fmt.Errorf("no free name for the diff result %s", base)
		}
		w.base = fmt.Sprintf("%s_%d", base, i)
	}
	// The run that owns the directory owns the name of the JSON report too:
	// every run takes a name only when both are free, and only one rename of
	// the directory can succeed. So a rename of the report cannot replace the
	// report of another run.
	jsonPath := w.base + ".json"
	if err := os.Rename(filepath.Join(w.base, reportTmpName), jsonPath); err != nil {
		os.RemoveAll(w.base)
		return DiffReportPaths{}, fmt.Errorf("create the diff report %s: %w", jsonPath, err)
	}
	return DiffReportPaths{Dir: w.base, JSON: jsonPath}, nil
}

func pathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// StoredDiff is a finished result directory.
type StoredDiff struct {
	Dir      string
	Manifest DiffResultManifest
}

// OpenDiffResult reads the manifest of a result directory. path is the
// directory, or a JSON report: then the directory with the same base name
// is read.
func OpenDiffResult(path string) (*StoredDiff, error) {
	// The reports are found next to the directory by its name, so a path
	// such as "." or "res/." must become the real name of the directory.
	dir, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve the path %s: %w", path, err)
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		if strings.EqualFold(filepath.Ext(dir), ".json") {
			dir = strings.TrimSuffix(dir, filepath.Ext(dir))
		}
	}
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("no diff result directory for %s: %w", path, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("%s is not a diff result directory", dir)
	}
	data, err := os.ReadFile(filepath.Join(dir, DiffResultManifestName))
	if err != nil {
		return nil, fmt.Errorf("%s is not a complete diff result: %w", dir, err)
	}
	// The directory is renamed before the JSON report is published next to
	// it. A run that stopped between the two steps leaves a directory with a
	// manifest, but no JSON report.
	if pathExists(filepath.Join(dir, reportTmpName)) || !pathExists(dir+".json") {
		return nil, fmt.Errorf("%s is not a complete diff result: the run stopped before it published %s.json",
			dir, dir)
	}
	var m DiffResultManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("read the manifest of %s: %w", dir, err)
	}
	if m.FormatVersion < 1 || m.FormatVersion > DiffResultFormatVersion {
		return nil, fmt.Errorf("unsupported diff result format version %d in %s: this ACE reads versions up to %d",
			m.FormatVersion, dir, DiffResultFormatVersion)
	}
	return &StoredDiff{Dir: dir, Manifest: m}, nil
}

// BlockFiles returns the paths of the block files of a pair, in block order.
func (r *StoredDiff) BlockFiles(p DiffResultPair) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(r.Dir, p.Dir))
	if err != nil {
		return nil, fmt.Errorf("read the block files of %s: %w", p.Pair, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
			files = append(files, filepath.Join(r.Dir, p.Dir, e.Name()))
		}
	}
	// A result that lost or got a block file after the run would give a
	// report with other rows than the run found.
	if len(files) != p.Blocks {
		return nil, fmt.Errorf("pair %s has %d block files in %s, but the manifest lists %d",
			p.Pair, len(files), filepath.Join(r.Dir, p.Dir), p.Blocks)
	}
	sort.Strings(files)
	return files, nil
}

// ReadDiffBlock calls fn for every entry of a block file, in file order.
func ReadDiffBlock(path string, fn func(e *DiffEntryLine) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rd := bufio.NewReaderSize(f, 256*1024)
	for n := 1; ; n++ {
		line, err := rd.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var e DiffEntryLine
			if uerr := json.Unmarshal(line, &e); uerr != nil {
				return fmt.Errorf("%s, line %d: %w", path, n, uerr)
			}
			if ferr := fn(&e); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
	}
}

// jsonArray writes the items of one JSON array, each on its own line.
type jsonArray struct {
	w     *bufio.Writer
	empty bool
}

func (a *jsonArray) add(item []byte) {
	if a.empty {
		a.w.WriteByte('\n')
		a.empty = false
	} else {
		a.w.WriteString(",\n")
	}
	a.w.Write(item)
}

// spool is a temporary file that holds one array until it is copied into
// the report.
type spool struct {
	f   *os.File
	arr jsonArray
}

func newSpool(dir string) (*spool, error) {
	f, err := os.CreateTemp(dir, ".spool-*")
	if err != nil {
		return nil, fmt.Errorf("create a temporary file for the diff report: %w", err)
	}
	return &spool{f: f, arr: jsonArray{w: bufio.NewWriterSize(f, 256*1024), empty: true}}, nil
}

// copyTo appends the items of the spool to out, as the items of an array
// that is still empty. It returns false if the spool has no items.
func (s *spool) copyTo(out *bufio.Writer) (bool, error) {
	if err := s.arr.w.Flush(); err != nil {
		return false, err
	}
	if s.arr.empty {
		return false, nil
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	_, err := io.Copy(out, s.f)
	return true, err
}

func (s *spool) remove() {
	s.f.Close()
	os.Remove(s.f.Name())
}

// writeDiffJSON writes the JSON report of a result to path: the same
// content as a report of format version 1, compact, with every row on a
// line of its own. For each pair, the rows of each node come in block
// order, and the row hashes of each node follow in the same order.
func writeDiffJSON(ctx context.Context, r *StoredDiff, path string) (err error) {
	f, err := CreateFileSecure(path)
	if err != nil {
		return fmt.Errorf("failed to create diffs file: %w", err)
	}
	defer func() {
		if cerr := f.Close(); err == nil && cerr != nil {
			err = fmt.Errorf("failed to close diffs file: %w", cerr)
		}
	}()
	out := bufio.NewWriterSize(f, 1024*1024)
	fmt.Fprintf(out, `{"format_version":%d,"diffs":{`, r.Manifest.RowFormatVersion)
	for i, p := range r.Manifest.Pairs {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteByte('\n')
		if err := writePairJSON(ctx, r, p, out); err != nil {
			return err
		}
	}
	out.WriteString("\n},\"summary\":")
	summary, err := json.Marshal(r.Manifest.Summary)
	if err != nil {
		return fmt.Errorf("failed to write diffs: %w", err)
	}
	out.Write(summary)
	out.WriteString("}\n")
	if err := out.Flush(); err != nil {
		return fmt.Errorf("failed to write diffs: %w", err)
	}
	return nil
}

// writePairJSON writes `"pair":{"rows":{...},"row_hashes":{...}}` for one
// pair. The nodes of the pair are written sorted by name. The block files
// are read once: the rows of the first node go straight to out, the other
// three arrays to temporary files that are copied in at the end.
func writePairJSON(ctx context.Context, r *StoredDiff, p DiffResultPair, out *bufio.Writer) error {
	files, err := r.BlockFiles(p)
	if err != nil {
		return err
	}
	first, second := p.Node1, p.Node2
	swap := second < first
	if swap {
		first, second = second, first
	}

	// The temporary files go into the result directory, which is still
	// under its temporary name, so that an error leaves nothing behind.
	dir := r.Dir
	var spools []*spool
	defer func() {
		for _, s := range spools {
			s.remove()
		}
	}()
	mk := func() (*spool, error) {
		s, err := newSpool(dir)
		if err == nil {
			spools = append(spools, s)
		}
		return s, err
	}
	rows2, err := mk()
	if err != nil {
		return err
	}
	hash1, err := mk()
	if err != nil {
		return err
	}
	hash2, err := mk()
	if err != nil {
		return err
	}

	key, _ := json.Marshal(p.Pair)
	n1, _ := json.Marshal(first)
	n2, _ := json.Marshal(second)
	out.Write(key)
	out.WriteString(`:{"rows":{`)
	out.Write(n1)
	out.WriteString(":[")
	rows1 := jsonArray{w: out, empty: true}

	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := ReadDiffBlock(file, func(e *DiffEntryLine) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			r1, h1, r2, h2 := e.Row1, e.Hash1, e.Row2, e.Hash2
			if swap {
				r1, h1, r2, h2 = r2, h2, r1, h1
			}
			if len(r1) > 0 {
				rows1.add(r1)
				hash1.arr.add(quoted(h1))
			}
			if len(r2) > 0 {
				rows2.arr.add(r2)
				hash2.arr.add(quoted(h2))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if !rows1.empty {
		out.WriteByte('\n')
	}
	out.WriteString("],")
	out.Write(n2)
	out.WriteString(":[")
	if err := copyArray(rows2, out); err != nil {
		return err
	}
	// A result without a hash version has no row hashes.
	if r.Manifest.HashVersion <= 0 {
		out.WriteString("]}}")
		return nil
	}
	out.WriteString(`]},"row_hashes":{`)
	out.Write(n1)
	out.WriteString(":[")
	if err := copyArray(hash1, out); err != nil {
		return err
	}
	out.WriteString("],")
	out.Write(n2)
	out.WriteString(":[")
	if err := copyArray(hash2, out); err != nil {
		return err
	}
	out.WriteString("]}}")
	return nil
}

func copyArray(s *spool, out *bufio.Writer) error {
	copied, err := s.copyTo(out)
	if err != nil {
		return fmt.Errorf("failed to write diffs: %w", err)
	}
	if copied {
		out.WriteByte('\n')
	}
	return nil
}

func quoted(s string) []byte {
	data, _ := json.Marshal(s)
	return data
}
