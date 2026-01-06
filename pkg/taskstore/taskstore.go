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

package taskstore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	StatusPending   = "PENDING"
	StatusRunning   = "RUNNING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

const (
	TaskTypeTableDiff          = "TABLE_DIFF"
	TaskTypeTableRepair        = "TABLE_REPAIR"
	TaskTypeTableRerun         = "TABLE_RERUN"
	TaskTypeSchemaDiff         = "SCHEMA_DIFF"
	TaskTypeRepsetDiff         = "REPSET_DIFF"
	TaskTypeSpockDiff          = "SPOCK_DIFF"
	TaskTypeMtreeBuild         = "MTREE_BUILD"
	TaskTypeMtreeUpdate        = "MTREE_UPDATE"
	TaskTypeMtreeDiff          = "MTREE_DIFF"
	TaskTypeMtreeInit          = "MTREE_INIT"
	TaskTypeMtreeTeardown      = "MTREE_TEARDOWN"
	TaskTypeMtreeTeardownTable = "MTREE_TEARDOWN_TABLE"
	TaskTypeMtreeListen        = "MTREE_LISTEN"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS ace_tasks (
    task_id        TEXT PRIMARY KEY,
    task_type      TEXT NOT NULL,
    task_status    TEXT NOT NULL,
    cluster_name   TEXT NOT NULL,
    task_context   TEXT,
    schema         TEXT,
    table_name     TEXT,
    repset_name    TEXT,
    diff_file_path TEXT,
    started_at     TEXT,
    finished_at    TEXT,
    time_taken     REAL
);`

var ErrNotFound = errors.New("task not found")

type Store struct {
	db *sql.DB
}

type Record struct {
	TaskID         string
	TaskType       string
	Status         string
	ClusterName    string
	SchemaName     string
	TableName      string
	RepsetName     string
	DiffFilePath   string
	StartedAt      time.Time
	FinishedAt     time.Time
	TimeTaken      float64
	TaskContext    map[string]any
	RawTaskContext string
}

type Recorder struct {
	store     *Store
	ownsStore bool
	created   bool
}

func NewRecorder(existing *Store, path string) (*Recorder, error) {
	if existing != nil {
		return &Recorder{store: existing}, nil
	}
	store, err := New(path)
	if err != nil {
		return nil, err
	}
	return &Recorder{store: store, ownsStore: true}, nil
}

func (r *Recorder) Store() *Store {
	if r == nil {
		return nil
	}
	return r.store
}

func (r *Recorder) OwnsStore() bool {
	if r == nil {
		return false
	}
	return r.ownsStore
}

func (r *Recorder) HasStore() bool {
	return r != nil && r.store != nil
}

func (r *Recorder) Created() bool {
	return r != nil && r.created
}

func (r *Recorder) Create(rec Record) error {
	if !r.HasStore() {
		return nil
	}
	if err := r.store.Create(rec); err != nil {
		return err
	}
	r.created = true
	return nil
}

func (r *Recorder) Update(rec Record) error {
	if !r.HasStore() || !r.created {
		return nil
	}
	return r.store.Update(rec)
}

func (r *Recorder) Close() error {
	if !r.OwnsStore() || r.store == nil {
		return nil
	}
	err := r.store.Close()
	r.store = nil
	return err
}

func New(path string) (*Store, error) {
	sqlitePath := resolvePath(path)
	if err := ensureDir(sqlitePath); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}

	db, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &Store{db: db}
	if err := s.ensureSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Get(taskID string) (Record, error) {
	var rec Record
	if strings.TrimSpace(taskID) == "" {
		return rec, fmt.Errorf("task id is required")
	}
	row := s.db.QueryRow(
		`SELECT task_id, task_type, task_status, cluster_name, task_context,
                schema, table_name, repset_name, diff_file_path,
                started_at, finished_at, time_taken
         FROM ace_tasks WHERE task_id = ?`, taskID)

	var (
		ctxVal      sql.NullString
		startedAt   sql.NullString
		finishedAt  sql.NullString
		diffFile    sql.NullString
		schemaName  sql.NullString
		tableName   sql.NullString
		repsetName  sql.NullString
		taskContext map[string]any
	)
	if err := row.Scan(
		&rec.TaskID,
		&rec.TaskType,
		&rec.Status,
		&rec.ClusterName,
		&ctxVal,
		&schemaName,
		&tableName,
		&repsetName,
		&diffFile,
		&startedAt,
		&finishedAt,
		&rec.TimeTaken,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, ErrNotFound
		}
		return Record{}, fmt.Errorf("fetch task %s: %w", taskID, err)
	}

	if schemaName.Valid {
		rec.SchemaName = schemaName.String
	}
	if tableName.Valid {
		rec.TableName = tableName.String
	}
	if repsetName.Valid {
		rec.RepsetName = repsetName.String
	}
	if diffFile.Valid {
		rec.DiffFilePath = diffFile.String
	}
	if startedAt.Valid {
		if t, err := time.Parse(time.RFC3339Nano, startedAt.String); err == nil {
			rec.StartedAt = t
		}
	}
	if finishedAt.Valid {
		if t, err := time.Parse(time.RFC3339Nano, finishedAt.String); err == nil {
			rec.FinishedAt = t
		}
	}
	if ctxVal.Valid && strings.TrimSpace(ctxVal.String) != "" {
		rec.RawTaskContext = ctxVal.String
		if err := json.Unmarshal([]byte(ctxVal.String), &taskContext); err == nil {
			rec.TaskContext = taskContext
		}
	}

	return rec, nil
}

func (s *Store) Create(rec Record) error {
	if err := rec.validateForCreate(); err != nil {
		return err
	}
	ctxVal, err := rec.contextValue()
	if err != nil {
		return fmt.Errorf("marshal task context: %w", err)
	}

	_, err = s.db.Exec(
		`INSERT INTO ace_tasks (
            task_id, task_type, task_status, cluster_name, task_context,
            schema, table_name, repset_name, diff_file_path,
            started_at, finished_at, time_taken
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.TaskID,
		rec.TaskType,
		rec.Status,
		rec.ClusterName,
		ctxVal,
		nullableString(rec.SchemaName),
		nullableString(rec.TableName),
		nullableString(rec.RepsetName),
		nullableString(rec.DiffFilePath),
		timeOrNil(rec.StartedAt),
		timeOrNil(rec.FinishedAt),
		rec.TimeTaken,
	)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return nil
}

func (s *Store) Update(rec Record) error {
	if strings.TrimSpace(rec.TaskID) == "" {
		return errors.New("task id is required")
	}
	ctxVal, err := rec.contextValue()
	if err != nil {
		return fmt.Errorf("marshal task context: %w", err)
	}

	res, err := s.db.Exec(
		`UPDATE ace_tasks SET
            task_status = ?,
            task_context = ?,
            diff_file_path = ?,
            finished_at = ?,
            time_taken = ?
        WHERE task_id = ?`,
		rec.Status,
		ctxVal,
		nullableString(rec.DiffFilePath),
		timeOrNil(rec.FinishedAt),
		rec.TimeTaken,
		rec.TaskID,
	)
	if err != nil {
		return fmt.Errorf("update task: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ensureSchema() error {
	if _, err := s.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("ensure ace_tasks schema: %w", err)
	}
	return nil
}

func (r Record) validateForCreate() error {
	if strings.TrimSpace(r.TaskID) == "" {
		return errors.New("task id is required")
	}
	if strings.TrimSpace(r.TaskType) == "" {
		return errors.New("task type is required")
	}
	if strings.TrimSpace(r.Status) == "" {
		return errors.New("task status is required")
	}
	if strings.TrimSpace(r.ClusterName) == "" {
		return errors.New("cluster name is required")
	}
	return nil
}

func (r Record) contextValue() (any, error) {
	if len(r.TaskContext) > 0 {
		blob, err := json.Marshal(r.TaskContext)
		if err != nil {
			return nil, err
		}
		return string(blob), nil
	}
	if strings.TrimSpace(r.RawTaskContext) != "" {
		return r.RawTaskContext, nil
	}
	return nil, nil
}

func resolvePath(path string) string {
	if strings.TrimSpace(path) != "" {
		return path
	}
	if env := os.Getenv("ACE_TASKS_DB"); strings.TrimSpace(env) != "" {
		return env
	}
	return filepath.Join(".", "ace_tasks.db")
}

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func nullableString(val string) any {
	if strings.TrimSpace(val) == "" {
		return nil
	}
	return val
}

func timeOrNil(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}
