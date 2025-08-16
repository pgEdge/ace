/////////////////////////////////////////////////////////////////////////////
//
// ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the pgEdge Community License:
//      https://www.pgedge.com/communitylicense
//
/////////////////////////////////////////////////////////////////////////////

package queries

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/types"
)

/*
TODO: Several queries in this file need to be updated with the correct placeholders
and parameters. For now, I've just used temporary names.
*/

type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

type Templates struct {
	EstimateRowCount     *template.Template
	GetPrimaryKey        *template.Template
	GetColumnTypes       *template.Template
	GetColumns           *template.Template
	CheckUserPrivileges  *template.Template
	SpockNodeAndSubInfo  *template.Template
	SpockRepSetInfo      *template.Template
	CheckSchemaExists    *template.Template
	GetTablesInSchema    *template.Template
	GetViewsInSchema     *template.Template
	GetFunctionsInSchema *template.Template
	GetIndicesInSchema   *template.Template
	CheckRepSetExists    *template.Template
	GetTablesInRepSet    *template.Template
	GetPkeyColumnTypes   *template.Template

	CreateMetadataTable             *template.Template
	GetPkeyOffsets                  *template.Template
	CreateSimpleMtreeTable          *template.Template
	CreateIndex                     *template.Template
	CreateCompositeType             *template.Template
	DropCompositeType               *template.Template
	CreateCompositeMtreeTable       *template.Template
	InsertCompositeBlockRanges      *template.Template
	CreateXORFunction               *template.Template
	GetPkeyType                     *template.Template
	UpdateMetadata                  *template.Template
	InsertBlockRanges               *template.Template
	InsertBlockRangesBatchSimple    *template.Template
	InsertBlockRangesBatchComposite *template.Template
	TDBlockHashSQL                  *template.Template
	MtreeLeafHashSQL                *template.Template
	UpdateLeafHashes                *template.Template
	GetBlockRanges                  *template.Template
	GetDirtyAndNewBlocks            *template.Template
	ClearDirtyFlags                 *template.Template
	BuildParentNodes                *template.Template
	GetRootNode                     *template.Template
	GetNodeChildren                 *template.Template
	GetLeafRanges                   *template.Template
	GetRowCountEstimate             *template.Template
	GetMaxValComposite              *template.Template
	UpdateMaxVal                    *template.Template
	GetMaxValSimple                 *template.Template
	GetCountComposite               *template.Template
	GetCountSimple                  *template.Template
	GetSplitPointComposite          *template.Template
	GetSplitPointSimple             *template.Template
	DeleteParentNodes               *template.Template
	GetMaxNodePosition              *template.Template
	UpdateBlockRangeEnd             *template.Template
	UpdateNodePositionsTemp         *template.Template
	DeleteBlock                     *template.Template
	UpdateNodePositionsSequential   *template.Template
	FindBlocksToSplit               *template.Template
	FindBlocksToMerge               *template.Template
	FindBlocksToMergeExpanded       *template.Template
	GetBlockCountComposite          *template.Template
	GetBlockCountSimple             *template.Template
	GetBlockSizeFromMetadata        *template.Template
	GetMaxNodeLevel                 *template.Template
	CompareBlocksSQL                *template.Template
	DropXORFunction                 *template.Template
	DropMetadataTable               *template.Template
	DropMtreeTable                  *template.Template
	GetBlockRowCount                *template.Template
	GetBlockWithCount               *template.Template
	GetBlockWithCountExpanded       *template.Template
	UpdateNodePosition              *template.Template
	GetMaxColumnSize                *template.Template
	UpdateBlockRangeStart           *template.Template
	GetMinValComposite              *template.Template
	GetMinValSimple                 *template.Template
	GetDirtyAndNewBlocksExpanded    *template.Template
	FindBlocksToSplitExpanded       *template.Template
	ResetPositionsByStart           *template.Template
	ResetPositionsByStartExpanded   *template.Template
	GetBulkSplitPoints              *template.Template
	UpdateBlockRangeStartComposite  *template.Template
	UpdateBlockRangeEndComposite    *template.Template
	UpdateNodePositionsWithOffset   *template.Template
}

var SQLTemplates = Templates{
	// A template isn't needed for this query; just keeping the struct uniform
	CreateMetadataTable: template.Must(template.New("createMetadataTable").Parse(`
		CREATE TABLE IF NOT EXISTS ace_mtree_metadata (
			schema_name text,
			table_name text,
			total_rows bigint,
			block_size int,
			num_blocks int,
			is_composite boolean NOT NULL DEFAULT false,
			last_updated timestamptz,
			PRIMARY KEY (schema_name, table_name)
		)`),
	),
	GetPrimaryKey: template.Must(template.New("getPrimaryKey").Parse(`
		SELECT
			kcu.column_name
		FROM
			information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE
			tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2;
	`)),
	GetColumnTypes: template.Must(template.New("getColumnTypes").Parse(`
		SELECT
			a.attname AS column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
		FROM
			pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
			JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
			LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE
			c.relname = $1
			AND a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY
			a.attnum;
	`)),
	GetColumns: template.Must(template.New("getColumns").Parse(`
		SELECT
			column_name
		FROM
			information_schema.columns
		WHERE
			table_schema = $1
			AND table_name = $2;
	`)),
	CheckUserPrivileges: template.Must(template.New("checkUserPrivileges").Parse(`
		WITH params AS (
			SELECT
				$1 :: text AS username,
				$2 :: text AS schema_name,
				$3 :: text AS table_name
		),
		table_check AS (
			SELECT
				c.relname AS table_name,
				n.nspname AS table_schema
			FROM
				pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE
				n.nspname = (
					SELECT
						schema_name
					FROM
						params
				)
				AND c.relname = (
					SELECT
						table_name
					FROM
						params
				)
		)
		SELECT
			CASE
				WHEN EXISTS (
					SELECT
						1
					FROM
						table_check
				) THEN has_table_privilege(
					(
						SELECT
							username
						FROM
							params
					),
					(
						SELECT
							quote_ident(table_schema) || '.' || quote_ident(table_name)
						FROM
							table_check
					),
					'SELECT'
				)
				ELSE FALSE
			END AS table_select,
			has_schema_privilege(
				(
					SELECT
						username
					FROM
						params
				),
				(
					SELECT
						schema_name
					FROM
						params
				),
				'CREATE'
			) AS table_create,
			CASE
				WHEN EXISTS (
					SELECT
						1
					FROM
						table_check
				) THEN has_table_privilege(
					(
						SELECT
							username
						FROM
							params
					),
					(
						SELECT
							quote_ident(table_schema) || '.' || quote_ident(table_name)
						FROM
							table_check
					),
					'INSERT'
				)
				ELSE FALSE
			END AS table_insert,
			CASE
				WHEN EXISTS (
					SELECT
						1
					FROM
						table_check
				) THEN has_table_privilege(
					(
						SELECT
							username
						FROM
							params
					),
					(
						SELECT
							quote_ident(table_schema) || '.' || quote_ident(table_name)
						FROM
							table_check
					),
					'UPDATE'
				)
				ELSE FALSE
			END AS table_update,
			CASE
				WHEN EXISTS (
					SELECT
						1
					FROM
						table_check
				) THEN has_table_privilege(
					(
						SELECT
							username
						FROM
							params
					),
					(
						SELECT
							quote_ident(table_schema) || '.' || quote_ident(table_name)
						FROM
							table_check
					),
					'DELETE'
				)
				ELSE FALSE
			END AS table_delete,
			has_table_privilege(
				(
					SELECT
						username
					FROM
						params
				),
				'information_schema.columns',
				'SELECT'
			) AS columns_select,
			has_table_privilege(
				(
					SELECT
						username
					FROM
						params
				),
				'information_schema.table_constraints',
				'SELECT'
			) AS table_constraints_select,
			has_table_privilege(
				(
					SELECT
						username
					FROM
						params
				),
				'information_schema.key_column_usage',
				'SELECT'
			) AS key_column_usage_select;
	`)),
	SpockNodeAndSubInfo: template.Must(template.New("spockNodeAndSubInfo").Parse(`
		SELECT
			n.node_id,
			n.node_name,
			n.location,
			n.country,
			s.sub_id,
			s.sub_name,
			s.sub_enabled,
			s.sub_replication_sets
		FROM
			spock.node n
			LEFT OUTER JOIN spock.subscription s ON s.sub_target = n.node_id
		WHERE
			s.sub_name IS NOT NULL;
	`)),
	SpockRepSetInfo: template.Must(template.New("spockRepSetInfo").Parse(`
		SELECT
			set_name,
			array_agg(nspname || '.' || relname ORDER BY nspname, relname) as relname
		FROM (
			SELECT
				set_name,
				nspname,
				relname
			FROM
				spock.tables
			ORDER BY
				set_name, nspname, relname
		) subquery
		GROUP BY
			set_name
		ORDER BY
			set_name;
	`)),
	CheckSchemaExists: template.Must(template.New("checkSchemaExists").Parse(
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1);`,
	)),
	GetTablesInSchema: template.Must(template.New("getTablesInSchema").Parse(`
		SELECT
			table_name
		FROM
			information_schema.tables
		WHERE
			table_schema = $1
			AND table_type = 'BASE TABLE';
	`)),
	GetViewsInSchema: template.Must(template.New("getViewsInSchema").Parse(`
		SELECT
			table_name
		FROM
			information_schema.views
		WHERE
			table_schema = $1;
	`)),
	GetFunctionsInSchema: template.Must(template.New("getFunctionsInSchema").Parse(`
		SELECT
			p.proname || '(' || COALESCE(pg_get_function_identity_arguments(p.oid), '') || ')' as function_signature
		FROM
			pg_proc p
			LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE
			n.nspname = $1;
	`)),
	GetIndicesInSchema: template.Must(template.New("getIndicesInSchema").Parse(
		`SELECT indexname FROM pg_indexes WHERE schemaname = $1;`,
	)),
	CheckRepSetExists: template.Must(template.New("checkRepSetExists").Parse(
		`SELECT set_name FROM spock.replication_set WHERE set_name = $1;`,
	)),
	GetTablesInRepSet: template.Must(template.New("getTablesInRepSet").Parse(
		`SELECT concat_ws('.', nspname, relname) FROM spock.tables where set_name = $1;`,
	)),
	GetPkeyColumnTypes: template.Must(template.New("getPkeyColumnTypes").Parse(`
		SELECT
			a.attname,
			pg_catalog.format_type(a.atttypid, a.atttypmod)
		FROM
			pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
			JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE
			n.nspname = $1
			AND c.relname = $2
			AND a.attname = ANY($3::text[])
			AND a.attnum > 0 AND NOT a.attisdropped;
	`)),
	GetPkeyOffsets: template.Must(template.New("pkeyOffsets").Parse(`
		WITH sampled_data AS (
			SELECT
				{{.KeyColumnsSelect}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
			TABLESAMPLE {{.TableSampleMethod}}({{.SamplePercent}})
			ORDER BY
				{{.KeyColumnsOrder}}
		),
		first_row AS (
			SELECT
				{{.KeyColumnsSelect}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
			ORDER BY
				{{.KeyColumnsOrder}}
			LIMIT 1
		),
		last_row AS (
			SELECT
				{{.KeyColumnsSelect}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
			ORDER BY
				{{.KeyColumnsOrderDesc}}
			LIMIT 1
		),
		sample_boundaries AS (
			SELECT
				{{.KeyColumnsSelect}},
				ntile({{.NtileCount}}) OVER (
					ORDER BY
						{{.KeyColumnsOrder}}
				) as bucket
			FROM
				sampled_data
		),
		block_starts AS (
			SELECT
				DISTINCT ON (bucket) {{.KeyColumnsSelect}}
			FROM
				sample_boundaries
			ORDER BY
				bucket,
				{{.KeyColumnsOrder}}
		),
		all_bounds AS (
			SELECT
				{{.FirstRowSelects}},
				0 as seq
			UNION ALL
			SELECT
				{{.KeyColumnsSelect}},
				1 as seq
			FROM
				block_starts
			WHERE
				ROW({{.KeyColumnsSelect}}) > {{.FirstRowTupleSelects}}
			UNION ALL
			SELECT
				{{.LastRowSelects}},
				2 as seq
		),
		ranges AS (
			SELECT
				{{.KeyColumnsSelect}},
				{{.RangeStartColumns}},
				{{.RangeEndColumns}},
				seq
			FROM
				all_bounds
		)
		SELECT
			{{.RangeOutputColumns}}
		FROM
			ranges
		ORDER BY
			seq;
	`)),
	CreateSimpleMtreeTable: template.Must(template.New("createSimpleMtreeTable").Parse(`
		CREATE TABLE {{.MtreeTable}} (
			node_level integer NOT NULL,
			node_position bigint NOT NULL,
			range_start {{.PkeyType}},
			range_end {{.PkeyType}},
			leaf_hash bytea,
			node_hash bytea,
			dirty boolean DEFAULT false,
			inserts_since_tree_update bigint DEFAULT 0,
			deletes_since_tree_update bigint DEFAULT 0,
			last_modified timestamptz DEFAULT current_timestamp,
			PRIMARY KEY (node_level, node_position)
		)`),
	),
	CreateIndex: template.Must(template.New("createIndex").Parse(`
		CREATE INDEX IF NOT EXISTS {{.IndexName}}
		ON {{.MtreeTable}} (range_start, range_end)
		WHERE
			node_level = 0;
	`)),
	CreateCompositeType: template.Must(template.New("createCompositeType").Parse(`
		CREATE TYPE {{.CompositeTypeName}} AS (
			{{.KeyTypeColumns}}
		)`),
	),
	DropCompositeType: template.Must(template.New("dropCompositeType").Parse(`
		DROP TYPE IF EXISTS {{.CompositeTypeName}} CASCADE;
	`)),
	CreateCompositeMtreeTable: template.Must(template.New("createCompositeMtreeTable").Parse(`
		CREATE TABLE {{.MtreeTable}} (
			node_level integer NOT NULL,
			node_position bigint NOT NULL,
			range_start {{.CompositeTypeName}},
			range_end {{.CompositeTypeName}},
			leaf_hash bytea,
			node_hash bytea,
			dirty boolean DEFAULT false,
			inserts_since_tree_update bigint DEFAULT 0,
			deletes_since_tree_update bigint DEFAULT 0,
			last_modified timestamptz DEFAULT current_timestamp,
			PRIMARY KEY (node_level, node_position)
		)`),
	),
	InsertCompositeBlockRanges: template.Must(template.New("insertCompositeBlockRanges").Parse(`
		INSERT INTO
			{{.MtreeTable}} (node_level, node_position, range_start, range_end)
		VALUES
			(0, $1, {{.StartExpr}}, {{.EndExpr}});
	`)),
	CreateXORFunction: template.Must(template.New("createXORFunction").Parse(`
		CREATE
		OR REPLACE FUNCTION bytea_xor(a bytea, b bytea) RETURNS bytea AS $$
		DECLARE
			result bytea;
			len int;
		BEGIN
			IF length(a) != length(b) THEN
				RAISE EXCEPTION 'bytea_xor inputs must be same length';
			END IF;
			len := length(a);
			result := a;
			FOR i IN 0..len - 1 LOOP
			result := set_byte(result, i, get_byte(a, i) # get_byte(b, i));
			END LOOP;
			RETURN result;
		END;
		$$ LANGUAGE plpgsql IMMUTABLE STRICT;
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT
					1
				FROM
					pg_operator
				WHERE
					oprname = '#'
					AND oprleft = 'bytea'::regtype
					AND oprright = 'bytea'::regtype
			) THEN
			CREATE OPERATOR # (
				LEFTARG = bytea,
				RIGHTARG = bytea,
				PROCEDURE = bytea_xor
			);
			END IF;
		END $$;
	`)),
	EstimateRowCount: template.Must(template.New("estimateRowCount").Parse(`
		SELECT
			(
				CASE
					WHEN s.n_live_tup > 0 THEN s.n_live_tup
					WHEN c.reltuples > 0 THEN c.reltuples
					ELSE pg_relation_size(c.oid) / (8192 * 0.7)
				END
			)::bigint as estimate
		FROM
			pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_stat_user_tables s ON s.schemaname = n.nspname
			AND s.relname = c.relname
		WHERE
			n.nspname = $1
			AND c.relname = $2
	`)),
	GetPkeyType: template.Must(template.New("getPkeyType").Parse(`
		SELECT
			a.atttypid::regtype::text
		FROM
			pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE
			n.nspname = $1
			AND c.relname = $2
			AND a.attname = $3
	`)),
	UpdateMetadata: template.Must(template.New("updateMetadata").Parse(`
		INSERT INTO
			ace_mtree_metadata (
				schema_name,
				table_name,
				total_rows,
				block_size,
				num_blocks,
				is_composite,
				last_updated
			)
		VALUES
			(
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				current_timestamp
			)
		ON CONFLICT (schema_name, table_name) DO
		UPDATE
		SET
			total_rows = EXCLUDED.total_rows,
			block_size = EXCLUDED.block_size,
			num_blocks = EXCLUDED.num_blocks,
			is_composite = EXCLUDED.is_composite,
			last_updated = EXCLUDED.last_updated
	`)),
	InsertBlockRanges: template.Must(template.New("insertBlockRanges").Parse(`
		INSERT INTO
			{{.MtreeTable}} (
				node_level,
				node_position,
				range_start,
				range_end,
				last_modified
			)
		VALUES
			(0, $1, $2, $3, current_timestamp)
	`)),
	InsertBlockRangesBatchSimple: template.Must(template.New("insertBlockRangesBatchSimple").Parse(`
        INSERT INTO {{.MtreeTable}} (node_level, node_position, range_start, range_end, last_modified)
        VALUES
        {{- range $i, $r := .Rows}}{{if $i}},{{end}}
        (0, {{$r.NodePos}}, {{$r.Start}}, {{$r.End}}, current_timestamp)
        {{- end }}
    `)),
	InsertBlockRangesBatchComposite: template.Must(template.New("insertBlockRangesBatchComposite").Parse(`
        INSERT INTO {{.MtreeTable}} (node_level, node_position, range_start, range_end, last_modified)
        VALUES
        {{- range $i, $r := .Rows}}{{if $i}},{{end}}
        (0, {{$r.NodePos}}, ROW({{$r.StartList}}), ROW({{$r.EndList}}), current_timestamp)
        {{- end }}
    `)),
	TDBlockHashSQL: template.Must(template.New("tdBlockHashSQL").Parse(`
        SELECT encode(digest(COALESCE(string_agg({{.TableAlias}}::text, '|' ORDER BY {{.PkOrderByStr}}), 'EMPTY_BLOCK'), 'sha256'), 'hex')
        FROM {{.SchemaIdent}}.{{.TableIdent}} AS {{.TableAlias}}
        WHERE ($1::boolean OR {{.PkComparisonExpression}} >= {{.StartValueExpression}})
        AND ({{.SkipMaxIdx}}::boolean OR {{.PkComparisonExpression}} < {{.EndValueExpression}})
    `)),
	MtreeLeafHashSQL: template.Must(template.New("mtreeLeafHashSQL").Parse(`
        SELECT digest(COALESCE(string_agg({{.TableAlias}}::text, '|' ORDER BY {{.PkOrderByStr}}), 'EMPTY_BLOCK'), 'sha256')
        FROM {{.SchemaIdent}}.{{.TableIdent}} AS {{.TableAlias}}
        WHERE ($1::boolean OR {{.PkComparisonExpression}} >= {{.StartValueExpression}})
        AND ({{.SkipMaxIdx}}::boolean OR {{.PkComparisonExpression}} <= {{.EndValueExpression}})
    `)),
	UpdateLeafHashes: template.Must(template.New("updateLeafHashes").Parse(`
		UPDATE
			{{.MtreeTable}} mt
		SET
			leaf_hash = $1,
			node_hash = $1,
			last_modified = current_timestamp
		WHERE
			node_position = $2
			AND mt.node_level = 0
		RETURNING
			mt.node_position
	`)),
	GetBlockRanges: template.Must(template.New("getBlockRanges").Parse(`
		SELECT
			node_position,
			range_start,
			range_end
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = 0
		ORDER BY
			node_position
	`)),
	GetDirtyAndNewBlocks: template.Must(template.New("getDirtyAndNewBlocks").Parse(`
		SELECT
			node_position,
			range_start,
			range_end
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = 0
			AND (
				dirty = true
				OR leaf_hash IS NULL
			)
		ORDER BY
			node_position
	`)),
	ClearDirtyFlags: template.Must(template.New("clearDirtyFlags").Parse(`
		UPDATE
			{{.MtreeTable}}
		SET
			dirty = false,
			inserts_since_tree_update = 0,
			deletes_since_tree_update = 0,
			last_modified = current_timestamp
		WHERE
			node_level = 0
			AND node_position = ANY($1)
	`)),
	BuildParentNodes: template.Must(template.New("buildParentNodes").Parse(`
		WITH pairs AS (
			SELECT
				node_level,
				node_position / 2 as parent_position,
				array_agg(node_hash ORDER BY node_position) as child_hashes
			FROM
				{{.MtreeTable}}
			WHERE
				node_level = $1
			GROUP BY
				node_level,
				node_position / 2
		),
		inserted AS (
			INSERT INTO
				{{.MtreeTable}} (
					node_level,
					node_position,
					node_hash,
					last_modified
				)
			SELECT
				$1 + 1,
				parent_position,
				CASE
					WHEN array_length(child_hashes, 1) = 1 THEN child_hashes[1]
					ELSE child_hashes[1] # child_hashes[2]
				END,
				current_timestamp
			FROM
				pairs
			RETURNING
				1
		)
		SELECT
			count(*)
		FROM
			inserted
	`)),
	GetRootNode: template.Must(template.New("getRootNode").Parse(`
		SELECT
			node_position,
			node_hash
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = (
				SELECT
					MAX(node_level)
				FROM
					{{.MtreeTable}}
			)
	`)),
	GetNodeChildren: template.Must(template.New("getNodeChildren").Parse(`
		SELECT
			node_level,
			node_position,
			node_hash
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = $1 - 1
			AND node_position / 2 = $2
		ORDER BY
			node_position
	`)),
	GetLeafRanges: template.Must(template.New("getLeafRanges").Parse(`
		SELECT
			range_start,
			range_end
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = 0
			AND node_position = ANY($1)
		ORDER BY
			node_position
	`)),
	GetRowCountEstimate: template.Must(template.New("getRowCountEstimate").Parse(`
		SELECT
			total_rows
		FROM
			ace_mtree_metadata
		WHERE
			schema_name = $1
			AND table_name = $2
	`)),
	GetMaxValComposite: template.Must(template.New("getMaxValComposite").Parse(`
		SELECT
			{{.PkeyCols}}
		FROM
			{{.SchemaIdent}}.{{.TableIdent}}
		WHERE
			({{.PkeyCols}}) >= ({{.PkeyValues}})
		ORDER BY
			({{.PkeyCols}}) DESC
		LIMIT
			1
	`)),
	UpdateMaxVal: template.Must(template.New("updateMaxVal").Parse(`
		UPDATE
			{{.MtreeTable}}
		SET
			range_end = $1
		WHERE
			node_level = 0
			AND node_position = $2
	`)),
	GetMaxValSimple: template.Must(template.New("getMaxValSimple").Parse(`
		SELECT
			{{.Key}}
		FROM
			{{.SchemaIdent}}.{{.TableIdent}}
		WHERE
			{{.Key}} >= $1
		ORDER BY
			{{.Key}} DESC
		LIMIT
			1
	`)),
	GetCountComposite: template.Must(template.New("getCountComposite").Parse(`
		SELECT
			count(*)
		FROM
			{{.SchemaIdent}}.{{.TableIdent}}
		WHERE
			{{.WhereClause}}
	`)),
	GetCountSimple: template.Must(template.New("getCountSimple").Parse(`
		SELECT
			count(*)
		FROM
			{{.SchemaIdent}}.{{.TableIdent}}
		WHERE
			{{.Key}} >= $1
			AND (
				{{.Key}} < $2
				OR $2::{{.PkeyType}} IS NULL
			)
	`)),
	GetBlockRowCount: template.Must(template.New("getBlockRowCount").Parse(`
		SELECT count(*)
		FROM {{.SchemaIdent}}.{{.TableIdent}}
		WHERE {{.WhereClause}}
	`)),
	GetSplitPointComposite: template.Must(template.New("getSplitPointComposite").Parse(`
		SELECT
			{{.PkeyCols}}
		FROM
			{{.SchemaIdent}}.{{.TableIdent}}
		WHERE
			{{.WhereClause}}
		ORDER BY
			{{.OrderCols}}
		OFFSET
			{{.OffsetPlaceholder}}
		LIMIT
			1
	`)),
	GetSplitPointSimple: template.Must(template.New("getSplitPointSimple").Parse(`
		SELECT
			{{.Key}}
		FROM
			{{.SchemaIdent}}.{{.TableIdent}}
		WHERE
			{{.Key}} >= $1
			AND (
				{{.Key}} < $2
				OR $3::{{.PkeyType}} IS NULL
			)
		ORDER BY
			{{.Key}}
		OFFSET
			$4
		LIMIT
			1
	`)),
	DeleteParentNodes: template.Must(template.New("deleteParentNodes").Parse(`
		DELETE FROM
			{{.MtreeTable}}
		WHERE
			node_level > 0
	`)),
	GetMaxNodePosition: template.Must(template.New("getMaxNodePosition").Parse(`
		SELECT
			MAX(node_position) + 1
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = 0
	`)),
	UpdateBlockRangeEnd: template.Must(template.New("updateBlockRangeEnd").Parse(`
		UPDATE
			{{.MtreeTable}}
		SET
			range_end = {{.RangeEndExpr}},
			dirty = true,
			last_modified = current_timestamp
		WHERE
			node_level = 0
			AND node_position = {{.NodePosition}}
	`)),
	UpdateNodePositionsTemp: template.Must(template.New("updateNodePositionsTemp").Parse(`
		UPDATE
			{{.MtreeTable}}
		SET
			node_position = node_position + $1
		WHERE
			node_level = 0
			AND node_position > $2
	`)),
	DeleteBlock: template.Must(template.New("deleteBlock").Parse(`
		DELETE FROM
			{{.MtreeTable}}
		WHERE
			node_level = 0
			AND node_position = $1
	`)),
	UpdateNodePositionsSequential: template.Must(template.New("updateNodePositionsSequential").Parse(`
		UPDATE
			{{.MtreeTable}}
		SET
			node_position = pos_seq
		FROM
			(
				SELECT
					node_position,
					row_number() OVER (
						ORDER BY
							node_position
					) + $1 as pos_seq
				FROM
					{{.MtreeTable}}
				WHERE
					node_level = 0
					AND node_position > $2
			) as seq
		WHERE
			{{.MtreeTable}}.node_position = seq.node_position
			AND node_level = 0
	`)),
	FindBlocksToSplit: template.Must(template.New("findBlocksToSplit").Parse(`
		SELECT
			node_position,
			range_start,
			range_end
		FROM
			{{.MtreeTable}}
		WHERE
			node_level = 0
			AND inserts_since_tree_update >= $1
			AND node_position = ANY($2)
	`)),
	FindBlocksToMerge: template.Must(template.New("findBlocksToMerge").Parse(`
		WITH BlockCounts AS (
			SELECT
				t1.node_position,
				t1.range_start,
				t1.range_end,
				COUNT(t2.*) AS actual_rows
			FROM {{.MtreeTable}} t1
			LEFT JOIN {{.SchemaIdent}}.{{.TableIdent}} t2 ON
				{{if .SimplePrimaryKey}}
					t2.{{index .Key 0}} >= t1.range_start AND (t2.{{index .Key 0}} <= t1.range_end OR t1.range_end IS NULL)
				{{else}}
					ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) >= t1.range_start AND (ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) <= t1.range_end OR t1.range_end IS NULL)
				{{end}}
			WHERE t1.node_position = ANY($1) AND t1.node_level = 0
			GROUP BY t1.node_position, t1.range_start, t1.range_end
		)
		SELECT node_position, range_start, range_end
		FROM BlockCounts
		WHERE actual_rows < $2
		ORDER BY node_position;
	`)),

	FindBlocksToMergeExpanded: template.Must(template.New("findBlocksToMergeExpanded").Parse(`
		WITH BlockCounts AS (
			SELECT
				t1.node_position,
				COUNT(t2.*) AS actual_rows
			FROM {{.MtreeTable}} t1
			LEFT JOIN {{.SchemaIdent}}.{{.TableIdent}} t2 ON
				ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) >= t1.range_start AND (ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) <= t1.range_end OR t1.range_end IS NULL)
			WHERE t1.node_position = ANY($1) AND t1.node_level = 0
			GROUP BY t1.node_position
		)
		SELECT t1.node_position,
			{{.StartAttrs}},
			{{.EndAttrs}}
		FROM {{.MtreeTable}} t1
		JOIN BlockCounts bc ON bc.node_position = t1.node_position
		WHERE bc.actual_rows < $2
		ORDER BY t1.node_position;
	`)),
	GetBlockCountComposite: template.Must(template.New("getBlockCountComposite").Parse(`
		WITH block_data AS (
			SELECT
				node_position,
				range_start,
				range_end
			FROM
				{{.MtreeTable}}
			WHERE
				node_level = 0
				AND node_position = $1
		)
		SELECT
			b.node_position,
			b.range_start,
			b.range_end,
			COUNT(t.*) AS cnt
		FROM
			block_data b
			LEFT JOIN {{.SchemaIdent}}.{{.TableIdent}} t ON ROW({{.PkeyCols}}) >= b.range_start
			AND (
				ROW({{.PkeyCols}}) <= b.range_end
				OR b.range_end IS NULL
			)
		GROUP BY
			b.node_position,
			b.range_start,
			b.range_end
		ORDER BY
			b.node_position
	`)),
	GetBlockCountSimple: template.Must(template.New("getBlockCountSimple").Parse(`
		SELECT
			node_position,
			range_start,
			range_end,
			count(t.{{.Key}})
		FROM
			{{.MtreeTable}} mt
			LEFT JOIN {{.SchemaIdent}}.{{.TableIdent}} t ON t.{{.Key}} >= mt.range_start
			AND (
				t.{{.Key}} <= mt.range_end
				OR mt.range_end IS NULL
			)
		WHERE
			mt.node_level = 0
			AND mt.node_position = $1
		GROUP BY
			mt.node_position,
			mt.range_start,
			mt.range_end
	`)),
	GetBlockSizeFromMetadata: template.Must(template.New("getBlockSizeFromMetadata").Parse(`
		SELECT
			block_size
		FROM
			ace_mtree_metadata
		WHERE
			schema_name = $1
			AND table_name = $2
	`)),
	GetMaxNodeLevel: template.Must(template.New("getMaxNodeLevel").Parse(`
		SELECT
			MAX(node_level)
		FROM
			{{.MtreeTable}}
	`)),
	CompareBlocksSQL: template.Must(template.New("compareBlocksSQL").Parse(`
		SELECT
			*
		FROM
			{{.TableName}}
		WHERE
			{{.WhereClause}}
	`)),
	DropXORFunction: template.Must(template.New("dropXORFunction").Parse(`
		DROP FUNCTION IF EXISTS bytea_xor(bytea, bytea) CASCADE
	`)),
	DropMetadataTable: template.Must(template.New("dropMetadataTable").Parse(`
		DROP TABLE IF EXISTS ace_mtree_metadata CASCADE
	`)),
	DropMtreeTable: template.Must(template.New("dropMtreeTable").Parse(`
		DROP TABLE IF EXISTS {{.MtreeTable}} CASCADE
	`)),
	GetBlockWithCount: template.Must(template.New("getBlockWithCount").Parse(`
		SELECT t1.node_position, t1.range_start, t1.range_end, COUNT(t2.*)
		FROM {{.MtreeTable}} t1
		LEFT JOIN {{.SchemaIdent}}.{{.TableIdent}} t2 ON
			{{if .IsComposite}}
				ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) >= t1.range_start AND (ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) <= t1.range_end OR t1.range_end IS NULL)
			{{else}}
				t2.{{index .Key 0}} >= t1.range_start AND (t2.{{index .Key 0}} <= t1.range_end OR t1.range_end IS NULL)
			{{end}}
		WHERE t1.node_position = $1 AND t1.node_level = 0
		GROUP BY t1.node_position, t1.range_start, t1.range_end
	`)),

	GetBlockWithCountExpanded: template.Must(template.New("getBlockWithCountExpanded").Parse(`
		SELECT t1.node_position,
			{{.StartAttrs}},
			{{.EndAttrs}},
			COUNT(t2.*)
		FROM {{.MtreeTable}} t1
		LEFT JOIN {{.SchemaIdent}}.{{.TableIdent}} t2 ON
			ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) >= t1.range_start AND (ROW({{- range $i, $k := .Key}}{{if $i}}, {{end}}t2.{{$k}}{{end}}) <= t1.range_end OR t1.range_end IS NULL)
		WHERE t1.node_position = $1 AND t1.node_level = 0
		GROUP BY t1.node_position, t1.range_start, t1.range_end
	`)),

	ResetPositionsByStart: template.Must(template.New("resetPositionsByStart").Parse(`
		WITH seq AS (
			SELECT node_position,
			       row_number() OVER (ORDER BY range_start) - 1 AS pos_seq
			FROM {{.MtreeTable}}
			WHERE node_level = 0
		)
		UPDATE {{.MtreeTable}} mt
		SET node_position = s.pos_seq
		FROM seq s
		WHERE mt.node_level = 0 AND mt.node_position = s.node_position
	`)),

	ResetPositionsByStartExpanded: template.Must(template.New("resetPositionsByStartExpanded").Parse(`
		WITH seq AS (
			SELECT node_position,
			       row_number() OVER (ORDER BY {{.StartAttrs}}) - 1 AS pos_seq
			FROM {{.MtreeTable}}
			WHERE node_level = 0
		)
		UPDATE {{.MtreeTable}} mt
		SET node_position = s.pos_seq
		FROM seq s
		WHERE mt.node_level = 0 AND mt.node_position = s.node_position
	`)),
	UpdateNodePosition: template.Must(template.New("updateNodePosition").Parse(`
		UPDATE {{.MtreeTable}}
		SET node_position = $1
		WHERE node_position = $2
	`)),
	GetMaxColumnSize: template.Must(template.New("getMaxColumnSize").Parse(`
		SELECT COALESCE(MAX(octet_length({{.ColumnIdent}})), 0) FROM {{.SchemaIdent}}.{{.TableIdent}}
	`)),
	UpdateBlockRangeStart: template.Must(template.New("updateBlockRangeStart").Parse(`
			UPDATE
				{{.MtreeTable}}
			SET
				range_start = {{.RangeStartExpr}},
				dirty = true,
				last_modified = current_timestamp
			WHERE
				node_level = 0
				AND node_position = {{.NodePosition}}
		`)),
	GetMinValComposite: template.Must(template.New("getMinValComposite").Parse(`
			SELECT
				{{.PkeyCols}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
			ORDER BY
				({{.PkeyCols}}) ASC
			LIMIT
				1
		`)),
	GetMinValSimple: template.Must(template.New("getMinValSimple").Parse(`
			SELECT
				{{.Key}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
			ORDER BY
				{{.Key}} ASC
			LIMIT
				1
		`)),
	GetDirtyAndNewBlocksExpanded: template.Must(template.New("getDirtyAndNewBlocksExpanded").Parse(`
			SELECT
				node_position,
				{{.StartAttrs}},
				{{.EndAttrs}}
			FROM
				{{.MtreeTable}}
			WHERE
				node_level = 0
				AND (
					dirty = true
					OR leaf_hash IS NULL
				)
			ORDER BY
				node_position
		`)),
	FindBlocksToSplitExpanded: template.Must(template.New("findBlocksToSplitExpanded").Parse(`
			SELECT
				node_position,
				{{.StartAttrs}},
				{{.EndAttrs}}
			FROM
				{{.MtreeTable}}
			WHERE
				node_level = 0
				AND inserts_since_tree_update >= $1
				AND node_position = ANY($2)
		`)),
	GetBulkSplitPoints: template.Must(template.New("getBulkSplitPoints").Parse(`
		WITH numbered AS (
			SELECT
				{{.PkeyColsStr}},
				row_number() OVER (ORDER BY {{.PkeyColsStr}}) AS rn
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
			{{if .WhereClause}}WHERE {{.WhereClause}}{{end}}
		)
		SELECT
			{{.PkeyColsStr}}
		FROM
			numbered
		WHERE
			rn % {{.BlockSizePlaceholder}} = 0
		ORDER BY
			{{.PkeyColsStr}}
	`)),
	UpdateBlockRangeStartComposite: template.Must(template.New("updateBlockRangeStartComposite").Parse(`
		UPDATE {{.MtreeTable}}
		SET range_start = {{if .IsNull}}NULL{{else}}ROW({{.Placeholders}})::{{.CompositeTypeName}}{{end}}
		WHERE node_position = {{.NodePositionPlaceholder}}
	`)),
	UpdateBlockRangeEndComposite: template.Must(template.New("updateBlockRangeEndComposite").Parse(`
		UPDATE {{.MtreeTable}}
		SET range_end = {{if .IsNull}}NULL{{else}}ROW({{.Placeholders}})::{{.CompositeTypeName}}{{end}}
		WHERE node_position = {{.NodePositionPlaceholder}}
	`)),
	UpdateNodePositionsWithOffset: template.Must(template.New("updateNodePositionsWithOffset").Parse(`
		UPDATE {{.MtreeTable}} SET node_position = node_position + $1 WHERE node_level = 0
	`)),
}

// For mocking
type DBQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func SanitiseIdentifier(ident string) error {
	if !validIdentifierRegex.MatchString(ident) {
		return fmt.Errorf("invalid identifier: %s", ident)
	}
	return nil
}

func RenderSQL(t *template.Template, data any) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to render SQL: %w", err)
	}
	return buf.String(), nil
}

func MaxColumnSize(ctx context.Context, db DBQuerier, schema, table, column string) (int64, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"ColumnIdent": pgx.Identifier{column}.Sanitize(),
	}

	query, err := RenderSQL(SQLTemplates.GetMaxColumnSize, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render MaxColumnSize SQL: %w", err)
	}

	var maxSize int64
	if err := db.QueryRow(ctx, query).Scan(&maxSize); err != nil {
		return 0, fmt.Errorf(
			"MaxColumnSize query failed for %s.%s.%s: %w",
			schema,
			table,
			column,
			err,
		)
	}

	return maxSize, nil
}

func GeneratePkeyOffsetsQuery(
	schema, table string,
	keyColumns []string,
	tableSampleMethod string,
	samplePercent float64,
	ntileCount int,
) (string, error) {
	if len(keyColumns) == 0 {
		return "", fmt.Errorf("keyColumns cannot be empty")
	}
	for _, ident := range append([]string{schema, table}, keyColumns...) {
		if err := SanitiseIdentifier(ident); err != nil {
			return "", fmt.Errorf("invalid identifier %q: %w", ident, err)
		}
	}
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	tableIdent := pgx.Identifier{table}.Sanitize()

	quotedKeyColsOriginal := make([]string, len(keyColumns))
	for i, c := range keyColumns {
		quotedKeyColsOriginal[i] = pgx.Identifier{c}.Sanitize()
	}

	keyColsSelect := strings.Join(quotedKeyColsOriginal, ",\n        ")
	keyColsOrder := strings.Join(quotedKeyColsOriginal, ", ")

	var descs []string
	for _, c := range keyColumns {
		descs = append(descs, fmt.Sprintf("%s DESC", pgx.Identifier{c}.Sanitize()))
	}
	keyColsOrderDesc := strings.Join(descs, ", ")

	var firstSelects, lastSelects, firstTuples []string
	for _, c := range keyColumns {
		quotedCol := pgx.Identifier{c}.Sanitize()
		firstSelects = append(firstSelects,
			fmt.Sprintf(`(SELECT %s FROM first_row) AS %s`, quotedCol, quotedCol))
		lastSelects = append(lastSelects,
			fmt.Sprintf(`(SELECT %s FROM last_row) AS %s`, quotedCol, quotedCol))
		firstTuples = append(firstTuples,
			fmt.Sprintf(`(SELECT %s FROM first_row)`, quotedCol))
	}

	var rangeStarts, rangeEnds []string
	for _, c := range keyColumns {
		quotedCol := pgx.Identifier{c}.Sanitize()
		aliasStart := fmt.Sprintf(`range_start_%s`, c)
		quotedAliasStart := pgx.Identifier{aliasStart}.Sanitize()

		aliasEnd := fmt.Sprintf(`range_end_%s`, c)
		quotedAliasEnd := pgx.Identifier{aliasEnd}.Sanitize()

		rangeStarts = append(rangeStarts, fmt.Sprintf(`%s AS %s`, quotedCol, quotedAliasStart))
		rangeEnds = append(rangeEnds, fmt.Sprintf(
			`LEAD(%s) OVER (ORDER BY seq, %s) AS %s`,
			quotedCol, keyColsOrder, quotedAliasEnd,
		))
	}

	var startComponentCols []string
	var endComponentCols []string
	for _, c := range keyColumns {
		aliasStart := fmt.Sprintf(`range_start_%s`, c)
		quotedAliasStart := pgx.Identifier{aliasStart}.Sanitize()
		startComponentCols = append(startComponentCols, quotedAliasStart)

		aliasEnd := fmt.Sprintf(`range_end_%s`, c)
		quotedAliasEnd := pgx.Identifier{aliasEnd}.Sanitize()
		endComponentCols = append(endComponentCols, quotedAliasEnd)
	}
	selectOutputCols := append(startComponentCols, endComponentCols...)

	data := map[string]any{
		"SchemaIdent":          schemaIdent,
		"TableIdent":           tableIdent,
		"TableSampleMethod":    tableSampleMethod,
		"SamplePercent":        samplePercent,
		"NtileCount":           ntileCount,
		"KeyColumnsSelect":     keyColsSelect,
		"KeyColumnsOrder":      keyColsOrder,
		"KeyColumnsOrderDesc":  keyColsOrderDesc,
		"FirstRowSelects":      strings.Join(firstSelects, ",\n        "),
		"LastRowSelects":       strings.Join(lastSelects, ",\n        "),
		"FirstRowTupleSelects": fmt.Sprintf("ROW(%s)", strings.Join(firstTuples, ",\n        ")),
		"RangeStartColumns":    strings.Join(rangeStarts, ",\n        "),
		"RangeEndColumns":      strings.Join(rangeEnds, ",\n        "),
		"RangeOutputColumns":   strings.Join(selectOutputCols, ",\n    "),
	}

	return RenderSQL(SQLTemplates.GetPkeyOffsets, data)
}

func CreateXORFunction(ctx context.Context, db *pgxpool.Pool) error {
	sql, err := RenderSQL(SQLTemplates.CreateXORFunction, nil)
	if err != nil {
		return fmt.Errorf("failed to render CreateXORFunction SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create xor function failed: %w", err)
	}

	return nil
}

func CreateMetadataTable(ctx context.Context, db *pgxpool.Pool) error {
	sql, err := RenderSQL(SQLTemplates.CreateMetadataTable, nil)
	if err != nil {
		return fmt.Errorf("failed to render CreateMetadataTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create metadata table failed: %w", err)
	}

	return nil
}

func CreateSimpleMtreeTable(ctx context.Context, db *pgxpool.Pool, mtreeTable, pkeyType string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
		"PkeyType":   pkeyType,
	}

	sql, err := RenderSQL(SQLTemplates.CreateSimpleMtreeTable, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateSimpleMtreeTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create simple mtree table for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func DropCompositeType(ctx context.Context, db *pgxpool.Pool, compositeTypeName string) error {
	data := map[string]interface{}{
		"CompositeTypeName": compositeTypeName,
	}

	sql, err := RenderSQL(SQLTemplates.DropCompositeType, data)
	if err != nil {
		return fmt.Errorf("failed to render DropCompositeType SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop composite type '%s' failed: %w", compositeTypeName, err)
	}

	return nil
}

func CreateCompositeType(ctx context.Context, db *pgxpool.Pool, compositeTypeName, keyTypeColumns string) error {
	data := map[string]interface{}{
		"CompositeTypeName": compositeTypeName,
		"KeyTypeColumns":    keyTypeColumns,
	}

	sql, err := RenderSQL(SQLTemplates.CreateCompositeType, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateCompositeType SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create composite type '%s' failed: %w", compositeTypeName, err)
	}

	return nil
}

func CreateCompositeMtreeTable(ctx context.Context, db *pgxpool.Pool, mtreeTable, compositeTypeName string) error {
	data := map[string]interface{}{
		"MtreeTable":        mtreeTable,
		"CompositeTypeName": compositeTypeName,
	}

	sql, err := RenderSQL(SQLTemplates.CreateCompositeMtreeTable, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateCompositeMtreeTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create composite mtree table for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func InsertBlockRanges(ctx context.Context, db *pgxpool.Pool, mtreeTable string, nodePosition int64, rangeStart, rangeEnd interface{}) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.InsertBlockRanges, data)
	if err != nil {
		return fmt.Errorf("failed to render InsertBlockRanges SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, nodePosition, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("query to insert block ranges for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func InsertCompositeBlockRanges(ctx context.Context, db *pgxpool.Pool, mtreeTable string, nodePosition int64, startTupleValues, endTupleValues string) error {
	data := map[string]interface{}{
		"MtreeTable":       mtreeTable,
		"StartTupleValues": startTupleValues,
		"EndTupleValues":   endTupleValues,
	}

	sql, err := RenderSQL(SQLTemplates.InsertCompositeBlockRanges, data)
	if err != nil {
		return fmt.Errorf("failed to render InsertCompositeBlockRanges SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, nodePosition)
	if err != nil {
		return fmt.Errorf("query to insert composite block ranges for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func InsertBlockRangesBatchSimple(ctx context.Context, db *pgxpool.Pool, mtreeTable string, ranges []types.BlockRange) error {
	if len(ranges) == 0 {
		return nil
	}

	const maxParams = 60000
	const paramsPerRow = 3
	chunkSize := maxParams / paramsPerRow
	if chunkSize < 1 {
		chunkSize = 1
	}

	for start := 0; start < len(ranges); start += chunkSize {
		end := start + chunkSize
		if end > len(ranges) {
			end = len(ranges)
		}

		type rowPlaceholders struct {
			NodePos string
			Start   string
			End     string
		}

		rowsMeta := make([]rowPlaceholders, 0, end-start)
		args := make([]any, 0, (end-start)*paramsPerRow)
		paramIdx := 1
		for i := start; i < end; i++ {
			r := ranges[i]
			rowsMeta = append(rowsMeta, rowPlaceholders{
				NodePos: fmt.Sprintf("$%d", paramIdx),
				Start:   fmt.Sprintf("$%d", paramIdx+1),
				End:     fmt.Sprintf("$%d", paramIdx+2),
			})
			paramIdx += 3

			args = append(args, r.NodePosition)
			var rs any
			var re any
			if len(r.RangeStart) > 0 {
				rs = r.RangeStart[0]
			}
			if len(r.RangeEnd) > 0 {
				re = r.RangeEnd[0]
			}
			args = append(args, rs, re)
		}

		data := map[string]any{
			"MtreeTable": mtreeTable,
			"Rows":       rowsMeta,
		}

		sql, err := RenderSQL(SQLTemplates.InsertBlockRangesBatchSimple, data)
		if err != nil {
			return fmt.Errorf("failed to render InsertBlockRangesBatchSimple SQL: %w", err)
		}

		if _, err := db.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("batch insert block ranges for '%s' failed: %w", mtreeTable, err)
		}
	}

	return nil
}

func InsertBlockRangesBatchComposite(ctx context.Context, db *pgxpool.Pool, mtreeTable string, ranges []types.BlockRange, keyLen int) error {
	if len(ranges) == 0 {
		return nil
	}

	if keyLen <= 0 {
		return fmt.Errorf("invalid keyLen")
	}

	const maxParams = 60000
	paramsPerRow := 1 + 2*keyLen
	if paramsPerRow <= 0 {
		paramsPerRow = 1
	}
	chunkSize := maxParams / paramsPerRow
	if chunkSize < 1 {
		chunkSize = 1
	}

	for start := 0; start < len(ranges); start += chunkSize {
		end := start + chunkSize
		if end > len(ranges) {
			end = len(ranges)
		}

		type rowPlaceholders struct {
			NodePos   string
			StartList string
			EndList   string
		}

		rowsMeta := make([]rowPlaceholders, 0, end-start)
		args := make([]any, 0, (end-start)*paramsPerRow)
		paramIdx := 1
		for i := start; i < end; i++ {
			r := ranges[i]
			nodePos := fmt.Sprintf("$%d", paramIdx)
			args = append(args, r.NodePosition)
			paramIdx++

			startPh := make([]string, keyLen)
			for k := 0; k < keyLen; k++ {
				startPh[k] = fmt.Sprintf("$%d", paramIdx)
				var v any
				if k < len(r.RangeStart) {
					v = r.RangeStart[k]
				}
				args = append(args, v)
				paramIdx++
			}

			endPh := make([]string, keyLen)
			for k := 0; k < keyLen; k++ {
				endPh[k] = fmt.Sprintf("$%d", paramIdx)
				var v any
				if k < len(r.RangeEnd) {
					v = r.RangeEnd[k]
				}
				args = append(args, v)
				paramIdx++
			}

			rowsMeta = append(rowsMeta, rowPlaceholders{
				NodePos:   nodePos,
				StartList: strings.Join(startPh, ", "),
				EndList:   strings.Join(endPh, ", "),
			})
		}

		data := map[string]any{
			"MtreeTable": mtreeTable,
			"Rows":       rowsMeta,
		}

		sql, err := RenderSQL(SQLTemplates.InsertBlockRangesBatchComposite, data)
		if err != nil {
			return fmt.Errorf("failed to render InsertBlockRangesBatchComposite SQL: %w", err)
		}

		if _, err := db.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("batch insert composite block ranges for '%s' failed: %w", mtreeTable, err)
		}
	}

	return nil
}

func GetPkeyOffsets(ctx context.Context, db *pgxpool.Pool, schema, table string, keyColumns []string, tableSampleMethod string, samplePercent float64, ntileCount int) ([]types.PkeyOffset, error) {
	sql, err := GeneratePkeyOffsetsQuery(schema, table, keyColumns, tableSampleMethod, samplePercent, ntileCount)
	if err != nil {
		return nil, fmt.Errorf("failed to generate GetPkeyOffsets SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query to get pkey offsets for '%s.%s' failed: %w", schema, table, err)
	}
	defer rows.Close()

	var offsets []types.PkeyOffset
	numKeyCols := len(keyColumns)
	for rows.Next() {
		values := make([]interface{}, numKeyCols*2)
		valuePtrs := make([]interface{}, numKeyCols*2)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan pkey offset: %w", err)
		}

		offset := types.PkeyOffset{
			RangeStart: values[:numKeyCols],
			RangeEnd:   values[numKeyCols:],
		}
		offsets = append(offsets, offset)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over pkey offsets: %w", err)
	}

	return offsets, nil
}

func BlockHashSQL(schema, table string, primaryKeyCols []string, mode string) (string, error) {
	if len(primaryKeyCols) == 0 {
		return "", fmt.Errorf("primaryKeyCols cannot be empty")
	}
	if err := SanitiseIdentifier(schema); err != nil {
		return "", err
	}
	if err := SanitiseIdentifier(table); err != nil {
		return "", err
	}

	for _, pkCol := range primaryKeyCols {
		if pkCol == "" {
			return "", fmt.Errorf("primary key column identifier cannot be empty")
		}
		if err := SanitiseIdentifier(pkCol); err != nil {
			return "", fmt.Errorf("invalid primary key column identifier %q: %w", pkCol, err)
		}
	}

	schemaIdent := pgx.Identifier{schema}.Sanitize()
	tableIdent := pgx.Identifier{table}.Sanitize()
	tableAlias := "_tbl_"

	quotedPKColIdents := make([]string, len(primaryKeyCols))
	for i, pkCol := range primaryKeyCols {
		quotedPKColIdents[i] = pgx.Identifier{pkCol}.Sanitize()
	}
	pkOrderByStr := strings.Join(quotedPKColIdents, ", ")

	pkComparisonExpression := ""
	if len(primaryKeyCols) == 1 {
		pkComparisonExpression = quotedPKColIdents[0]
	} else {
		pkComparisonExpression = fmt.Sprintf("ROW(%s)", strings.Join(quotedPKColIdents, ", "))
	}

	startPlaceholders := make([]string, len(primaryKeyCols))
	for i := range primaryKeyCols {
		startPlaceholders[i] = fmt.Sprintf("$%d", 2+i)
	}
	startValueExpression := ""
	if len(primaryKeyCols) == 1 {
		startValueExpression = startPlaceholders[0]
	} else {
		startValueExpression = fmt.Sprintf("ROW(%s)", strings.Join(startPlaceholders, ", "))
	}

	skipMaxCheckPlaceholderIndex := 2 + len(primaryKeyCols)

	endPlaceholders := make([]string, len(primaryKeyCols))
	for i := range primaryKeyCols {
		endPlaceholders[i] = fmt.Sprintf("$%d", skipMaxCheckPlaceholderIndex+1+i)
	}
	endValueExpression := ""
	if len(primaryKeyCols) == 1 {
		endValueExpression = endPlaceholders[0]
	} else {
		endValueExpression = fmt.Sprintf("ROW(%s)", strings.Join(endPlaceholders, ", "))
	}

	var tmpl *template.Template
	switch mode {
	case "TD_BLOCK_HASH":
		tmpl = SQLTemplates.TDBlockHashSQL
	case "MTREE_LEAF_HASH":
		tmpl = SQLTemplates.MtreeLeafHashSQL
	default:
		return "", fmt.Errorf("invalid mode: %s", mode)
	}

	data := map[string]any{
		"SchemaIdent":            schemaIdent,
		"TableIdent":             tableIdent,
		"TableAlias":             tableAlias,
		"PkOrderByStr":           pkOrderByStr,
		"PkComparisonExpression": pkComparisonExpression,
		"StartValueExpression":   startValueExpression,
		"SkipMaxIdx":             fmt.Sprintf("$%d", skipMaxCheckPlaceholderIndex),
		"EndValueExpression":     endValueExpression,
	}
	return RenderSQL(tmpl, data)
}

// GetColumns retrieves the column names for a given table.
func GetColumns(ctx context.Context, db *pgxpool.Pool, schema, table string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetColumns, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetColumns SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query to get columns failed for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, nil
	}

	return columns, nil
}

func GetPrimaryKey(ctx context.Context, db *pgxpool.Pool, schema, table string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPrimaryKey, nil)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(context.Background(), sql, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}

	return keys, nil
}

func GetColumnTypes(ctx context.Context, db *pgxpool.Pool, schema, table string) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetColumnTypes, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	types := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, err
		}
		types[columnName] = dataType
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(types) == 0 {
		return nil, fmt.Errorf("could not fetch column types for %s.%s", schema, table)
	}

	return types, nil
}

func CheckUserPrivileges(ctx context.Context, db *pgxpool.Pool, username, schema, table string) (*types.UserPrivileges, error) {
	sql, err := RenderSQL(SQLTemplates.CheckUserPrivileges, nil)
	if err != nil {
		return nil, err
	}

	var privileges types.UserPrivileges
	err = db.QueryRow(ctx, sql, username, schema, table).Scan(
		&privileges.TableSelect,
		&privileges.TableCreate,
		&privileges.TableInsert,
		&privileges.TableUpdate,
		&privileges.TableDelete,
		&privileges.ColumnsSelect,
		&privileges.TableConstraintsSelect,
		&privileges.KeyColumnUsageSelect,
	)
	if err != nil {
		return nil, err
	}

	return &privileges, nil
}

func GetSpockNodeAndSubInfo(ctx context.Context, db *pgxpool.Pool) ([]types.SpockNodeAndSubInfo, error) {
	sql, err := RenderSQL(SQLTemplates.SpockNodeAndSubInfo, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var infos []types.SpockNodeAndSubInfo
	for rows.Next() {
		var info types.SpockNodeAndSubInfo
		if err := rows.Scan(
			&info.NodeID,
			&info.NodeName,
			&info.Location,
			&info.Country,
			&info.SubID,
			&info.SubName,
			&info.SubEnabled,
			&info.SubReplicationSets,
		); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return infos, nil
}

func GetSpockRepSetInfo(ctx context.Context, db *pgxpool.Pool) ([]types.SpockRepSetInfo, error) {
	sql, err := RenderSQL(SQLTemplates.SpockRepSetInfo, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var infos []types.SpockRepSetInfo
	for rows.Next() {
		var info types.SpockRepSetInfo
		if err := rows.Scan(
			&info.SetName,
			&info.RelName,
		); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return infos, nil
}

func CheckSchemaExists(ctx context.Context, db *pgxpool.Pool, schema string) (bool, error) {
	sql, err := RenderSQL(SQLTemplates.CheckSchemaExists, nil)
	if err != nil {
		return false, fmt.Errorf("failed to render CheckSchemaExists SQL: %w", err)
	}

	var exists bool
	err = db.QueryRow(ctx, sql, schema).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query to check if schema '%s' exists failed: %w", schema, err)
	}

	return exists, nil
}

func GetTablesInSchema(ctx context.Context, db *pgxpool.Pool, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetTablesInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetTablesInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get tables in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over tables: %w", err)
	}

	return tables, nil
}

func GetViewsInSchema(ctx context.Context, db *pgxpool.Pool, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetViewsInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetViewsInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get views in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("failed to scan view name: %w", err)
		}
		views = append(views, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over views: %w", err)
	}

	return views, nil
}

func GetFunctionsInSchema(ctx context.Context, db *pgxpool.Pool, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetFunctionsInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetFunctionsInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get functions in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var functions []string
	for rows.Next() {
		var funcName string
		if err := rows.Scan(&funcName); err != nil {
			return nil, fmt.Errorf("failed to scan function name: %w", err)
		}
		functions = append(functions, funcName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over functions: %w", err)
	}

	return functions, nil
}

func GetIndicesInSchema(ctx context.Context, db *pgxpool.Pool, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetIndicesInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetIndicesInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get indices in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var indices []string
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			return nil, fmt.Errorf("failed to scan index name: %w", err)
		}
		indices = append(indices, indexName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over indices: %w", err)
	}

	return indices, nil
}

func CheckRepSetExists(ctx context.Context, db *pgxpool.Pool, repSet string) (bool, error) {
	sql, err := RenderSQL(SQLTemplates.CheckRepSetExists, nil)
	if err != nil {
		return false, fmt.Errorf("failed to render CheckRepSetExists SQL: %w", err)
	}

	var exists bool
	err = db.QueryRow(ctx, sql, repSet).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query to check if rep set '%s' exists failed: %w", repSet, err)
	}

	return exists, nil
}

func GetTablesInRepSet(ctx context.Context, db *pgxpool.Pool, repSet string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetTablesInRepSet, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetTablesInRepSet SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, repSet)
	if err != nil {
		return nil, fmt.Errorf("query to get tables in rep set '%s' failed: %w", repSet, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over tables: %w", err)
	}

	return tables, nil
}

func GetRowCountEstimate(ctx context.Context, db *pgxpool.Pool, schema, table string) (int64, error) {
	sql, err := RenderSQL(SQLTemplates.EstimateRowCount, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to render EstimateRowCount SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get row count estimate for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetPkeyColumnTypes(ctx context.Context, db *pgxpool.Pool, schema, table string, pkeys []string) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPkeyColumnTypes, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetPkeyColumnTypes SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, table, pkeys)
	if err != nil {
		return nil, fmt.Errorf("query to get pkey column types for '%s.%s' failed: %w", schema, table, err)
	}
	defer rows.Close()

	types := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("failed to scan pkey column type: %w", err)
		}
		types[colName] = colType
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over pkey column types: %w", err)
	}

	return types, nil
}

func GetPkeyType(ctx context.Context, db *pgxpool.Pool, schema, table, pkey string) (string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPkeyType, nil)
	if err != nil {
		return "", fmt.Errorf("failed to render GetPkeyType SQL: %w", err)
	}

	var pkeyType string
	err = db.QueryRow(ctx, sql, schema, table, pkey).Scan(&pkeyType)
	if err != nil {
		return "", fmt.Errorf("query to get pkey type for '%s.%s.%s' failed: %w", schema, table, pkey, err)
	}

	return pkeyType, nil
}

func UpdateMetadata(ctx context.Context, db *pgxpool.Pool, schema, table string, totalRows int64, blockSize, numBlocks int, isComposite bool) error {
	sql, err := RenderSQL(SQLTemplates.UpdateMetadata, nil)
	if err != nil {
		return fmt.Errorf("failed to render UpdateMetadata SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, schema, table, totalRows, blockSize, numBlocks, isComposite)
	if err != nil {
		return fmt.Errorf("query to update metadata for '%s.%s' failed: %w", schema, table, err)
	}

	return nil
}

func ComputeLeafHashes(ctx context.Context, db *pgxpool.Pool, schema, table string, simpleKey bool, key []string, start []any, end []any) ([]byte, error) {
	sql, err := BlockHashSQL(schema, table, key, "MTREE_LEAF_HASH" /* mode */)
	if err != nil {
		return nil, err
	}

	// Build args: $1 skipMinCheck, [start values], $skipMaxCheck, [end values]
	// When no start, set skipMinCheck=true; likewise for end
	args := make([]any, 0, 2+len(start)+len(end))
	skipMin := len(start) == 0 || start[0] == nil
	args = append(args, skipMin)
	args = append(args, start...)

	skipMax := len(end) == 0 || end[0] == nil
	args = append(args, skipMax)
	args = append(args, end...)

	var leafHash []byte
	if err := db.QueryRow(ctx, sql, args...).Scan(&leafHash); err != nil {
		return nil, fmt.Errorf("query to compute leaf hashes for '%s.%s' failed: %w", schema, table, err)
	}
	return leafHash, nil
}

func UpdateLeafHashes(ctx context.Context, db *pgxpool.Pool, mtreeTable string, leafHash []byte, nodePosition int64) (int64, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.UpdateLeafHashes, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render UpdateLeafHashes SQL: %w", err)
	}

	var updatedNodePosition int64
	err = db.QueryRow(ctx, sql, leafHash, nodePosition).Scan(&updatedNodePosition)
	if err != nil {
		return 0, fmt.Errorf("query to update leaf hashes for '%s' failed: %w", mtreeTable, err)
	}

	return updatedNodePosition, nil
}

func GetBlockRanges(ctx context.Context, db *pgxpool.Pool, mtreeTable string) ([]types.BlockRange, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockRanges, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockRanges SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query to get block ranges for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var blockRanges []types.BlockRange
	for rows.Next() {
		var blockRange types.BlockRange
		if err := rows.Scan(&blockRange.NodePosition, &blockRange.RangeStart, &blockRange.RangeEnd); err != nil {
			return nil, fmt.Errorf("failed to scan block range: %w", err)
		}
		blockRanges = append(blockRanges, blockRange)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over block ranges: %w", err)
	}

	return blockRanges, nil
}

func ClearDirtyFlags(ctx context.Context, db *pgxpool.Pool, mtreeTable string, nodePositions []int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.ClearDirtyFlags, data)
	if err != nil {
		return fmt.Errorf("failed to render ClearDirtyFlags SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, nodePositions)
	if err != nil {
		return fmt.Errorf("query to clear dirty flags for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func BuildParentNodes(ctx context.Context, db *pgxpool.Pool, mtreeTable string, nodeLevel int) (int, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.BuildParentNodes, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render BuildParentNodes SQL: %w", err)
	}

	var count int
	err = db.QueryRow(ctx, sql, nodeLevel).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to build parent nodes for '%s' failed: %w", mtreeTable, err)
	}

	return count, nil
}

func GetRootNode(ctx context.Context, db *pgxpool.Pool, mtreeTable string) (*types.RootNode, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetRootNode, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetRootNode SQL: %w", err)
	}

	var rootNode types.RootNode
	err = db.QueryRow(ctx, sql).Scan(&rootNode.NodePosition, &rootNode.NodeHash)
	if err != nil {
		return nil, fmt.Errorf("query to get root node for '%s' failed: %w", mtreeTable, err)
	}

	return &rootNode, nil
}

func GetNodeChildren(ctx context.Context, db *pgxpool.Pool, mtreeTable string, nodeLevel, nodePosition int) ([]types.NodeChild, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetNodeChildren, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetNodeChildren SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, nodeLevel, nodePosition)
	if err != nil {
		return nil, fmt.Errorf("query to get node children for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var children []types.NodeChild
	for rows.Next() {
		var child types.NodeChild
		if err := rows.Scan(&child.NodeLevel, &child.NodePosition, &child.NodeHash); err != nil {
			return nil, fmt.Errorf("failed to scan node child: %w", err)
		}
		children = append(children, child)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over node children: %w", err)
	}

	return children, nil
}

func GetLeafRanges(ctx context.Context, db *pgxpool.Pool, mtreeTable string, nodePositions []int64) ([]types.LeafRange, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetLeafRanges, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetLeafRanges SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, nodePositions)
	if err != nil {
		return nil, fmt.Errorf("query to get leaf ranges for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var ranges []types.LeafRange
	for rows.Next() {
		var r types.LeafRange
		if err := rows.Scan(&r.RangeStart, &r.RangeEnd); err != nil {
			return nil, fmt.Errorf("failed to scan leaf range: %w", err)
		}
		ranges = append(ranges, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over leaf ranges: %w", err)
	}

	return ranges, nil
}

func GetRowCountEstimateFromMetadata(ctx context.Context, db *pgxpool.Pool, schema, table string) (int64, error) {
	sql, err := RenderSQL(SQLTemplates.GetRowCountEstimate, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetRowCountEstimate SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get row count estimate from metadata for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetMaxValComposite(ctx context.Context, db *pgxpool.Pool, schema, table, pkeyCols, pkeyValues string) ([]interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    pkeyCols,
		"PkeyValues":  pkeyValues,
	}

	sql, err := RenderSQL(SQLTemplates.GetMaxValComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMaxValComposite SQL: %w", err)
	}

	// Scan each key attribute into separate destinations
	// Note: pkeyCols is already a comma-separated list of sanitised identifiers
	numCols := strings.Count(pkeyCols, ",") + 1
	dest := make([]interface{}, numCols)
	destPtrs := make([]interface{}, numCols)
	for i := range destPtrs {
		destPtrs[i] = &dest[i]
	}
	if err := db.QueryRow(ctx, sql).Scan(destPtrs...); err != nil {
		return nil, fmt.Errorf("query to get max val composite for '%s.%s' failed: %w", schema, table, err)
	}
	return dest, nil
}

func UpdateMaxVal(ctx context.Context, db *pgxpool.Pool, mtreeTable string, rangeEnd interface{}, nodePosition int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.UpdateMaxVal, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateMaxVal SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, rangeEnd, nodePosition)
	if err != nil {
		return fmt.Errorf("query to update max val for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func GetMaxValSimple(ctx context.Context, db *pgxpool.Pool, schema, table, key string, rangeStart interface{}) (interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}

	sql, err := RenderSQL(SQLTemplates.GetMaxValSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMaxValSimple SQL: %w", err)
	}

	var maxVal interface{}
	err = db.QueryRow(ctx, sql, rangeStart).Scan(&maxVal)
	if err != nil {
		return nil, fmt.Errorf("query to get max val simple for '%s.%s' failed: %w", schema, table, err)
	}

	return maxVal, nil
}

func GetCountComposite(ctx context.Context, db *pgxpool.Pool, schema, table, whereClause string) (int64, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"WhereClause": whereClause,
	}

	sql, err := RenderSQL(SQLTemplates.GetCountComposite, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetCountComposite SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get count composite for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetCountSimple(ctx context.Context, db *pgxpool.Pool, schema, table, key, pkeyType string, rangeStart, rangeEnd interface{}) (int64, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
		"PkeyType":    pkeyType,
	}

	sql, err := RenderSQL(SQLTemplates.GetCountSimple, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetCountSimple SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, rangeStart, rangeEnd).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get count simple for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetBlockRowCount(ctx context.Context, tx pgx.Tx, schema string, table string, keyColumns []string, isComposite bool, start, end []any) (int64, error) {
	var whereClause string
	var args []any

	if isComposite {
		var conditions []string
		var startPlaceholders, endPlaceholders []string

		if len(start) > 0 {
			for i := range start {
				startPlaceholders = append(startPlaceholders, fmt.Sprintf("$%d", len(args)+i+1))
			}
			conditions = append(conditions, fmt.Sprintf("ROW(%s) >= ROW(%s)", strings.Join(keyColumns, ", "), strings.Join(startPlaceholders, ", ")))
			args = append(args, start...)
		}

		if len(end) > 0 && end[0] != nil {
			for i := range end {
				endPlaceholders = append(endPlaceholders, fmt.Sprintf("$%d", len(args)+i+1))
			}
			conditions = append(conditions, fmt.Sprintf("ROW(%s) <= ROW(%s)", strings.Join(keyColumns, ", "), strings.Join(endPlaceholders, ", ")))
			args = append(args, end...)
		}
		whereClause = strings.Join(conditions, " AND ")
	} else {
		var conditions []string
		if len(start) > 0 && start[0] != nil {
			conditions = append(conditions, fmt.Sprintf("%s >= $1", keyColumns[0]))
			args = append(args, start[0])
		}
		if len(end) > 0 && end[0] != nil {
			conditions = append(conditions, fmt.Sprintf("%s <= $%d", keyColumns[0], len(args)+1))
			args = append(args, end[0])
		}
		whereClause = strings.Join(conditions, " AND ")
	}

	if whereClause == "" {
		whereClause = "TRUE"
	}

	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"WhereClause": whereClause,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockRowCount, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetBlockRowCount SQL: %w", err)
	}

	var count int64
	err = tx.QueryRow(ctx, sql, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get block row count for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func FindBlocksToSplit(ctx context.Context, conn *pgx.Conn, mtreeTable string, insertsSinceUpdate int, nodePositions []int64, simplePrimaryKey bool) ([]types.BlockRange, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.FindBlocksToSplit, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToSplit SQL: %w", err)
	}

	rows, err := conn.Query(ctx, sql, insertsSinceUpdate, nodePositions)
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var blocks []types.BlockRange
	for rows.Next() {
		var br types.BlockRange
		if simplePrimaryKey {
			var start, end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block to split: %w", err)
			}
			if start != nil {
				br.RangeStart = []any{start}
			}
			if end != nil {
				br.RangeEnd = []any{end}
			}
		} else {
			var start, end pgtype.CompositeType
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block to split: %w", err)
			}
			if start.Get() != nil {
				var values []any
				start.AssignTo(&values)
				br.RangeStart = values
			}
			if end.Get() != nil {
				var values []any
				end.AssignTo(&values)
				br.RangeEnd = values
			}
		}
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over blocks to split: %w", err)
	}
	return blocks, nil
}

func GetDirtyAndNewBlocksTx(ctx context.Context, tx pgx.Tx, mtreeTable string, simplePrimaryKey bool, key []string) ([]types.BlockRange, error) {
	if simplePrimaryKey {
		data := map[string]interface{}{
			"MtreeTable": mtreeTable,
		}
		sql, err := RenderSQL(SQLTemplates.GetDirtyAndNewBlocks, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render GetDirtyAndNewBlocks SQL: %w", err)
		}
		rows, err := tx.Query(ctx, sql)
		if err != nil {
			return nil, fmt.Errorf("query to get dirty and new blocks for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()

		var blocks []types.BlockRange
		for rows.Next() {
			var br types.BlockRange
			var start any
			var end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block range row: %w", err)
			}
			if start != nil {
				br.RangeStart = []any{start}
			}
			if end != nil {
				br.RangeEnd = []any{end}
			}
			blocks = append(blocks, br)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over block ranges: %w", err)
		}
		return blocks, nil
	}

	// Composite primary key: expand attributes using template to avoid binary composite decoding
	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
		"StartAttrs": strings.Join(startAttrs, ", "),
		"EndAttrs":   strings.Join(endAttrs, ", "),
	}
	sql, err := RenderSQL(SQLTemplates.GetDirtyAndNewBlocksExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetDirtyAndNewBlocksExpanded SQL: %w", err)
	}

	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query to get dirty and new blocks for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var blocks []types.BlockRange
	numCols := 1 + len(key) + len(key)
	for rows.Next() {
		dest := make([]any, numCols)
		destPtrs := make([]any, numCols)
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded block range row: %w", err)
		}
		var br types.BlockRange
		if v, ok := dest[0].(int64); ok {
			br.NodePosition = v
		} else {
			// allow numeric types that can be cast to int64
			switch t := dest[0].(type) {
			case int32:
				br.NodePosition = int64(t)
			case int:
				br.NodePosition = int64(t)
			default:
				return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
			}
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):])
		br.RangeStart = startVals
		br.RangeEnd = endVals
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded block ranges: %w", err)
	}
	return blocks, nil
}

func FindBlocksToSplitTx(ctx context.Context, tx pgx.Tx, mtreeTable string, insertsSinceUpdate int, nodePositions []int64, simplePrimaryKey bool, key []string) ([]types.BlockRange, error) {
	if simplePrimaryKey {
		data := map[string]interface{}{
			"MtreeTable": mtreeTable,
		}
		sql, err := RenderSQL(SQLTemplates.FindBlocksToSplit, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render FindBlocksToSplit SQL: %w", err)
		}
		rows, err := tx.Query(ctx, sql, insertsSinceUpdate, nodePositions)
		if err != nil {
			return nil, fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()

		var blocks []types.BlockRange
		for rows.Next() {
			var br types.BlockRange
			var start any
			var end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block to split: %w", err)
			}
			if start != nil {
				br.RangeStart = []any{start}
			}
			if end != nil {
				br.RangeEnd = []any{end}
			}
			blocks = append(blocks, br)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over blocks to split: %w", err)
		}
		return blocks, nil
	}

	// Composite primary key: expand composite attributes using template
	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
		"StartAttrs": strings.Join(startAttrs, ", "),
		"EndAttrs":   strings.Join(endAttrs, ", "),
	}
	sql, err := RenderSQL(SQLTemplates.FindBlocksToSplitExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToSplitExpanded SQL: %w", err)
	}

	rows, err := tx.Query(ctx, sql, insertsSinceUpdate, nodePositions)
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var blocks []types.BlockRange
	numCols := 1 + len(key) + len(key)
	for rows.Next() {
		dest := make([]any, numCols)
		destPtrs := make([]any, numCols)
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded block to split: %w", err)
		}
		var br types.BlockRange
		if v, ok := dest[0].(int64); ok {
			br.NodePosition = v
		} else {
			switch t := dest[0].(type) {
			case int32:
				br.NodePosition = int64(t)
			case int:
				br.NodePosition = int64(t)
			default:
				return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
			}
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):])
		br.RangeStart = startVals
		br.RangeEnd = endVals
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded blocks to split: %w", err)
	}
	return blocks, nil
}

func GetMaxNodePositionTx(ctx context.Context, tx pgx.Tx, mtreeTable string) (int64, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.GetMaxNodePosition, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetMaxNodePosition SQL: %w", err)
	}
	var pos int64
	if err := tx.QueryRow(ctx, sql).Scan(&pos); err != nil {
		return 0, fmt.Errorf("query to get max node position for '%s' failed: %w", mtreeTable, err)
	}
	return pos, nil
}

func UpdateBlockRangeEndTx(ctx context.Context, tx pgx.Tx, mtreeTable string, rangeEnd any, nodePosition int64) error {
	data := map[string]interface{}{
		"MtreeTable":   mtreeTable,
		"RangeEndExpr": "$1",
		"NodePosition": "$2",
	}
	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeEnd, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeEnd SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, rangeEnd, nodePosition); err != nil {
		return fmt.Errorf("query to update block range end for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func InsertBlockRangesTx(ctx context.Context, tx pgx.Tx, mtreeTable string, nodePosition int64, rangeStart, rangeEnd interface{}) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.InsertBlockRanges, data)
	if err != nil {
		return fmt.Errorf("failed to render InsertBlockRanges SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, nodePosition, rangeStart, rangeEnd); err != nil {
		return fmt.Errorf("query to insert block ranges for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func InsertCompositeBlockRangesTx(ctx context.Context, tx pgx.Tx, mtreeTable string, nodePosition int64, startVals, endVals []any) error {
	startPh := make([]string, len(startVals))
	args := make([]any, 0, 1+len(startVals)+len(endVals))
	args = append(args, nodePosition)
	argIdx := 2
	for i := range startVals {
		startPh[i] = fmt.Sprintf("$%d", argIdx)
		args = append(args, startVals[i])
		argIdx++
	}

	var endExpr string
	if endVals == nil {
		endExpr = "NULL"
	} else {
		endPh := make([]string, len(endVals))
		for i := range endVals {
			endPh[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, endVals[i])
			argIdx++
		}
		endExpr = fmt.Sprintf("ROW(%s)", strings.Join(endPh, ", "))
	}
	startExpr := fmt.Sprintf("ROW(%s)", strings.Join(startPh, ", "))

	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
		"StartExpr":  startExpr,
		"EndExpr":    endExpr,
	}

	stmt, err := RenderSQL(SQLTemplates.InsertCompositeBlockRanges, data)
	if err != nil {
		return fmt.Errorf("failed to render InsertCompositeBlockRanges SQL: %w", err)
	}

	if _, err := tx.Exec(ctx, stmt, args...); err != nil {
		return fmt.Errorf("query to insert composite block ranges for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func UpdateBlockRangeEndCompositeTx(ctx context.Context, tx pgx.Tx, mtreeTable string, compositeTypeName string, endVals []any, pos int64) error {
	args := []any{}
	isNull := len(endVals) == 0

	data := map[string]any{
		"MtreeTable":              pgx.Identifier{mtreeTable}.Sanitize(),
		"IsNull":                  isNull,
		"CompositeTypeName":       pgx.Identifier{compositeTypeName}.Sanitize(),
		"NodePositionPlaceholder": fmt.Sprintf("$%d", len(endVals)+1),
	}

	if !isNull {
		placeholders := make([]string, len(endVals))
		for i, val := range endVals {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, val)
		}
		data["Placeholders"] = strings.Join(placeholders, ", ")
	}
	args = append(args, pos)

	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeEndComposite, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeEndCompositeTx SQL: %w", err)
	}

	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("query to update composite block range end for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func UpdateBlockRangeStartTx(ctx context.Context, tx pgx.Tx, mtreeTable string, rangeStart any, nodePosition int64) error {
	data := map[string]interface{}{
		"MtreeTable":     mtreeTable,
		"RangeStartExpr": "$1",
		"NodePosition":   "$2",
	}
	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeStart, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeStart SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, rangeStart, nodePosition); err != nil {
		return fmt.Errorf("query to update block range start for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func UpdateBlockRangeStartCompositeTx(ctx context.Context, tx pgx.Tx, mtreeTable string, compositeTypeName string, startVals []any, pos int64) error {
	args := []any{}
	isNull := len(startVals) == 0

	data := map[string]any{
		"MtreeTable":              pgx.Identifier{mtreeTable}.Sanitize(),
		"IsNull":                  isNull,
		"CompositeTypeName":       pgx.Identifier{compositeTypeName}.Sanitize(),
		"NodePositionPlaceholder": fmt.Sprintf("$%d", len(startVals)+1),
	}

	if !isNull {
		placeholders := make([]string, len(startVals))
		for i, val := range startVals {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, val)
		}
		data["Placeholders"] = strings.Join(placeholders, ", ")
	}
	args = append(args, pos)

	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeStartComposite, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeStartCompositeTx SQL: %w", err)
	}

	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("query to update composite block range start for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func GetSplitPointSimpleTx(ctx context.Context, tx pgx.Tx, schema, table, key, pkeyType string, rangeStart, rangeEnd interface{}, offset int64) (interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
		"PkeyType":    pkeyType,
	}
	sql, err := RenderSQL(SQLTemplates.GetSplitPointSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetSplitPointSimple SQL: %w", err)
	}
	var splitPoint interface{}
	if err := tx.QueryRow(ctx, sql, rangeStart, rangeEnd, pkeyType, offset).Scan(&splitPoint); err != nil {
		return nil, fmt.Errorf("query to get split point simple for '%s.%s' failed: %w", schema, table, err)
	}
	return splitPoint, nil
}

func GetSplitPointCompositeTx(ctx context.Context, tx pgx.Tx, schema, table string, pkeyCols []string, startVals, endVals []any, offset int64) ([]interface{}, error) {
	cols := make([]string, len(pkeyCols))
	for i, c := range pkeyCols {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	colsStr := strings.Join(cols, ", ")
	orderCols := fmt.Sprintf("(%s)", colsStr)

	var whereParts []string
	args := make([]any, 0, len(startVals)+len(endVals)+1)
	argIdx := 1

	if len(startVals) > 0 {
		startPh := make([]string, len(startVals))
		for i := range startVals {
			startPh[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, startVals[i])
			argIdx++
		}
		whereParts = append(whereParts, fmt.Sprintf("(%s) >= (%s)", colsStr, strings.Join(startPh, ", ")))
	}

	if len(endVals) > 0 {
		hasNonNil := false
		for i := range endVals {
			if endVals[i] != nil {
				hasNonNil = true
				break
			}
		}
		if hasNonNil {
			endPh := make([]string, len(endVals))
			for i := range endVals {
				endPh[i] = fmt.Sprintf("$%d", argIdx)
				args = append(args, endVals[i])
				argIdx++
			}
			whereParts = append(whereParts, fmt.Sprintf("(%s) <= (%s)", colsStr, strings.Join(endPh, ", ")))
		}
	}
	whereClause := "TRUE"
	if len(whereParts) > 0 {
		whereClause = strings.Join(whereParts, " AND ")
	}

	// The next placeholder index is for OFFSET
	offsetPh := fmt.Sprintf("$%d", argIdx)
	args = append(args, offset)

	data := map[string]interface{}{
		"SchemaIdent":       pgx.Identifier{schema}.Sanitize(),
		"TableIdent":        pgx.Identifier{table}.Sanitize(),
		"PkeyCols":          colsStr,
		"WhereClause":       whereClause,
		"OrderCols":         orderCols,
		"OffsetPlaceholder": offsetPh,
	}
	sql, err := RenderSQL(SQLTemplates.GetSplitPointComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetSplitPointComposite SQL: %w", err)
	}
	sp := make([]interface{}, len(pkeyCols))
	destPtrs := make([]interface{}, len(pkeyCols))
	for i := range destPtrs {
		destPtrs[i] = &sp[i]
	}

	if err := tx.QueryRow(ctx, sql, args...).Scan(destPtrs...); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get split point composite for '%s.%s' failed: %w", schema, table, err)
	}
	return sp, nil
}

func GetMaxValSimpleTx(ctx context.Context, tx pgx.Tx, schema, table, key string, rangeStart interface{}) (interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}
	sql, err := RenderSQL(SQLTemplates.GetMaxValSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMaxValSimple SQL: %w", err)
	}
	var maxVal interface{}
	if err := tx.QueryRow(ctx, sql, rangeStart).Scan(&maxVal); err != nil {
		return nil, fmt.Errorf("query to get max val simple for '%s.%s' failed: %w", schema, table, err)
	}
	return maxVal, nil
}

func GetMaxValCompositeTx(ctx context.Context, tx pgx.Tx, schema, table string, pkeyCols []string, pkeyValues []any) ([]interface{}, error) {
	cols := make([]string, len(pkeyCols))
	for i, c := range pkeyCols {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	colsStr := strings.Join(cols, ", ")

	valsPh := make([]string, len(pkeyValues))
	args := make([]any, len(pkeyValues))
	for i, v := range pkeyValues {
		valsPh[i] = fmt.Sprintf("$%d", i+1)
		args[i] = v
	}
	valsStr := strings.Join(valsPh, ", ")

	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    colsStr,
		"PkeyValues":  fmt.Sprintf("ROW(%s)", valsStr),
	}
	sql, err := RenderSQL(SQLTemplates.GetMaxValComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMaxValComposite SQL: %w", err)
	}
	dest := make([]interface{}, len(pkeyCols))
	destPtrs := make([]interface{}, len(pkeyCols))
	for i := range destPtrs {
		destPtrs[i] = &dest[i]
	}
	if err := tx.QueryRow(ctx, sql, args...).Scan(destPtrs...); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get max val composite for '%s.%s' failed: %w", schema, table, err)
	}
	return dest, nil
}

func GetMinValCompositeTx(ctx context.Context, tx pgx.Tx, schema, table string, pkeyCols []string) ([]interface{}, error) {
	cols := make([]string, len(pkeyCols))
	for i, c := range pkeyCols {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	colsStr := strings.Join(cols, ", ")

	data := map[string]any{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    colsStr,
	}
	sql, err := RenderSQL(SQLTemplates.GetMinValComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMinValComposite SQL: %w", err)
	}

	dest := make([]interface{}, len(pkeyCols))
	destPtrs := make([]interface{}, len(pkeyCols))
	for i := range destPtrs {
		destPtrs[i] = &dest[i]
	}
	if err := tx.QueryRow(ctx, sql).Scan(destPtrs...); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get min val composite for '%s.%s' failed: %w", schema, table, err)
	}
	return dest, nil
}

func GetMinValSimpleTx(ctx context.Context, tx pgx.Tx, schema, table, key string) (interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}
	sql, err := RenderSQL(SQLTemplates.GetMinValSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMinValSimple SQL: %w", err)
	}
	var minVal interface{}
	if err := tx.QueryRow(ctx, sql).Scan(&minVal); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get min val simple for '%s.%s' failed: %w", schema, table, err)
	}
	return minVal, nil
}

func ClearDirtyFlagsTx(ctx context.Context, tx pgx.Tx, mtreeTable string, nodePositions []int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.ClearDirtyFlags, data)
	if err != nil {
		return fmt.Errorf("failed to render ClearDirtyFlags SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, nodePositions); err != nil {
		return fmt.Errorf("query to clear dirty flags for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func DeleteParentNodesTx(ctx context.Context, tx pgx.Tx, mtreeTable string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.DeleteParentNodes, data)
	if err != nil {
		return fmt.Errorf("failed to render DeleteParentNodes SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("query to delete parent nodes for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func BuildParentNodesTx(ctx context.Context, tx pgx.Tx, mtreeTable string, nodeLevel int) (int, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.BuildParentNodes, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render BuildParentNodes SQL: %w", err)
	}
	var count int
	if err := tx.QueryRow(ctx, sql, nodeLevel).Scan(&count); err != nil {
		return 0, fmt.Errorf("query to build parent nodes for '%s' failed: %w", mtreeTable, err)
	}
	return count, nil
}

func GetPkeyTypeTx(ctx context.Context, tx pgx.Tx, schema, table, pkey string) (string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPkeyType, nil)
	if err != nil {
		return "", fmt.Errorf("failed to render GetPkeyType SQL: %w", err)
	}
	var pkeyType string
	if err := tx.QueryRow(ctx, sql, schema, table, pkey).Scan(&pkeyType); err != nil {
		return "", fmt.Errorf("query to get pkey type for '%s.%s.%s' failed: %w", schema, table, pkey, err)
	}
	return pkeyType, nil
}

func FindBlocksToMergeComposite(ctx context.Context, db *pgxpool.Pool, mtreeTable, schema, table string, keyColumns []string, nodePositions []int64, mergeThreshold float64) ([]types.BlockRange, error) {
	return findBlocksToMerge(ctx, db, mtreeTable, schema, table, keyColumns, false, nodePositions, mergeThreshold)
}

func FindBlocksToMergeSimple(ctx context.Context, db *pgxpool.Pool, mtreeTable, schema, table, key string, nodePositions []int64, mergeThreshold float64) ([]types.BlockRange, error) {
	return findBlocksToMerge(ctx, db, mtreeTable, schema, table, []string{key}, true, nodePositions, mergeThreshold)
}

func findBlocksToMerge(ctx context.Context, db DBTX, mtreeTable, schema, table string, key []string, simplePrimaryKey bool, nodePositions []int64, mergeThreshold float64) ([]types.BlockRange, error) {
	// Expand candidate positions to include adjacent neighbors
	posSet := make(map[int64]struct{}, len(nodePositions)*3)
	for _, p := range nodePositions {
		posSet[p] = struct{}{}
		if p > 0 {
			posSet[p-1] = struct{}{}
		}
		posSet[p+1] = struct{}{}
	}
	expandedPositions := make([]int64, 0, len(posSet))
	for p := range posSet {
		expandedPositions = append(expandedPositions, p)
	}

	sanitizedKeys := make([]string, len(key))
	for i, k := range key {
		sanitizedKeys[i] = pgx.Identifier{k}.Sanitize()
	}

	blockSize, err := GetBlockSizeFromMetadata(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}
	mergeThresholdValue := float64(blockSize) * mergeThreshold

	if simplePrimaryKey {
		data := map[string]any{
			"MtreeTable":       mtreeTable,
			"SchemaIdent":      pgx.Identifier{schema}.Sanitize(),
			"TableIdent":       pgx.Identifier{table}.Sanitize(),
			"SimplePrimaryKey": simplePrimaryKey,
			"Key":              sanitizedKeys,
		}
		sql, err := RenderSQL(SQLTemplates.FindBlocksToMerge, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render FindBlocksToMerge SQL: %w", err)
		}
		rows, err := db.Query(ctx, sql, expandedPositions, mergeThresholdValue)
		if err != nil {
			return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()
		var blocks []types.BlockRange
		for rows.Next() {
			var br types.BlockRange
			var start, end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block to merge: %w", err)
			}
			br.RangeStart = []any{start}
			br.RangeEnd = []any{end}
			blocks = append(blocks, br)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over blocks to merge: %w", err)
		}
		return blocks, nil
	}

	// Composite key path: expand attributes to avoid binary composite decoding
	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable":  pgx.Identifier{mtreeTable}.Sanitize(),
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         sanitizedKeys,
		"StartAttrs":  strings.Join(startAttrs, ", "),
		"EndAttrs":    strings.Join(endAttrs, ", "),
	}
	sql, err := RenderSQL(SQLTemplates.FindBlocksToMergeExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToMergeExpanded SQL: %w", err)
	}
	rows, err := db.Query(ctx, sql, expandedPositions, mergeThresholdValue)
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()
	var blocks []types.BlockRange
	for rows.Next() {
		dest := make([]any, 1+len(key)+len(key))
		destPtrs := make([]any, len(dest))
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded block to merge: %w", err)
		}
		var br types.BlockRange
		switch v := dest[0].(type) {
		case int64:
			br.NodePosition = v
		case int32:
			br.NodePosition = int64(v)
		case int:
			br.NodePosition = int64(v)
		default:
			return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):])
		br.RangeStart = startVals
		br.RangeEnd = endVals
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded blocks to merge: %w", err)
	}
	return blocks, nil
}

func GetBlockCountComposite(ctx context.Context, db *pgxpool.Pool, mtreeTable, schema, table, pkeyCols string, nodePosition int64) (*types.BlockCountComposite, error) {
	data := map[string]interface{}{
		"MtreeTable":  mtreeTable,
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    pkeyCols,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockCountComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockCountComposite SQL: %w", err)
	}

	var blockCount types.BlockCountComposite
	err = db.QueryRow(ctx, sql, nodePosition).Scan(&blockCount.NodePosition, &blockCount.RangeStart, &blockCount.RangeEnd, &blockCount.Count)
	if err != nil {
		return nil, fmt.Errorf("query to get block count composite for '%s' failed: %w", mtreeTable, err)
	}

	return &blockCount, nil
}

func GetBlockCountSimple(ctx context.Context, db *pgxpool.Pool, mtreeTable, schema, table, key string, nodePosition int64) (*types.BlockCountSimple, error) {
	data := map[string]interface{}{
		"MtreeTable":  mtreeTable,
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockCountSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockCountSimple SQL: %w", err)
	}

	var blockCount types.BlockCountSimple
	err = db.QueryRow(ctx, sql, nodePosition).Scan(&blockCount.NodePosition, &blockCount.RangeStart, &blockCount.RangeEnd, &blockCount.Count)
	if err != nil {
		return nil, fmt.Errorf("query to get block count simple for '%s' failed: %w", mtreeTable, err)
	}

	return &blockCount, nil
}

func GetBlockSizeFromMetadata(ctx context.Context, pool DBTX, schema, table string) (int, error) {
	data := map[string]interface{}{}
	query, err := RenderSQL(SQLTemplates.GetBlockSizeFromMetadata, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetBlockSizeFromMetadata SQL: %w", err)
	}
	var blockSize int
	err = pool.QueryRow(ctx, query, schema, table).Scan(&blockSize)
	if err != nil {
		return 0, fmt.Errorf("query to get block size from metadata for '%s.%s' failed: %w", schema, table, err)
	}
	return blockSize, nil
}

func GetMaxNodeLevel(ctx context.Context, pool DBTX, mtreeTable string) (int, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetMaxNodeLevel, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetMaxNodeLevel SQL: %w", err)
	}

	var maxLevel int
	err = pool.QueryRow(ctx, sql).Scan(&maxLevel)
	if err != nil {
		return 0, fmt.Errorf("query to get max node level for '%s' failed: %w", mtreeTable, err)
	}

	return maxLevel, nil
}

func DropXORFunction(ctx context.Context, pool DBTX) error {
	sql, err := RenderSQL(SQLTemplates.DropXORFunction, nil)
	if err != nil {
		return fmt.Errorf("failed to render DropXORFunction SQL: %w", err)
	}

	_, err = pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop xor function failed: %w", err)
	}

	return nil
}

func DropMetadataTable(ctx context.Context, pool DBTX) error {
	sql, err := RenderSQL(SQLTemplates.DropMetadataTable, nil)
	if err != nil {
		return fmt.Errorf("failed to render DropMetadataTable SQL: %w", err)
	}

	_, err = pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop metadata table failed: %w", err)
	}

	return nil
}

func DropMtreeTable(ctx context.Context, pool DBTX, mtreeTable string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.DropMtreeTable, data)
	if err != nil {
		return fmt.Errorf("failed to render DropMtreeTable SQL: %w", err)
	}

	_, err = pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop mtree table for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func DeleteParentNodes(ctx context.Context, pool DBTX, mtreeTable string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.DeleteParentNodes, data)
	if err != nil {
		return fmt.Errorf("failed to render DeleteParentNodes SQL: %w", err)
	}

	_, err = pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to delete parent nodes for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func FindBlocksToMergeTx(ctx context.Context, tx pgx.Tx, mtreeTableName string, simplePrimaryKey bool, schema string, table string, key []string, mergeThreshold float64, blockPositions []int64) ([]types.BlockRange, error) {
	return findBlocksToMerge(ctx, tx, mtreeTableName, schema, table, key, simplePrimaryKey, blockPositions, mergeThreshold)
}

func GetBlockWithCountTx(ctx context.Context, tx pgx.Tx, mtreeTable, schema, table string, key []string, isComposite bool, position int64) (*types.BlockRangeWithCount, error) {
	sanitizedKeys := make([]string, len(key))
	for i, k := range key {
		sanitizedKeys[i] = pgx.Identifier{k}.Sanitize()
	}

	data := map[string]interface{}{
		"MtreeTable":  pgx.Identifier{mtreeTable}.Sanitize(),
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"IsComposite": isComposite,
		"Key":         sanitizedKeys,
	}

	var query string
	var err error
	if isComposite {
		// Expand attributes to avoid composite binary scan
		startAttrs := make([]string, len(key))
		endAttrs := make([]string, len(key))
		for i, k := range key {
			attr := pgx.Identifier{k}.Sanitize()
			startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
			endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
		}
		data["StartAttrs"] = strings.Join(startAttrs, ", ")
		data["EndAttrs"] = strings.Join(endAttrs, ", ")
		query, err = RenderSQL(SQLTemplates.GetBlockWithCountExpanded, data)
	} else {
		query, err = RenderSQL(SQLTemplates.GetBlockWithCount, data)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockWithCount SQL: %w", err)
	}

	var block types.BlockRangeWithCount
	var count int64
	var start, end any

	row := tx.QueryRow(ctx, query, position)

	if isComposite {
		// node_position, start attrs..., end attrs..., count
		dest := make([]any, 1+len(key)+len(key)+1)
		destPtrs := make([]any, len(dest))
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := row.Scan(destPtrs...); err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if v, ok := dest[0].(int64); ok {
			block.NodePosition = v
		} else if v2, ok := dest[0].(int32); ok {
			block.NodePosition = int64(v2)
		} else if v3, ok := dest[0].(int); ok {
			block.NodePosition = int64(v3)
		} else {
			return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):1+len(key)+len(key)])
		if c, ok := dest[len(dest)-1].(int64); ok {
			count = c
		} else if c2, ok := dest[len(dest)-1].(int32); ok {
			count = int64(c2)
		} else if c3, ok := dest[len(dest)-1].(int); ok {
			count = int64(c3)
		} else {
			return nil, fmt.Errorf("unexpected type for count: %T", dest[len(dest)-1])
		}
		block.RangeStart = startVals
		block.RangeEnd = endVals
	} else {
		err := row.Scan(&block.NodePosition, &start, &end, &count)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		block.RangeStart = []any{start}
		block.RangeEnd = []any{end}
	}
	block.Count = count
	return &block, nil
}

func UpdateNodePositionTx(ctx context.Context, tx pgx.Tx, mtreeTable string, oldPosition, newPosition int64) error {
	data := map[string]interface{}{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
	}
	query, err := RenderSQL(SQLTemplates.UpdateNodePosition, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateNodePosition SQL: %w", err)
	}
	_, err = tx.Exec(ctx, query, newPosition, oldPosition)
	return err
}

func DeleteBlockTx(ctx context.Context, tx pgx.Tx, mtreeTable string, position int64) error {
	data := map[string]interface{}{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
	}
	query, err := RenderSQL(SQLTemplates.DeleteBlock, data)
	if err != nil {
		return fmt.Errorf("failed to render DeleteBlock SQL: %w", err)
	}
	_, err = tx.Exec(ctx, query, position)
	return err
}

func UpdateNodePositionsSequentialTx(ctx context.Context, tx pgx.Tx, mtreeTable string, startPosition int64) error {
	data := map[string]interface{}{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
	}
	query, err := RenderSQL(SQLTemplates.UpdateNodePositionsSequential, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateNodePositionsSequential SQL: %w", err)
	}
	_, err = tx.Exec(ctx, query, startPosition, startPosition)
	return err
}

func ResetPositionsByStartTx(ctx context.Context, tx pgx.Tx, mtreeTable string, key []string, isComposite bool) error {
	data := map[string]any{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
	}
	var query string
	var err error
	if isComposite {
		startAttrs := make([]string, len(key))
		for i, k := range key {
			startAttrs[i] = fmt.Sprintf("(range_start).%s", pgx.Identifier{k}.Sanitize())
		}
		data["StartAttrs"] = strings.Join(startAttrs, ", ")
		query, err = RenderSQL(SQLTemplates.ResetPositionsByStartExpanded, data)
	} else {
		query, err = RenderSQL(SQLTemplates.ResetPositionsByStart, data)
	}
	if err != nil {
		return fmt.Errorf("failed to render ResetPositionsByStart SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, query); err != nil {
		return fmt.Errorf("query to reset positions failed: %w", err)
	}
	return nil
}

func ComputeLeafHashesTx(ctx context.Context, tx pgx.Tx, schema, table string, cols []string, simpleKey bool, key []string, start []any, end []any) ([]byte, error) {
	// Re-use BlockHashSQL logic. Ensure to pass tx to Scan, not db.
	sql, err := BlockHashSQL(schema, table, key, "MTREE_LEAF_HASH")
	if err != nil {
		return nil, err
	}

	args := make([]any, 0, 2+len(start)+len(end))
	skipMin := len(start) == 0 || start[0] == nil
	args = append(args, skipMin)
	args = append(args, start...)
	skipMax := len(end) == 0 || end[0] == nil
	args = append(args, skipMax)
	args = append(args, end...)

	var leafHash []byte
	if err := tx.QueryRow(ctx, sql, args...).Scan(&leafHash); err != nil {
		return nil, fmt.Errorf("query to compute leaf hashes for '%s.%s' failed: %w", schema, table, err)
	}
	return leafHash, nil
}

func UpdateLeafHashesTx(ctx context.Context, tx pgx.Tx, mtreeTable string, leafHash []byte, nodePosition int64) (int64, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.UpdateLeafHashes, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render UpdateLeafHashes SQL: %w", err)
	}
	var updatedPos int64
	err = tx.QueryRow(ctx, sql, leafHash, nodePosition).Scan(&updatedPos)
	if err != nil {
		return 0, fmt.Errorf("query to update leaf hashes for '%s' failed: %w", mtreeTable, err)
	}
	return updatedPos, nil
}

func UpdateNodePositionsTempTx(ctx context.Context, tx pgx.Tx, mtreeTable string, offset int64) error {
	data := map[string]any{
		"MtreeTable": pgx.Identifier{mtreeTable}.Sanitize(),
	}
	sql, err := RenderSQL(SQLTemplates.UpdateNodePositionsWithOffset, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateNodePositionsTempTx SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, offset); err != nil {
		return fmt.Errorf("query to update node positions to temp failed: %w", err)
	}
	return nil
}

func GetBulkSplitPointsTx(ctx context.Context, tx pgx.Tx, schema, table string, key []string, isComposite bool, start, end []any, blockSize int) ([][]any, error) {
	args := []any{}
	paramIndex := 1

	sanitisedKeyCols := make([]string, len(key))
	for i, k := range key {
		sanitisedKeyCols[i] = pgx.Identifier{k}.Sanitize()
	}
	pkeyColsStr := strings.Join(sanitisedKeyCols, ", ")

	var conditions []string
	if start != nil {
		if isComposite {
			placeholders := make([]string, len(key))
			for i := 0; i < len(key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i)
			}
			conditions = append(conditions, fmt.Sprintf("(%s) >= (%s)", pkeyColsStr, strings.Join(placeholders, ", ")))
			args = append(args, start...)
			paramIndex += len(key)
		} else {
			conditions = append(conditions, fmt.Sprintf("%s >= $%d", sanitisedKeyCols[0], paramIndex))
			args = append(args, start[0])
			paramIndex++
		}
	}
	if end != nil {
		if isComposite {
			placeholders := make([]string, len(key))
			for i := 0; i < len(key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i)
			}
			conditions = append(conditions, fmt.Sprintf("(%s) <= (%s)", pkeyColsStr, strings.Join(placeholders, ", ")))
			args = append(args, end...)
			paramIndex += len(key)
		} else {
			conditions = append(conditions, fmt.Sprintf("%s <= $%d", sanitisedKeyCols[0], paramIndex))
			args = append(args, end[0])
			paramIndex++
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	blockSizePlaceholder := fmt.Sprintf("$%d", paramIndex)
	args = append(args, blockSize)

	data := map[string]any{
		"SchemaIdent":          pgx.Identifier{schema}.Sanitize(),
		"TableIdent":           pgx.Identifier{table}.Sanitize(),
		"PkeyColsStr":          pkeyColsStr,
		"WhereClause":          whereClause,
		"BlockSizePlaceholder": blockSizePlaceholder,
	}

	query, err := RenderSQL(SQLTemplates.GetBulkSplitPoints, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBulkSplitPoints SQL: %w", err)
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute bulk split points query: %w", err)
	}
	defer rows.Close()

	var splitPoints [][]any
	for rows.Next() {
		dest := make([]any, len(key))
		destPtrs := make([]any, len(key))
		for i := range dest {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan split point row: %w", err)
		}
		splitPoints = append(splitPoints, dest)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error iterating over split point rows: %w", rows.Err())
	}

	return splitPoints, nil
}
