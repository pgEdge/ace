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

import "text/template"

type Templates struct {
	EstimateRowCount     *template.Template
	GetPrimaryKey        *template.Template
	GetColumnTypes       *template.Template
	GetColumns           *template.Template
	CheckUserPrivileges  *template.Template
	SpockNodeAndSubInfo  *template.Template
	SpockRepSetInfo      *template.Template
	EnsurePgcrypto       *template.Template
	GetSpockNodeNames    *template.Template
	CheckSchemaExists    *template.Template
	GetTablesInSchema    *template.Template
	GetViewsInSchema     *template.Template
	GetFunctionsInSchema *template.Template
	GetIndicesInSchema   *template.Template
	CheckRepSetExists    *template.Template
	GetTablesInRepSet    *template.Template
	GetPkeyColumnTypes   *template.Template

	CreateMetadataTable              *template.Template
	GetPkeyOffsets                   *template.Template
	CreateSimpleMtreeTable           *template.Template
	CreateIndex                      *template.Template
	CreateCompositeType              *template.Template
	DropCompositeType                *template.Template
	CreateCompositeMtreeTable        *template.Template
	InsertCompositeBlockRanges       *template.Template
	CreateXORFunction                *template.Template
	GetPkeyType                      *template.Template
	UpdateMetadata                   *template.Template
	InsertBlockRanges                *template.Template
	InsertBlockRangesBatchSimple     *template.Template
	InsertBlockRangesBatchComposite  *template.Template
	TDBlockHashSQL                   *template.Template
	MtreeLeafHashSQL                 *template.Template
	UpdateLeafHashes                 *template.Template
	UpdateLeafHashesBatch            *template.Template
	GetBlockRanges                   *template.Template
	GetDirtyAndNewBlocks             *template.Template
	ClearDirtyFlags                  *template.Template
	BuildParentNodes                 *template.Template
	GetRootNode                      *template.Template
	GetNodeChildren                  *template.Template
	GetLeafRanges                    *template.Template
	GetLeafRangesExpanded            *template.Template
	GetRowCountEstimate              *template.Template
	GetMaxValComposite               *template.Template
	UpdateMaxVal                     *template.Template
	GetMaxValSimple                  *template.Template
	GetCountComposite                *template.Template
	GetCountSimple                   *template.Template
	GetSplitPointComposite           *template.Template
	GetSplitPointSimple              *template.Template
	DeleteParentNodes                *template.Template
	GetMaxNodePosition               *template.Template
	UpdateBlockRangeEnd              *template.Template
	UpdateNodePositionsTemp          *template.Template
	DeleteBlock                      *template.Template
	UpdateNodePositionsSequential    *template.Template
	FindBlocksToSplit                *template.Template
	FindBlocksToMerge                *template.Template
	FindBlocksToMergeExpanded        *template.Template
	GetBlockCountComposite           *template.Template
	GetBlockCountSimple              *template.Template
	GetBlockSizeFromMetadata         *template.Template
	GetMaxNodeLevel                  *template.Template
	CompareBlocksSQL                 *template.Template
	DropXORFunction                  *template.Template
	DropMetadataTable                *template.Template
	DropMtreeTable                   *template.Template
	GetBlockRowCount                 *template.Template
	GetBlockWithCount                *template.Template
	GetBlockWithCountExpanded        *template.Template
	UpdateNodePosition               *template.Template
	GetMaxColumnSize                 *template.Template
	UpdateBlockRangeStart            *template.Template
	GetMinValComposite               *template.Template
	GetMinValSimple                  *template.Template
	GetDirtyAndNewBlocksExpanded     *template.Template
	FindBlocksToSplitExpanded        *template.Template
	ResetPositionsByStart            *template.Template
	ResetPositionsByStartFromTemp    *template.Template
	ResetPositionsByStartExpanded    *template.Template
	GetBulkSplitPoints               *template.Template
	UpdateBlockRangeStartComposite   *template.Template
	UpdateBlockRangeEndComposite     *template.Template
	UpdateAllLeafNodePositionsToTemp *template.Template
	MarkBlockDirty                   *template.Template
	CreateCDCMetadataTable           *template.Template
	UpdateCDCMetadata                *template.Template
	AlterPublicationAddTable         *template.Template
	CreatePublication                *template.Template
	CreateReplicationSlot            *template.Template
	DropPublication                  *template.Template
	DropReplicationSlot              *template.Template
	DropCDCMetadataTable             *template.Template
	GetCDCMetadata                   *template.Template
	UpdateMtreeCounters              *template.Template
	GetReplicationSlotPID            *template.Template
	TerminateBackend                 *template.Template
	CheckPIDExists                   *template.Template
	CreateSchema                     *template.Template
	AlterPublicationDropTable        *template.Template
	DeleteMetadata                   *template.Template
	RemoveTableFromCDCMetadata       *template.Template
	GetSpockOriginLSNForNode         *template.Template
	GetSpockSlotLSNForNode           *template.Template
	GetNativeOriginLSNForNode        *template.Template
	GetNativeSlotLSNForNode          *template.Template
	GetReplicationOriginByName       *template.Template
	CreateReplicationOrigin          *template.Template
	SetupReplicationOriginSession    *template.Template
	ResetReplicationOriginSession    *template.Template
	SetupReplicationOriginXact       *template.Template
	ResetReplicationOriginXact       *template.Template
}

var SQLTemplates = Templates{
	// A template isn't needed for this query; just keeping the struct uniform
	CreateMetadataTable: template.Must(template.New("createMetadataTable").Parse(`
		CREATE TABLE IF NOT EXISTS spock.ace_mtree_metadata (
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
	CreatePublication: template.Must(template.New("createPublication").Parse(`
		CREATE PUBLICATION {{.PublicationName}}
	`)),
	CreateReplicationSlot: template.Must(template.New("createReplicationSlot").Parse(`
		SELECT pg_create_logical_replication_slot('{{.SlotName}}', 'pgoutput')
	`)),

	AlterPublicationAddTable: template.Must(template.New("alterPublicationAddTable").Parse(`
		ALTER PUBLICATION {{.PublicationName}} ADD TABLE {{.TableName}}
	`)),

	AlterPublicationDropTable: template.Must(template.New("alterPublicationDropTable").Parse(`
		ALTER PUBLICATION {{.PublicationName}} DROP TABLE {{.TableName}}
	`)),

	RemoveTableFromCDCMetadata: template.Must(template.New("removeTableFromCDCMetadata").Parse(`
		UPDATE spock.ace_cdc_metadata
		SET tables = array_remove(tables, $1)
		WHERE publication_name = $2
	`)),

	MarkBlockDirty: template.Must(template.New("markBlockDirty").Parse(`
		UPDATE {{.MtreeTable}}
		SET dirty = true
		WHERE
			node_level = 0
			AND (
				'{{.PkeyValue}}' >= range_start AND (
					'{{.PkeyValue}}' <= range_end OR range_end IS NULL
				)
			)
	`)),

	UpdateCDCMetadata: template.Must(template.New("updateCdcMetadata").Parse(`
		INSERT INTO
			spock.ace_cdc_metadata (
				publication_name,
				slot_name,
				start_lsn,
				tables,
				last_updated
			)
		VALUES
			(
				$1,
				$2,
				$3,
				$4,
				current_timestamp
			)
		ON CONFLICT (publication_name) DO
		UPDATE
		SET
			slot_name = EXCLUDED.slot_name,
			start_lsn = EXCLUDED.start_lsn,
			tables = EXCLUDED.tables,
			last_updated = EXCLUDED.last_updated
	`)),

	DropPublication: template.Must(template.New("dropPublication").Parse(`
		DROP PUBLICATION IF EXISTS {{.PublicationName}}
	`)),
	DropReplicationSlot: template.Must(template.New("dropReplicationSlot").Parse(`
		SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{{.SlotName}}'
	`)),
	GetReplicationSlotPID: template.Must(template.New("getReplicationSlotPID").Parse(`
		SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1 AND active = true
	`)),
	TerminateBackend: template.Must(template.New("terminateBackend").Parse(`
		SELECT pg_terminate_backend($1)
	`)),
	CheckPIDExists: template.Must(template.New("checkPIDExists").Parse(`
		SELECT pid FROM pg_stat_activity WHERE pid = $1
	`)),
	DropCDCMetadataTable: template.Must(template.New("dropCDCMetadataTable").Parse(`
		DROP TABLE IF EXISTS spock.ace_cdc_metadata
	`)),

	GetCDCMetadata: template.Must(template.New("getCDCMetadata").Parse(`
		SELECT
			slot_name,
			start_lsn,
			tables
		FROM
			spock.ace_cdc_metadata
		WHERE
			publication_name = $1
	`)),

	UpdateMtreeCounters: template.Must(template.New("updateMtreeCounters").Parse(`
		WITH pkeys_to_update AS (
			SELECT unnest(@inserts::text[]) AS pkey, 'insert' AS op
			UNION ALL
			SELECT unnest(@deletes::text[]) AS pkey, 'delete' AS op
			UNION ALL
			SELECT unnest(@updates::text[]) AS pkey, 'update' AS op
		),
		first_block AS (
			SELECT
				node_position,
				range_start
			FROM
				{{.MtreeTable}}
			WHERE
				node_level = 0
			ORDER BY
				range_start ASC
			LIMIT 1
		),
		new_min_pkey AS (
			SELECT MIN(p.pkey) as pkey
			FROM pkeys_to_update p
			WHERE p.op = 'insert' AND (
				{{if .IsComposite}}
					p.pkey::{{.CompositeTypeName}} < (SELECT range_start FROM first_block)
				{{else}}
					p.pkey::{{.PkeyType}} < (SELECT range_start FROM first_block)
				{{end}}
			)
		),
		blocks_to_update AS (
			SELECT
				mt.node_position,
				SUM(CASE WHEN p.op = 'insert' THEN 1 ELSE 0 END) AS insert_count,
				SUM(CASE WHEN p.op = 'delete' THEN 1 ELSE 0 END) AS delete_count
			FROM
				{{.MtreeTable}} mt
			JOIN
				pkeys_to_update p ON (
					{{if .IsComposite}}
						p.pkey::{{.CompositeTypeName}} >= mt.range_start AND (mt.range_end IS NULL OR p.pkey::{{.CompositeTypeName}} <= mt.range_end)
					{{else}}
						p.pkey::{{.PkeyType}} >= mt.range_start AND (mt.range_end IS NULL OR p.pkey::{{.PkeyType}} <= mt.range_end)
					{{end}}
				) OR (
					mt.node_position = (SELECT node_position FROM first_block) AND
					{{if .IsComposite}}
						p.pkey::{{.CompositeTypeName}} < (SELECT range_start FROM first_block)
					{{else}}
						p.pkey::{{.PkeyType}} < (SELECT range_start FROM first_block)
					{{end}}
				)
			WHERE
				mt.node_level = 0
			GROUP BY
				mt.node_position
		)
		UPDATE
			{{.MtreeTable}} mt
		SET
			dirty = true,
			inserts_since_tree_update = mt.inserts_since_tree_update + b.insert_count,
			deletes_since_tree_update = mt.deletes_since_tree_update + b.delete_count,
			last_modified = current_timestamp,
			range_start = CASE
				WHEN mt.node_position = (SELECT node_position FROM first_block) AND (SELECT pkey FROM new_min_pkey) IS NOT NULL
				THEN
					{{if .IsComposite}}
						(SELECT pkey FROM new_min_pkey)::{{.CompositeTypeName}}
					{{else}}
						(SELECT pkey FROM new_min_pkey)::{{.PkeyType}}
					{{end}}
				ELSE mt.range_start
			END
		FROM
			blocks_to_update b
		WHERE
			mt.node_level = 0
			AND mt.node_position = b.node_position;
	`)),

	CreateCDCMetadataTable: template.Must(template.New("createCDCMetadataTable").Parse(`
		CREATE TABLE IF NOT EXISTS spock.ace_cdc_metadata (
			publication_name text PRIMARY KEY,
			slot_name text,
			start_lsn text,
			tables text[],
			last_updated timestamptz
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
			c.relname = $2
			AND n.nspname = $1
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
			AND table_name = $2
		ORDER BY
			ordinal_position;
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
	EnsurePgcrypto: template.Must(template.New("ensurePgcrypto").Parse(`
		CREATE EXTENSION IF NOT EXISTS pgcrypto;
	`)),
	GetSpockNodeNames: template.Must(template.New("getSpockNodeNames").Parse(`
		SELECT
			node_id::text,
			node_name
		FROM
			spock.node;
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
				{{- if .HasFilter }}
			WHERE
				{{.Filter}}
				{{- end }}
			ORDER BY
				{{.KeyColumnsOrder}}
		),
		first_row AS (
			SELECT
				{{.KeyColumnsSelect}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
				{{- if .HasFilter }}
			WHERE
				{{.Filter}}
				{{- end }}
			ORDER BY
				{{.KeyColumnsOrder}}
			LIMIT 1
		),
		last_row AS (
			SELECT
				{{.KeyColumnsSelect}}
			FROM
				{{.SchemaIdent}}.{{.TableIdent}}
				{{- if .HasFilter }}
			WHERE
				{{.Filter}}
				{{- end }}
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
		OR REPLACE FUNCTION spock.bytea_xor(a bytea, b bytea) RETURNS bytea AS $$
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
				PROCEDURE = spock.bytea_xor
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
			spock.ace_mtree_metadata (
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
	DeleteMetadata: template.Must(template.New("deleteMetadata").Parse(`
		DELETE FROM spock.ace_mtree_metadata WHERE schema_name = $1 AND table_name = $2
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
        WHERE {{.WhereClause}}
    `)),
	MtreeLeafHashSQL: template.Must(template.New("mtreeLeafHashSQL").Parse(`
        SELECT digest(COALESCE(string_agg({{.TableAlias}}::text, '|' ORDER BY {{.PkOrderByStr}}), 'EMPTY_BLOCK'), 'sha256')
        FROM {{.SchemaIdent}}.{{.TableIdent}} AS {{.TableAlias}}
        WHERE {{.WhereClause}}
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
	UpdateLeafHashesBatch: template.Must(template.New("updateLeafHashesBatch").Parse(`
		UPDATE
			{{.MtreeTable}} mt
		SET
			leaf_hash = $1,
			node_hash = $1,
			last_modified = current_timestamp
		WHERE
			node_position = $2
			AND mt.node_level = 0
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
	GetLeafRangesExpanded: template.Must(template.New("getLeafRangesExpanded").Parse(`
		SELECT
			{{.StartAttrs}},
			{{.EndAttrs}}
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
			spock.ace_mtree_metadata
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
			WHERE t1.node_level = 0
			{{if .UsePositionFilter}} AND t1.node_position = ANY({{.PositionPlaceholder}}){{end}}
			GROUP BY t1.node_position, t1.range_start, t1.range_end
		)
		SELECT node_position, range_start, range_end
		FROM BlockCounts
		WHERE actual_rows < {{.MergeValPlaceholder}}
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
			WHERE t1.node_level = 0
			{{if .UsePositionFilter}} AND t1.node_position = ANY({{.PositionPlaceholder}}){{end}}
			GROUP BY t1.node_position
		)
		SELECT t1.node_position,
			{{.StartAttrs}},
			{{.EndAttrs}}
		FROM {{.MtreeTable}} t1
		JOIN BlockCounts bc ON bc.node_position = t1.node_position
		WHERE bc.actual_rows < {{.MergeValPlaceholder}}
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
			spock.ace_mtree_metadata
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
		DROP FUNCTION IF EXISTS spock.bytea_xor(bytea, bytea) CASCADE
	`)),
	DropMetadataTable: template.Must(template.New("dropMetadataTable").Parse(`
		DROP TABLE IF EXISTS spock.ace_mtree_metadata CASCADE
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

	ResetPositionsByStartFromTemp: template.Must(template.New("resetPositionsByStartFromTemp").Parse(`
		WITH seq AS (
			SELECT node_position,
				   row_number() OVER (ORDER BY range_start) - 1 AS pos_seq
			FROM {{.MtreeTable}}
			WHERE node_level = 0 AND node_position >= $1
		)
		UPDATE {{.MtreeTable}} mt
		SET node_position = s.pos_seq
		FROM seq s
		WHERE mt.node_level = 0 AND mt.node_position = s.node_position
`)),

	ResetPositionsByStartExpanded: template.Must(template.New("resetPositionsByStartExpanded").Parse(`
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
			(rn - 1) % {{.BlockSizePlaceholder}} = 0 AND rn > 1
		ORDER BY
			{{.PkeyColsStr}}
	`)),
	UpdateBlockRangeStartComposite: template.Must(template.New("updateBlockRangeStartComposite").Parse(`
		UPDATE {{.MtreeTable}}
		SET range_start = {{if .IsNull}}NULL{{else}}ROW({{.Placeholders}})::{{.CompositeTypeName}}{{end}},
			dirty = true,
			last_modified = current_timestamp
		WHERE node_position = {{.NodePositionPlaceholder}} AND node_level = 0
	`)),
	UpdateBlockRangeEndComposite: template.Must(template.New("updateBlockRangeEndComposite").Parse(`
		UPDATE {{.MtreeTable}}
		SET range_end = {{if .IsNull}}NULL{{else}}ROW({{.Placeholders}})::{{.CompositeTypeName}}{{end}},
			dirty = true,
			last_modified = current_timestamp
		WHERE node_position = {{.NodePositionPlaceholder}} AND node_level = 0
	`)),
	UpdateAllLeafNodePositionsToTemp: template.Must(template.New("updateAllLeafNodePositionsToTemp").Parse(`
		UPDATE {{.MtreeTable}} SET node_position = node_position + $1 WHERE node_level = 0
	`)),
	CreateSchema: template.Must(template.New("createSchema").Parse(`
		CREATE SCHEMA IF NOT EXISTS {{.SchemaName}}
	`)),
	GetSpockOriginLSNForNode: template.Must(template.New("getSpockOriginLSNForNode").Parse(`
		SELECT ros.remote_lsn::text
		FROM pg_catalog.pg_replication_origin_status ros
		JOIN pg_catalog.pg_replication_origin ro ON ro.roident = ros.local_id
		JOIN spock.subscription s ON ro.roname LIKE '%' || s.sub_name
		JOIN spock.node o ON o.node_id = s.sub_origin
		WHERE o.node_name = $1
			AND ros.remote_lsn IS NOT NULL
		LIMIT 1
	`)),
	GetSpockSlotLSNForNode: template.Must(template.New("getSpockSlotLSNForNode").Parse(`
		SELECT rs.confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots rs
		JOIN spock.subscription s ON rs.slot_name = s.sub_slot_name
		JOIN spock.node o ON o.node_id = s.sub_origin
		WHERE o.node_name = $1
			AND rs.confirmed_flush_lsn IS NOT NULL
		ORDER BY rs.confirmed_flush_lsn DESC
		LIMIT 1
	`)),
	GetNativeOriginLSNForNode: template.Must(template.New("getNativeOriginLSNForNode").Parse(`
		SELECT ros.remote_lsn::text
		FROM pg_catalog.pg_replication_origin_status ros
		JOIN pg_catalog.pg_replication_origin ro ON ro.roident = ros.local_id
		JOIN pg_catalog.pg_subscription s ON ro.roname LIKE 'pg_%' || s.oid::text
		WHERE s.subname LIKE '%' || $1 || '%'
			AND ros.remote_lsn IS NOT NULL
		LIMIT 1
	`)),
	GetNativeSlotLSNForNode: template.Must(template.New("getNativeSlotLSNForNode").Parse(`
		SELECT rs.confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots rs
		JOIN pg_catalog.pg_subscription s ON rs.slot_name = s.subslotname
		WHERE s.subname LIKE '%' || $1 || '%'
			AND rs.confirmed_flush_lsn IS NOT NULL
		ORDER BY rs.confirmed_flush_lsn DESC
		LIMIT 1
	`)),
	GetReplicationOriginByName: template.Must(template.New("getReplicationOriginByName").Parse(`
		SELECT roident FROM pg_replication_origin WHERE roname = $1
	`)),
	CreateReplicationOrigin: template.Must(template.New("createReplicationOrigin").Parse(`
		SELECT pg_replication_origin_create($1)
	`)),
	SetupReplicationOriginSession: template.Must(template.New("setupReplicationOriginSession").Parse(`
		SELECT pg_replication_origin_session_setup($1)
	`)),
	ResetReplicationOriginSession: template.Must(template.New("resetReplicationOriginSession").Parse(`
		SELECT pg_replication_origin_session_reset()
	`)),
	SetupReplicationOriginXact: template.Must(template.New("setupReplicationOriginXact").Parse(`
		SELECT pg_replication_origin_xact_setup($1, $2)
	`)),
	ResetReplicationOriginXact: template.Must(template.New("resetReplicationOriginXact").Parse(`
		SELECT pg_replication_origin_xact_reset()
	`)),
}
