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

import (
	"text/template"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/pkg/config"
)

// aceTemplateFuncs provides the {{aceSchema}} function to SQL templates.
// It runs at render time, after config is loaded.
var aceTemplateFuncs = template.FuncMap{
	"aceSchema": func() string { return pgx.Identifier{config.Get().MTree.Schema}.Sanitize() },
}

type Templates struct {
	EstimateRowCount         *template.Template
	GetPrimaryKey            *template.Template
	GetColumnTypes           *template.Template
	GetColumns               *template.Template
	CheckUserPrivileges      *template.Template
	SpockNodeAndSubInfo      *template.Template
	SpockRepSetInfo          *template.Template
	EnsurePgcrypto           *template.Template
	GetSpockNodeNames        *template.Template
	CheckSchemaExists        *template.Template
	GetTablesInSchema        *template.Template
	GetForeignTablesInSchema *template.Template
	GetViewsInSchema         *template.Template
	GetFunctionsInSchema     *template.Template
	GetIndicesInSchema       *template.Template
	CheckRepSetExists        *template.Template
	GetTablesInRepSet        *template.Template
	GetPkeyColumnTypes       *template.Template
	GetRelationTree          *template.Template

	// Structure-comparison descriptors. Each reads one aspect of table
	// structure for every table named in $2, scoped to schema $1. Each
	// query is small, independently readable, and testable.
	GetColumnDescriptors     *template.Template
	GetReplicaIdentityKey    *template.Template
	GetConstraintDescriptors *template.Template
	GetPartitionDescriptors  *template.Template
	GetDomainDescriptors     *template.Template
	GetRangeDescriptors      *template.Template
	GetCompositeAttributes   *template.Template
	GetEnumLabels            *template.Template
	GetTypeReferences        *template.Template
	GetDatabaseLocale        *template.Template
	QuoteIdentifiers         *template.Template

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
	UpdateLeafHashesBatch           *template.Template

	GetDirtyAndNewBlocks       *template.Template
	ClearDirtyFlags            *template.Template
	MarkLeavesDirtyByPositions *template.Template
	BuildParentNodes           *template.Template
	GetRootNode                *template.Template
	GetNodeChildren            *template.Template
	GetLeafRanges              *template.Template
	GetLeafRangesExpanded      *template.Template
	GetRowCountEstimate        *template.Template
	GetMaxValComposite         *template.Template
	UpdateMaxVal               *template.Template
	GetMaxValSimple            *template.Template
	GetCountComposite          *template.Template
	GetCountSimple             *template.Template

	DeleteParentNodes             *template.Template
	GetMaxNodePosition            *template.Template
	UpdateBlockRangeEnd           *template.Template
	UpdateNodePositionsTemp       *template.Template
	DeleteBlock                   *template.Template
	UpdateNodePositionsSequential *template.Template
	FindBlocksToSplit             *template.Template
	FindBlocksToMerge             *template.Template
	FindBlocksToMergeExpanded     *template.Template
	GetBlockCountComposite        *template.Template
	GetBlockCountSimple           *template.Template
	GetBlockSizeFromMetadata      *template.Template
	GetMaxNodeLevel               *template.Template
	CompareBlocksSQL              *template.Template

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
	GetReplicationOriginNames        *template.Template
	GetNativeNodeOriginNames         *template.Template
	EnsureHashVersionColumn          *template.Template
	GetHashVersion                   *template.Template
	MarkAllLeavesDirty               *template.Template
	UpdateHashVersion                *template.Template
	GetReplicationOriginByName       *template.Template
	CreateReplicationOrigin          *template.Template
	SetupReplicationOriginSession    *template.Template
	ResetReplicationOriginSession    *template.Template
	SetupReplicationOriginXact       *template.Template
	ResetReplicationOriginXact       *template.Template

	InitCDCMetadata     *template.Template
	CurrentWalInsertLSN *template.Template
}

var SQLTemplates = Templates{
	// A template isn't needed here; kept for struct uniformity.
	CreateMetadataTable: template.Must(template.New("createMetadataTable").Funcs(aceTemplateFuncs).Parse(`
		CREATE TABLE IF NOT EXISTS {{aceSchema}}.ace_mtree_metadata (
			schema_name text,
			table_name text,
			total_rows bigint,
			block_size int,
			num_blocks int,
			is_composite boolean NOT NULL DEFAULT false,
			hash_version int NOT NULL DEFAULT 2,
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

	RemoveTableFromCDCMetadata: template.Must(template.New("removeTableFromCDCMetadata").Funcs(aceTemplateFuncs).Parse(`
		UPDATE {{aceSchema}}.ace_cdc_metadata
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

	UpdateCDCMetadata: template.Must(template.New("updateCdcMetadata").Funcs(aceTemplateFuncs).Parse(`
		INSERT INTO
			{{aceSchema}}.ace_cdc_metadata (
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

	// On conflict every column including pub_commit_lsn is refreshed,
	// since reaching this path means a re-init produced a fresh
	// publication with a new commit LSN that later reads must see.
	InitCDCMetadata: template.Must(template.New("initCdcMetadata").Funcs(aceTemplateFuncs).Parse(`
		INSERT INTO
			{{aceSchema}}.ace_cdc_metadata (
				publication_name,
				slot_name,
				start_lsn,
				pub_commit_lsn,
				tables,
				last_updated
			)
		VALUES
			(
				$1,
				$2,
				$3,
				$4,
				$5,
				current_timestamp
			)
		ON CONFLICT (publication_name) DO
		UPDATE
		SET
			slot_name = EXCLUDED.slot_name,
			start_lsn = EXCLUDED.start_lsn,
			pub_commit_lsn = EXCLUDED.pub_commit_lsn,
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
	DropCDCMetadataTable: template.Must(template.New("dropCDCMetadataTable").Funcs(aceTemplateFuncs).Parse(`
		DROP TABLE IF EXISTS {{aceSchema}}.ace_cdc_metadata
	`)),

	// pub_commit_lsn is extracted via to_jsonb(row) ->> 'pub_commit_lsn' so
	// the query still works on pre-migration ace_cdc_metadata tables
	// missing that column: ->> then returns NULL and COALESCE yields an
	// empty string, treated as "invariant uncheckable, warn and skip".
	// CreateCDCMetadataTable's additive ALTER TABLE backfills the column
	// on the next MtreeInit so later reads use it directly.
	GetCDCMetadata: template.Must(template.New("getCDCMetadata").Funcs(aceTemplateFuncs).Parse(`
		SELECT
			m.slot_name,
			m.start_lsn,
			m.tables,
			COALESCE(to_jsonb(m) ->> 'pub_commit_lsn', '') AS pub_commit_lsn
		FROM
			{{aceSchema}}.ace_cdc_metadata AS m
		WHERE
			m.publication_name = $1
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

	CreateCDCMetadataTable: template.Must(template.New("createCDCMetadataTable").Funcs(aceTemplateFuncs).Parse(`
		CREATE TABLE IF NOT EXISTS {{aceSchema}}.ace_cdc_metadata (
			publication_name text PRIMARY KEY,
			slot_name text,
			start_lsn text,
			pub_commit_lsn text,
			tables text[],
			last_updated timestamptz
		);
		-- Forward-compatible addition for clusters that ran older versions.
		ALTER TABLE {{aceSchema}}.ace_cdc_metadata
			ADD COLUMN IF NOT EXISTS pub_commit_lsn text;`),
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
			n.node_id::bigint,
			n.node_name,
			n.location,
			n.country,
			s.sub_id::bigint,
			s.sub_name,
			s.sub_enabled,
			s.sub_replication_sets,
			COALESCE(o.node_name, '') AS sub_origin_name
		FROM
			spock.node n
			LEFT OUTER JOIN spock.subscription s ON s.sub_target = n.node_id
			LEFT OUTER JOIN spock.node o ON o.node_id = s.sub_origin
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
			WHERE
				set_name IS NOT NULL
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
	// GetRelationTree walks pg_inherits from one table down to every
	// descendant in a single query. Depth 0 is the table itself. relkind
	// tells heap (r), partitioned (p), and foreign (f) relations apart.
	GetRelationTree: template.Must(template.New("getRelationTree").Parse(`
		WITH RECURSIVE tree AS (
			SELECT c.oid, c.relkind, 0 AS depth, NULL::oid AS parent_oid
			FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
		  UNION ALL
			SELECT c.oid, c.relkind, t.depth + 1, i.inhparent
			FROM tree t
			JOIN pg_catalog.pg_inherits i ON i.inhparent = t.oid
			JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
		)
		SELECT
			n.nspname,
			c.relname,
			t.relkind::text,
			t.depth,
			COALESCE(pn.nspname || '.' || pc.relname, '') AS parent
		FROM tree t
		JOIN pg_catalog.pg_class c ON c.oid = t.oid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_class pc ON pc.oid = t.parent_oid
		LEFT JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
		ORDER BY t.depth, n.nspname, c.relname;
	`)),
	GetTablesInSchema: template.Must(template.New("getTablesInSchema").Parse(`
		SELECT
			table_name
		FROM
			information_schema.tables
		WHERE
			table_schema = $1
			AND table_type = 'BASE TABLE';
	`)),
	GetForeignTablesInSchema: template.Must(template.New("getForeignTablesInSchema").Parse(`
		SELECT
			table_name
		FROM
			information_schema.tables
		WHERE
			table_schema = $1
			AND table_type = 'FOREIGN'
		ORDER BY table_name;
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
		`SELECT EXISTS(SELECT 1 FROM spock.replication_set WHERE set_name = $1);`,
	)),
	GetTablesInRepSet: template.Must(template.New("getTablesInRepSet").Parse(
		`SELECT nspname, relname FROM spock.tables WHERE set_name = $1;`,
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
	// GetColumnDescriptors reads every property of every live, non-dropped
	// column of the given tables that structure comparison cares about:
	// type, nullability, identity/generated-ness, any per-column options,
	// the collation and its version, and the default expression. Ordered
	// by column name, since column order carries no structural meaning.
	//
	// A column's type identity is carried as (type_namespace, type_name,
	// atttypmod), which stays meaningful across independently initialized
	// clusters even though object OIDs differ between them. atttypid is
	// selected only so CollectSnapshot can use it, on this node's own
	// connection, to fetch domain/range/composite/enum descriptors for
	// the types actually in play.
	//
	// type_kind is pg_type.typtype: 'b' base, 'd' domain, 'e' enum,
	// 'r' range, 'c' composite. It lets the comparison layer decide
	// whether a type-name mismatch can be reasoned about via the
	// built-in narrowing tables or must be treated as a plain difference.
	GetColumnDescriptors: template.Must(template.New("getColumnDescriptors").Parse(`
		SELECT
			c.relname,
			a.attname,
			a.atttypid,
			a.atttypmod,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_text,
			tn.nspname                                       AS type_namespace,
			t.typname                                        AS type_name,
			t.typtype::text                                  AS type_kind,
			a.attnotnull,
			a.attidentity::text,
			a.attgenerated::text,
			COALESCE(a.attoptions::text, '')                AS options,
			-- A collation's identity is (namespace, name), for the same
			-- reason a type's is: two schemas can each hold a collation
			-- named "en_US" that resolve differently.
			COALESCE(cn.nspname, '')                         AS collnamespace,
			COALESCE(co.collname, '')                        AS collname,
			COALESCE(co.collprovider::text, '')              AS collprovider,
			-- collversion is what the catalog recorded when the collation
			-- was created or last REFRESHed - NOT the version of the
			-- collation library running now. An unrefreshed glibc upgrade
			-- leaves this string matching on both nodes while the two nodes
			-- actually sort differently, so GetDatabaseLocale is what
			-- catches the common case; this field only catches a node whose
			-- catalog was refreshed against a different library.
			COALESCE(co.collversion, '')                     AS collversion,
			COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS default_expr
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
		JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
		LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
		WHERE n.nspname = $1
			AND c.relname = ANY($2::text[])
			AND a.attnum > 0
			AND NOT a.attisdropped
		-- COLLATE "C" on every text ordering in this file. Row order is an
		-- input to comparison wherever it is preserved rather than re-sorted
		-- (array_agg of a domain's CHECKs, a composite's attributes), and a
		-- database-default collation differing between two nodes would
		-- otherwise reorder identical catalogs. Pinning the collation costs
		-- nothing and removes the question.
		ORDER BY c.relname COLLATE "C", a.attname COLLATE "C";
	`)),
	// GetDomainDescriptors resolves what a domain (typtype='d') actually
	// constrains, for every domain OID in $1: one level of typbasetype,
	// so a domain-over-domain's further narrowing is only caught when
	// that inner domain is itself directly used by some compared column.
	// The base type is named portably (namespace, name, typmod), like a
	// column's own type. Constraints are aggregated as an array, one row
	// per domain, sorted by definition text, since CHECK (VALUE ...)
	// constraints on a domain get invented names like table constraints
	// do.
	//
	// $1 is an oid[] gathered from this node's own GetColumnDescriptors
	// result and stays local to this node; only the resolved names below
	// are compared across nodes.
	GetDomainDescriptors: template.Must(template.New("getDomainDescriptors").Parse(`
		SELECT
			t.oid,
			n.nspname,
			t.typname,
			bn.nspname                                            AS base_namespace,
			bt.typname                                            AS base_name,
			-- typtype belongs to the base type's identity: dropping an enum
			-- and recreating the name as a domain leaves (namespace, name)
			-- untouched, so without this the substitution is invisible.
			bt.typtype::text                                      AS base_kind,
			t.typtypmod,
			-- Display only, never compared: the base type as a person writes
			-- it, modifier included ("character varying(20)"), so a report
			-- about a domain narrowed from varchar(20) to varchar(10) shows
			-- the length rather than printing "varchar" on both sides. The
			-- comparison itself keys on (base_namespace, base_name,
			-- typtypmod) above, exactly as a column's type does.
			pg_catalog.format_type(t.typbasetype, t.typtypmod)    AS base_text,
			t.typnotnull,
			COALESCE(pg_catalog.pg_get_expr(t.typdefaultbin, 0), t.typdefault, '') AS default_expr,
			COALESCE(chk.defs, '{}')                              AS check_defs
		FROM pg_catalog.pg_type t
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_catalog.pg_type bt ON bt.oid = t.typbasetype
		JOIN pg_catalog.pg_namespace bn ON bn.oid = bt.typnamespace
		LEFT JOIN LATERAL (
			SELECT array_agg(pg_catalog.pg_get_constraintdef(ct.oid, true) ORDER BY pg_catalog.pg_get_constraintdef(ct.oid, true) COLLATE "C") AS defs
			FROM pg_catalog.pg_constraint ct
			WHERE ct.contypid = t.oid AND ct.contype = 'c'
		) chk ON true
		WHERE t.oid = ANY($1::oid[]) AND t.typtype = 'd';
	`)),
	// GetRangeDescriptors resolves a range type's (typtype='r') subtype and
	// the functions/collation/opclass that define its ordering, since any
	// of these changes what "the same range" means. Functions are printed
	// via ::regprocedure::text for a portable, schema-qualified signature
	// (e.g. "public.my_canon(daterange)").
	GetRangeDescriptors: template.Must(template.New("getRangeDescriptors").Parse(`
		SELECT
			t.oid,
			n.nspname,
			t.typname,
			sn.nspname                                       AS subtype_namespace,
			st.typname                                       AS subtype_name,
			-- See base_kind in GetDomainDescriptors.
			st.typtype::text                                 AS subtype_kind,
			-- Collation and operator class are named (namespace, name) for
			-- the same reason types are: an unqualified opcname is only
			-- unique within one namespace and access method.
			CASE WHEN co.oid IS NULL THEN ''
				ELSE con.nspname || '.' || co.collname
			END                                              AS collation,
			CASE WHEN oc.oid IS NULL THEN ''
				ELSE ocn.nspname || '.' || oc.opcname
			END                                              AS opclass,
			CASE WHEN r.rngcanonical = 0 THEN '' ELSE r.rngcanonical::pg_catalog.regprocedure::text END AS canonical,
			CASE WHEN r.rngsubdiff = 0 THEN '' ELSE r.rngsubdiff::pg_catalog.regprocedure::text END     AS subtype_diff
		FROM pg_catalog.pg_type t
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
		JOIN pg_catalog.pg_type st ON st.oid = r.rngsubtype
		JOIN pg_catalog.pg_namespace sn ON sn.oid = st.typnamespace
		LEFT JOIN pg_catalog.pg_collation co ON co.oid = r.rngcollation
		LEFT JOIN pg_catalog.pg_namespace con ON con.oid = co.collnamespace
		LEFT JOIN pg_catalog.pg_opclass oc ON oc.oid = r.rngsubopc
		LEFT JOIN pg_catalog.pg_namespace ocn ON ocn.oid = oc.opcnamespace
		WHERE t.oid = ANY($1::oid[]) AND t.typtype = 'r';
	`)),
	// GetCompositeAttributes reads a composite type's (typtype='c') own
	// attributes, one row per attribute, ordered by attnum: a composite
	// type's attnum order is its wire/row-literal layout, fixed at
	// CREATE TYPE time.
	GetCompositeAttributes: template.Must(template.New("getCompositeAttributes").Parse(`
		SELECT
			t.oid,
			n.nspname,
			t.typname,
			-- attnum, not a dense 1..n counter: DROP ATTRIBUTE leaves gaps,
			-- and an ordinal that renumbers after a gap makes every later
			-- attribute look changed when only one was dropped.
			a.attnum,
			a.attname,
			an.nspname                                       AS attr_type_namespace,
			at.typname                                       AS attr_type_name,
			-- See base_kind in GetDomainDescriptors.
			at.typtype::text                                 AS attr_type_kind,
			a.atttypmod,
			-- Display only, never compared (see base_text above).
			pg_catalog.format_type(a.atttypid, a.atttypmod)  AS attr_type_text,
			CASE WHEN co.oid IS NULL THEN ''
				ELSE cn.nspname || '.' || co.collname
			END                                              AS collation
		FROM pg_catalog.pg_type t
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_catalog.pg_attribute a ON a.attrelid = t.typrelid
		JOIN pg_catalog.pg_type at ON at.oid = a.atttypid
		JOIN pg_catalog.pg_namespace an ON an.oid = at.typnamespace
		LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
		WHERE t.oid = ANY($1::oid[]) AND t.typtype = 'c'
			AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY t.oid, a.attnum;
	`)),
	// GetEnumLabels reads an enum type's (typtype='e') labels in
	// enumsortorder, since order is the defining property of an enum.
	GetEnumLabels: template.Must(template.New("getEnumLabels").Parse(`
		SELECT
			t.oid,
			n.nspname,
			t.typname,
			e.enumlabel
		FROM pg_catalog.pg_type t
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
		WHERE t.oid = ANY($1::oid[]) AND t.typtype = 'e'
		ORDER BY t.oid, e.enumsortorder;
	`)),
	// GetTypeReferences reports, for each type OID in $1, its typtype and
	// every other type it is built out of. The caller walks this to a fixed
	// point, so a type is compared however deeply it is buried.
	//
	// Looking only at a column's own typtype misses most of the interesting
	// cases: an array type is itself a base type ('b'), so a "status[]"
	// column hides the enum entirely, and a composite's attribute or a
	// domain's base type can be a user-defined type that no column mentions
	// directly. Every edge that can carry a user-defined type is reported:
	//
	//   typelem       array   -> element type (varlena arrays only, so that
	//                            point/line, which also set typelem, are not
	//                            mistaken for arrays)
	//   typbasetype   domain  -> base type, including domain over domain
	//   rngsubtype    range   -> subtype
	//   rngtypid      multirange -> its range (typtype 'm', PostgreSQL 14+)
	//   typrelid      composite -> each live attribute's type
	//
	// A zero OID means "no such edge". $1 and the OIDs returned are this
	// node's own and never leave it; only the descriptors resolved from them
	// are compared across nodes.
	GetTypeReferences: template.Must(template.New("getTypeReferences").Parse(`
		SELECT
			t.oid,
			t.typtype::text,
			CASE WHEN t.typlen = -1 AND t.typelem <> 0
				THEN t.typelem ELSE 0
			END                                                    AS element_oid,
			t.typbasetype                                          AS base_oid,
			COALESCE((
				SELECT r.rngsubtype FROM pg_catalog.pg_range r
				WHERE r.rngtypid = t.oid
			), 0)                                                  AS range_subtype_oid,
			COALESCE((
				SELECT r.rngtypid FROM pg_catalog.pg_range r
				WHERE r.rngmultitypid = t.oid
			), 0)                                                  AS multirange_range_oid,
			COALESCE((
				SELECT array_agg(a.atttypid ORDER BY a.attnum)
				FROM pg_catalog.pg_attribute a
				WHERE a.attrelid = t.typrelid
					AND a.attnum > 0
					AND NOT a.attisdropped
			), '{}'::oid[])                                        AS attribute_type_oids
		FROM pg_catalog.pg_type t
		WHERE t.oid = ANY($1::oid[]);
	`)),
	// GetDatabaseLocale reads the collation settings of the database this
	// connection is attached to.
	//
	// This is the collation fact that matters most for two nodes meant to
	// hold the same rows, and the one a per-column check cannot see: a text
	// column that does not name a collation resolves to the database
	// default, which appears in pg_attribute as the "default" collation on
	// every node regardless of what LC_COLLATE the database was actually
	// created with. Two nodes, one initdb'd en_US.UTF-8 and one C, therefore
	// agree column by column while sorting differently - and a unique index
	// that disagrees about which strings are duplicates is a data-loss
	// hazard, not a cosmetic one.
	//
	// The provider and locale columns are read through to_jsonb rather than
	// named directly, because their names move: datlocprovider arrived in
	// PostgreSQL 15, and the ICU locale is daticulocale in 15 and 16 but
	// datlocale from 17 on. Naming a column that does not exist fails at
	// parse time even on the branch that would not have executed, so the
	// version differences cannot be handled with a CASE; ->> on a row
	// converted to jsonb simply yields NULL for a key that is not there.
	GetDatabaseLocale: template.Must(template.New("getDatabaseLocale").Parse(`
		SELECT
			d.datname,
			d.datcollate,
			d.datctype,
			COALESCE(pg_catalog.to_jsonb(d) ->> 'datlocprovider', '') AS locale_provider,
			COALESCE(
				pg_catalog.to_jsonb(d) ->> 'datlocale',
				pg_catalog.to_jsonb(d) ->> 'daticulocale',
				''
			)                                                          AS locale
		FROM pg_catalog.pg_database d
		WHERE d.datname = pg_catalog.current_database();
	`)),
	// QuoteIdentifiers renders every distinct identifier in $1 the way
	// PostgreSQL itself would need to write it back to mean the same thing
	// unambiguously, using quote_ident() — the same function pg_dump and
	// this file's own deparse queries rely on, since correct quoting also
	// requires case folding and a reserved-word list that varies across
	// major versions. This keeps identifiers that collide when joined by
	// a bare "." (table "a.b" column "c" vs. table "a" column "b.c") from
	// colliding in a person-facing report either.
	QuoteIdentifiers: template.Must(template.New("quoteIdentifiers").Parse(`
		SELECT DISTINCT
			raw,
			pg_catalog.quote_ident(raw) AS quoted
		FROM pg_catalog.unnest($1::text[]) AS raw;
	`)),
	// GetReplicaIdentityKey reads, per table, the replica identity mode
	// (relreplident) and the key columns and operator classes of the index
	// that mode actually designates, in index-column order, since (a,b) and
	// (b,a) are not the same key.
	//
	// Which index that is depends on the mode: 'i' means the index flagged
	// indisreplident, and 'd' - the default, and the common case - means the
	// primary key. Resolving only the 'i' case would leave key_columns empty
	// for almost every real table, so the comparison layer would compare ""
	// against "" and report agreement no matter how the two nodes' primary
	// keys differed. Modes 'f' (whole row) and 'n' (nothing) designate no
	// index and come back empty; relreplident itself carries that.
	//
	// Exactly one index can match, so the aggregate is unambiguous: 'd'
	// looks only at indisprimary and 'i' only at indisreplident, and
	// PostgreSQL sets indisreplident on at most one index per table. The
	// ORDER BY/LIMIT is belt and braces, preferring an explicitly designated
	// index if a future PostgreSQL ever allows both flags at once.
	//
	// Expression index columns (indkey entry 0) have no pg_attribute row and
	// would be dropped silently by the join below. PostgreSQL rejects
	// expression indexes for both PRIMARY KEY and REPLICA IDENTITY USING
	// INDEX, so no such column can reach here; key_length is returned so the
	// caller can still verify that nothing was dropped.
	GetReplicaIdentityKey: template.Must(template.New("getReplicaIdentityKey").Parse(`
		SELECT
			c.relname,
			c.relreplident::text,
			COALESCE(key_cols.key_columns, '{}')     AS key_columns,
			COALESCE(key_cols.key_opclasses, '{}')   AS key_opclasses,
			COALESCE(key_cols.key_length, 0)         AS key_length
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN LATERAL (
			SELECT
				array_agg(a.attname ORDER BY ik.ord)  AS key_columns,
				-- Named (namespace, name), the same way GetRangeDescriptors
				-- names a range's operator class: an unqualified opcname is
				-- only unique within one namespace and access method, so two
				-- nodes keying a column on same-named operator classes from
				-- different schemas - one ordering text by collation, one by
				-- byte pattern - would otherwise compare equal.
				array_agg(ocn.nspname || '.' || oc.opcname ORDER BY ik.ord)
					                                  AS key_opclasses,
				i.indnkeyatts                         AS key_length
			FROM pg_catalog.pg_index i
			-- indkey holds indnatts entries, but only the first indnkeyatts
			-- of them are key columns; the rest are INCLUDE payload, which
			-- is not part of the row identity. They were previously dropped
			-- only as a side effect of indclass being shorter than indkey -
			-- an out-of-bounds array access doing the filtering. Say it.
			JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS ik(attnum, ord)
				ON ik.ord <= i.indnkeyatts
			JOIN pg_catalog.pg_attribute a
				ON a.attrelid = i.indrelid AND a.attnum = ik.attnum
			-- int2vector/oidvector are 0-indexed by long-standing PostgreSQL
			-- convention, and WITH ORDINALITY starts at 1, hence "ord - 1".
			JOIN pg_catalog.pg_opclass oc ON oc.oid = i.indclass[(ik.ord - 1)::int]
			JOIN pg_catalog.pg_namespace ocn ON ocn.oid = oc.opcnamespace
			WHERE i.indrelid = c.oid
				AND (
					i.indisreplident
					OR (c.relreplident = 'd' AND i.indisprimary)
				)
			GROUP BY i.indexrelid, i.indisreplident, i.indnkeyatts
			ORDER BY i.indisreplident DESC
			LIMIT 1
		) key_cols ON true
		WHERE n.nspname = $1
			AND c.relname = ANY($2::text[]);
	`)),
	// GetConstraintDescriptors reads PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY
	// and EXCLUDE constraints. Omits the constraint's own name (conname):
	// PostgreSQL invents names for unnamed constraints, so the same
	// constraint can be named differently on two nodes with no structural
	// difference. Comparison goes by condef, so rows are ordered by
	// condef too.
	GetConstraintDescriptors: template.Must(template.New("getConstraintDescriptors").Parse(`
		SELECT
			c.relname,
			ct.contype::text,
			pg_catalog.pg_get_constraintdef(ct.oid, true) AS condef,
			ct.condeferrable,
			ct.convalidated
		FROM pg_catalog.pg_constraint ct
		JOIN pg_catalog.pg_class c ON c.oid = ct.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
			AND c.relname = ANY($2::text[])
			AND ct.contype IN ('p', 'u', 'c', 'f', 'x')
		-- The ordering expression is spelled out rather than referring to the
		-- "condef" output alias: an alias is only visible to ORDER BY when it
		-- stands alone, and adding COLLATE makes it an expression, where it
		-- is not.
		ORDER BY c.relname COLLATE "C", pg_catalog.pg_get_constraintdef(ct.oid, true) COLLATE "C";
	`)),
	// GetPartitionDescriptors reads, per table, its own partition bound (if
	// it is itself a partition of something) and its partitioning key (if
	// it is itself partitioned). A table can be both, neither, or one of
	// the two; empty strings mean "not applicable", not "unknown".
	GetPartitionDescriptors: template.Must(template.New("getPartitionDescriptors").Parse(`
		SELECT
			c.relname,
			COALESCE(pg_catalog.pg_get_expr(c.relpartbound, c.oid), '')  AS partition_bound,
			CASE WHEN c.relkind = 'p'
				THEN COALESCE(pg_catalog.pg_get_partkeydef(c.oid), '')
				ELSE ''
			END AS partition_key
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
			AND c.relname = ANY($2::text[]);
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
	CreateXORFunction: template.Must(template.New("createXORFunction").Funcs(aceTemplateFuncs).Parse(`
		CREATE
		OR REPLACE FUNCTION {{aceSchema}}.bytea_xor(a bytea, b bytea) RETURNS bytea AS $$
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
				PROCEDURE = {{aceSchema}}.bytea_xor
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
	UpdateMetadata: template.Must(template.New("updateMetadata").Funcs(aceTemplateFuncs).Parse(`
		INSERT INTO
			{{aceSchema}}.ace_mtree_metadata (
				schema_name,
				table_name,
				total_rows,
				block_size,
				num_blocks,
				is_composite,
				hash_version,
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
				$7,
				current_timestamp
			)
		ON CONFLICT (schema_name, table_name) DO
		UPDATE
		SET
			total_rows = EXCLUDED.total_rows,
			block_size = EXCLUDED.block_size,
			num_blocks = EXCLUDED.num_blocks,
			is_composite = EXCLUDED.is_composite,
			hash_version = EXCLUDED.hash_version,
			last_updated = EXCLUDED.last_updated
	`)),
	DeleteMetadata: template.Must(template.New("deleteMetadata").Funcs(aceTemplateFuncs).Parse(`
		DELETE FROM {{aceSchema}}.ace_mtree_metadata WHERE schema_name = $1 AND table_name = $2
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
        SELECT encode(digest(COALESCE(string_agg({{.RowTextExpr}}, '|' ORDER BY {{.PkOrderByStr}}), 'EMPTY_BLOCK'), 'sha256'), 'hex')
        FROM {{.SchemaIdent}}.{{.TableIdent}} AS {{.TableAlias}}
        WHERE {{.WhereClause}}
    `)),
	MtreeLeafHashSQL: template.Must(template.New("mtreeLeafHashSQL").Parse(`
        SELECT digest(COALESCE(string_agg({{.RowTextExpr}}, '|' ORDER BY {{.PkOrderByStr}}), 'EMPTY_BLOCK'), 'sha256')
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
	MarkLeavesDirtyByPositions: template.Must(template.New("markLeavesDirtyByPositions").Parse(`
		UPDATE
			{{.MtreeTable}}
		SET
			dirty = true,
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
	GetRowCountEstimate: template.Must(template.New("getRowCountEstimate").Funcs(aceTemplateFuncs).Parse(`
		SELECT
			total_rows
		FROM
			{{aceSchema}}.ace_mtree_metadata
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
	GetBlockSizeFromMetadata: template.Must(template.New("getBlockSizeFromMetadata").Funcs(aceTemplateFuncs).Parse(`
		SELECT
			block_size
		FROM
			{{aceSchema}}.ace_mtree_metadata
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
	DropXORFunction: template.Must(template.New("dropXORFunction").Funcs(aceTemplateFuncs).Parse(`
		DROP FUNCTION IF EXISTS {{aceSchema}}.bytea_xor(bytea, bytea) CASCADE
	`)),
	DropMetadataTable: template.Must(template.New("dropMetadataTable").Funcs(aceTemplateFuncs).Parse(`
		DROP TABLE IF EXISTS {{aceSchema}}.ace_mtree_metadata CASCADE
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
	EnsureHashVersionColumn: template.Must(template.New("ensureHashVersionColumn").Funcs(aceTemplateFuncs).Parse(`
		ALTER TABLE {{aceSchema}}.ace_mtree_metadata
		ADD COLUMN IF NOT EXISTS hash_version int NOT NULL DEFAULT 1
	`)),
	GetHashVersion: template.Must(template.New("getHashVersion").Funcs(aceTemplateFuncs).Parse(`
		SELECT COALESCE(
			(SELECT hash_version FROM {{aceSchema}}.ace_mtree_metadata
			 WHERE schema_name = $1 AND table_name = $2),
			1
		)
	`)),
	MarkAllLeavesDirty: template.Must(template.New("markAllLeavesDirty").Parse(`
		UPDATE {{.MtreeTable}}
		SET dirty = true
		WHERE node_level = 0
	`)),
	UpdateHashVersion: template.Must(template.New("updateHashVersion").Funcs(aceTemplateFuncs).Parse(`
		UPDATE {{aceSchema}}.ace_mtree_metadata
		SET hash_version = $1, last_updated = current_timestamp
		WHERE schema_name = $2 AND table_name = $3
	`)),
	GetNativeOriginLSNForNode: template.Must(template.New("getNativeOriginLSNForNode").Parse(`
		SELECT ros.remote_lsn::text
		FROM pg_catalog.pg_replication_origin_status ros
		JOIN pg_catalog.pg_replication_origin ro ON ro.roident = ros.local_id
		JOIN pg_catalog.pg_subscription s ON ro.roname LIKE 'pg_%' || s.oid::text
		WHERE s.subname ~ ('\m' || $1 || '\M')
			AND ros.remote_lsn IS NOT NULL
		LIMIT 1
	`)),
	GetNativeSlotLSNForNode: template.Must(template.New("getNativeSlotLSNForNode").Parse(`
		SELECT rs.confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots rs
		JOIN pg_catalog.pg_subscription s ON rs.slot_name = s.subslotname
		WHERE s.subname ~ ('\m' || $1 || '\M')
			AND rs.confirmed_flush_lsn IS NOT NULL
		ORDER BY rs.confirmed_flush_lsn DESC
		LIMIT 1
	`)),
	GetReplicationOriginNames: template.Must(template.New("getReplicationOriginNames").Parse(`
		SELECT roident::text, roname FROM pg_replication_origin;
	`)),
	// GetNativeNodeOriginNames maps pg_replication_origin entries to their
	// corresponding pg_subscription names. This provides the native PG
	// equivalent of GetSpockNodeNames — mapping origin IDs (used by
	// pg_xact_commit_timestamp_origin) to human-readable node identifiers.
	GetNativeNodeOriginNames: template.Must(template.New("getNativeNodeOriginNames").Parse(`
		SELECT ro.roident::text, s.subname
		FROM pg_catalog.pg_replication_origin ro
		JOIN pg_catalog.pg_subscription s ON ro.roname = 'pg_' || s.oid::text
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

	// CurrentWalInsertLSN returns pg_current_wal_insert_lsn() as text. Used
	// at MtreeInit time, just after CREATE PUBLICATION, as the lower bound
	// for any future replication start LSN. The slot's consistent point is
	// guaranteed to be >= this value because the slot is created after this
	// transaction commits.
	CurrentWalInsertLSN: template.Must(template.New("currentWalInsertLSN").Parse(`
		SELECT pg_current_wal_insert_lsn()::text
	`)),
}
