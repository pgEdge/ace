-----------------------------------------------------------------------------
--
-- ACE - Active Consistency Engine
--
-- Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
--
-- This software is released under the pgEdge Community License:
--      https://www.pgedge.com/communitylicense
--
-----------------------------------------------------------------------------

-- name: EstimateRowCount :one
SELECT
    (
        CASE
            WHEN s.n_live_tup > 0 THEN s.n_live_tup
            WHEN c.reltuples > 0 THEN c.reltuples
            ELSE pg_relation_size(c.oid) / (8192 * 0.7)
        END
    ) :: bigint AS estimate
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.schemaname = n.nspname
    AND s.relname = c.relname
WHERE
    n.nspname = pggen.arg('schema_name')
    AND c.relname = pggen.arg('table_name');

-- name: GetPrimaryKey :many
SELECT
    kcu.column_name
FROM
    information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE
    tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = pggen.arg('schema_name')
    AND tc.table_name = pggen.arg('table_name');

-- name: GetColumnTypes :many
SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
FROM
    pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
    JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE
    c.relname = pggen.arg('table_name')
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY
    a.attnum;

-- name: GetColumns :many
SELECT
    column_name
FROM
    information_schema.columns
WHERE
    table_schema = pggen.arg('schema_name')
    AND table_name = pggen.arg('table_name');


-- name: CheckUserPrivileges :one
WITH params AS (
    SELECT
        pggen.arg('username') :: text AS username,
        pggen.arg('schema_name') :: text AS schema_name,
        pggen.arg('table_name') :: text AS table_name
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


--name: SpockNodeAndSubInfo :many
SELECT
		n.node_id,
		n.node_name,
		n.location,
		n.country,
		s.sub_id,
		s.sub_name,
		s.sub_enabled,
		s.sub_replication_sets
	FROM spock.node n
	LEFT OUTER JOIN spock.subscription s
	ON s.sub_target = n.node_id
	WHERE s.sub_name IS NOT NULL;

--name: SpockRepSetInfo :many
SELECT
set_name,
array_agg(nspname || '.' || relname ORDER BY nspname, relname) as relname
FROM (
	SELECT
		set_name,
		nspname,
		relname
	FROM spock.tables
	ORDER BY set_name, nspname, relname
) subquery
GROUP BY set_name
ORDER BY set_name;

--name: CheckSchemaExists :one
SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = pggen.arg('schema_name'));

--name: GetTablesInSchema :many
SELECT table_name FROM information_schema.tables WHERE table_schema = pggen.arg('schema_name') AND table_type = 'BASE TABLE';

--name: GetViewsInSchema :many
SELECT table_name FROM information_schema.views WHERE table_schema = pggen.arg('schema_name');

--name: GetFunctionsInSchema :many
SELECT p.proname || '(' || COALESCE(pg_get_function_identity_arguments(p.oid), '') || ')' as function_signature
FROM pg_proc p
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = pggen.arg('schema_name');

--name: GetIndicesInSchema :many
SELECT indexname FROM pg_indexes WHERE schemaname = pggen.arg('schema_name');

--name: CheckRepSetExists :one
SELECT set_name FROM spock.replication_set WHERE set_name = pggen.arg('set_name');

--name: GetTablesInRepSet :many
SELECT concat_ws('.', nspname, relname) FROM spock.tables where set_name = pggen.arg('set_name');

--name: GetPkeyColumnTypes :many
SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = pggen.arg('schema_name')
AND c.relname = pggen.arg('table_name')
AND a.attname = ANY(pggen.arg('pkey_columns')::text[])
AND a.attnum > 0 AND NOT a.attisdropped;
