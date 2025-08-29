#############################################################################
#
# ACE - Active Consistency Engine
#
# Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
#
# This software is released under the PostgreSQL License:
# https://opensource.org/license/postgresql
#
#############################################################################

#!/usr/bin/env bash
# visualise.sh
# Flags:
#   -s <schema>    (default: public)
#   -t <table>     (required)  -> full table: ace_mtree_<schema>_<table>
#   -H <host>      (optional)  -> psql -h
#   -U <user>	   (default: admin) -> psql -U
#   -d <dbname>    (default: demo) -> psql -d
#   -h             horizontal  (default orientation is vertical)
#   -v             vertical
#
# Uses PG* env for user and db Example:
#   PGUSER=admin PGDATABASE=demo ./visualise.sh -s public -t customers_small -H localhost -v

set -euo pipefail

schema="public"
table=""
host=""
user="${PGUSER:-admin}"
dbname="${PGDATABASE:-demo}"
orientation="vertical"   # default

while getopts ":s:t:H:U:d:hv" opt; do
  case "$opt" in
    s) schema="$OPTARG" ;;
    t) table="$OPTARG" ;;
    H) host="$OPTARG" ;;
    U) user="$OPTARG" ;;
    d) dbname="$OPTARG" ;;
    h) orientation="horizontal" ;;
    v) orientation="vertical" ;;
    \?) echo "Unknown option: -$OPTARG" >&2; exit 2 ;;
    :)  echo "Option -$OPTARG requires an argument" >&2; exit 2 ;;
  esac
done

if [[ -z "$table" ]]; then
  echo "Usage: $0 -t <table> [-s <schema=public>] [-H <host>] [-U <user>] [-d <dbname>] [-h|-v]" >&2
  exit 2
fi

fq_table="ace_mtree_${schema}_${table}"

SQL=$(cat <<SQL_EOF
WITH RECURSIVE
nodes AS (
  SELECT
    node_level,
    node_position,
    encode(node_hash, 'hex')  AS node_hash_hex,
    encode(leaf_hash, 'hex')  AS leaf_hash_hex,
    (range_start)::text       AS range_start_txt,
    (range_end)::text         AS range_end_txt
  FROM "$schema"."$fq_table"
),
maxl AS (
  SELECT max(node_level) AS max_level FROM nodes
),
walk AS (
  -- root(s): max level
  SELECT
    n.node_level,
    n.node_position,
    0 AS depth,
    CASE
      WHEN n.node_level = 0 THEN
        format('[%s -> %s] %s',
               coalesce(n.range_start_txt,'empty'),
               coalesce(n.range_end_txt,'empty'),
               left(coalesce(n.leaf_hash_hex, n.node_hash_hex, ''), 6))
      ELSE
        left(coalesce(n.node_hash_hex, ''), 6)
    END AS label,
    ''::text AS path
  FROM nodes n
  JOIN maxl m ON n.node_level = m.max_level

  UNION ALL

  -- children: (level-1, pos=2p or 2p+1)
  SELECT
    c.node_level,
    c.node_position,
    p.depth + 1,
    CASE
      WHEN c.node_level = 0 THEN
        format('[%s -> %s] %s',
               coalesce(c.range_start_txt,'empty'),
               coalesce(c.range_end_txt,'empty'),
               left(coalesce(c.leaf_hash_hex, c.node_hash_hex, ''), 6))
      ELSE
        left(coalesce(c.node_hash_hex, ''), 6)
    END AS label,
    p.path || CASE WHEN c.node_position = p.node_position*2 THEN '0' ELSE '1' END AS path
  FROM walk p
  JOIN nodes c
    ON c.node_level = p.node_level - 1
   AND (c.node_position = p.node_position*2 OR c.node_position = p.node_position*2 + 1)
)
SELECT depth, label
FROM walk
ORDER BY path;
SQL_EOF
)

tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT

# Build psql args (host optional)
psql_args=(-AXqt -F $'\t' -v ON_ERROR_STOP=1 -c "$SQL")
[[ -n "$host" ]] && psql_args=(-h "$host" "${psql_args[@]}")
[[ -n "$user" ]] && psql_args=(-U "$user" "${psql_args[@]}")
[[ -n "$dbname" ]] && psql_args=(-d "$dbname" "${psql_args[@]}")

psql "${psql_args[@]}" \
| awk -F'\t' '{
    depth = $1 + 1;   # root => "#"
    label = $2;
    pref = "";
    for (i = 0; i < depth; i++) pref = pref "#";
    print pref " " label
  }' > "$tmp"

if [[ ! -s "$tmp" ]]; then
  echo "[visualise] No rows produced. Check PG* env/connection and table: \"$schema\".\"$fq_table\"" >&2
  exit 3
fi

# Render with astree if available; otherwise print headings.
if command -v astree >/dev/null 2>&1; then
  if ! astree "$orientation" --input "$tmp"; then
    echo "[visualise] astree failed; printing headings instead:" >&2
    cat "$tmp"
  fi
else
  echo "[visualise] astree not found; printing headings:" >&2
  cat "$tmp"
fi
