#!/usr/bin/env bash
#
# Create one node: a new cluster that runs on 127.0.0.1 and holds the
# regression database of its own PostgreSQL version.
#
# Usage: setup-node.sh <pgdir> <datadir> <port> <encoding> <profile>
#
#   pgdir     directory made by build-pg.sh
#   datadir   data directory of the new cluster; removed first if it exists
#   port      TCP port of the server
#   encoding  server encoding, for example UTF8 or LATIN1
#   profile   "default" keeps the server defaults; "changed" sets other
#             defaults for the output settings of the regression database
#             (see below)
#
# The server log is <datadir>.log; the regression test output stays in
# <datadir>.regress.

set -euo pipefail

pgdir=${1:?usage: setup-node.sh <pgdir> <datadir> <port> <encoding> <profile>}
data=${2:?}
port=${3:?}
encoding=${4:?}
profile=${5:?}

bin="$pgdir/install/bin"
rm -rf "$data" "$data.regress"
mkdir -p "$(dirname "$data")"
data=$(cd "$(dirname "$data")" && pwd)/$(basename "$data")

"$bin/initdb" -D "$data" -E "$encoding" --locale=C --auth=trust \
	-U postgres >"$data.initdb.log" 2>&1 ||
	{ cat "$data.initdb.log"; exit 1; }

# 32 connections per node for each of a few ACE tasks, and 20 parallel
# regression tests. wal_level = logical is for the mtree commands.
# ACE requires track_commit_timestamp = on: it reads the commit time and
# origin of each row that differs.
cat >>"$data/postgresql.conf" <<EOF
port = $port
listen_addresses = '127.0.0.1'
unix_socket_directories = '$data'
max_connections = 300
wal_level = logical
track_commit_timestamp = on
max_wal_senders = 10
max_replication_slots = 10
fsync = off
synchronous_commit = off
full_page_writes = off
EOF

"$bin/pg_ctl" -D "$data" -l "$data.log" -w start >/dev/null

# make installcheck-parallel with the standard parallel schedule. A test
# that fails (exit status 1) does not stop the run: only the data that the
# tests leave in the regression database is used. A fatal error of
# pg_regress (status 2) can stop the schedule half way and leave a partly
# loaded database, so it stops the setup. Before PostgreSQL 15, pg_regress writes
# the tests that it makes from input/*.source into sql/ and expected/ of the
# output directory.
mkdir -p "$data.regress/sql" "$data.regress/expected"
rc=0
(
	cd "$pgdir/src/src/test/regress"
	PGHOST=127.0.0.1 PGPORT=$port PGUSER=postgres \
		./pg_regress --inputdir=. --bindir="$bin" --dlpath=. \
		--outputdir="$data.regress" --max-concurrent-tests=20 \
		--schedule=parallel_schedule >"$data.regress/run.log" 2>&1
) || rc=$?
if [ "$rc" -gt 1 ]; then
	cat "$data.regress/run.log"
	echo "pg_regress failed with exit status $rc" >&2
	exit "$rc"
fi
tail -n 3 "$data.regress/run.log"

# Other defaults for the settings that change how the server prints
# values or resolves names. ACE sets its own values for most of them on
# each connection, and the run checks that a node with other defaults
# gives the same result.
if [ "$profile" = changed ]; then
	# The locale of lc_monetary must have the encoding of the database. The
	# server converts the currency symbol from the encoding of the locale
	# to the encoding of the database, and a symbol that the database
	# encoding does not have (the euro sign in LATIN1) makes the output of
	# every money value fail.
	case "$encoding" in
	UTF8) codeset=utf8 ;;
	LATIN1) codeset=iso88591 ;;
	*) codeset= ;;
	esac
	monetary=C
	for lang in de_DE en_US; do
		if [ -n "$codeset" ] && locale -a 2>/dev/null | grep -qix "$lang.$codeset"; then
			monetary=$lang.$codeset
			break
		fi
	done
	echo "lc_monetary = $monetary"
	"$bin/psql" -X -q -h 127.0.0.1 -p "$port" -U postgres -d regression \
		-v ON_ERROR_STOP=1 <<EOF
ALTER DATABASE regression SET DateStyle = 'SQL, DMY';
ALTER DATABASE regression SET IntervalStyle = 'sql_standard';
ALTER DATABASE regression SET TimeZone = 'Asia/Kolkata';
ALTER DATABASE regression SET extra_float_digits = 0;
ALTER DATABASE regression SET bytea_output = 'escape';
ALTER DATABASE regression SET lc_monetary = '$monetary';
ALTER DATABASE regression SET search_path = public, pg_catalog;
ALTER DATABASE regression SET xmloption = 'document';
EOF
	# A new session gets the new defaults; it must print a money value.
	"$bin/psql" -X -q -A -t -h 127.0.0.1 -p "$port" -U postgres -d regression \
		-v ON_ERROR_STOP=1 -c 'SELECT 1234.5::money'
fi
