#!/usr/bin/env bash
#
# Copy the regression database into another node with pg_dump and
# pg_restore, and check that the copy is complete.
#
# Usage: dump-restore.sh dump <pgdir> <port> <dir>
#        dump-restore.sh restore <pgdir> <port> <dir>
#
#   pgdir  directory made by build-pg.sh; its programs make and load the dump
#   port   TCP port of the server on 127.0.0.1
#   dir    directory for the dump files and the logs
#
# "dump" saves the roles and the regression database of a node.
#
# "restore" drops the regression database of a node, creates the roles of
# the saved node that this node does not have, and loads the saved
# database. The new database gets the encoding, the locale and the
# database settings of the saved one. The objects are created again in the
# order of the dump, and the server gives them new OIDs from a counter that
# the regression tests have already moved. Then the new database is dumped
# again, and this dump must have the same lines as the saved one, except
# for the column order of tables with inherited columns, which pg_dump does
# not keep (see dump-lines.py).
#
# Any error stops the script, so a run never compares a partial copy.

set -euo pipefail

mode=${1:?usage: dump-restore.sh dump|restore <pgdir> <port> <dir>}
pgdir=${2:?}
port=${3:?}
dir=${4:?}

bin="$pgdir/install/bin"
conn="host=127.0.0.1 hostaddr=127.0.0.1 port=$port user=postgres"
here=$(cd "$(dirname "$0")" && pwd)

# Statistics change when autovacuum analyzes a table, so they are left out
# of the dumps that are compared. Older pg_dump versions do not dump them.
dump_opts=(-Fc)
if "$bin/pg_dump" --help | grep -q -- '--no-statistics'; then
	dump_opts+=(--no-statistics)
fi

run() {
	local log=$1
	shift
	"$@" >>"$log" 2>&1 || {
		cat "$log"
		echo "failed: $*" >&2
		exit 1
	}
}

# The lines of a dump in a form that does not depend on the order of the
# lines and of the columns of a table (see dump-lines.py).
dump_lines() {
	"$bin/pg_restore" --create -f - "$1" | python3 "$here/dump-lines.py"
}

case "$mode" in
dump)
	rm -rf "$dir"
	mkdir -p "$dir"
	log="$dir/dump.log"
	run "$log" "$bin/pg_dump" "${dump_opts[@]}" -f "$dir/regression.dump" \
		-d "$conn dbname=regression"
	run "$log" "$bin/pg_dumpall" --globals-only -f "$dir/globals.sql" \
		-d "$conn dbname=postgres"
	# A script that creates each role only when the node does not have it
	# yet. The server quotes the names, so any name is safe here.
	run "$log" "$bin/psql" -X -q -A -t -v ON_ERROR_STOP=1 \
		-d "$conn dbname=postgres" -o "$dir/roles.sql" -c "
SELECT format('SELECT %L WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = %L) \\gexec',
              format('CREATE ROLE %I', rolname), rolname)
FROM pg_catalog.pg_roles
WHERE rolname !~ '^pg_'
ORDER BY rolname"
	;;
restore)
	log="$dir/restore.log"
	: >"$log"
	psql=("$bin/psql" -X -q -v ON_ERROR_STOP=1 -d "$conn dbname=postgres")
	run "$log" "${psql[@]}" -c 'DROP DATABASE IF EXISTS regression WITH (FORCE)'
	run "$log" "${psql[@]}" -f "$dir/roles.sql"
	# The roles exist now. The rest of the global objects (role options,
	# memberships, settings) is loaded without the CREATE ROLE commands,
	# which would fail for a role that the node already has.
	grep -v '^CREATE ROLE ' "$dir/globals.sql" >"$dir/globals-no-create.sql"
	run "$log" "${psql[@]}" -f "$dir/globals-no-create.sql"
	run "$log" "$bin/pg_restore" --create --exit-on-error -d "$conn dbname=postgres" \
		"$dir/regression.dump"
	run "$log" "$bin/pg_dump" "${dump_opts[@]}" -f "$dir/restored.dump" \
		-d "$conn dbname=regression"
	if ! diff -u <(dump_lines "$dir/regression.dump") <(dump_lines "$dir/restored.dump") \
		>"$dir/dump.diff"; then
		head -n 100 "$dir/dump.diff"
		echo "the dump of the restored database differs from the saved dump, see $dir/dump.diff" >&2
		exit 1
	fi
	rm -f "$dir/dump.diff"
	;;
*)
	echo "unknown mode: $mode" >&2
	exit 2
	;;
esac
