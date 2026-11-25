#!/usr/bin/env bash
#
# Run one case of the regression database test: create two nodes, load the
# regression database on each, and run ACE on them with run.py. Then
# replace the database on n2 with a copy made by pg_dump and pg_restore
# (see dump-restore.sh) and run ACE again.
#
# Usage: run-cell.sh <cell> <pgdir1> <pgdir2> <workdir> <ace>
#
#   cell     name of the case in known.tsv (old, new or mixed); the second
#            run uses the name <cell>-restored
#   pgdir1   directory made by build-pg.sh for node n1
#   pgdir2   directory made by build-pg.sh for node n2
#   workdir  new directory for the data directories, logs and results
#   ace      ACE binary
#
# Node n1 is a UTF8 server with default settings. Node n2 is a LATIN1
# server with other defaults for the output settings (see setup-node.sh).
# Environment: PORT1 and PORT2 (default 5501 and 5502), and RUN_ARGS with
# extra options for run.py.

set -euo pipefail

cell=${1:?usage: run-cell.sh <cell> <pgdir1> <pgdir2> <workdir> <ace>}
pg1=$(cd "${2:?}" && pwd)
pg2=$(cd "${3:?}" && pwd)
workdir=${4:?}
ace=$(cd "$(dirname "${5:?}")" && pwd)/$(basename "$5")
port1=${PORT1:-5501}
port2=${PORT2:-5502}
here=$(cd "$(dirname "$0")" && pwd)

mkdir -p "$workdir"
workdir=$(cd "$workdir" && pwd)

stop() {
	"$pg1/install/bin/pg_ctl" -D "$workdir/n1" -m fast stop >/dev/null 2>&1 || true
	"$pg2/install/bin/pg_ctl" -D "$workdir/n2" -m fast stop >/dev/null 2>&1 || true
}
trap stop EXIT

echo "== node n1: $("$pg1/install/bin/postgres" --version), UTF8, default settings"
"$here/setup-node.sh" "$pg1" "$workdir/n1" "$port1" UTF8 default
echo "== node n2: $("$pg2/install/bin/postgres" --version), LATIN1, changed settings"
"$here/setup-node.sh" "$pg2" "$workdir/n2" "$port2" LATIN1 changed

# The newer psql of the two reads the reference rows from both servers.
v1=$("$pg1/install/bin/pg_config" --version | grep -o '[0-9]*' | head -n 1)
v2=$("$pg2/install/bin/pg_config" --version | grep -o '[0-9]*' | head -n 1)
psql="$pg1/install/bin/psql"
[ "$v2" -gt "$v1" ] && psql="$pg2/install/bin/psql"

# The second run compares n1 with a restored copy of a regression
# database on n2. With one major version, n2 gets the database of n1. With
# two versions, n2 gets the database it already has. The dump is made now,
# before the first run deletes rows on n2.
src=$port1
[ "$v1" != "$v2" ] && src=$port2
echo "== dump of the regression database on port $src"
"$here/dump-restore.sh" dump "$pg2" "$src" "$workdir/dump"

ace_run() {
	# shellcheck disable=SC2086
	python3 "$here/run.py" --ace "$ace" --psql "$psql" --port1 "$port1" \
		--port2 "$port2" --workdir "$1" --cell "$2" ${RUN_ARGS:-}
}

# A failed run does not stop the script: the second run gives its own
# results. The script fails at the end if either run failed.
echo "== ACE"
rc=0
ace_run "$workdir/run" "$cell" || rc=1

echo "== node n2: new regression database from the dump"
"$here/dump-restore.sh" restore "$pg2" "$port2" "$workdir/dump"

echo "== ACE on the restored database"
ace_run "$workdir/run-restored" "$cell-restored" || rc=1
exit "$rc"
