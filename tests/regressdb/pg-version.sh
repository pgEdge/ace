#!/usr/bin/env bash
#
# Print the latest released minor version of a PostgreSQL major version,
# for example "18.7" for "18". Beta and RC releases are not used.
#
# Usage: pg-version.sh <major>

set -euo pipefail

major=${1:?usage: pg-version.sh <major>}

curl -fsSL https://ftp.postgresql.org/pub/source/ |
	grep -o "v${major}\.[0-9]*/" |
	tr -d 'v/' |
	sort -t. -k2,2n |
	tail -n 1 |
	grep . || { echo "no release found for PostgreSQL $major" >&2; exit 1; }
