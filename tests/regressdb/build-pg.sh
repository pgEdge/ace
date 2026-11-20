#!/usr/bin/env bash
#
# Build one PostgreSQL release from the source tarball.
#
# Usage: build-pg.sh <version> <dir>
#
# <dir>/install gets the installed server and client programs.
# <dir>/src keeps the built source tree: the regression tests run from there.
# If <dir>/install/bin/postgres exists, the script does nothing, so a
# restored CI cache is used as it is.

set -euo pipefail

version=${1:?usage: build-pg.sh <version> <dir>}
dir=${2:?usage: build-pg.sh <version> <dir>}

if [ -x "$dir/install/bin/postgres" ]; then
	echo "PostgreSQL $version is already built in $dir"
	exit 0
fi

mkdir -p "$dir"
dir=$(cd "$dir" && pwd)
rm -rf "$dir/src" "$dir/install"

curl -fsSL "https://ftp.postgresql.org/pub/source/v$version/postgresql-$version.tar.bz2" |
	tar -xj -C "$dir"
mv "$dir/postgresql-$version" "$dir/src"

cd "$dir/src"
# libxml and ICU give the regression tests their xml and ICU collation
# data. Readline is not needed: psql runs only scripts here.
./configure --prefix="$dir/install" --with-libxml --with-icu \
	--without-readline >"$dir/configure.log" 2>&1 ||
	{ tail -n 50 "$dir/configure.log"; exit 1; }
make -j"$(nproc)" -s >"$dir/build.log" 2>&1 ||
	{ tail -n 50 "$dir/build.log"; exit 1; }
make -s install >>"$dir/build.log" 2>&1 ||
	{ tail -n 50 "$dir/build.log"; exit 1; }

# The regression tests need only pg_regress, the test libraries and the
# test files. Without the object files the CI cache is much smaller.
find "$dir/src" -name '*.o' -delete

"$dir/install/bin/postgres" --version
