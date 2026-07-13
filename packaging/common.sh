#!/usr/bin/env bash
# common.sh - Common environment variables

# Default PostgreSQL version and derived values
export PG_VERSION="${PG_VERSION:-17.7}"
export PG_MAJOR_VERSION="$(echo "$PG_VERSION" | cut -d. -f1)"

export ACE_REPO="https://github.com/pgEdge/ace.git"
export ACE_BRANCH="${COMPONENT_BRANCH:-v2.0.0}"
export ACE_VERSION=${COMPONENT_VERSION:-2.0.0}
export ACE_BUILDNUM=${COMPONENT_BUILDNUM:-1}

# DEB only: move a pre-release pretag (e.g. BUILDNUM='rc1_1') into the upstream
# VERSION with a leading '~' (2.0.0~rc1, BUILDNUM=1) so '~' sorts pre-releases
# BELOW stable in dpkg/reprepro. Gated on apt-get so RPM keeps the pretag in
# the Release field. pgedge-parse-release-tag already normalises the tag suffix
# (e.g. v2.0.0-test.1 -> buildnum 'test1_1'), so no dots reach here.
if command -v apt-get &>/dev/null; then
    if [[ "$ACE_BUILDNUM" == *_* ]]; then
        ACE_PRETAG="${ACE_BUILDNUM%%_*}"
        export ACE_VERSION="${ACE_VERSION}~${ACE_PRETAG}"
        ACE_BUILDNUM="${ACE_BUILDNUM#*_}"
    fi
fi

export REPO_TYPE="${REPO_TYPE:-daily}"
