#!/bin/bash
set -euo pipefail

RHEL="$(rpm --eval %rhel)"
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
  ARCH="arm64"
fi

CWD="$(pwd)"

# Directory where the release pipeline stages the pre-built GoReleaser
# tarball (ace.tar.gz) and NOTICE.txt. When present, prepare() prefers
# these staged files over downloading from the GitHub release, so
# simulate_tag builds work without a published release.
ARTIFACT_DIR="${ARTIFACT_DIR:-${CWD}/release-artifacts}"

prepare() {
  setup_dnf_build_env

  echo "Copying packaging files..."
  cp ${COMPONENT_NAME}/rpm/ace.spec ~/rpmbuild/SPECS/

  echo "Obtaining official source tarball and docs..."
  if [ -f "${ARTIFACT_DIR}/ace.tar.gz" ]; then
    echo "Using staged tarball ${ARTIFACT_DIR}/ace.tar.gz"
    cp "${ARTIFACT_DIR}/ace.tar.gz" ~/rpmbuild/SOURCES/ace_Linux_${ARCH}.tar.gz
  else
    echo "Downloading release tarball for ${ACE_BRANCH} (${ARCH})..."
    wget -q "https://github.com/pgEdge/ace/releases/download/${ACE_BRANCH}/ace_Linux_${ARCH}.tar.gz" \
      -O ~/rpmbuild/SOURCES/ace_Linux_${ARCH}.tar.gz
  fi
  cp ${COMPONENT_NAME}/common/pgedge-ace.* ~/rpmbuild/SOURCES/

  if [ -f "${ARTIFACT_DIR}/NOTICE.txt" ]; then
    echo "Using staged NOTICE.txt from ${ARTIFACT_DIR}"
    cp "${ARTIFACT_DIR}/NOTICE.txt" ~/rpmbuild/SOURCES/NOTICE.txt
  else
    echo "Downloading NOTICE.txt from ace repo..."
    curl -fsSL -o ~/rpmbuild/SOURCES/NOTICE.txt \
      https://raw.githubusercontent.com/pgEdge/ace/${ACE_BRANCH}/NOTICE.txt
  fi

  # This function is for debugging purpose if you have your own keys. GH workflow does not need it.
  #import_gpg_keys

  echo "🔧 Installing RPM build dependencies..."
  dnf builddep -y \
    --define "ace_version ${ACE_VERSION}" \
    --define "ace_buildnum ${ACE_BUILDNUM}" \
    --define "arch ${ARCH}" \
    ~/rpmbuild/SPECS/ace.spec
}

build() {
  echo "Building RPM and SRPM..."
  QA_RPATHS=$(( 0xffff )) rpmbuild -ba ~/rpmbuild/SPECS/ace.spec \
    --define "ace_version ${ACE_VERSION}" \
    --define "ace_buildnum ${ACE_BUILDNUM}" \
    --define "arch ${ARCH}"
}

post_build() {
  echo "📤 Copying built RPMs to /output..."
  mkdir -p /output
  cp -v ~/rpmbuild/RPMS/*/*.rpm /output/ || echo "No binary RPMs found"
  cp -v ~/rpmbuild/SRPMS/*.src.rpm /output/ || echo "No SRPM found"

  sign_rpms /output/*.rpm
  validate_signatures /output/*.rpm
}

