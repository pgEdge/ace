#!/usr/bin/env bash
set -euo pipefail

# Environment variables
BUILD_DIR="/tmp/pg_deb_build"
SRC_DIR="${BUILD_DIR}/src"

CWD="$(pwd)"

# Directory where the release pipeline stages the pre-built GoReleaser
# tarball (ace.tar.gz) and NOTICE.txt. When present, prepare() prefers
# these staged files over downloading from the GitHub release, so
# simulate_tag builds work without a published release.
ARTIFACT_DIR="${ARTIFACT_DIR:-${CWD}/release-artifacts}"

export DEBIAN_FRONTEND=noninteractive
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
  ARCH="arm64"
fi

prepare() {

  setup_apt_build_env

  # This function is for debugging purpose if you have your own keys. GH workflow does not need it.
  #import_gpg_keys
  
  echo "Cloning ace Debian packaging repo..."
  rm -rf "$SRC_DIR"
  mkdir -p $SRC_DIR

  echo "Fetching ace source code"
  if [ -f "${ARTIFACT_DIR}/ace.tar.gz" ]; then
    echo "Using staged tarball ${ARTIFACT_DIR}/ace.tar.gz"
    cp "${ARTIFACT_DIR}/ace.tar.gz" "${BUILD_DIR}/ace_Linux_${ARCH}.tar.gz"
  else
    echo "Downloading release tarball for ${ACE_BRANCH} (${ARCH})..."
    wget -q "https://github.com/pgEdge/ace/releases/download/${ACE_BRANCH}/ace_Linux_${ARCH}.tar.gz" \
      -O "${BUILD_DIR}/ace_Linux_${ARCH}.tar.gz"
  fi
  tar -C "$BUILD_DIR" -xvzf "${BUILD_DIR}/ace_Linux_${ARCH}.tar.gz"
  # The tarball contains exactly one top-level ace-<...> directory. Its version
  # suffix may differ from ACE_VERSION: common.sh rewrites ACE_VERSION to
  # X.Y.Z~pretag for DEB pre-releases, while the workflow normalises the staged
  # dir to the plain X.Y.Z. Locate it by glob so the extract works regardless
  # of the internal directory's suffix.
  shopt -s nullglob
  extracted_dirs=("$BUILD_DIR"/ace-*/)
  shopt -u nullglob
  mv "${extracted_dirs[0]}"* "$BUILD_DIR"/
  rm -rf "${extracted_dirs[0]}"

  echo "Moving Debian packaging into source directory..."
  cp -rp "${CWD}/${COMPONENT_NAME}/deb/debian" "$BUILD_DIR/"
  cp ${COMPONENT_NAME}/common/pgedge-ace.* $BUILD_DIR/debian/

  if [ -f "${ARTIFACT_DIR}/NOTICE.txt" ]; then
    echo "Using staged NOTICE.txt from ${ARTIFACT_DIR}"
    cp "${ARTIFACT_DIR}/NOTICE.txt" "$BUILD_DIR/NOTICE.txt"
  else
    echo "Downloading NOTICE.txt from ace repo..."
    curl -fsSL -o "$BUILD_DIR/NOTICE.txt" \
      https://raw.githubusercontent.com/pgEdge/ace/${ACE_BRANCH}/NOTICE.txt
  fi

  echo "Installing build dependencies..."
  cd "$BUILD_DIR/"
  sudo apt-get update
  sudo apt-get build-dep -y .
}

build() {
  
  cd "$BUILD_DIR/"
  echo "Building Debian package..."
  DISTRO=$(lsb_release -cs)
  rm -f debian/changelog
cat > debian/changelog <<EOF
pgedge-ace (${ACE_VERSION}-${ACE_BUILDNUM}.${DISTRO}) stable; urgency=medium

  * Initial pgedge-ace package.

 -- Muhammad Aqeel <muhammad.aqeel@pgedge.com>  $(date -R)
EOF

  dpkg-buildpackage -us -uc -b
}

post_build() {
  echo "Copying .deb packages to output..."
  sudo mkdir -p "/output"
  # Rename .ddeb files to .deb files
  rename_ddeb_packages $BUILD_DIR
  sudo cp "$BUILD_DIR"/../*.deb "/output" || echo "No .deb packages found."
}

