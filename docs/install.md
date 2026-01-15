# Installing ACE

Choose the option that fits your environment:

- Go install (fastest if you have Go toolchains)
- Download a release tarball
- Run the published container image

## Install via Go

Requires Go 1.20+ with CGO enabled (SQLite). Make sure build deps are present (`libsqlite3-dev`/`sqlite-devel` on Linux).

```sh
# Latest release
go install github.com/pgedge/ace/cmd/ace@latest

# Specific version
go install github.com/pgedge/ace/cmd/ace@v1.5.3
```

The binary lands in `GOBIN` if set, otherwise `$GOPATH/bin` (defaults to `~/go/bin`). Add that to your `PATH`:

```sh
export PATH="$(go env GOBIN || go env GOPATH)/bin:$PATH"
```

## Download a release tarball

Grab the prebuilt archive for your platform from GitHub Releases and unpack the `ace` binary:

```sh
ACE_VER=v1.5.3
OS=Linux    # or Darwin or Windows
ARCH=x86_64 # or arm64

curl -fsSL "https://github.com/pgedge/ace/releases/download/${ACE_VER}/ace_${OS}_${ARCH}.tar.gz" -o /tmp/ace.tgz
tar -xzf /tmp/ace.tgz -C /tmp
sudo install -m 0755 /tmp/ace/ace /usr/local/bin/ace
```

Notes:
- macOS uses `Darwin`; Windows archives are `.zip`.
- On Windows, place `ace.exe` somewhere on your `PATH` (e.g., `%USERPROFILE%\\bin`).

## Run with Docker/Podman

Use the published multi-arch image (amd64/arm64):

```sh
docker run --rm -it ghcr.io/pgedge/ace:latest --help
```

For real use, mount your configs and workspace; see [Running ACE with Docker](./docker.md) for full examples (ephemeral runs, API server, scheduler).

## After install: bootstrap config

Generate starter configs (if you don’t have them yet):

```sh
ace config init --path ./ace.yaml
ace cluster init --path ./pg_service.conf
```

Edit both files for your clusters/nodes before running commands.
