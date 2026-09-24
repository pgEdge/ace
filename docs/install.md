# Installing ACE

ACE supports Linux and macOS only. It does not build on Windows. On a
Windows host, run ACE in WSL or in the container image. Keep its output
directory on the Linux file system, not on a Windows drive: ACE protects
reports with POSIX file modes, and Windows drives do not keep them.

Choose the option that fits your environment:

- Go install (fastest if you have Go toolchains)
- Download a release tarball
- Run the published container image

## Install via Go

Requires Go 1.26+. No CGO or system SQLite libraries needed — the project uses a pure-Go SQLite driver.

```sh
# Latest release
go install github.com/pgedge/ace/cmd/ace@latest

# Specific version — update to the latest tag from GitHub Releases
go install github.com/pgedge/ace/cmd/ace@v1.9.0
```

The binary lands in `GOBIN` if set, otherwise `$GOPATH/bin` (defaults to `~/go/bin`). Add that to your `PATH`:

```sh
export PATH="$(go env GOBIN || go env GOPATH)/bin:$PATH"
```

## Download a release tarball

Grab the prebuilt archive for your platform from GitHub Releases and unpack the `ace` binary:

```sh
ACE_VER=v1.9.0  # update to the latest tag from GitHub Releases
OS=Linux    # or Darwin
ARCH=x86_64 # or arm64

curl -fsSL "https://github.com/pgedge/ace/releases/download/${ACE_VER}/ace_${OS}_${ARCH}.tar.gz" -o /tmp/ace.tgz
tar -xzf /tmp/ace.tgz -C /tmp
sudo install -m 0755 /tmp/ace/ace /usr/local/bin/ace
```

Notes:
- macOS uses `Darwin`.

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
