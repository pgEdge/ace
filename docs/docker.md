# Running ACE with Docker

You can run ACE entirely from the published container images without installing Go or compiling binaries. Images are published to `ghcr.io/pgedge/ace` as multi-arch manifests for `linux/amd64` and `linux/arm64` with tags:

- `:latest` – tracks the newest release.
- `:vX.Y.Z` – matches the Git tag of each release.

The image entrypoint is `ace`, so any arguments you add are passed directly to the CLI.

## Bootstrap configuration (generated inside the container)

If you do not have `ace.yaml` or `pg_service.conf`, generate them with the image and write them to your host:

```sh
# Generate ace.yaml into the current directory
docker run --rm -it -v "$PWD:/workspace" ghcr.io/pgedge/ace:latest \
  config init --path /workspace/ace.yaml

# Generate pg_service.conf into the current directory
docker run --rm -it -v "$PWD:/workspace" ghcr.io/pgedge/ace:latest \
  cluster init --path /workspace/pg_service.conf
```

Edit both files on the host to set your clusters, nodes, TLS paths, and defaults before running other commands.

## What to mount into the container

- `ace.yaml`: mount to `/etc/ace/ace.yaml` (or set `ACE_CONFIG` to your path).
- `pg_service.conf`: mount to `/etc/pg_service.conf` (or set `ACE_PGSERVICEFILE`).
- Certificates for the API server: paths must match `server.tls_cert_file`, `server.tls_key_file`, and optional `server.client_crl_file` in `ace.yaml`.
- A workspace directory for outputs (diff JSON, HTML reports, logs): mount to `/workspace` so files persist on the host.

## One-off CLI runs (ephemeral)

Use the container as a throwaway CLI by mounting your config and service files, plus a workspace for outputs:

```sh
docker run --rm -it \
  -v "$PWD/ace.yaml:/etc/ace/ace.yaml:ro" \
  -v "$PWD/pg_service.conf:/etc/pg_service.conf:ro" \
  -v "$PWD:/workspace" \
  ghcr.io/pgedge/ace:latest \
  table-diff --nodes=n1,n2 my-cluster public.orders
```

- The working directory inside the container is `/workspace`. Diff files, reports, and logs written there will land in your host directory.
- Set `ACE_PGSERVICEFILE` if you prefer a custom location for `pg_service.conf`.

## Running the REST API server (long-lived)

Start the API server when you want HTTP access to ACE. Configure TLS paths in `ace.yaml` (`server.tls_cert_file`, `server.tls_key_file`, optional `server.client_crl_file`, and `allowed_common_names`), mount the certs, and publish the port:

```sh
docker run -d --name ace-api \
  -p 5000:5000 \
  -v "$PWD/config/ace.yaml:/etc/ace/ace.yaml:ro" \
  -v "$PWD/config/pg_service.conf:/etc/pg_service.conf:ro" \
  -v "$PWD/certs:/certs:ro" \
  -e ACE_CONFIG=/etc/ace/ace.yaml \
  ghcr.io/pgedge/ace:latest \
  server
```

- Replace `5000:5000` if you change `server.listen_port`.
- Ensure the cert/key paths in `ace.yaml` point to files under the mounted `certs` directory (for example `/certs/server.crt`).
- Logs stream to `docker logs ace-api`.

## Running the background scheduler

To run scheduled jobs defined in `schedule_jobs` and `schedule_config` inside `ace.yaml`, launch the scheduler in a long-running container:

```sh
docker run -d --name ace-scheduler \
  -v "$PWD/ace.yaml:/etc/ace/ace.yaml:ro" \
  -v "$PWD/pg_service.conf:/etc/pg_service.conf:ro" \
  -v "$PWD:/workspace" \
  -e ACE_CONFIG=/etc/ace/ace.yaml \
  ghcr.io/pgedge/ace:latest \
  start --component=scheduler
```

- The scheduler reads the same config and service definitions as the CLI.
- Output from scheduled jobs (diff JSON/HTML, reports) is written under `/workspace`, so keep that volume mounted.
- Use `docker logs ace-scheduler` to monitor runs; stop with `docker rm -f ace-scheduler`.
