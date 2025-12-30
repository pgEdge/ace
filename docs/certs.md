# Certificates and mTLS for ACE

This guide explains how to generate certificates and wire them into ACE and PostgreSQL, with an emphasis on the HTTP API server and role mapping.

## Why the API uses mTLS and role switching

- API callers authenticate to ACE with mutual TLS; ACE validates the client certificate and uses the certificate CN as the database role to assume.
- ACE does not have the caller's private key, so it cannot connect to PostgreSQL as the caller directly.
- ACE connects to PostgreSQL using its own DB credentials (password or client cert), performs any privileged setup work, then runs user-specific operations with `SET ROLE`.
- This preserves least-privilege for API callers without inventing a separate ACE-only auth system.

Because of this, the ACE DB user must be a member of the roles you want the API to assume:

```sql
-- Example: ACE connects as ace_user and can SET ROLE api_user.
CREATE ROLE ace_user LOGIN;
CREATE ROLE api_user NOLOGIN;

GRANT api_user TO ace_user;
-- Then grant privileges to api_user, not to ace_user directly.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO api_user;
```

## Certificate types you will likely need

- API server certificate: used by the ACE HTTP server (`server.tls_cert_file`, `server.tls_key_file`).
- API client certificates: used by API callers to authenticate to ACE; CN becomes the role ACE tries to `SET ROLE` to.
- PostgreSQL client certificate for ACE: used when `cert_auth.use_cert_auth=true` so ACE can connect to PostgreSQL via mTLS.
- PostgreSQL server certificate: used by PostgreSQL to serve TLS to clients.

You may reuse the same CA for API and PostgreSQL, or separate them. You can also reuse the same client cert for ACE API and for ACE-to-Postgres, but separation is usually cleaner and easier to rotate.

## Generate certs with EasyRSA (example)

### 1) Initialize PKI and build a CA

```sh
easyrsa init-pki
easyrsa build-ca
```

### 2) Build the ACE API server certificate

Include SANs for the hostnames/IPs clients will use:

```sh
EASYRSA_SAN="DNS:n1,IP:192.168.0.105,IP:127.0.0.1" \
  easyrsa build-server-full n1 nopass
```

### 3) Build API client certificates (one per caller)

```sh
easyrsa build-client-full api_user nopass
easyrsa build-client-full admin_user nopass
```

The CN you choose (e.g., `api_user`) should match a PostgreSQL role ACE will `SET ROLE` to.

### 4) Build a PostgreSQL client cert for ACE (optional but recommended)

```sh
easyrsa build-client-full ace_user nopass
```

Use a dedicated CN to represent ACE's service account in PostgreSQL (often the `DBUser` in `pg_service.conf`).

### 5) Build a PostgreSQL server certificate (if needed)

```sh
EASYRSA_SAN="DNS:pg1,IP:10.0.0.10" \
  easyrsa build-server-full pg1 nopass
```

## Distribute files

- ACE HTTP server: `server.tls_cert_file`/`server.tls_key_file` and `cert_auth.ca_cert_file` (CA that signed API client certs).
- API callers: their client cert/key and the API CA cert.
- PostgreSQL server: its server cert/key and the CA cert that signed client certs.
- ACE to PostgreSQL: ACE client cert/key and the CA cert that signed the PostgreSQL server cert.

Keep private keys readable only by the owning service account (e.g., `chmod 600`).

## Configure PostgreSQL for client cert auth

At a minimum:

1) Enable SSL and point at server cert/key and CA:

```conf
# postgresql.conf
ssl = on
ssl_cert_file = '/path/to/server.crt'
ssl_key_file = '/path/to/server.key'
ssl_ca_file = '/path/to/ca.crt'
```

2) Require client certs in `pg_hba.conf` (example):

```conf
# pg_hba.conf
hostssl all all 0.0.0.0/0 cert clientcert=verify-full
```

Optional: map certificate subjects to database roles with `pg_ident.conf`.

1) Add a mapped `pg_hba.conf` entry:

```conf
# pg_hba.conf
hostssl all all 0.0.0.0/0 cert clientcert=verify-full map=ace_cert_map
```

2) Map certificate subjects to roles in `pg_ident.conf`:

```conf
# pg_ident.conf
# MAPNAME        SYSTEM-USERNAME         PG-USERNAME
ace_cert_map     "CN=api_user"           api_user
ace_cert_map     "CN=admin_user"         admin_user
```

If your cert subject includes additional attributes (for example `CN=api_user,OU=...`), match the full subject or use a regex. Use `openssl x509 -in client.crt -noout -subject` to see the exact subject string.

The exact HBA entry depends on your network and role mapping needs; use the PostgreSQL docs as the source of truth.

## Configure ACE

### 1) HTTP API server TLS (ace.yaml)

```yaml
server:
  listen_address: "0.0.0.0"
  listen_port: 5000
  tls_cert_file: "/path/to/api-server.crt"
  tls_key_file: "/path/to/api-server.key"
  client_crl_file: "/path/to/client.crl"  # optional
  allowed_common_names: ["api_user", "admin_user"]  # optional allowlist
```

### 2) PostgreSQL client certs for ACE (ace.yaml + pg_service.conf)

Enable cert-based auth for ACE connections:

```yaml
cert_auth:
  use_cert_auth: true
  ace_user_cert_file: "/path/to/ace-user.crt"
  ace_user_key_file: "/path/to/ace-user.key"
  ca_cert_file: "/path/to/pg-ca.crt"
```

If needed, override SSL parameters per node in `pg_service.conf`:

```conf
[mycluster]
host=10.0.0.10
port=5432
dbname=mydb
user=ace_user
sslmode=verify-full
sslcert=/path/to/ace-user.crt
sslkey=/path/to/ace-user.key
sslrootcert=/path/to/pg-ca.crt
```

## Summary of the runtime flow

1) API client connects to ACE over mTLS using its client cert.
2) ACE validates the client certificate and (optionally) allowlisted CNs.
3) ACE connects to PostgreSQL using its service credentials (password or ACE client cert).
4) For user-scoped operations, ACE runs `SET ROLE <client CN>`.
5) ACE executes the task and records status in the task store.

If any part of this chain fails (missing role grant, invalid CN, cert/key mismatch), the API call will fail with an authorization or validation error.
