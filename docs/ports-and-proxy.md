# Ports and Proxies — Troubleshooting Guide

Two things stop most first-time installs of the HEARTS360 Toolkit:

1. **A port is already taken.** The stack publishes `3000`, `5432`, `8080` and
   `5050`, all of which are commonly used by something else.
2. **The network intercepts or blocks TLS.** Building and starting the stack
   downloads Grafana plugins, Python packages and the `pg_cron` source. On a
   corporate network those downloads fail, usually with an SSL error.

Both are now configurable. Nothing in this guide is required on a normal home
or cloud network — the defaults are unchanged.

---

## Part 1 — Ports

### Quick fix

```bash
# Linux / macOS
./scripts/heart360-up.sh

# Windows (PowerShell)
.\scripts\heart360-up.ps1
```

The script probes every port, picks the next free one where there is a clash,
writes the result to `.env`, starts the stack, and prints the final URLs. Ports
are remembered, so restarts keep the same addresses.

Use `--check` / `-Check` to see what it would do without changing anything.

### Manual fix

```bash
cp .env.example .env
```

Then edit:

```dotenv
HEART360_GRAFANA_PORT=3100
HEART360_POSTGRES_PORT=55432
HEART360_FILEBROWSER_PORT=18080
HEART360_PGADMIN_PORT=15050
```

```bash
docker compose up -d
```

Only the **host** side of each mapping changes. Containers still talk to each
other on the internal ports, so no other configuration needs adjusting.

### What each port is for

| Service     | Internal | Default host port | Variable                     |
| ----------- | -------- | ----------------- | ---------------------------- |
| Grafana     | 3000     | 3000              | `HEART360_GRAFANA_PORT`      |
| PostgreSQL  | 5432     | 5432              | `HEART360_POSTGRES_PORT`     |
| FileBrowser | 80       | 8080              | `HEART360_FILEBROWSER_PORT`  |
| pgAdmin     | 80       | 5050              | `HEART360_PGADMIN_PORT`      |
| Autodoc DB  | 5432     | 5433              | `HEART360_AUTODOC_DB_PORT`   |

Typical conflicts: `3000` — Node/React dev servers, Gitea; `5432` — a locally
installed PostgreSQL; `8080` — Tomcat, Jenkins, Spring Boot; `5050` — Kubeflow
or another pgAdmin.

### Error messages and what they mean

| Message | Meaning |
| --- | --- |
| `Bind for 0.0.0.0:3000 failed: port is already allocated` | Another process (often another container) holds the port. |
| `Ports are not available ... bind: An attempt was made to access a socket in a way forbidden by its access permissions` | **Windows only.** Usually a Hyper-V/WSL *reserved* port range rather than a real listener. `netstat` shows nothing. `heart360-up.ps1` detects this because it attempts a real bind. |
| `The container name "/grafana" is already in use` | A previous stack is still present. `docker compose down`, or set the `HEART360_*_CONTAINER` variables (see below). |

To see the Windows reserved ranges:

```powershell
netsh interface ipv4 show excludedportrange protocol=tcp
```

### Grafana on a non-default port

Grafana builds absolute links from `GF_SERVER_ROOT_URL`. The helper scripts set
this automatically; if you edit `.env` by hand, set it too:

```dotenv
HEART360_GRAFANA_PORT=3100
HEART360_GRAFANA_ROOT_URL=http://localhost:3100/
```

### Restricting who can reach the stack

By default the ports bind to `0.0.0.0` — every interface — which is required by
the EC2 deployment. On a laptop, or on any host still using the default
passwords, bind to loopback instead:

```dotenv
HEART360_BIND_ADDRESS=127.0.0.1
```

### Running two stacks on one machine

Different ports are not enough, because container names are fixed. Give each
instance its own project and container names:

```dotenv
COMPOSE_PROJECT_NAME=h360tk_training
HEART360_GRAFANA_CONTAINER=h360tk_training_grafana
HEART360_POSTGRES_CONTAINER=h360tk_training_postgres
HEART360_PGADMIN_CONTAINER=h360tk_training_pgadmin
HEART360_FILEBROWSER_CONTAINER=h360tk_training_filebrowser
HEART360_FILEPROC_CONTAINER=h360tk_training_fileproc
```

> Changing `COMPOSE_PROJECT_NAME` on an **existing** deployment points Compose
> at a different set of named volumes, so pgAdmin's saved state starts empty.

---

## Part 2 — Proxies and TLS

### The symptom

```
SSLError(1, '[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] ssl/tls alert handshake failure')
```

Despite the name, this is almost never a problem with SSLv3. It means the peer
sent a TLS `handshake_failure` alert — the connection was refused *during*
negotiation. In practice it is one of:

| Cause | Why it produces this error |
| --- | --- |
| A middlebox filtering by SNI hostname | The firewall inspects the hostname in the ClientHello and terminates the handshake. Very common for `files.pythonhosted.org`. |
| Egress only allowed via a proxy | Direct TLS is reset by the firewall; the client never uses the proxy because `HTTPS_PROXY` is not set inside the container. |
| A TLS-inspecting proxy that cannot negotiate modern parameters | Older appliances reject TLS 1.3, or the cipher list permitted by OpenSSL 3's default security level. |

A different message means a different fix:

| Message | Fix |
| --- | --- |
| `CERTIFICATE_VERIFY_FAILED` | TLS interception — install the corporate CA (Step 2). |
| `SSLV3_ALERT_HANDSHAKE_FAILURE` | Handshake refused — Step 1, then Step 3 or 4. |
| `EOF occurred in violation of protocol` | Connection cut mid-handshake — usually SNI filtering. Step 3 or 4. |
| `tls: handshake timeout` during `docker pull` | Docker's own egress. Configure the proxy in Docker Desktop → Settings → Resources → Proxies. |

### Diagnose first

```bash
docker compose logs file-processor
```

On failure the file processor prints an explicit probe: whether direct TLS to
`pypi.org` works, which OpenSSL version is in use, and whether a proxy is
configured. Reproduce it on demand with:

```bash
docker compose run --rm --entrypoint sh file-processor /scripts/install_python_deps.sh
```

### Step 1 — Route egress through the proxy

This alone resolves most handshake failures, because it turns a blocked direct
connection into a `CONNECT` tunnel the firewall allows.

```dotenv
HTTP_PROXY=http://proxy.example.com:8080
HTTPS_PROXY=http://proxy.example.com:8080
NO_PROXY=localhost,127.0.0.1,postgres,grafana,pgadmin,filebrowser-quantum,file-upload-trigger
```

Keeping the service names in `NO_PROXY` matters: without it, container-to-container
traffic can be sent to a proxy that cannot route it.

Then rebuild so the build stage picks the values up as build args:

```bash
docker compose build --no-cache
docker compose up -d
```

Docker's own image pulls do **not** read `.env`. Set the proxy in
Docker Desktop → **Settings → Resources → Proxies**, or in
`/etc/systemd/system/docker.service.d/http-proxy.conf` on Linux.

### Step 2 — Trust the interception CA

Fixes `CERTIFICATE_VERIFY_FAILED`.

1. Export your organisation's root CA in **Base-64 encoded X.509** form.
   * Windows: `certmgr.msc` → *Trusted Root Certification Authorities* → export.
   * macOS: Keychain Access → *System Roots* → export as `.pem`.
2. Save it into `certs/` as `corp-root.crt`.
3. `docker compose build --no-cache && docker compose up -d`

The certificate is installed into the system trust store of the Grafana,
PostgreSQL and file-processor images, and is also bind-mounted at runtime so
adding a new certificate does not always require a rebuild.

### Step 3 — Use internal mirrors

The most robust option on a locked-down network, and the one to prefer if your
organisation runs Nexus or Artifactory.

```dotenv
PIP_INDEX_URL=https://nexus.example.com/repository/pypi/simple
PIP_TRUSTED_HOST=nexus.example.com
HEART360_PG_CRON_REPO=https://gitmirror.example.com/citusdata/pg_cron.git
HEART360_GF_PLUGIN_REPO=https://grafana-mirror.example.com
HEART360_PGADMIN_IMAGE=registry.example.com/dpage/pgadmin4:9.14
HEART360_FILEBROWSER_IMAGE=registry.example.com/gtstef/filebrowser:stable-slim
HEART360_FILEPROC_IMAGE=registry.example.com/devodev/inotify
```

> Point these at a registry your organisation controls. An arbitrary public
> mirror is an unreviewed supply-chain dependency.

### Step 4 — Relax transport security (in order of preference)

**4a. Alpine packages over plain HTTP.** Low risk: `apk` verifies every index
and package against RSA keys baked into the image, so integrity is preserved and
only confidentiality of the download is lost.

```dotenv
HEART360_APK_INSECURE_HTTP=true
```

**4b. Older TLS parameters.** Sets `MinProtocol=TLSv1.0` and
`CipherString=DEFAULT@SECLEVEL=1`. Use when the proxy itself cannot complete a
modern handshake.

```dotenv
HEART360_TLS_LEGACY=true
```

**4c. Skip certificate verification.** Applies to `pip`, `grafana cli` and
`git`. This removes authenticity checks from downloaded code and makes the build
vulnerable to tampering. Acceptable only as a temporary measure on a trusted
network while you obtain the proper CA for Step 2. **Never leave it enabled.**

```dotenv
HEART360_TLS_INSECURE=true
```

### Step 5 — No egress at all (air-gapped)

Pre-download everything on a connected machine.

**Python packages.** The file-processor image is Alpine/musl based, so glibc
(`manylinux`) wheels will not install:

```bash
pip download \
  --only-binary=:all: \
  --platform musllinux_1_2_x86_64 \
  --python-version 3.12 \
  --dest ./vendor/wheels \
  pandas openpyxl psycopg2-binary python-calamine
```

Copy `vendor/wheels/` to the target host. The installer tries the wheelhouse
before touching the network, so no egress is needed.

**Grafana plugins.** They are baked into the image at build time. On the target
host, stop Grafana from re-checking `grafana.com` on every start:

```dotenv
HEART360_GF_INSTALL_PLUGINS=
```

**Fail loudly.** By default the file processor still starts if dependency
installation fails, so an already-working image is not taken down by a transient
outage. On an air-gapped host you usually want the opposite:

```dotenv
HEART360_REQUIRE_DEPS=true
```

### Verifying the fix

```bash
docker compose logs file-processor | grep heart360
```

Expected on a healthy stack:

```
[heart360-init] Doing the init ...
[heart360-deps] all Python dependencies already present, nothing to do
[heart360-init] dependencies ready
[heart360-init] starting inotify watcher on /data
```

---

## Part 3 — Related pitfalls

### Line endings on Windows

Cloning on Windows with the default `core.autocrlf=true` used to rewrite the
shell scripts to CRLF, and the Linux containers then failed with:

```
/scripts/fileproc.sh: line 1: #!/bin/bash^M: not found
syntax error: unexpected "elif" (expecting "then")
```

A `.gitattributes` file now pins these files to LF. If you cloned before it
existed, refresh your working copy:

```bash
git rm --cached -r . && git reset --hard
```

### Grafana starting before PostgreSQL

Grafana stores its own state in PostgreSQL, so on a cold start it used to
crash-loop until the database finished initialising. PostgreSQL now has a
health check and the other services wait for it, so `docker compose up -d`
comes up cleanly on the first try. Initial start-up takes longer because the
database is genuinely being created.

<a id="hardening"></a>

## Part 4 — Hardening before real use

The credentials in this repository are **published in a public git history**.
Anyone can read them. Before the stack is reachable by anyone other than you:

1. Set `HEART360_BIND_ADDRESS=127.0.0.1`, or restrict access at the firewall or
   security group. PostgreSQL and pgAdmin in particular should not be exposed.
2. Rotate every credential. On a **fresh** database volume, set the variables in
   `.env` and make the matching change in `pg_init_scripts/00_schemas.sql`,
   `pgadmin_config/pgpass` and `grafana_provisioning/datasources/*.yml`, since
   those roles are created by the init scripts. On an **existing** volume, use
   `ALTER USER ... WITH PASSWORD ...` and then update the same files.
3. Change the Grafana admin password (`grafana_provisioning/config/grafana.ini`
   currently sets `admin`/`admin`).
4. Change the FileBrowser and pgAdmin passwords.
5. Put a TLS-terminating reverse proxy in front of Grafana; it currently serves
   plain HTTP, so credentials cross the network in the clear.
