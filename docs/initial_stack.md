# Initial Stack

A local data lakehouse: MinIO (storage) + Lakekeeper (Iceberg REST catalog) +
Keycloak (authentication) + OpenFGA (authorization) + Trino/SQLPad (ad-hoc SQL)
+ local PySpark (per-user reads/writes) + JupyterHub (browser-based per-user
notebooks backed by a shared Spark standalone cluster). All components run
via `docker compose up -d`, except local PySpark, which runs on your machine
and talks to the stack over `localhost`.

Access to tables/namespaces is granted declaratively in
[`reconciler/grants.yaml`](../reconciler/grants.yaml) and applied by
[`reconciler/reconcile.py`](../reconciler/reconcile.py) — see that file for
the current group → permission mapping.

## Credentials

All dev-only, throwaway values (see [Notes](#notes-on-this-dev-setup)).

| Where | URL | Username / ID | Password / secret |
|---|---|---|---|
| MinIO console | http://localhost:9001 | `minioadmin` | `test1234` |
| Keycloak admin console (realm `master`) | http://localhost:8080 | `admin` | `test1234` |
| Keycloak realm `lakehouse` — platform admin | http://localhost:8080/realms/lakehouse/account | `vkieuvongngam` (email `vkieuvongngam@gmail.com`) | `test1234` |
| Keycloak realm `lakehouse` — mock users | http://localhost:8080/realms/lakehouse/account | `alice`, `bob`, `carol`, `dave`, `erin`, `frank` | `test1234` (same for all six) |
| SQLPad | http://localhost:3000 | `admin@lakehouse.local` | `test1234` |
| Lakekeeper console | http://localhost:8181 | logs in via Keycloak — use any user/admin above | — |
| Trino | http://localhost:8090 | no auth; any `X-Trino-User` header is accepted (dev-only) | — |
| JupyterHub | http://localhost:8000 | logs in via Keycloak — use any user/admin above | — |

Every human/console login uses the same password (`test1234`) for convenience
in this dev stack. Postgres passwords, the Lakekeeper encryption key, and
OAuth2 client secrets are left as generated random values below since they're
never typed into a login prompt.

Keycloak confidential-client secrets (only needed if you're calling APIs
directly, e.g. from a script): `trino` = `f472d1eaa5660b41eedf5bcbdea54822`,
`reconciler` = `8e804e302000d95938c4baff238db8a4`,
`translation-pipeline` = `3b72bdba62d4eb78ca21d0d735bcaa05`,
`table-definitions` = `86af3400c9e378c65237b57e4957b92b`,
`jupyterhub` = `ac97e820f7d7c765acb9f419ce4705c7`.

All of the above live in [`.env`](../.env) (source of truth) and are mirrored
into [`keycloak/realm-lakehouse.json`](../keycloak/realm-lakehouse.json) — see
the note on why they're duplicated instead of templated.

## Components

### MinIO — object store
S3-compatible storage backing every Iceberg table's data and metadata files.
- Console: http://localhost:9001 (see [Credentials](#credentials))
- S3 API: http://localhost:9000
- Bucket `lakehouse` and a scoped access key for Lakekeeper are created by the
  one-shot `mc-init` service (`minio/init.sh`) on first `docker compose up`.

### Keycloak — authentication (OIDC)
Issues identity tokens for every human and machine identity in the stack
(the real admin, the six mock users, and every service-account client).
Realm `lakehouse` is imported declaratively from
[`keycloak/realm-lakehouse.json`](../keycloak/realm-lakehouse.json).
- Console: http://localhost:8080 (see [Credentials](#credentials))
- Token endpoint: `http://localhost:8080/realms/lakehouse/protocol/openid-connect/token`
- Mock users and the platform admin are listed in [Credentials](#credentials).
- Every `users` entry has a pinned `id` — **do not remove these.** Lakekeeper/
  OpenFGA grants are keyed to that `id`, not the email/`client_id`; an
  unpinned entry gets a fresh random `id` on every realm re-import, silently
  orphaning its grants the next time Keycloak restarts. Adding a new
  user/service-account here always needs an explicit `id` too. See
  [`keycloak_restart_broke_authz.md`](keycloak_restart_broke_authz.md) for
  what happens when this slips.

### OpenFGA — authorization (fine-grained access control)
Stores the actual permission tuples (who can select/modify what) as a
relationship graph. Lakekeeper is the only component that talks to it
directly — it's the backend behind every grant in `grants.yaml`.
- Playground (inspect the authz model & tuples): http://localhost:3001/playground/
- Not exposed for direct API use outside the stack; internal-only otherwise.

### Lakekeeper — Iceberg REST catalog
The catalog every query engine (Trino, Spark) talks to for table metadata. It
validates every request's Keycloak token, checks the request against OpenFGA,
and vends scoped, short-lived S3 credentials for reads/writes it approves.
- REST catalog + management API: http://localhost:8181
- Bundled web console: http://localhost:8181
- Swagger UI (full API reference): http://localhost:8181/swagger-ui/
- Warehouse `local` (bucket `lakehouse`, prefix `warehouse/`) and the demo
  namespaces/tables are created by the one-shot `bootstrap` service
  (`lakekeeper/bootstrap.py`).

### Trino + SQLPad — ad-hoc SQL / observability
Trino queries Iceberg tables directly via the Lakekeeper REST catalog.
SQLPad is the browser SQL editor in front of it.

**Design note:** Trino/SQLPad run as one shared, trusted identity (the
`trino` Keycloak client) with broad read access granted at the warehouse
level, rather than enforcing access per human user. This is a deliberate
simplification: true per-user enforcement in Trino requires Trino's own
OAuth2 browser-login flow, and SQLPad's Trino driver only supports a fixed
username per saved connection — it can't drive that flow. So this pairing is
positioned as a shared observability tool, not an access-control boundary.
**Per-user read/write enforcement is demonstrated through PySpark instead**
(see below), where each mock user authenticates individually and Lakekeeper/
OpenFGA enforces their specific grants.
- Trino: http://localhost:8090
- SQLPad: http://localhost:3000 (login: `admin@lakehouse.local` / see `.env`)
  — pre-configured with a `Lakekeeper (via Trino)` connection.

### Local PySpark — per-user reads/writes
**One-time setup** — make the advertised storage hostname resolvable on your
machine. Lakekeeper hands the storage endpoint to every client, so that name has
to resolve for them; this is the local stand-in for the DNS record a real
deployment would have (see [`CORS_issues.md`](CORS_issues.md)):
```bash
echo "127.0.0.1 minio.localhost" | sudo tee -a /etc/hosts
```

Then set up the project once:
```bash
cd spark
uv sync
cp .env.example .env    # optional: skip the username/password prompt — see below
```

**[`query_orders.py`](../spark/query_orders.py)** — non-interactive:
```bash
uv run python query_orders.py            # defaults to alice/test1234 (or .env)
uv run python query_orders.py carol test1234
```
Verified output:
```
+--------+----------------+-------+
|order_id|        customer| amount|
+--------+----------------+-------+
|       1|       acme-corp| 1250.0|
...
```

**[`local_pyspark_example.ipynb`](../spark/local_pyspark_example.ipynb)** — the
same flow interactively: `uv run jupyter notebook`.

**[`compact_orders.ipynb`](../spark/compact_orders.ipynb)** — Iceberg table
compaction (`CALL lakekeeper.system.rewrite_data_files(...)`), which rewrites a
table's small files into fewer, larger ones without changing data. Needs
`modify` permission, same as a write. Verified: 4 small files → 1
(`rewritten_data_files_count=4, added_data_files_count=1`), row count
unchanged (8→8), and a read-only user is correctly **denied** (confirmed with
enough files present that there was genuine work to do — with too few files
to trigger the rewrite threshold, a read-only user's call is a harmless no-op
that "succeeds" without ever attempting to write, which looks like a
permission gap but isn't).

Authorization is enforced through the Spark path exactly as declared in
`grants.yaml` — verified from the host for every operation above:

| user | group | read `sales.orders` | write | compact |
|---|---|---|---|---|
| `alice` | `data-eng-writers` | ✅ | ✅ | ✅ |
| `carol` | `sales-analytics-readonly` | ✅ | ❌ denied | ❌ denied |
| `erin` | `sensitive-translation-readers` | ❌ `TABLE_OR_VIEW_NOT_FOUND` (Lakekeeper masks the 403) | — | — |

**No more retyping a password**: `get_credentials()` in `query_orders.py`
(shared by both notebooks) reads `SPARK_USERNAME`/`SPARK_PASSWORD` from
`spark/.env` if present, falling back to an interactive prompt only when
they're unset. `.env` is gitignored; `.env.example` documents it and is
committed. These are the same published dev-only mock-user passwords, not a
new secret to manage.

Note there is no `s3.endpoint` in the Spark config, deliberately: Lakekeeper
returns the storage endpoint in every `LoadTable` response and Iceberg gives
that server-provided table config precedence, so setting it client-side is
silently ignored. Hence the DNS entry above rather than a client override.

`setuptools` is a listed dependency on purpose: Python 3.12 removed `distutils`
(PEP 632) but PySpark 3.5 still imports it in `pyspark.sql.pandas.utils`, so any
pandas interop (`createDataFrame(pandas_df)`, i.e. the write cell) fails with
`ModuleNotFoundError: No module named 'distutils'` without it. setuptools ships
the supported compatibility shim.

A Jupyter kernel named **"Lakehouse PySpark (3.12)"** is registered against
`spark/.venv`, so the notebooks also work from an existing Jupyter install.
Remove it with `jupyter kernelspec remove lakehouse-spark`.

### JupyterHub — browser-based multi-user notebooks
A second, additive way to run PySpark against the lakehouse: no local Python
setup, no `/etc/hosts` edit, and a real distributed Spark cluster instead of
`local[*]`. Design/build write-up: [`team_jupyter_hub.ipynb`](team_jupyter_hub.ipynb).

- Hub: http://localhost:8000 — sign in via "Sign in with Keycloak" as any
  mock user (see [Credentials](#credentials)); same trust boundary as the
  Lakekeeper console (any Keycloak user may log in to the Hub itself — the
  data-access boundary is Lakekeeper/OpenFGA, enforced by each notebook's own
  separate Keycloak login, exactly as in the local PySpark flow above).
- At spawn time you pick a session size — Small/Medium/Large (2/4/8 cores) —
  which caps how many of the shared cluster's 8 cores your driver may claim
  (`spark.cores.max`). Multiple users can pick different sizes concurrently;
  Spark's master arbitrates the shared pool.
- `spark-master` (`spark-master` service, UI at http://localhost:8082) +
  2x `spark-worker` (4 cores each, no published ports) form the shared
  standalone cluster, started by `docker compose up -d` like everything else.
- The singleuser notebook image isn't started by `docker compose up -d` (it
  has no long-running command) — build it once, and again after any change
  to `spark/query_orders.py` or `spark/docker/`:
  ```bash
  docker compose build spark-notebook
  ```
- Inside a spawned notebook, `from query_orders import get_credentials,
  get_spark_session` works exactly as in the local flow — it's the same
  file, extended (not forked) to read `SPARK_MASTER_URL`/`SPARK_CORES_MAX`/
  `KEYCLOAK_TOKEN_URL`/`CATALOG_URL` from the environment so it can target
  the in-cluster Docker-network endpoints instead of `localhost`/`local[*]`.

Known limitations, by design for this dev stack (see the design doc for the
full rationale): the Hub's `jupyterhub` service needs
`/var/run/docker.sock` mounted to spawn per-user containers — root-equivalent
host access, in the same spirit as this stack's other dev-only shortcuts
below. Spawned notebook containers are ephemeral (no per-user persistent
storage yet). Signing in to the Hub and logging in to Lakekeeper inside the
notebook are two separate steps against the same Keycloak realm — no token
passthrough in this iteration.

### Access control: `reconciler/grants.yaml` + `reconcile.py`
The single source of truth for who can do what. Declares groups (mapped to
Lakekeeper roles) with members by email or service-account client ID, and
grants (group → resource → permission). Re-run after any edit:
```
docker compose run --rm reconciler
```
It's idempotent — safe to run repeatedly, and only additive (it doesn't
revoke access removed from the file; see the comment at the top of
`reconcile.py`).

### `lakekeeper/bootstrap.py`
One-shot platform setup, run automatically once Lakekeeper is healthy:
bootstraps Lakekeeper itself, grants the real admin full rights, creates the
`local` warehouse, grants Trino's shared identity read access to it, and
seeds the demo namespaces/tables (`sales`, `sales_reporting`, `content`;
tables `sales.orders`, `content.translated_documents_sensitive`) that
`grants.yaml` references. `sales.orders` is seeded with 5 dummy rows (only
if the table is currently empty, so re-running never duplicates data).
Idempotent — re-run any time with `docker compose run --rm bootstrap`.

## Python tooling

The three Python folders — [`spark/`](../spark), [`reconciler/`](../reconciler)
and [`lakekeeper/`](../lakekeeper) — are each a self-contained **uv** project:
Python 3.12 (pinned in `.python-version`), dependencies and lock in
`pyproject.toml` + `uv.lock`, with **ruff** (lint + format) and **pyright**
(type checking) configured per project and kept in a `dev` dependency group.
There are no `requirements.txt` files anywhere and none should be added.

```bash
cd reconciler          # or spark / lakekeeper
uv sync                # create .venv and install (incl. dev tools)
uv run ruff check .    # lint  (also lints notebook cells)
uv run ruff format .   # format
uv run pyright .       # type check
```

`reconciler/` and `lakekeeper/` also run **inside Docker** as one-shot jobs,
using the same locked dependency set — the compose services use the
`ghcr.io/astral-sh/uv:python3.12-bookworm-slim` image and run
`uv sync --frozen --no-dev && uv run python <script>.py`. Two details make that
work: the project is bind-mounted read-only, so `UV_PROJECT_ENVIRONMENT`
puts the venv at `/opt/venv` outside the mount, and `UV_CACHE_DIR` points at a
shared `uv_cache` named volume so repeat runs don't re-download wheels.
`--frozen` means the container installs exactly what `uv.lock` pins and never
silently re-resolves.

## Ports

| Service | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| MinIO S3 API | http://localhost:9000 |
| Keycloak | http://localhost:8080 |
| OpenFGA Playground | http://localhost:3001/playground/ |
| Lakekeeper (catalog + management API + console) | http://localhost:8181 |
| Trino | http://localhost:8090 |
| SQLPad | http://localhost:3000 |
| JupyterHub | http://localhost:8000 |
| Spark master UI (standalone cluster, 2 workers x 4 cores) | http://localhost:8082 |

## Notes on this dev setup

- All secrets in `.env` and `keycloak/realm-lakehouse.json` are throwaway
  values checked into this repo on purpose, for local use only. Keycloak's
  `--import-realm` doesn't reliably substitute env-var placeholders in the
  realm JSON, so client secrets and passwords are duplicated (hardcoded) in
  both files — keep them in sync by hand if you change one.
- MinIO storage credentials for the warehouse are long-lived static keys
  (`sts-enabled: false`), not vended STS credentials — simpler to set up
  locally; scoped per-request credential vending via MinIO STS is a possible
  future hardening step. Trino is likewise configured with these same static
  keys directly (`s3.aws-access-key`/`s3.aws-secret-key` in
  `trino/catalog/lakekeeper.properties`) rather than Lakekeeper's per-request
  vended credentials — the latter didn't work out of the box against a
  static-credential (non-STS) warehouse in testing.
- **The storage endpoint is `http://minio.localhost:9000`, not `http://minio:9000`.**
  The endpoint saved in a warehouse's storage profile is handed to *every*
  client, and the Lakekeeper console's data preview (Warehouse → table →
  Preview) runs DuckDB-WASM **in your browser** — which is not on the Docker
  network and cannot resolve `minio`. Equally, containers can't use
  `localhost`. `minio.localhost` is the one name that resolves in both
  places: a `networks.lakehouse_net.aliases` entry on the `minio` service
  covers in-cluster clients, and browsers resolve the reserved `.localhost`
  TLD to loopback per RFC 6761, reaching the published port. This mirrors
  production, where the object store has one real DNS name resolvable by
  both cluster workloads and users' browsers.
  - The console reports *any* failed storage fetch as a generic "CORS
    Error" regardless of cause — the actual failure here was DNS. This cost
    a lot of debugging time; the full write-up is in
    [`CORS_issues.md`](CORS_issues.md). **Read that first** if the preview
    ever breaks again.
  - Two supporting settings also matter for the preview:
    `MINIO_API_CORS_ALLOW_ORIGIN=*` on the `minio` service (per-bucket CORS
    via `mc cors` is an AIStor/paid feature, so global is the only option
    here), and `mc anonymous set download` on the bucket in
    `minio/init.sh` — the browser sends *unsigned* GETs, which MinIO
    otherwise rejects with `403 AccessDenied`. That last one makes every
    object world-readable to anyone who can reach MinIO's port directly,
    bypassing Lakekeeper/OpenFGA. Acceptable for a localhost dev stack;
    do not carry it into a real deployment.
  - Host-run clients (local PySpark/Jupyter) use the system resolver, which
    does *not* implement the `.localhost` rule browsers do, and the endpoint
    can't be overridden client-side (Lakekeeper sends it per-table and
    Iceberg lets server config win). They need a one-line hosts entry —
    see [`CORS_issues.md`](CORS_issues.md).
- ROPC (Resource Owner Password Credentials) is used for local PySpark login
  because it's the simplest non-interactive flow for a notebook. It's
  deprecated in OAuth 2.1 and should never be used outside local dev.
- JupyterHub's `jupyterhub` service mounts `/var/run/docker.sock` so it can
  spawn one container per logged-in user (DockerSpawner) — root-equivalent
  access to the host's Docker daemon. Acceptable only because this is a
  localhost dev stack; a real deployment would need a proper spawner (e.g.
  KubeSpawner against an actual Kubernetes cluster) instead. Spawned
  notebook containers are ephemeral by default (no per-user persistent
  volume yet), and signing in to the Hub via Keycloak SSO is a separate step
  from each notebook's own Lakekeeper login (no token passthrough) — see
  [`team_jupyter_hub.ipynb`](team_jupyter_hub.ipynb) for the full design
  write-up and the corrections that came up during the build.
