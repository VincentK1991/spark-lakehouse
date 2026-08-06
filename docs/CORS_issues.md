# The "CORS Error" in the Lakekeeper console preview

**TL;DR — it was not a CORS problem.** The Lakekeeper console reports *any*
failed object-storage fetch as a CORS error. The real failure was DNS: the
browser could not resolve the hostname `minio`. The fix is to give the object
store **one hostname that resolves both inside Docker and in the browser**:

```yaml
# docker-compose.yml — minio service
networks:
  lakehouse_net:
    aliases:
      - minio.localhost        # resolves inside Docker via this alias

# docker-compose.yml — bootstrap service
MINIO_ENDPOINT: http://minio.localhost:9000   # stored in the warehouse profile
```

`minio.localhost` resolves inside Docker because of the network alias, and in
the browser because browsers resolve the reserved `.localhost` TLD to loopback
(RFC 6761), hitting MinIO's published port. Nothing to install, no hosts-file
edit, no IP addresses.

## The symptom

Warehouse → `sales.orders` → **Preview** tab:

> **Failed to load preview**
> CORS Error: Cannot access object storage from the browser. DuckDB tried to
> read Iceberg metadata files from object storage but the request was blocked
> by CORS policy. Please contact your administrator to configure CORS on your
> storage bucket.

…plus a panel showing a CORS JSON policy to add to the bucket. Following that
advice does not fix it, because CORS is not the cause.

## Why it happens

The Lakekeeper console previews data with **DuckDB-WASM running in your
browser**. The browser fetches Iceberg metadata/data files *directly* from
object storage, using the endpoint URL stored in the warehouse's storage
profile.

That endpoint used to be `http://minio:9000`. `minio` is a Docker Compose
service name — it only resolves **inside** the Docker network. Your browser
runs on your machine, which has never heard of a host called `minio`.

The reverse is equally true, which is what makes this awkward: `localhost:9000`
works from your machine but means "myself" inside a container, so it can't be
used either. Lakekeeper validates storage connectivity *from its own container*
when a warehouse is created, so the endpoint must work on both sides.

Upstream acknowledges this: Lakekeeper's own compose examples note that
"DuckDB WASM in the Lakekeeper UI does not work with the Docker Compose
examples because the S3 endpoint is only accessible inside the Docker network."

## How to diagnose this class of bug

**Do not trust the error text.** Open DevTools → Network (or drive a real
browser) and read the actual failure. Here it was unambiguous:

```
GET http://minio:9000/lakehouse/warehouse/.../snap-....avro
    net::ERR_NAME_NOT_RESOLVED
```

`ERR_NAME_NOT_RESOLVED` is DNS. A genuine CORS block looks completely
different — the request *reaches* the server and the browser rejects the
response for a missing/mismatched `Access-Control-Allow-Origin` header.

Quick way to tell them apart:

```bash
# Is CORS actually configured? (a real preflight)
curl -i -X OPTIONS http://localhost:9000/lakehouse/ \
  -H "Origin: http://localhost:8181" \
  -H "Access-Control-Request-Method: GET"
# -> look for: Access-Control-Allow-Origin: http://localhost:8181

# Can the *browser's* machine even resolve the endpoint host?
getent hosts minio        # empty => the browser can't reach it either
```

## Why the obvious fixes are wrong

| Attempt | Why it fails |
|---|---|
| Add a CORS policy to the bucket | CORS was never the problem. Also, per-bucket CORS (`mc cors`) is an AIStor/paid feature; community MinIO only has the global `MINIO_API_CORS_ALLOW_ORIGIN`. |
| Add `127.0.0.1 minio` to `/etc/hosts` | Works, but needs root on every machine that opens the console, and silently breaks for teammates who skip it. |
| Point the endpoint at the host's LAN IP (`172.x.x.x`) | Works from both sides, but the IP is DHCP-assigned and machine-specific — it changes on reboot and is meaningless to anyone else. Not something to ship. |
| Give `bootstrap` `network_mode: host` and use `localhost:9000` | Warehouse creation fails: Lakekeeper validates the endpoint from *its own* bridge-networked container, where `localhost:9000` is itself. Observed as `FileDecompressionError: invalid gzip header` — MinIO's error XML being parsed as a data file. |
| Feed the JVM a private hosts file via `-Djdk.net.hosts.file` | Tried and **rejected.** It works, but `jdk.net.hosts.file` is a JDK *testing* hook, it fixes exactly one process (not Flink/DBeaver/a real cluster/a teammate), and it replaces DNS wholesale for that JVM — which then forced a second workaround (`--jars` instead of `--packages`, because Maven Central stopped resolving). A fix that needs a second fix to prop it up is a smell. It hides the defect instead of repairing it. |

## The fix, and why it's the principled one

In production this problem doesn't exist: the object store has a **real DNS
name** (`s3.company.com`, `minio.prod.internal`) that resolves for cluster
workloads and users' browsers alike. The single-endpoint-for-everyone
requirement is architectural, not a dev-only quirk.

`minio.localhost` reproduces exactly that property locally:

- **Inside Docker** — the `minio.localhost` network alias on the `minio`
  service; Docker's embedded DNS resolves it to the container IP.
- **In the browser** — RFC 6761 reserves the `.localhost` TLD for loopback and
  browsers implement it natively, so it resolves to `127.0.0.1` and hits the
  published port `9000`.

Verified both directions:

```bash
# inside the Docker network
docker run --rm --network spark-lakehouse_lakehouse_net alpine \
  wget -qO- http://minio.localhost:9000/minio/health/live   # 200

# in a real browser, no flags, no hosts entry -> 200
```

### Two supporting settings the preview also needs

Both are already applied; the preview fails without them.

1. **Global CORS on MinIO** — `MINIO_API_CORS_ALLOW_ORIGIN=*` on the `minio`
   service. Once DNS is fixed, the browser *does* need CORS headers. Per-bucket
   CORS is AIStor-only, so global is the only option in community MinIO.

2. **Anonymous read on the bucket** — `mc anonymous set download` in
   `minio/init.sh`. The browser sends **unsigned** GETs (it has no S3
   credentials); MinIO otherwise returns `403 AccessDenied` — which the console
   also reports as "CORS Error". ⚠️ This makes every object world-readable to
   anyone who can reach MinIO's port, **bypassing Lakekeeper/OpenFGA entirely**.
   Fine for a localhost dev stack; never carry it into a real deployment.

### Required one-time host setup (for local PySpark)

**Browsers** implement the `.localhost` rule; **the JVM does not** — it uses the
system resolver, and glibc has no such special case. So host-run
**PySpark/Jupyter** fails with:

```
software.amazon.awssdk.core.exception.SdkClientException:
  Received an UnknownHostException when attempting to interact with a service.
```

You cannot fix this from the client side. Lakekeeper returns the storage
endpoint in **every `LoadTable` response**, and Iceberg gives that
server-provided table config precedence over client catalog properties, so
`spark.sql.catalog.<name>.s3.endpoint` is silently ignored:

```json
// GET /catalog/v1/{prefix}/namespaces/sales/tables/orders  ->  "config"
{ "s3.endpoint": "http://minio.localhost:9000/", ... }
```

Because one endpoint string is handed to every client, that one name has to
resolve everywhere. Add it to your machine's hosts file — once:

```bash
echo "127.0.0.1 minio.localhost" | sudo tee -a /etc/hosts
```

(On Windows, `C:\Windows\System32\drivers\etc\hosts` as Administrator.)

This is not a workaround so much as the local stand-in for what production
does with real DNS: the object store has one hostname that resolves for
in-cluster workloads and end users alike. Nothing needs rebuilding — the
warehouse already stores `minio.localhost`. Verify with:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://minio.localhost:9000/minio/health/live  # 200
```

Trino is unaffected: it runs in-cluster and uses its own
`s3.endpoint=http://minio:9000` from `trino/catalog/lakekeeper.properties`.

## If it breaks again

1. Read the **Network tab**, not the error banner.
   - `ERR_NAME_NOT_RESOLVED` → DNS. Check the alias and the stored endpoint.
   - `403 AccessDenied` → the bucket lost its anonymous-download policy.
   - Missing `Access-Control-Allow-Origin` → genuinely CORS.
2. Check what endpoint the warehouse actually stores — it is baked in at
   creation time, so **changing it requires recreating the warehouse**
   (`docker compose down -v && docker compose up -d`):
   ```bash
   curl -s http://localhost:8181/management/v1/warehouse \
     -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | grep endpoint
   ```
3. A stale browser session shows `401`s on `/management/v1/info` and bounces
   you to the login page. That's unrelated — `down -v` rotates Keycloak's
   signing keys. Log out and back in.
