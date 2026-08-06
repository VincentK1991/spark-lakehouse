# Restarting Keycloak silently broke every grant in the stack

**TL;DR — Keycloak assigns a fresh random `id` to every user and
service-account on each realm import, unless the realm file pins one.**
`keycloak/realm-lakehouse.json` didn't, so restarting the Keycloak container
rotated everyone's underlying identity — including the identity that
originally bootstrapped Lakekeeper as its own technical admin. Lakekeeper and
OpenFGA store every grant keyed to that `id`, not to the email/`client_id`, so
the whole permission model was instantly orphaned: even the `reconciler`
service account could no longer list roles. Recovery required wiping
Lakekeeper's and OpenFGA's Postgres volumes and re-bootstrapping from
scratch. The fix is to pin explicit `id` fields for every identity in the
realm file, so re-imports are actually idempotent.

## What happened

Adding a new Keycloak service-account client (`table-definitions`, for
[`table_definitions/apply.py`](../table_definitions/apply.py)) to
`keycloak/realm-lakehouse.json` only takes effect on the next realm import —
Keycloak reads that file once, at container start (`start-dev
--import-realm`). So the client was added to the JSON, then the container was
recreated to pick it up (`docker compose up -d --force-recreate keycloak`).

The very next `docker compose run --rm reconciler` failed immediately:

```
HTTP error: 403 {"error":{"message":"Project action `can_list_roles`
forbidden on project `00000000-0000-0000-0000-000000000000`", ...}}
```

`reconciler` had run successfully minutes earlier. Nothing about its
permissions had been touched. The only thing that changed was that Keycloak
restarted.

## Why it happened

Lakekeeper (via OpenFGA) authorizes every request against the token's `sub`
claim — Keycloak's internal `id` for that user or service-account, a UUID.
Nothing in `keycloak/realm-lakehouse.json`'s `users` entries specified an
`id`, so Keycloak generates one at random on every import. Same email, same
`client_id`, same password — different underlying identity every time the
realm is (re)imported.

That alone would just mean "re-run the reconciler to re-add memberships,"
which is exactly what its docstring already expects
(`reconcile.py`: *"Re-run after editing this file"*). But one more thing was
keyed to the old UUID: **Lakekeeper's own bootstrap admin.**
[`lakekeeper/bootstrap.py`](../lakekeeper/bootstrap.py) authenticates as the
`reconciler` client and calls `POST /v1/bootstrap` — "first caller becomes
the technical admin" (its own docstring). That admin grant is a Lakekeeper/
OpenFGA-side permission tied to whatever UUID `reconciler`'s service account
had *at that moment*. `ensure_bootstrapped()` checks a persisted flag and
no-ops if Lakekeeper is already bootstrapped, so it never re-grants that
admin identity on a later run.

Once Keycloak restarted, `reconciler`'s service account got a new UUID.
Lakekeeper was still "bootstrapped" (that flag lives in `postgres-lakekeeper`,
untouched by a Keycloak restart) — but the identity that flag had granted
admin rights to no longer existed. No one, including `reconciler` itself, was
recognized as a project admin anymore. Every other grant (alice's, carol's,
`translation-pipeline`'s, ...) was equally orphaned for the same reason —
`reconciler` simply hit the wall first because it needs project-admin-level
API calls (`/v1/role`), where a plain per-table `select` grant would have
failed just as silently the next time that user queried a table.

## Why "just restart Keycloak" seemed safe

Keycloak's own state isn't persisted in this stack — there's no volume for
it in `docker-compose.yml`, by design: `start-dev --import-realm` re-seeds
identical realm state every boot, which is what makes editing the realm file
and restarting a normal, expected workflow here. That ephemerality is fine
*by itself*. It only becomes a problem when a **different, persisted**
system (Lakekeeper + OpenFGA, backed by their own Postgres volumes) has
already captured a snapshot of an identity that was supposed to be stable
but wasn't pinned. The bug wasn't "Keycloak resets" — that's intentional —
it was "nothing guaranteed the reset produced the *same* identities."

## The fix

Pin an explicit `id` for every entry in `keycloak/realm-lakehouse.json`'s
`users` array — both the six mock humans / real admin, and every
service-account's own `users` entry (Keycloak auto-creates
`service-account-<client_id>` users for `serviceAccountsEnabled: true`
clients, but only pins their `id` if the JSON says so):

```json
{
  "id": "169f6a4a-fdd9-4cd8-a2fd-6076f55a51db",
  "username": "alice",
  "email": "alice@company.com",
  ...
}
```

Three of the four service-account clients (`trino`, `translation-pipeline`,
`table-definitions`) previously had no explicit `users` entry at all — their
service-account user was created implicitly, with a random `id`, on first
token request. They now have one, purely to pin the `id`.

With every `id` fixed, `docker compose up -d --force-recreate keycloak`
reimports the *same* identities every time, and `docker compose run --rm
reconciler` alone is sufficient to restore every grant — as originally
documented.

## Recovering from this specific incident

Pinning IDs going forward doesn't fix identities that already drifted — the
UUID Lakekeeper's bootstrap-admin grant pointed to was gone, and there is no
API to reassign it once the identity holding it no longer exists. Recovery
was a full reset of the two Postgres-backed services that store
identity-keyed state, leaving MinIO (object data) and `uv_cache` untouched:

```bash
docker compose down
docker volume rm spark-lakehouse_postgres_lakekeeper_data \
                  spark-lakehouse_postgres_openfga_data
docker compose up -d          # re-runs bootstrap automatically
docker compose run --rm reconciler
```

**Cost:** any catalog object that existed only in Lakekeeper's Postgres and
wasn't part of `bootstrap.py`'s seed list was lost — its underlying Parquet/
Iceberg metadata files stay in MinIO as orphaned, unregistered objects, but
the table itself stops resolving. Concretely, an ad hoc `sales.orders_v2`
table (created earlier straight from a notebook, before
`table_definitions/apply.py` existed) had to be recreated; a genuinely
important table would have needed restoring from wherever its DDL/backfill
logic lived outside this stack, which for anything other than a throwaway
dev demo is the real argument for defining tables as reviewable contracts
(see `table_definitions/`) rather than only as notebook side effects.

## Lesson learned

When one system is deliberately ephemeral/reseedable (Keycloak here) and a
second system persists state that references identities from the first
(Lakekeeper/OpenFGA's grants, keyed by Keycloak's internal `sub`), **the join
key between them has to be pinned, not auto-generated** — "idempotent
re-import" is only true if nothing downstream ever captured a snapshot of the
generated value. A `client_id` or an email being stable is not the same
guarantee as the underlying `id` being stable, and OAuth2 tokens carry the
latter.

Concretely, for this stack: any future edit to
`keycloak/realm-lakehouse.json` that requires restarting Keycloak (adding a
client, changing a client's config) is safe now that every identity's `id`
is pinned. If a new identity is ever added to the realm file *without* an
explicit `id`, this will happen again, silently, the next time the container
restarts for any reason — not just this file's future edits.

## If it breaks again

1. Symptom: `docker compose run --rm reconciler` (or any Spark login) fails
   with `403 ... forbidden`, and nothing about grants.yaml/groups.yaml
   changed. Check whether Keycloak was recently restarted or recreated.
2. Check every entry in `keycloak/realm-lakehouse.json`'s `users` array has
   an `id`. If a new client/user was added without one, add it (any valid
   UUID; `python3 -c "import uuid; print(uuid.uuid4())"`) *before* the next
   restart — that prevents the next occurrence, but does not fix one already
   in progress.
3. If Lakekeeper's own bootstrap-admin identity is already orphaned (the
   symptom above, with `reconciler` itself failing on `/v1/role`), there is
   no lighter fix than the full reset above — `ensure_bootstrapped()` won't
   re-grant admin to a new identity once its persisted flag says
   "bootstrapped", so nothing short of resetting that flag (via a DB wipe)
   restores it.
