"""JupyterHub config: Keycloak OIDC SSO login + DockerSpawner, with a
Small/Medium/Large spawn-time choice that caps how many of the shared
Spark standalone cluster's 8 cores a session's driver may claim.

NOTE: DockerSpawner (unlike KubeSpawner) has no built-in `profile_list`
trait -- that convenience wrapper is KubeSpawner-specific, not part of the
base `Spawner` class. This config uses the spawner-agnostic
`options_form`/`options_from_form`/`pre_spawn_hook` mechanism instead,
which is what `profile_list` itself is built on. (Earlier design notes in
docs/team_jupyter_hub.ipynb assumed `profile_list` would work here
directly; this file is the corrected version -- see that notebook's
"Executor sizing model" section for the write-up.)
"""

# ruff: noqa: F821
# pyright: reportUndefinedVariable=false
# `c` and `get_config` are injected into this file's globals by JupyterHub's
# config loader (`jupyterhub -f jupyterhub_config.py`) before exec'ing it --
# not real names, hence the blanket undefined-variable suppression above,
# scoped to this one exec'd config file only.

import os
import sys

c = get_config()

# ─────────────────────────── Authentication: Keycloak OIDC ───────────────────────────
c.JupyterHub.authenticator_class = "generic-oauth"

c.GenericOAuthenticator.client_id = os.environ["JUPYTERHUB_CLIENT_ID"]
c.GenericOAuthenticator.client_secret = os.environ["JUPYTERHUB_CLIENT_SECRET"]
c.GenericOAuthenticator.authorize_url = os.environ["OAUTH2_AUTHORIZE_URL"]
c.GenericOAuthenticator.token_url = os.environ["OAUTH2_TOKEN_URL"]
c.GenericOAuthenticator.userdata_url = os.environ["OAUTH2_USERDATA_URL"]
c.GenericOAuthenticator.login_service = "Keycloak"
c.GenericOAuthenticator.username_claim = "preferred_username"
c.GenericOAuthenticator.scope = ["openid", "profile", "email"]

# Any Keycloak user in the `lakehouse` realm may log in to the Hub itself --
# the same trust boundary Lakekeeper's own console already uses (see
# docs/initial_stack.md's credentials table: "logs in via Keycloak -- use
# any user/admin above"). Actual data access is enforced downstream by
# Lakekeeper/OpenFGA via each notebook's own separate Keycloak login, per
# reconciler/grants.yaml -- unchanged by this file.
c.Authenticator.allow_all = True

# ─────────────────────────── Spawner: DockerSpawner ───────────────────────────
c.JupyterHub.spawner_class = "docker"

c.DockerSpawner.image = os.environ["SPARK_NOTEBOOK_IMAGE"]
c.DockerSpawner.network_name = os.environ["DOCKER_NETWORK_NAME"]
c.DockerSpawner.remove = True
# The Hub and every spawned singleuser container are siblings on the same
# Docker network, not reachable via published host ports.
c.DockerSpawner.use_internal_ip = True

c.JupyterHub.hub_ip = "0.0.0.0"
# The Hub container's own hostname on lakehouse_net (== the compose service
# name "jupyterhub") -- how spawned singleuser containers reach the Hub API.
c.JupyterHub.hub_connect_ip = "jupyterhub"

# ─────────────────────────── Spawn-time cluster-size choice ───────────────────────────
_CORES_CHOICES = {"2": "Small (2 cores)", "4": "Medium (4 cores)", "8": "Large (8 cores)"}

c.Spawner.options_form = (
    "<label for='cores_max'>Spark cluster size for this session</label>"
    "<select name='cores_max' id='cores_max'>"
    + "".join(
        f"<option value='{value}'{' selected' if value == '2' else ''}>{label}</option>"
        for value, label in _CORES_CHOICES.items()
    )
    + "</select>"
)


def _options_from_form(formdata):
    cores_max = formdata.get("cores_max", ["2"])[0]
    if cores_max not in _CORES_CHOICES:
        cores_max = "2"
    return {"cores_max": cores_max}


c.Spawner.options_from_form = _options_from_form


def _pre_spawn_hook(spawner):
    cores_max = spawner.user_options.get("cores_max", "2")
    spawner.environment["SPARK_CORES_MAX"] = cores_max
    spawner.log.info("Spawning %s with SPARK_CORES_MAX=%s", spawner.user.name, cores_max)


c.Spawner.pre_spawn_hook = _pre_spawn_hook

# ─────────────────────────── Session hygiene: idle culler ───────────────────────────
# Reaps servers idle past the timeout so their Spark drivers release cores
# back to the shared pool for everyone else -- see "Session hygiene" in
# docs/team_jupyter_hub.ipynb.
c.JupyterHub.services = [
    {
        "name": "idle-culler",
        "admin": True,
        "command": [
            sys.executable,
            "-m",
            "jupyterhub_idle_culler",
            "--timeout=3600",
        ],
    }
]

c.JupyterHub.db_url = "sqlite:////srv/jupyterhub/state/jupyterhub.sqlite"
