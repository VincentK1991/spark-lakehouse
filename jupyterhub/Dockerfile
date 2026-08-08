# The JupyterHub Hub itself. Runs as root (default): DockerSpawner needs
# read/write access to the mounted /var/run/docker.sock to launch per-user
# singleuser containers, which is root-equivalent host access regardless of
# the container's own user -- see docs/team_jupyter_hub.ipynb "Security
# tradeoffs". Matches the ghcr.io/astral-sh/uv base this repo already uses
# for the bootstrap/reconciler one-shot jobs in docker-compose.yml.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# JupyterHub's default proxy (ConfigurableHTTPProxy) shells out to the
# `configurable-http-proxy` npm package -- not something uv/pip can install.
RUN apt-get update \
 && apt-get install -y --no-install-recommends nodejs npm \
 && npm install -g configurable-http-proxy \
 && apt-get purge -y npm \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /srv/jupyterhub
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
RUN uv sync --frozen --no-dev

COPY jupyterhub_config.py .

ENV PATH="/opt/venv/bin:${PATH}"
EXPOSE 8000
CMD ["jupyterhub", "-f", "/srv/jupyterhub/jupyterhub_config.py"]
