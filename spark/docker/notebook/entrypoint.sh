#!/bin/bash
# Puts the baked-in $SPARK_HOME/python (and its bundled py4j) on PYTHONPATH,
# plus /opt/notebook-app (where query_orders.py lives -- the notebook's own
# working directory is /home/spark, not /opt/notebook-app, so without this
# `from query_orders import ...` fails with ModuleNotFoundError), before
# handing off to whatever command JupyterHub's DockerSpawner passes (normally
# jupyterhub-singleuser). The Spark path is computed at container start via a
# glob rather than hardcoded, so it survives a SPARK_IMAGE_TAG bump without
# needing an edit here too.
set -eo pipefail

py4j_zip="$(ls "${SPARK_HOME}"/python/lib/py4j-*-src.zip)"
export PYTHONPATH="/opt/notebook-app:${SPARK_HOME}/python:${py4j_zip}:${PYTHONPATH:-}"

exec "$@"
