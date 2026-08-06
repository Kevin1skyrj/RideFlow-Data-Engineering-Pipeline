# RideFlow Airflow image.
#
# This is the FIRST custom image in the project, and the reason is specific:
# Kafka and Postgres exist as published images, so building our own would
# reimplement something that already exists. Airflow does not ship with dbt, so
# there is a genuine gap to fill.
#
# ── Why dbt lives in its own virtualenv ──────────────────────────────────────
# Airflow pins dozens of shared transitive dependencies (Jinja2, click, pydantic,
# packaging, ...) and dbt pins its own. Installing dbt into Airflow's environment
# means one of them loses, and the failure surfaces later as a mysterious
# runtime error rather than an install conflict.
#
# An isolated venv sidesteps the negotiation entirely: Airflow keeps its
# dependency tree, dbt keeps its own, and the DAG invokes dbt as a subprocess
# through an explicit interpreter path. This is why the DAG uses BashOperator
# rather than a Python dbt library call - the two cannot share a process.

FROM apache/airflow:3.3.0-python3.12

USER root

# git: dbt deps fetches packages from git remotes.
# procps: `ps` is used by Airflow health checks and by dbt's process handling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Versions pinned to match requirements.txt exactly. A mismatch between the
# host-built warehouse and the container-built one would produce models that
# compile differently in each place - the kind of drift that only shows up as an
# unexplained difference in output.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.0 \
        dbt-duckdb==1.10.1 \
        duckdb==1.5.5 \
        pyarrow==25.0.0

# Providers the DAG needs. Installed into Airflow's own environment, where they
# belong - these are Airflow extensions, not dbt dependencies.
RUN pip install --no-cache-dir \
        "apache-airflow-providers-standard" \
        "duckdb==1.5.5" \
        "pyarrow==25.0.0"

ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Fail fast at build time if either environment is broken, rather than
# discovering it when the first DAG run fails.
RUN /home/airflow/dbt-venv/bin/dbt --version \
    && python -c "import airflow; print('airflow', airflow.__version__)"
