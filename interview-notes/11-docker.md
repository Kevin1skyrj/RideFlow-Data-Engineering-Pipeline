# 11 — Docker and Docker Compose

## Core concepts

- Dockerfile: instructions for building an image.
- Image: immutable packaged filesystem and metadata.
- Container: running image instance with isolated process/filesystem view.
- Volume/bind mount: persistent or shared data outside a container layer.
- Network: service-to-service connectivity and DNS.
- Health check: readiness/liveness signal used by Compose and operators.
- Compose: declarative multi-container application topology.

## RideFlow usage

Kafka uses a broker and one-shot topic-init container. Airflow uses a custom
image plus API server, scheduler, DAG processor, triggerer, init container, and
PostgreSQL metadata store. Health checks prevent “process started” from being
mistaken for “service ready.”

## Questions and answers

### “Why containerize Kafka and Airflow?”

> They have multiple services and version-sensitive dependencies. Compose makes
> their topology and health checks reproducible and avoids fragile host installs,
> especially on Windows.

### “Image versus container?”

> An image is the immutable template; a container is a running instance with a
> writable layer and runtime configuration. Multiple containers can start from
> one image.

### “Bind mount versus volume?”

> A bind mount maps an explicit host path and is convenient for code/data
> sharing. A named volume is managed by Docker and is convenient for service
> state. Both require backup and permission planning in production.

### “Why not Kubernetes?”

> Compose satisfies local reproducibility. Kubernetes becomes justified for
> multi-host scheduling, self-healing, rollout, and service-scale requirements;
> adding it here would not improve the data guarantees being tested.
