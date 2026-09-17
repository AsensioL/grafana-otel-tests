# Grafana OTel tests

```
run.sh                  # start (or pass through) docker compose
python/                 # Flask and OTLP example apps
docker/
  compose.yaml          # Grafana, Prometheus, Loki, Tempo, Alloy
  config/               # per-service container config
    alloy/
    grafana/provisioning/datasources/
    loki/
    prometheus/
    tempo/
```

# Docker Compose

## Docker Compose Installation

### MacOS

```bash
# softwareupdate --install-rosetta # may be necessary
brew install docker
brew install colima
colima start # Re run after a computer reboot (it is a service)
brew install docker-compose
# IMPORTANT: Follow instructions after running brew install docker-compose to enable the feature

### TEST
docker run --rm hello-world
```

## Docker Compose use

Docker Compose groups all the Docker containers so they all run at the same time and
see each other as if on the same network (they resolve each other's hostname from their
service name in `docker/compose.yaml`).

From the repository root:

* Start with
  * `docker compose -f docker/compose.yaml up`
  * or `./run.sh`
* Stop (without deleting data) with
  * `docker compose -f docker/compose.yaml down`
  * or `./run.sh down`

## Docker Data Retention (volumes)

Grafana dashboards and Prometheus/Loki/Tempo data live in named Docker volumes
(`grafana-data`, `prometheus-data`, `loki-data`, `tempo-data`).

### Managing retained data (keep/delete)

Stopping docker compose with `./run.sh down` leaves the volumes in place.
To delete those volumes (irreversible: metrics, logs, traces, and Grafana state):

```bash
./run.sh down -v
# or:
docker compose -f docker/compose.yaml --env-file docker/.env down -v
```

List leftover volumes with `docker volume ls` and remove one by name with
`docker volume rm grafana-otel-tests_grafana-data` (project prefix plus the
volume name).

## Settings documentation for docker-compose improvements

Most of the existing settings come from talking to ChatGPT, Gemini, online documentation
and examples.
I later found additional examples/sources to help with this (first link):
- **Examples of docker compose (for multiple setups)**: https://github.com/grafana/tempo/tree/main/example/docker-compose
- Enable Tempo HTTP streaming (required for TraceQL): https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-queries/configure-traceql-metrics/#activate-and-configure-the-local-blocks-processor
- Enable Tempo TraceQL metrics: https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-queries/configure-traceql-metrics/#activate-and-configure-the-local-blocks-processor
- Grafana Data sources: https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources
- How to configure each data source (`docker/config/grafana/provisioning/datasources/datasources.yml`):
  - https://grafana.com/docs/grafana/latest/datasources/prometheus/configure/#provision-the-prometheus-data-source
  - https://grafana.com/docs/grafana/latest/datasources/loki/#provisioning-examples
  - https://grafana.com/docs/grafana/latest/datasources/tempo/configure-tempo-data-source/#example-file
-

# Python Telemetry (with automatic & manual instrumentation)

# Installation

Steps:
```bash
# Configure Python virtual environment
python -m venv .venv
.venv/bin/activate

# Installation basics
pip3 install -r python/requirements.txt
opentelemetry-bootstrap -a install
```

## Use Flask example (telemetry sent to console)

```python
cd python
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
opentelemetry-instrument \
    --metrics_exporter console \
    --logs_exporter console \
    --traces_exporter console \
    --service_name dice-server \
    flask run -p 8080
```

## Use Flask example (telemetry sent to OTLP)

```python
cd python
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
opentelemetry-instrument \
    --metrics_exporter otlp \
    --logs_exporter otlp \
    --logs_exporter otlp \
    --service_name dice-server \
    flask run -p 8080
```
