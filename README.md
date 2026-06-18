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

Docker Compose groups all the Docker container so they all run at the same time and
see each other as if on the same network (they resolve each other's hostname from their
`docker-compose.yml` name).
* Start with `docker compose up`
* Stop with `docker compose down`

## Settings documentation for docker-compose improvements

Most of the existing settings come from talking to ChatGPT, Gemini, online documentation
and examples.
I later found additional examples/sources to help with this (first link):
- **Examples of docker compose (for multiple setups)**: https://github.com/grafana/tempo/tree/main/example/docker-compose
- Enable Tempo HTTP streaming (required for TraceQL): https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-queries/configure-traceql-metrics/#activate-and-configure-the-local-blocks-processor
- Enable Tempo TraceQL metrics: https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-queries/configure-traceql-metrics/#activate-and-configure-the-local-blocks-processor
- Grafana Data sources: https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources
- How to configure each data source (`grafana-datasources.yml`):
  - https://grafana.com/docs/grafana/latest/datasources/prometheus/configure/#provision-the-prometheus-data-source
  - https://grafana.com/docs/grafana/latest/datasources/loki/#provisioning-examples
  - https://grafana.com/docs/grafana/latest/datasources/tempo/configure-tempo-data-source/#example-file
- Configuration details for `config.alloy`: https://grafana.com/docs/alloy/latest/reference/components/otelcol/otelcol.exporter.otlp/

# Python Telemetry (with automatic & manual instrumentation)

# Installation

Steps:
```bash
# Configure Python virtual environment
python -m venv .venv
.venv/bin/activate

# Installation basics
pip3 install -r requirements.txt
opentelemetry-bootstrap -a install
```

## Use Flask example (telemetry sent to console)

```python
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
export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
export OTEL_METRIC_EXPORT_INTERVAL=1000
opentelemetry-instrument \
    --metrics_exporter otlp \
    --logs_exporter otlp \
    --traces_exporter otlp \
    --service_name dice-server \
    flask run -p 8080
```
