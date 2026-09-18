#!/usr/bin/env python3
import json
import logging
import os
import sys
from typing import Optional

import requests
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource


# The goal of this tool is to act as a forwarding system between Particle
# events and an OTel backend. Events from Particle are logged on
# `tool.particle`; this tool's own logs go to `tool.internal`.

TOOL_LOGGER_NAME = "tool.internal"
PARTICLE_LOGGER_NAME = "tool.particle"
DEFAULT_OTLP_ENDPOINT = "http://127.0.0.1:4317"
SERVICE_NAME = "particle-event-forwarder"
CONSOLE_ENV = "TOOL_INTERNAL_CONSOLE"
CODE_LOCATION_ATTRS = (
    "code.file.path",
    "code.function.name",
    "code.line.number",
)


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


class ParticleLoggingHandler(LoggingHandler):
    """OTel handler that omits source-location attributes on forwarded events."""

    @staticmethod
    def _get_attributes(record: logging.LogRecord):
        attributes = LoggingHandler._get_attributes(record)
        for key in CODE_LOCATION_ATTRS:
            attributes.pop(key, None)
        return attributes


def configure_logging(log_record_processor=None, endpoint=None, console=None):
    """Set up OTel + stdlib loggers. No exporters are started at import time.

    Pass `log_record_processor` to inject a test processor and skip OTLP.
    Set `console=True` (or TOOL_INTERNAL_CONSOLE=1) to also print tool.internal
    logs to stderr. Particle events are never echoed to the terminal.
    """
    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "service.instance.id": os.uname().nodename,
        }
    )
    logger_provider = LoggerProvider(resource=resource)

    if log_record_processor is None:
        otlp_endpoint = endpoint or os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT
        )
        log_record_processor = BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=otlp_endpoint, insecure=True)
        )
        set_logger_provider(logger_provider)
    logger_provider.add_log_record_processor(log_record_processor)

    tool_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    particle_handler = ParticleLoggingHandler(
        level=logging.NOTSET, logger_provider=logger_provider
    )
    tool_logger = logging.getLogger(TOOL_LOGGER_NAME)
    particle_logger = logging.getLogger(PARTICLE_LOGGER_NAME)
    for lg, handler in (
        (tool_logger, tool_handler),
        (particle_logger, particle_handler),
    ):
        lg.handlers.clear()
        lg.setLevel(logging.INFO)
        lg.addHandler(handler)
        lg.propagate = False

    if console is None:
        console = _env_flag(CONSOLE_ENV)
    if console:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        tool_logger.addHandler(stream_handler)

    return logger_provider, tool_logger, particle_logger


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        logging.getLogger(TOOL_LOGGER_NAME).error(
            "Missing required environment variable: %s", name
        )
        sys.exit(1)
    return value


class ParticleEventParser:
    def __init__(self):
        self.bbuffer = b""
        self.current_event = ""
        self.parsed_events = []

    def _logger(self):
        return logging.getLogger(TOOL_LOGGER_NAME)

    def _reset_event(self):
        self.current_event = ""

    def feed(self, raw_chunk):
        """Feed binary data to the event parser"""
        # Example raw chunk (might come split in more than one chunk)
        # event: HttpRequestStatistics
        # data: {"data":"{\"testType\":\"standard\",\"url\":\"https://doorserviceapidsa.blob.core.windows.net/reader-firmware-updates/gen5_2_5_0_2.dat\",\"responseCode\":-3,\"contentLength\":-1,\"requestQueueTime\":620077284,\"receivedHeaderTime\":0,\"requestCompletionTime\":620091682}","ttl":60,"published_at":"2026-07-28T16:53:05.616Z","coreid":"e00fce682e79a004f1e69a2b","userid":"5d447a8c6c0ad300017dd72e","version":91,"public":false,"productID":13961}

        if not raw_chunk:
            return

        self.bbuffer += raw_chunk

        while (nlc := self.bbuffer.find(b"\n")) != -1:
            bline = self.bbuffer[:nlc].rstrip(b"\r")
            self.bbuffer = self.bbuffer[nlc + 1 :]

            if bline == b"" or bline.startswith(b":"):
                continue

            if bline.startswith(b"event:"):
                name = bline[len(b"event:") :].strip().decode("utf-8", errors="replace")
                if not name:
                    self._logger().error("Ignoring event line with empty name")
                    continue
                if self.current_event:
                    self._logger().error(
                        "New event %r started before data for %r; dropping incomplete event",
                        name,
                        self.current_event,
                    )
                self.current_event = name
            elif bline.startswith(b"data:"):
                if not self.current_event:
                    self._logger().error(
                        "Data line received with no current event; dropping line"
                    )
                    continue
                event_data_raw = bline[len(b"data:") :].strip().decode("utf-8", errors="replace")
                try:
                    event_data = json.loads(event_data_raw)
                except json.JSONDecodeError as exc:
                    self._logger().error(
                        "Invalid JSON in event %r: %s", self.current_event, exc
                    )
                    self._reset_event()
                    continue
                if not isinstance(event_data, dict):
                    self._logger().error(
                        "Event %r data is not a JSON object; dropping",
                        self.current_event,
                    )
                    self._reset_event()
                    continue
                event_data["name"] = self.current_event
                self.parsed_events.append(event_data)
                self._reset_event()
            else:
                self._logger().error(
                    "Unexpected SSE line while parsing (event=%r): %r",
                    self.current_event,
                    bline[:200],
                )
                self._reset_event()

    def pull(self) -> Optional[dict]:
        if len(self.parsed_events) == 0:
            return None
        return self.parsed_events.pop(0)


def _forward_event(particle_logger, ev: dict) -> None:
    extra = {
        "publish_time": ev.get("published_at", ""),
        "device_id": ev.get("coreid", ""),
        "device_version": ev.get("version", ""),
        "event_name": ev.get("name", ""),
    }
    particle_logger.info(ev.get("data", ""), extra=extra)


def _run(tool_logger, particle_logger) -> None:
    product_id = require_env("PARTICLE_PRODUCT_ID")
    auth_token = require_env("PARTICLE_AUTH_TOKEN")
    streaming_endpoint = f"https://api.particle.io/v1/products/{product_id}/events/"
    headers = {"Authorization": "Bearer " + auth_token}
    pep = ParticleEventParser()

    try:
        response = requests.get(streaming_endpoint, headers=headers, stream=True)
    except requests.RequestException as exc:
        tool_logger.error("Particle stream request failed: %s", exc)
        sys.exit(1)

    with response as r:
        if not r.ok:
            body = r.content[:500].decode("utf-8", errors="replace")
            tool_logger.error(
                "Particle stream request failed: HTTP %s %s body=%s",
                r.status_code,
                r.reason,
                body,
            )
            sys.exit(1)

        tool_logger.info("Connection established, parsing events...")

        for raw_chunk in r.iter_content(chunk_size=8192):
            # Received raw_chunk looks like this:
            #   event: HttpRequestStatistics
            #   data: {"data":"...","ttl":60,"published_at":"...","coreid":"...","userid":"...","version":91,"public":false,"productID":13961}
            #
            # Note that 'data' is not necessarily JSON formatted

            pep.feed(raw_chunk)
            while (ev := pep.pull()) is not None:
                _forward_event(particle_logger, ev)


def main() -> None:
    logger_provider, tool_logger, particle_logger = configure_logging()
    try:
        _run(tool_logger, particle_logger)
    except KeyboardInterrupt:
        tool_logger.info("Interrupted, shutting down")
    finally:
        logger_provider.shutdown()


if __name__ == "__main__":
    main()
