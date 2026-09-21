#!/usr/bin/env python3
import json
import logging
import os
import sys
import threading
import time
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
PARTICLE_API_BASE = "https://api.particle.io/v1"
DEVICES_PER_PAGE = 20000
DEVICE_REFRESH_INTERVAL_S = 24 * 60 * 60
STREAM_CONNECT_TIMEOUT_S = 30.0
# Read timeout also covers a half-open socket after the laptop sleeps.
STREAM_READ_TIMEOUT_S = 120.0
STREAM_RETRY_INITIAL_S = 1.0
STREAM_RETRY_MAX_S = 60.0
DEVICE_FIELD_MISSING = "NA!"
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


class _PublishedDeviceList:
    """Immutable generation + id-indexed devices; swapped as a single pointer."""

    __slots__ = ("generation", "devices")

    def __init__(self, generation: int, devices: dict):
        self.generation = generation
        self.devices = devices


def _index_devices(devices: list) -> dict:
    indexed = {}
    for device in devices:
        device_id = device.get("id")
        if device_id is None:
            continue
        indexed[device_id] = device
    return indexed


class DeviceList:
    """Product devices shared between the refresh thread and the main thread.

    The main thread checks `has_newer()` / `generation()` with no lock. A
    successful `replace()` publishes a new id-keyed dict and bumps the
    generation; only then does `snapshot_if_newer()` take the lock to copy.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._published = _PublishedDeviceList(0, {})

    def replace(self, devices: list) -> None:
        by_id = _index_devices(devices)
        with self._lock:
            next_gen = self._published.generation + 1
            self._published = _PublishedDeviceList(next_gen, by_id)

    def generation(self) -> int:
        return self._published.generation

    def has_newer(self, seen_generation: int) -> bool:
        return self._published.generation != seen_generation

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._published.devices)

    def snapshot_if_newer(self, seen_generation: int) -> tuple[int, Optional[dict]]:
        published = self._published
        if published.generation == seen_generation:
            return seen_generation, None
        with self._lock:
            published = self._published
            if published.generation == seen_generation:
                return seen_generation, None
            return published.generation, dict(published.devices)


def fetch_product_devices(product_id: str, headers: dict) -> list:
    """Fetch the product device list, paging when total_records exceeds perPage."""
    logger = logging.getLogger(TOOL_LOGGER_NAME)
    devices = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        url = (
            f"{PARTICLE_API_BASE}/products/{product_id}/devices"
            f"?perPage={DEVICES_PER_PAGE}&page={page}"
        )
        response = requests.get(url, headers=headers)
        if not response.ok:
            body = response.content[:500].decode("utf-8", errors="replace")
            logger.error(
                "Product device list request failed: HTTP %s %s body=%s",
                response.status_code,
                response.reason,
                body,
            )
            response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Product device list response is not a JSON object")

        page_devices = payload.get("devices") or []
        if not isinstance(page_devices, list):
            raise ValueError("Product device list 'devices' field is not an array")
        devices.extend(page_devices)

        if page == 1:
            meta = payload.get("meta") or {}
            if not isinstance(meta, dict):
                meta = {}
            total_records = meta.get("total_records") or 0
            # meta looks like {'total_pages': 1, 'total_records': 11148}
            if total_records > DEVICES_PER_PAGE:
                reported_pages = meta.get("total_pages") or 0
                needed_pages = (
                    total_records + DEVICES_PER_PAGE - 1
                ) // DEVICES_PER_PAGE
                total_pages = max(int(reported_pages), needed_pages)

        page += 1

    logger.info("Fetched %s product devices", len(devices))
    return devices


def _refresh_device_list(
    product_id: str, headers: dict, device_list: DeviceList
) -> None:
    devices = fetch_product_devices(product_id, headers)
    device_list.replace(devices)


def _device_refresh_loop(
    product_id: str,
    headers: dict,
    stop_event: threading.Event,
    device_list: DeviceList,
) -> None:
    logger = logging.getLogger(TOOL_LOGGER_NAME)
    # Initial fetch runs on the main thread before the event stream starts.
    while not stop_event.wait(DEVICE_REFRESH_INTERVAL_S):
        try:
            _refresh_device_list(product_id, headers, device_list)
        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            logger.error("Product device list request failed: %s", exc)


def _device_name_and_groups(devices: dict, coreid) -> tuple[str, str]:
    device = devices.get(coreid) if coreid else None
    if not isinstance(device, dict):
        return DEVICE_FIELD_MISSING, DEVICE_FIELD_MISSING

    name = device.get("name")
    if not isinstance(name, str):
        name = DEVICE_FIELD_MISSING

    groups = device.get("groups")
    if not isinstance(groups, list):
        groups_text = DEVICE_FIELD_MISSING
    else:
        groups_text = ",".join(str(group) for group in groups)

    return name, groups_text


def _next_stream_retry_delay(current: float) -> float:
    return min(current * 2, STREAM_RETRY_MAX_S)


def _iter_stream_events(
    response,
    pep: ParticleEventParser,
    device_list: DeviceList,
    particle_logger,
    seen_generation: int,
    devices: dict,
) -> tuple[int, dict, bool]:
    """Consume one SSE response. Returns (generation, devices, got_data)."""
    got_data = False
    for raw_chunk in response.iter_content(chunk_size=8192):
        # Received raw_chunk looks like this:
        #   event: HttpRequestStatistics
        #   data: {"data":"...","ttl":60,"published_at":"...","coreid":"...","userid":"...","version":91,"public":false,"productID":13961}
        #
        # Note that 'data' is not necessarily JSON formatted
        got_data = True

        if device_list.has_newer(seen_generation):
            seen_generation, updated = device_list.snapshot_if_newer(seen_generation)
            if updated is not None:
                devices = updated

        pep.feed(raw_chunk)
        while (ev := pep.pull()) is not None:
            _forward_event(particle_logger, ev, devices)
    return seen_generation, devices, got_data


def _forward_event(particle_logger, ev: dict, devices: dict) -> None:
    coreid = ev.get("coreid", "")
    device_name, device_groups = _device_name_and_groups(devices, coreid)
    extra = {
        "publish_time": ev.get("published_at", ""),
        "device_id": coreid,
        "device_version": ev.get("version", ""),
        "event_name": ev.get("name", ""),
        "device_name": device_name,
        "device_groups": device_groups,
    }
    particle_logger.info(ev.get("data", ""), extra=extra)


def _run(tool_logger, particle_logger) -> None:
    product_id = require_env("PARTICLE_PRODUCT_ID")
    auth_token = require_env("PARTICLE_AUTH_TOKEN")
    streaming_endpoint = f"https://api.particle.io/v1/products/{product_id}/events/"
    headers = {"Authorization": "Bearer " + auth_token}
    device_list = DeviceList()
    try:
        _refresh_device_list(product_id, headers, device_list)
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        tool_logger.error("Product device list request failed: %s", exc)
        sys.exit(1)

    stop_refresh = threading.Event()
    refresh_thread = threading.Thread(
        target=_device_refresh_loop,
        args=(product_id, headers, stop_refresh, device_list),
        name="particle-device-refresh",
        daemon=True,
    )
    refresh_thread.start()

    retry_s = STREAM_RETRY_INITIAL_S
    seen_generation = 0
    devices = {}
    seen_generation, snapshot = device_list.snapshot_if_newer(seen_generation)
    if snapshot is not None:
        devices = snapshot

    try:
        while True:
            pep = ParticleEventParser()
            try:
                response = requests.get(
                    streaming_endpoint,
                    headers=headers,
                    stream=True,
                    timeout=(STREAM_CONNECT_TIMEOUT_S, STREAM_READ_TIMEOUT_S),
                )
            except requests.RequestException as exc:
                tool_logger.error(
                    "Particle stream request failed: %s; retrying in %.0fs",
                    exc,
                    retry_s,
                )
                time.sleep(retry_s)
                retry_s = _next_stream_retry_delay(retry_s)
                continue

            with response as r:
                if not r.ok:
                    body = r.content[:500].decode("utf-8", errors="replace")
                    tool_logger.error(
                        "Particle stream request failed: HTTP %s %s body=%s",
                        r.status_code,
                        r.reason,
                        body,
                    )
                    if r.status_code in (401, 403):
                        sys.exit(1)
                    time.sleep(retry_s)
                    retry_s = _next_stream_retry_delay(retry_s)
                    continue

                tool_logger.info("Connection established, parsing events...")
                try:
                    seen_generation, devices, got_data = _iter_stream_events(
                        r,
                        pep,
                        device_list,
                        particle_logger,
                        seen_generation,
                        devices,
                    )
                except requests.RequestException as exc:
                    tool_logger.warning(
                        "Particle stream disconnected: %s; reconnecting in %.0fs",
                        exc,
                        retry_s,
                    )
                    time.sleep(retry_s)
                    retry_s = _next_stream_retry_delay(retry_s)
                    continue

            if got_data:
                retry_s = STREAM_RETRY_INITIAL_S
            tool_logger.warning(
                "Particle event stream ended; reconnecting in %.0fs", retry_s
            )
            time.sleep(retry_s)
            retry_s = _next_stream_retry_delay(retry_s)
    finally:
        stop_refresh.set()


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
