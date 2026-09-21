#!/usr/bin/env python3
import importlib.util
import io
import json
import logging
import os
import socket
import sys
import threading
import unittest
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, patch

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    LogRecordExportResult,
    SimpleLogRecordProcessor,
)

_PYTHON_DIR = Path(__file__).resolve().parent
_FORWARDER_PATH = _PYTHON_DIR / "particle-event-forwarder.py"
_spec = importlib.util.spec_from_file_location(
    "particle_event_forwarder", _FORWARDER_PATH
)
_forwarder = importlib.util.module_from_spec(_spec)
sys.modules["particle_event_forwarder"] = _forwarder
_spec.loader.exec_module(_forwarder)

CODE_LOCATION_ATTRS = _forwarder.CODE_LOCATION_ATTRS
CONSOLE_ENV = _forwarder.CONSOLE_ENV
DEFAULT_OTLP_ENDPOINT = _forwarder.DEFAULT_OTLP_ENDPOINT
PARTICLE_LOGGER_NAME = _forwarder.PARTICLE_LOGGER_NAME
TOOL_LOGGER_NAME = _forwarder.TOOL_LOGGER_NAME
ParticleEventParser = _forwarder.ParticleEventParser
DeviceList = _forwarder.DeviceList
configure_logging = _forwarder.configure_logging
fetch_product_devices = _forwarder.fetch_product_devices
_forward_event = _forwarder._forward_event
_parse_webhook_response_event = _forwarder._parse_webhook_response_event
_is_hook_sent_event = _forwarder._is_hook_sent_event
DEVICE_FIELD_MISSING = _forwarder.DEVICE_FIELD_MISSING
require_env = _forwarder.require_env
DEVICES_PER_PAGE = _forwarder.DEVICES_PER_PAGE
PARTICLE_API_BASE = _forwarder.PARTICLE_API_BASE


SAMPLE_EVENT = {
    "data": '{"testType":"standard","responseCode":200}',
    "ttl": 60,
    "published_at": "2026-07-28T17:12:19.609Z",
    "coreid": "e00fce682e79a004f1e69a2b",
    "userid": "5d447a8c6c0ad300017dd72e",
    "version": 91,
    "public": False,
    "productID": 13961,
}


def _sse(event_name: str, payload: dict, newline: bytes = b"\n") -> bytes:
    return (
        b"event: "
        + event_name.encode()
        + newline
        + b"data: "
        + json.dumps(payload).encode()
        + newline
    )


def _otlp_reachable(host: str = "127.0.0.1", port: int = 4317, timeout: float = 0.25) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _quiet_tool_loggers() -> None:
    for name in (TOOL_LOGGER_NAME, PARTICLE_LOGGER_NAME):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.addHandler(logging.NullHandler())
        lg.propagate = False


class ParticleEventParserTests(unittest.TestCase):
    def setUp(self):
        _quiet_tool_loggers()

    def test_ok_empty_and_comment_lines_are_skipped(self):
        pep = ParticleEventParser()
        pep.feed(b":ok\n\n: keep-alive\n")
        pep.feed(_sse("HttpRequestStatistics", SAMPLE_EVENT))
        ev = pep.pull()
        self.assertIsNotNone(ev)
        self.assertEqual(ev["name"], "HttpRequestStatistics")
        self.assertEqual(ev["coreid"], SAMPLE_EVENT["coreid"])
        self.assertIsNone(pep.pull())

    def test_split_chunks(self):
        blob = _sse("HttpRequestStatistics", SAMPLE_EVENT)
        mid = len(blob) // 2
        pep = ParticleEventParser()
        pep.feed(blob[:mid])
        self.assertIsNone(pep.pull())
        pep.feed(blob[mid:])
        ev = pep.pull()
        self.assertEqual(ev["name"], "HttpRequestStatistics")
        self.assertEqual(ev["data"], SAMPLE_EVENT["data"])

    def test_leftover_buffer_until_newline(self):
        pep = ParticleEventParser()
        pep.feed(b'event: Partial\ndata: {"data":"x"')
        self.assertIsNone(pep.pull())
        self.assertEqual(pep.current_event, "Partial")
        pep.feed(b',"ttl":1,"published_at":"t","coreid":"d","version":1}\n')
        ev = pep.pull()
        self.assertEqual(ev["name"], "Partial")
        self.assertEqual(ev["data"], "x")

    def test_several_events_in_one_chunk(self):
        first = dict(SAMPLE_EVENT, data="one")
        second = dict(SAMPLE_EVENT, data="two")
        pep = ParticleEventParser()
        pep.feed(_sse("Alpha", first) + b"\n" + _sse("Beta", second))
        self.assertEqual(pep.pull()["name"], "Alpha")
        self.assertEqual(pep.pull()["name"], "Beta")
        self.assertIsNone(pep.pull())

    def test_crlf_lines(self):
        pep = ParticleEventParser()
        pep.feed(_sse("CrlfEvent", SAMPLE_EVENT, newline=b"\r\n"))
        ev = pep.pull()
        self.assertEqual(ev["name"], "CrlfEvent")
        self.assertEqual(ev["coreid"], SAMPLE_EVENT["coreid"])

    def test_invalid_json_recovers(self):
        pep = ParticleEventParser()
        pep.feed(b"event: Broken\ndata: not-json\n")
        self.assertIsNone(pep.pull())
        self.assertEqual(pep.current_event, "")
        pep.feed(_sse("Recovered", SAMPLE_EVENT))
        self.assertEqual(pep.pull()["name"], "Recovered")

    def test_data_without_event_is_dropped(self):
        pep = ParticleEventParser()
        pep.feed(b'data: {"data":"orphan"}\n')
        self.assertIsNone(pep.pull())
        pep.feed(_sse("AfterOrphan", SAMPLE_EVENT))
        self.assertEqual(pep.pull()["name"], "AfterOrphan")

    def test_unexpected_line_resets_and_recovers(self):
        pep = ParticleEventParser()
        pep.feed(b"event: Pending\nfoo: bar\n")
        self.assertEqual(pep.current_event, "")
        self.assertIsNone(pep.pull())
        pep.feed(_sse("Ok", SAMPLE_EVENT))
        self.assertEqual(pep.pull()["name"], "Ok")

    def test_event_without_data_is_replaced(self):
        pep = ParticleEventParser()
        pep.feed(b"event: First\nevent: Second\n")
        self.assertEqual(pep.current_event, "Second")
        pep.feed(b"data: " + json.dumps(SAMPLE_EVENT).encode() + b"\n")
        ev = pep.pull()
        self.assertEqual(ev["name"], "Second")


class RequireEnvTests(unittest.TestCase):
    def setUp(self):
        _quiet_tool_loggers()

    def test_returns_value_when_set(self):
        with patch.dict(os.environ, {"PARTICLE_PRODUCT_ID": "13961"}, clear=False):
            self.assertEqual(require_env("PARTICLE_PRODUCT_ID"), "13961")

    def test_exits_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit) as cm:
                require_env("PARTICLE_AUTH_TOKEN")
            self.assertEqual(cm.exception.code, 1)

    def test_exits_when_empty(self):
        with patch.dict(os.environ, {"PARTICLE_AUTH_TOKEN": ""}, clear=False):
            with self.assertRaises(SystemExit) as cm:
                require_env("PARTICLE_AUTH_TOKEN")
            self.assertEqual(cm.exception.code, 1)


class ConfigureLoggingTests(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemoryLogRecordExporter()
        self.provider, self.tool_logger, self.particle_logger = configure_logging(
            log_record_processor=SimpleLogRecordProcessor(self.exporter)
        )

    def tearDown(self):
        self.provider.shutdown()
        _quiet_tool_loggers()

    def test_particle_extra_attributes_and_logger_names(self):
        self.particle_logger.info(
            SAMPLE_EVENT["data"],
            extra={
                "publish_time": SAMPLE_EVENT["published_at"],
                "device_id": SAMPLE_EVENT["coreid"],
                "device_version": SAMPLE_EVENT["version"],
                "event_name": "HttpRequestStatistics",
            },
        )
        self.tool_logger.info("internal heartbeat")

        logs = self.exporter.get_finished_logs()
        self.assertEqual(len(logs), 2)

        particle, tool = logs
        self.assertEqual(particle.instrumentation_scope.name, PARTICLE_LOGGER_NAME)
        self.assertEqual(tool.instrumentation_scope.name, TOOL_LOGGER_NAME)
        self.assertNotEqual(
            particle.instrumentation_scope.name, tool.instrumentation_scope.name
        )

        attrs = dict(particle.log_record.attributes)
        self.assertEqual(attrs["publish_time"], SAMPLE_EVENT["published_at"])
        self.assertEqual(attrs["device_id"], SAMPLE_EVENT["coreid"])
        self.assertEqual(attrs["device_version"], SAMPLE_EVENT["version"])
        self.assertEqual(attrs["event_name"], "HttpRequestStatistics")
        for key in CODE_LOCATION_ATTRS:
            self.assertNotIn(key, attrs)
        self.assertEqual(particle.log_record.body, SAMPLE_EVENT["data"])
        self.assertEqual(tool.log_record.body, "internal heartbeat")

        tool_attrs = dict(tool.log_record.attributes)
        for key in CODE_LOCATION_ATTRS:
            self.assertIn(key, tool_attrs)


class ForwardEventTests(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemoryLogRecordExporter()
        self.provider, _tool_logger, self.particle_logger = configure_logging(
            log_record_processor=SimpleLogRecordProcessor(self.exporter)
        )

    def tearDown(self):
        self.provider.shutdown()
        _quiet_tool_loggers()

    def _attrs(self):
        logs = self.exporter.get_finished_logs()
        self.assertEqual(len(logs), 1)
        return dict(logs[0].log_record.attributes)

    def test_includes_device_name_and_joined_groups(self):
        devices = {
            SAMPLE_EVENT["coreid"]: {
                "id": SAMPLE_EVENT["coreid"],
                "name": "front-door",
                "groups": ["prod", "west"],
            }
        }
        _forward_event(self.particle_logger, SAMPLE_EVENT, devices)
        attrs = self._attrs()
        self.assertEqual(attrs["device_name"], "front-door")
        self.assertEqual(attrs["device_groups"], "prod,west")

    def test_missing_device_or_fields_are_placeholder(self):
        _forward_event(self.particle_logger, SAMPLE_EVENT, {})
        attrs = self._attrs()
        self.assertEqual(attrs["device_name"], DEVICE_FIELD_MISSING)
        self.assertEqual(attrs["device_groups"], DEVICE_FIELD_MISSING)

    def test_partial_device_fields_are_placeholder(self):
        devices = {SAMPLE_EVENT["coreid"]: {"id": SAMPLE_EVENT["coreid"]}}
        _forward_event(self.particle_logger, SAMPLE_EVENT, devices)
        attrs = self._attrs()
        self.assertEqual(attrs["device_name"], DEVICE_FIELD_MISSING)
        self.assertEqual(attrs["device_groups"], DEVICE_FIELD_MISSING)

    def test_webhook_response_without_coreid_parses_event_name(self):
        device_id = SAMPLE_EVENT["coreid"]
        ev = dict(
            SAMPLE_EVENT,
            coreid="particle-internal",
            name=f"{device_id}/hook-response/HttpWatchdog/0",
        )
        devices = {
            device_id: {
                "id": device_id,
                "name": "front-door",
                "groups": ["prod"],
            }
        }
        _forward_event(self.particle_logger, ev, devices)
        attrs = self._attrs()
        self.assertEqual(attrs["device_id"], device_id)
        self.assertEqual(attrs["event_name"], "HttpWatchdog")
        self.assertEqual(attrs["event_type"], "hook-response")
        self.assertEqual(attrs["attempt"], 0)
        self.assertEqual(attrs["device_name"], "front-door")
        self.assertEqual(attrs["device_groups"], "prod")

    def test_webhook_response_event_name_may_contain_slashes(self):
        device_id = SAMPLE_EVENT["coreid"]
        ev = dict(
            SAMPLE_EVENT,
            coreid="particle-internal",
            name=f"{device_id}/hook-response/spark/device/last_reset/2",
        )
        _forward_event(self.particle_logger, ev, {})
        attrs = self._attrs()
        self.assertEqual(attrs["device_id"], device_id)
        self.assertEqual(attrs["event_type"], "hook-response")
        self.assertEqual(attrs["event_name"], "spark/device/last_reset")
        self.assertEqual(attrs["attempt"], 2)

    def test_hook_sent_two_part_name_is_not_logged(self):
        ev = dict(SAMPLE_EVENT, name="hook-sent/AF-Access")
        _forward_event(self.particle_logger, ev, {})
        self.assertEqual(len(self.exporter.get_finished_logs()), 0)

    def test_hook_sent_special_event_is_not_logged(self):
        device_id = SAMPLE_EVENT["coreid"]
        ev = dict(
            SAMPLE_EVENT,
            coreid="particle-internal",
            name=f"{device_id}/hook-sent/AF-Telemetry/0",
        )
        _forward_event(self.particle_logger, ev, {})
        self.assertEqual(len(self.exporter.get_finished_logs()), 0)

    def test_webhook_pattern_ignored_when_coreid_present(self):
        ev = dict(
            SAMPLE_EVENT,
            name=f"{SAMPLE_EVENT['coreid']}/hook-response/HttpWatchdog/0",
        )
        _forward_event(self.particle_logger, ev, {})
        attrs = self._attrs()
        self.assertEqual(attrs["device_id"], SAMPLE_EVENT["coreid"])
        self.assertEqual(
            attrs["event_name"],
            f"{SAMPLE_EVENT['coreid']}/hook-response/HttpWatchdog/0",
        )
        self.assertNotIn("hook_type", attrs)
        self.assertNotIn("attempt", attrs)

    def test_empty_coreid_without_webhook_pattern_is_unchanged(self):
        ev = dict(SAMPLE_EVENT, coreid="", name="HttpWatchdog")
        _forward_event(self.particle_logger, ev, {})
        attrs = self._attrs()
        self.assertEqual(attrs["device_id"], "")
        self.assertEqual(attrs["event_name"], "HttpWatchdog")
        self.assertNotIn("hook_type", attrs)
        self.assertNotIn("attempt", attrs)


class ParseWebhookResponseEventTests(unittest.TestCase):
    def test_parses_four_segment_name(self):
        parsed = _parse_webhook_response_event(
            "e00fce682e79a004f1e69a2b/hook-response/HttpWatchdog/0"
        )
        self.assertEqual(
            parsed,
            {
                "device_id": "e00fce682e79a004f1e69a2b",
                "hook_type": "hook-response",
                "event_name": "HttpWatchdog",
                "attempt": 0,
            },
        )

    def test_rejects_non_matching_names(self):
        self.assertIsNone(_parse_webhook_response_event(""))
        self.assertIsNone(_parse_webhook_response_event("HttpWatchdog"))
        self.assertIsNone(_parse_webhook_response_event("id/hook-response/event"))
        self.assertIsNone(_parse_webhook_response_event("id/hook-response/event/x"))


class HookSentEventFilterTests(unittest.TestCase):
    def test_detects_hook_sent_names(self):
        self.assertTrue(_is_hook_sent_event("hook-sent/AF-Access"))
        self.assertTrue(_is_hook_sent_event("hook-sent/AF-V2-SetManifest"))
        self.assertTrue(
            _is_hook_sent_event("e00fce682e79a004f1e69a2b/hook-sent/AF-Telemetry/0")
        )
        webhook = {
            "device_id": "d",
            "hook_type": "hook-sent",
            "event_name": "AF-Access",
            "attempt": 0,
        }
        self.assertTrue(_is_hook_sent_event("d/hook-sent/AF-Access/0", webhook))

    def test_keeps_hook_response_and_normal_names(self):
        self.assertFalse(_is_hook_sent_event("HttpWatchdog"))
        self.assertFalse(_is_hook_sent_event("hook-response/AF-Access/0"))
        webhook = {
            "device_id": "d",
            "hook_type": "hook-response",
            "event_name": "AF-Access",
            "attempt": 0,
        }
        self.assertFalse(_is_hook_sent_event("d/hook-response/AF-Access/0", webhook))


def _stderr_stream_handlers(logger):
    return [h for h in logger.handlers if type(h) is logging.StreamHandler]


class ConsoleLoggingTests(unittest.TestCase):
    def tearDown(self):
        _quiet_tool_loggers()

    def _configure(self, **kwargs):
        exporter = InMemoryLogRecordExporter()
        provider, tool_logger, particle_logger = configure_logging(
            log_record_processor=SimpleLogRecordProcessor(exporter),
            **kwargs,
        )
        self.addCleanup(provider.shutdown)
        return exporter, tool_logger, particle_logger

    def test_console_off_by_default(self):
        _exporter, tool_logger, particle_logger = self._configure()
        self.assertEqual(_stderr_stream_handlers(tool_logger), [])
        self.assertEqual(_stderr_stream_handlers(particle_logger), [])

    def test_console_echoes_tool_internal_only(self):
        buf = io.StringIO()
        with patch.object(sys, "stderr", buf):
            _exporter, tool_logger, particle_logger = self._configure(console=True)
            tool_logger.info("console-visible message")
            particle_logger.info("particle should stay off stdout")
        output = buf.getvalue()
        self.assertIn("console-visible message", output)
        self.assertNotIn("particle should stay off stdout", output)
        self.assertEqual(len(_stderr_stream_handlers(tool_logger)), 1)
        self.assertEqual(_stderr_stream_handlers(particle_logger), [])

    def test_console_env_flag_enables_stream_handler(self):
        with patch.dict(os.environ, {CONSOLE_ENV: "1"}, clear=False):
            _exporter, tool_logger, particle_logger = self._configure()
        self.assertEqual(len(_stderr_stream_handlers(tool_logger)), 1)
        self.assertEqual(_stderr_stream_handlers(particle_logger), [])


class _RecordingExporter:
    """Forwards to a real exporter and records the result for assertions."""

    def __init__(self, inner):
        self._inner = inner
        self.results = []

    def export(self, batch):
        result = self._inner.export(batch)
        self.results.append(result)
        return result

    def shutdown(self):
        self._inner.shutdown()

    def force_flush(self, timeout_millis=30000):
        if hasattr(self._inner, "force_flush"):
            return self._inner.force_flush(timeout_millis)
        return True


class LiveOtlpTests(unittest.TestCase):
    def setUp(self):
        if not _otlp_reachable():
            self.skipTest("Alloy OTLP not running on 127.0.0.1:4317")

    def test_export_tool_log(self):
        inner = OTLPLogExporter(endpoint=DEFAULT_OTLP_ENDPOINT, insecure=True)
        recorder = _RecordingExporter(inner)
        provider, tool_logger, _particle_logger = configure_logging(
            log_record_processor=SimpleLogRecordProcessor(recorder)
        )
        marker = f"live-otlp-test-{uuid.uuid4()}"
        try:
            tool_logger.info(marker)
            self.assertTrue(provider.force_flush(timeout_millis=10000))
            self.assertEqual(recorder.results, [LogRecordExportResult.SUCCESS])
        finally:
            provider.shutdown()
            _quiet_tool_loggers()


class FetchProductDevicesTests(unittest.TestCase):
    def setUp(self):
        _quiet_tool_loggers()

    def tearDown(self):
        _quiet_tool_loggers()

    def _json_response(self, payload, status_code=200):
        response = MagicMock()
        response.ok = status_code == 200
        response.status_code = status_code
        response.reason = "OK" if status_code == 200 else "Error"
        response.json.return_value = payload
        response.content = json.dumps(payload).encode()
        if status_code != 200:
            response.raise_for_status.side_effect = _forwarder.requests.HTTPError(
                f"{status_code} Error"
            )
        return response

    def test_single_page_does_not_fetch_more(self):
        payload = {
            "devices": [{"id": "a"}, {"id": "b"}],
            "meta": {"total_pages": 1, "total_records": 11148},
        }
        with patch.object(_forwarder.requests, "get", return_value=self._json_response(payload)) as get:
            devices = fetch_product_devices("13961", {"Authorization": "Bearer t"})
        self.assertEqual(devices, payload["devices"])
        self.assertEqual(get.call_count, 1)
        url = get.call_args.args[0]
        self.assertIn(f"{PARTICLE_API_BASE}/products/13961/devices", url)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["perPage"], [str(DEVICES_PER_PAGE)])
        self.assertEqual(query["page"], ["1"])

    def test_pages_when_total_records_exceeds_per_page(self):
        page1 = {
            "devices": [{"id": "p1"}],
            "meta": {"total_pages": 2, "total_records": DEVICES_PER_PAGE + 1},
        }
        page2 = {
            "devices": [{"id": "p2"}],
            "meta": {"total_pages": 2, "total_records": DEVICES_PER_PAGE + 1},
        }

        def fake_get(url, headers=None):
            page = parse_qs(urlparse(url).query)["page"][0]
            if page == "1":
                return self._json_response(page1)
            if page == "2":
                return self._json_response(page2)
            self.fail(f"unexpected page {page}")

        with patch.object(_forwarder.requests, "get", side_effect=fake_get) as get:
            devices = fetch_product_devices("13961", {"Authorization": "Bearer t"})
        self.assertEqual(devices, [{"id": "p1"}, {"id": "p2"}])
        self.assertEqual(get.call_count, 2)

    def test_replace_is_visible_on_snapshot(self):
        device_list = DeviceList()
        self.assertEqual(device_list.snapshot(), {})
        incoming = [{"id": "a", "name": "one"}, {"id": "b", "name": "two"}]
        device_list.replace(incoming)
        incoming.append({"id": "c"})
        self.assertEqual(
            device_list.snapshot(),
            {"a": {"id": "a", "name": "one"}, "b": {"id": "b", "name": "two"}},
        )

    def test_snapshot_if_newer_skips_copy_until_replace(self):
        device_list = DeviceList()
        gen, updated = device_list.snapshot_if_newer(0)
        self.assertEqual(gen, 0)
        self.assertIsNone(updated)

        device_list.replace([{"id": "a"}])
        gen, updated = device_list.snapshot_if_newer(0)
        self.assertEqual(gen, 1)
        self.assertEqual(updated, {"a": {"id": "a"}})

        gen, updated = device_list.snapshot_if_newer(gen)
        self.assertEqual(gen, 1)
        self.assertIsNone(updated)

        device_list.replace([{"id": "b"}])
        gen, updated = device_list.snapshot_if_newer(gen)
        self.assertEqual(gen, 2)
        self.assertEqual(updated, {"b": {"id": "b"}})

    def test_unchanged_check_does_not_take_lock(self):
        device_list = DeviceList()
        device_list.replace([{"id": "a"}])
        gen = device_list.generation()
        self.assertFalse(device_list.has_newer(gen))

        class _LockProbe:
            def __enter__(self):
                raise AssertionError("lock should not be acquired")

            def __exit__(self, *args):
                return False

        device_list._lock = _LockProbe()
        self.assertFalse(device_list.has_newer(gen))
        self.assertEqual(device_list.generation(), gen)
        seen, updated = device_list.snapshot_if_newer(gen)
        self.assertEqual(seen, gen)
        self.assertIsNone(updated)

    def test_refresh_loop_publishes_to_device_list(self):
        device_list = DeviceList()
        stop = threading.Event()
        published = [{"id": "dev-1"}]

        def fake_fetch(product_id, headers):
            stop.set()
            return published

        with patch.object(_forwarder, "fetch_product_devices", side_effect=fake_fetch):
            with patch.object(_forwarder, "DEVICE_REFRESH_INTERVAL_S", 0):
                _forwarder._device_refresh_loop("13961", {}, stop, device_list)
        self.assertEqual(device_list.snapshot(), {"dev-1": {"id": "dev-1"}})

    def test_initial_refresh_fills_device_list(self):
        device_list = DeviceList()
        published = [{"id": "startup"}]
        with patch.object(
            _forwarder, "fetch_product_devices", return_value=published
        ) as fetch:
            _forwarder._refresh_device_list("13961", {}, device_list)
        fetch.assert_called_once_with("13961", {})
        self.assertEqual(device_list.snapshot(), {"startup": {"id": "startup"}})

    def test_run_exits_when_initial_device_pull_fails(self):
        with patch.dict(
            os.environ,
            {"PARTICLE_PRODUCT_ID": "13961", "PARTICLE_AUTH_TOKEN": "token"},
            clear=False,
        ):
            with patch.object(
                _forwarder,
                "_refresh_device_list",
                side_effect=_forwarder.requests.ConnectionError("down"),
            ):
                with self.assertRaises(SystemExit) as cm:
                    _forwarder._run(logging.getLogger("tool"), logging.getLogger("particle"))
        self.assertEqual(cm.exception.code, 1)

    def _run_with_stream_gets(self, get_side_effect):
        tool_logger = logging.getLogger("tool")
        particle_logger = logging.getLogger("particle")
        with patch.dict(
            os.environ,
            {"PARTICLE_PRODUCT_ID": "13961", "PARTICLE_AUTH_TOKEN": "token"},
            clear=False,
        ), patch.object(_forwarder, "_refresh_device_list"), patch.object(
            _forwarder.time, "sleep"
        ), patch.object(
            _forwarder.requests, "get", side_effect=get_side_effect
        ) as get:
            try:
                _forwarder._run(tool_logger, particle_logger)
            except KeyboardInterrupt:
                pass
            return get

    def test_run_reconnects_after_stream_ends(self):
        forwarded = []

        def capture(logger, ev, devices):
            forwarded.append(ev)

        first = MagicMock()
        first.ok = True
        first.status_code = 200
        first.iter_content.return_value = iter(
            [_sse("HttpRequestStatistics", SAMPLE_EVENT)]
        )
        first.__enter__.return_value = first
        first.__exit__.return_value = False

        with patch.object(_forwarder, "_forward_event", side_effect=capture):
            get = self._run_with_stream_gets(
                [first, KeyboardInterrupt("stop after reconnect")]
            )
        self.assertEqual(get.call_count, 2)
        self.assertEqual(len(forwarded), 1)
        self.assertEqual(forwarded[0]["coreid"], SAMPLE_EVENT["coreid"])
        self.assertEqual(
            get.call_args.kwargs["timeout"],
            (_forwarder.STREAM_CONNECT_TIMEOUT_S, _forwarder.STREAM_READ_TIMEOUT_S),
        )

    def test_run_retries_stream_connect_error(self):
        get = self._run_with_stream_gets(
            [
                _forwarder.requests.ConnectionError("refused"),
                KeyboardInterrupt("stop after retry"),
            ]
        )
        self.assertEqual(get.call_count, 2)

    def test_run_reconnects_after_read_error(self):
        first = MagicMock()
        first.ok = True
        first.status_code = 200
        first.iter_content.side_effect = _forwarder.requests.ReadTimeout("idle")
        first.__enter__.return_value = first
        first.__exit__.return_value = False

        get = self._run_with_stream_gets(
            [first, KeyboardInterrupt("stop after disconnect")]
        )
        self.assertEqual(get.call_count, 2)

    def test_run_exits_on_stream_http_401(self):
        unauthorized = MagicMock()
        unauthorized.ok = False
        unauthorized.status_code = 401
        unauthorized.reason = "Unauthorized"
        unauthorized.content = b"nope"
        unauthorized.__enter__.return_value = unauthorized
        unauthorized.__exit__.return_value = False

        with patch.dict(
            os.environ,
            {"PARTICLE_PRODUCT_ID": "13961", "PARTICLE_AUTH_TOKEN": "token"},
            clear=False,
        ), patch.object(_forwarder, "_refresh_device_list"), patch.object(
            _forwarder.requests, "get", return_value=unauthorized
        ):
            with self.assertRaises(SystemExit) as cm:
                _forwarder._run(
                    logging.getLogger("tool"), logging.getLogger("particle")
                )
        self.assertEqual(cm.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
