#!/usr/bin/env python3
import importlib.util
import io
import json
import logging
import os
import socket
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

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
configure_logging = _forwarder.configure_logging
require_env = _forwarder.require_env


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


if __name__ == "__main__":
    unittest.main()
