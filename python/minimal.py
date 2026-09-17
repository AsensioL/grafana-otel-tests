import logging
from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor

grpc = False

if grpc:
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    log_exporter = OTLPLogExporter(endpoint="http://localhost:4317", insecure=True)
else:
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    log_exporter = OTLPLogExporter(endpoint="http://localhost:4318/v1/logs")

log_provider = LoggerProvider()
log_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))

handler = LoggingHandler(level=logging.INFO, logger_provider=log_provider)
logger = logging.getLogger("demo")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Also print to console for demo
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

logger.info("Hello, logs are flowing!")
