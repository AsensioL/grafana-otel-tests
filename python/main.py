import time
import logging

from opentelemetry import metrics
from opentelemetry.sdk.resources import Resource

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

# ---- Resource ----
resource = Resource.create({
    "service.name": "demo-python-app",
    "host.id": "PC-ALPHA-01"                   # your alphanumeric ID
})

# ---- Metrics ----
metric_exporter = OTLPMetricExporter(endpoint="http://localhost:4317", insecure=True) # This points to Alloy gRPC endpoint
metric_reader = PeriodicExportingMetricReader(metric_exporter)

metrics.set_meter_provider(
    MeterProvider(resource=resource, metric_readers=[metric_reader])
)

meter = metrics.get_meter("demo-meter")
counter = meter.create_counter(
    "demo_requests_total",
    description="Number of demo requests"
)

# ---- Logs ----
log_exporter = OTLPLogExporter(endpoint="http://localhost:4317", insecure=True)  # This points to Alloy gRPC endpoint
# log_exporter = OTLPLogExporter(endpoint="http://localhost:4318/v1/logs", insecure=True)
log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(
    BatchLogRecordProcessor(log_exporter)
)

handler = LoggingHandler(level=logging.INFO, logger_provider=log_provider)
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("demo")
#logger.setLevel(logging.INFO)
#logger.addHandler(handler)



# ---- Loop ----
i = 0
while True:
    #counter.add(1, {"route": "/demo"})
    logger.info("Hello from Python demo", extra={"iteration": i})
    i += 1
    time.sleep(2)
