import os
import argparse
import time
from threading import Lock
from collections import deque

os.environ['PROMETHEUS_DISABLE_CREATED_SERIES'] = "1"

import prometheus_client as prom
try:
    from prometheus_client.registry import Collector
except:
    from prometheus_client.registry import CollectorRegistry as Collector
from prometheus_client.core import GaugeMetricFamily

from vast_client_tools.drivers.base import DriverBase
from vast_client_tools.nfsops import STATKEYS


class PrometheusDriver(DriverBase, Collector):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--prom-exporter-host", default="::",
        help="Prometheus exporter host."
    )
    parser.add_argument(
        "--prom-exporter-port", default=9000, type=int,
        help="Prometheus exporter port."
    )
    parser.add_argument(
        "--buffer-size", default=1000, type=int,
        help="Number of samples stored locally for processing by the Prometheus exporter. "
             "If the number of samples exceeds this value, the oldest samples will be discarded."
    )

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(prom_exporter_host={self.prom_exporter_host},"
            f" prom_exporter_port={self.prom_exporter_port},"
            f" buffer_size={self.buffer_size})"
        )

    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        self.lock = Lock()
        self.prom_exporter_host = args.prom_exporter_host
        self.prom_exporter_port = args.prom_exporter_port
        self.buffer_size = args.buffer_size
        self.local_buffer = deque(maxlen=self.buffer_size)

        prom.REGISTRY.unregister(prom.PROCESS_COLLECTOR)
        prom.REGISTRY.unregister(prom.PLATFORM_COLLECTOR)
        prom.REGISTRY.unregister(prom.GC_COLLECTOR)
        prom.REGISTRY.register(self)
        exporter = prom.start_http_server(port=self.prom_exporter_port, addr=self.prom_exporter_host)
        if exporter:
            self.exporter = exporter[0]
        self.logger.info(f"{self} has been initialized.")

    async def teardown(self):
        if hasattr(self, "exporter"):
            self.logger.info("Shutting down Prometheus exporter.")
            self.exporter.shutdown()

    async def store_sample(self, data):
        self.local_buffer.append(data)
        buffer_max_size = self.local_buffer.maxlen
        buffer_usage_percent = (len(self.local_buffer) / buffer_max_size) * 100
        # Check if buffer usage exceeds 80%
        if buffer_usage_percent > 80:
            self.logger.warning(
                f"Buffer usage is at {buffer_usage_percent:.2f}%. "
                "Prometheus is taking samples too slowly."
            )

    def _create_gauge(self, name, help_text, labels, value):
        gauge = GaugeMetricFamily(name, help_text, labels=labels.keys())
        gauge.add_metric(labels.values(), value)
        return gauge

    def collect(self):
        # Make sure only 1 prometheus request can be processed at time.
        with self.lock:
            samples_count = len(self.local_buffer)
            if samples_count == 0:
                return
            self.logger.debug(f"Found {samples_count} sample(s).")
            while self.local_buffer:
                data = self.local_buffer.popleft()
                for _, entry in data.iterrows():
                    labels_kwargs = {
                        "HOSTNAME": entry.HOSTNAME,
                        "UID": str(entry.UID),
                        "COMM": entry.COMM,
                        "MOUNT": entry.MOUNT,
                        "REMOTE_PATH": entry.REMOTE_PATH,
                    }
                    if self.common_args.envs:
                        for env in self.common_args.envs:
                            try:
                                labels_kwargs.update({env: entry.TAGS[env]})
                            except:
                                labels_kwargs.update({env: ""})
                    for s in STATKEYS.keys():
                        yield self._create_gauge("vnfs_" + s, "vnfs_" + STATKEYS[s], labels_kwargs, entry[s])
