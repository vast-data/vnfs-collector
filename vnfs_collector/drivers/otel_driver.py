# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Vast Data Ltd.

import argparse

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from vnfs_collector.drivers.base import DriverBase
from vnfs_collector.nfsops import STATKEYS


class OtelDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--otel-collector-host", default="localhost",
        help="OpenTelemetry collector host."
    )
    parser.add_argument(
        "--otel-collector-port", default=4317, type=int,
        help="OpenTelemetry collector gRPC port."
    )
    parser.add_argument(
        "--otel-insecure", action="store_true", default=True,
        help="Use insecure connection to the collector."
    )

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(otel_collector_host={self.otel_collector_host},"
            f" otel_collector_port={self.otel_collector_port},"
            f" interval={self.common_args.interval})"
        )

    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        self.otel_collector_host = args.otel_collector_host
        self.otel_collector_port = args.otel_collector_port
        self.otel_insecure = args.otel_insecure

        endpoint = f"{self.otel_collector_host}:{self.otel_collector_port}"
        export_interval_millis = self.common_args.interval * 1000

        resource = Resource.create({"service.name": "vnfs-collector"})
        self.exporter = OTLPMetricExporter(endpoint=endpoint, insecure=self.otel_insecure)
        self.reader = PeriodicExportingMetricReader(
            self.exporter,
            export_interval_millis=export_interval_millis
        )
        self.provider = MeterProvider(resource=resource, metric_readers=[self.reader])
        metrics.set_meter_provider(self.provider)

        self.meter = metrics.get_meter("vnfs-collector")
        self._setup_gauges()

        self.logger.info(f"{self} has been initialized.")

    def _setup_gauges(self):
        """Create gauges for each stat key."""
        self.gauges = {}
        for stat_key, description in STATKEYS.items():
            gauge = self.meter.create_gauge(
                name=f"vnfs_{stat_key.lower()}",
                description=description,
            )
            self.gauges[stat_key] = gauge

    async def teardown(self):
        if hasattr(self, "provider"):
            self.logger.info("Shutting down OTEL exporter.")
            self.provider.shutdown()

    async def store_sample(self, data):
        for _, entry in data.iterrows():
            attributes = {
                "hostname": entry.HOSTNAME,
                "uid": str(entry.UID),
                "comm": entry.COMM,
                "mount": entry.MOUNT,
                "remote_path": entry.REMOTE_PATH,
            }
            if self.common_args.envs:
                for env in self.common_args.envs:
                    try:
                        attributes[env.lower()] = entry.TAGS[env]
                    except:
                        attributes[env.lower()] = ""

            for stat_key, gauge in self.gauges.items():
                gauge.set(entry[stat_key], attributes)
