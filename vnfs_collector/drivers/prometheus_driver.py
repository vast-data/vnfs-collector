# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import os
import argparse
from threading import Lock
import pandas as pd

os.environ['PROMETHEUS_DISABLE_CREATED_SERIES'] = "1"

import prometheus_client as prom
try:
    from prometheus_client.registry import Collector
except:
    from prometheus_client.registry import CollectorRegistry as Collector
from prometheus_client.core import GaugeMetricFamily

from vnfs_collector.drivers.base import DriverBase
from vnfs_collector.nfsops import STATKEYS, group_stats


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

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(prom_exporter_host={self.prom_exporter_host},"
            f" prom_exporter_port={self.prom_exporter_port})"
        )

    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        self.lock = Lock()
        self.prom_exporter_host = args.prom_exporter_host
        self.prom_exporter_port = args.prom_exporter_port
        self.latest_sample = None

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

    async def store_samples(self, samples: list):
        with self.lock:
            if not samples:
                self.latest_sample = None
                return

            # Concatenate all samples in the batch
            combined = pd.concat(samples, ignore_index=True)

            # Aggregate by MOUNT, COMM, TAGS (sum stats, group by labels)
            self.latest_sample = group_stats(combined, ["MOUNT", "COMM", "TAGS"])

    def _create_gauge(self, name, help_text, labels, value):
        gauge = GaugeMetricFamily(name, help_text, labels=labels.keys())
        gauge.add_metric(labels.values(), value)
        return gauge

    def collect(self):
        with self.lock:
            if self.latest_sample is None:
                return
            data = self.latest_sample

        self.logger.debug(f"Found last sample")
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
