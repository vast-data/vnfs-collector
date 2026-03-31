import pytest
import argparse
from unittest.mock import patch, MagicMock
from vnfs_collector.drivers import OtelDriver


@pytest.mark.asyncio
async def test_otel_driver_setup():
    with patch("vnfs_collector.drivers.otel_driver.OTLPMetricExporter") as mock_exporter, \
         patch("vnfs_collector.drivers.otel_driver.PeriodicExportingMetricReader") as mock_reader, \
         patch("vnfs_collector.drivers.otel_driver.MeterProvider") as mock_provider, \
         patch("vnfs_collector.drivers.otel_driver.metrics") as mock_metrics:

        mock_meter = MagicMock()
        mock_metrics.get_meter.return_value = mock_meter

        driver = OtelDriver(common_args=argparse.Namespace(envs=[], interval=5))
        await driver.setup()

        mock_exporter.assert_called_once_with(endpoint="localhost:4317", insecure=True)
        mock_reader.assert_called_once()
        mock_provider.assert_called_once()

        assert driver.otel_collector_host == "localhost"
        assert driver.otel_collector_port == 4317


@pytest.mark.asyncio
async def test_otel_driver_setup_custom_args():
    with patch("vnfs_collector.drivers.otel_driver.OTLPMetricExporter") as mock_exporter, \
         patch("vnfs_collector.drivers.otel_driver.PeriodicExportingMetricReader") as mock_reader, \
         patch("vnfs_collector.drivers.otel_driver.MeterProvider"), \
         patch("vnfs_collector.drivers.otel_driver.metrics") as mock_metrics:

        mock_meter = MagicMock()
        mock_metrics.get_meter.return_value = mock_meter

        driver = OtelDriver(common_args=argparse.Namespace(envs=[], interval=10))
        await driver.setup(args=[
            "--otel-collector-host", "otel.example.com",
            "--otel-collector-port", "4318",
        ])

        mock_exporter.assert_called_once_with(endpoint="otel.example.com:4318", insecure=True)
        # interval is 10 seconds = 10000 milliseconds
        mock_reader.assert_called_once()
        call_kwargs = mock_reader.call_args[1]
        assert call_kwargs["export_interval_millis"] == 10000

        assert driver.otel_collector_host == "otel.example.com"
        assert driver.otel_collector_port == 4318


@pytest.mark.asyncio
async def test_store_sample(data):
    with patch("vnfs_collector.drivers.otel_driver.OTLPMetricExporter"), \
         patch("vnfs_collector.drivers.otel_driver.PeriodicExportingMetricReader"), \
         patch("vnfs_collector.drivers.otel_driver.MeterProvider"), \
         patch("vnfs_collector.drivers.otel_driver.metrics") as mock_metrics:

        mock_meter = MagicMock()
        mock_gauge = MagicMock()
        mock_meter.create_gauge.return_value = mock_gauge
        mock_metrics.get_meter.return_value = mock_meter

        driver = OtelDriver(common_args=argparse.Namespace(envs=["JOB"], interval=5))
        await driver.setup()

        await driver.store_sample(data)

        # Verify gauges were set for each row and each stat key
        assert mock_gauge.set.call_count > 0


@pytest.mark.asyncio
async def test_teardown():
    with patch("vnfs_collector.drivers.otel_driver.OTLPMetricExporter"), \
         patch("vnfs_collector.drivers.otel_driver.PeriodicExportingMetricReader"), \
         patch("vnfs_collector.drivers.otel_driver.MeterProvider") as mock_provider_cls, \
         patch("vnfs_collector.drivers.otel_driver.metrics") as mock_metrics:

        mock_provider = MagicMock()
        mock_provider_cls.return_value = mock_provider
        mock_meter = MagicMock()
        mock_metrics.get_meter.return_value = mock_meter

        driver = OtelDriver(common_args=argparse.Namespace(envs=[], interval=5))
        await driver.setup()
        await driver.teardown()

        mock_provider.shutdown.assert_called_once()


@pytest.mark.asyncio
async def test_driver_str():
    with patch("vnfs_collector.drivers.otel_driver.OTLPMetricExporter"), \
         patch("vnfs_collector.drivers.otel_driver.PeriodicExportingMetricReader"), \
         patch("vnfs_collector.drivers.otel_driver.MeterProvider"), \
         patch("vnfs_collector.drivers.otel_driver.metrics") as mock_metrics:

        mock_meter = MagicMock()
        mock_metrics.get_meter.return_value = mock_meter

        driver = OtelDriver(common_args=argparse.Namespace(envs=[], interval=5))
        await driver.setup()

        driver_str = str(driver)
        assert "OtelDriver" in driver_str
        assert "localhost" in driver_str
        assert "4317" in driver_str
        assert "interval=5" in driver_str
