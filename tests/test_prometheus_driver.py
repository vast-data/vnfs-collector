import pytest
import argparse
from unittest.mock import patch, MagicMock
from vnfs_collector.drivers import PrometheusDriver


@pytest.mark.asyncio
async def test_prometheus_driver_setup():
    # Create a mock for the exporter
    with patch("prometheus_client.start_http_server") as mock_exporter:
        driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
        await driver.setup()
        # Check if the exporter was started
        mock_exporter.assert_called_once_with(port=9000, addr="::")

        # Check if the driver was registered and logger info was called
        assert driver.prom_exporter_host == "::"
        assert driver.prom_exporter_port == 9000
        assert driver.latest_sample is None


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_sample():
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
    await driver.setup()

    sample_data = MagicMock()
    await driver.store_sample(sample_data)

    # Check if the sample data is stored as latest
    assert driver.latest_sample == sample_data


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_sample_replaces_previous():
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
    await driver.setup()

    sample_data1 = MagicMock()
    sample_data2 = MagicMock()

    await driver.store_sample(sample_data1)
    await driver.store_sample(sample_data2)

    # Latest sample should be the second one
    assert driver.latest_sample == sample_data2


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_collect_is_readonly(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()
    await driver.store_sample(data)

    # Collect metrics twice
    metrics1 = list(driver.collect())
    metrics2 = list(driver.collect())

    # Both collections should return the same metrics (collect is readonly)
    assert len(metrics1) == len(metrics2)
    assert len(metrics1) > 0
    # Latest sample should still be there
    assert driver.latest_sample is not None


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_collect_metrics(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()
    await driver.store_sample(data)

    # Mock `_create_gauge`
    with patch.object(
        driver, "_create_gauge", return_value=MagicMock()
    ) as mock_create_gauge:
        metrics = list(driver.collect())
        # Verify that metrics were collected
        assert len(metrics) > 0
        assert mock_create_gauge.call_count == 260
