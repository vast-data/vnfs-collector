import pytest
import argparse
from unittest.mock import patch, MagicMock
from vast_client_tools.drivers import PrometheusDriver


@pytest.mark.asyncio
async def test_prometheus_driver_setup():
    # Create a mock for the exporter
    with patch("prometheus_client.start_http_server") as mock_exporter:
        driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
        await driver.setup()
        # Check if the exporter was started
        mock_exporter.assert_called_once_with(port=9000, addr="::")

        # Check if the driver was registered and logger info was called
        assert driver.prometheus_host == "::"
        assert driver.prometheus_port == 9000
        assert driver.buffer_size == 1000
        assert driver.local_buffer.maxlen == 1000


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_sample_buffer_warning():
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
    await driver.setup()

    # Add samples to buffer
    for _ in range(800):  # Assuming the buffer size is 1000
        await driver.store_sample(MagicMock())

    # Check if the logger warning was called for buffer usage
    with patch.object(driver.logger, "warning") as mock_warning:
        await driver.store_sample(MagicMock())
        mock_warning.assert_called()


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_sample():
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
    await driver.setup()

    sample_data = MagicMock()  # Mock DataFrame row
    await driver.store_sample(sample_data)

    # Check if the sample data is in the buffer
    assert len(driver.local_buffer) == 1
    assert driver.local_buffer[0] == sample_data


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_collect_metrics(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()
    driver.local_buffer.append(data)

    # Mock `_create_gauge`
    with patch.object(
        driver, "_create_gauge", return_value=MagicMock()
    ) as mock_create_gauge:
        metrics = list(driver.collect())
        # Verify that metrics were collected
        assert len(metrics) > 0
        assert mock_create_gauge.call_count == 236
