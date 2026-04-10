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
async def test_store_sample_single(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()

    await driver.store_samples([data])

    # Check if the sample data is stored (aggregated)
    assert driver.latest_sample is not None


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_sample_aggregates_batch(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()

    # Store a batch of identical samples
    batch = [data, data]
    await driver.store_samples(batch)

    # The aggregated result should have summed stats
    assert driver.latest_sample is not None
    # After aggregation, we should have fewer or equal rows than original
    assert len(driver.latest_sample) <= len(data)


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_collect_is_readonly(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()
    await driver.store_samples([data])

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
    await driver.store_samples([data])

    # Mock `_create_gauge`
    with patch.object(
        driver, "_create_gauge", return_value=MagicMock()
    ) as mock_create_gauge:
        metrics = list(driver.collect())
        # Verify that metrics were collected
        assert len(metrics) > 0
        # After aggregation, the call count depends on number of unique label combinations
        assert mock_create_gauge.call_count > 0


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_empty_batch():
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=[]))
    await driver.setup()

    await driver.store_samples([])

    # Empty batch should set latest_sample to None
    assert driver.latest_sample is None


@pytest.mark.asyncio
@patch("prometheus_client.start_http_server", MagicMock())
@patch("prometheus_client.REGISTRY.unregister", MagicMock())
async def test_store_empty_batch_clears_stale_data(data):
    driver = PrometheusDriver(common_args=argparse.Namespace(envs=["JOB"]))
    await driver.setup()

    # First store some data
    await driver.store_samples([data])
    assert driver.latest_sample is not None

    # Then store empty batch (simulates all workloads stopped)
    await driver.store_samples([])

    # Stale data should be cleared
    assert driver.latest_sample is None
