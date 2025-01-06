import pytest
from unittest.mock import patch, MagicMock
import argparse
from vast_client_tools.drivers import VdbDriver
from vast_client_tools.drivers.vdb_driver import VDBValidationError
import pyarrow as pa

COMMON_ARGS = dict(
    db_endpoint="https://test-db-endpoint.com",
    db_access_key="access-key",
    db_secret_key="secret-key",
    db_bucket="test-bucket",
    db_schema="test-schema",
    db_table="test-table",
    db_ssl_verify=True,
)


@pytest.fixture
def vdb_driver():
    common_args = argparse.Namespace(
        vdb_schema_refresh_interval=10, envs=[], envs_from_vdb_schema=True
    )
    return VdbDriver(common_args=common_args)


mock_cols = [
    ("ENV_var1", pa.string()),
    ("ENV_var2", pa.string()),
    ("SIMPLE_var1", pa.int64()),
    ("SIMPLE_var2", pa.float32()),
]


@pytest.mark.asyncio
@patch.object(VdbDriver, "_get_vdb_columns", MagicMock(return_value=mock_cols))
async def test_vdb_driver_initialization(vdb_driver):
    # Test if the driver initializes correctly with proper arguments
    args = dict(
        db_endpoint="https://test-db-endpoint.com",
        db_access_key="access-key",
        db_secret_key="secret-key",
        db_bucket="test-bucket",
        db_schema="test-schema",
        db_table="test-table",
        db_ssl_verify=True,
    )
    await vdb_driver.setup(namespace=args)

    assert vdb_driver.db_endpoint == "test-db-endpoint.com"
    assert vdb_driver.db_access_key == "access-key"
    assert vdb_driver.db_secret_key == "secret-key"
    assert vdb_driver.db_bucket == "test-bucket"
    assert vdb_driver.db_schema == "test-schema"
    assert vdb_driver.db_table == "test-table"
    assert vdb_driver.db_ssl_verify is True


@pytest.mark.asyncio
@patch("vastdb.api.VastdbApi")
async def test_refresh_vdb_schema(mock_vastdb_api, vdb_driver):
    # Test schema refresh and column fetching
    mock_vastdb_api.return_value.list_schemas.return_value = True
    mock_vastdb_api.return_value.list_tables.return_value = ("", "", ["test-table"])
    mock_vastdb_api.return_value.list_columns.return_value = [
        [
            ("ENV_VAR_1", pa.string()),
            ("ENV_VAR_2", pa.string()),
            ("column_1", pa.int32()),
        ]
    ]
    await vdb_driver.setup(namespace=COMMON_ARGS)
    assert vdb_driver.arrow_schema == pa.schema(
        [
            pa.field("ENV_VAR_1", pa.string()),
            pa.field("ENV_VAR_2", pa.string()),
            pa.field("column_1", pa.int32()),
        ]
    )
    # Check if the common environment variables are updated
    assert vdb_driver.common_args.envs == ["VAR_1", "VAR_2"]


@pytest.mark.asyncio
@patch("vastdb.api.VastdbApi")
async def test_vdb_validation_error(mock_vastdb_api, vdb_driver):
    # Test VDBValidationError when schema or table is missing
    mock_vastdb_api.return_value.list_schemas.return_value = (
        False  # Simulate non-existent schema
    )
    mock_vastdb_api.return_value.list_tables.return_value = (
        "",
        "",
        [],
    )  # Simulate no tables
    # Expect VDBValidationError to be raised when schema is not found
    with pytest.raises(VDBValidationError, match="Schema test-schema does not exist."):
        await vdb_driver.setup(namespace=COMMON_ARGS)


@pytest.mark.asyncio
@patch("vastdb.api.VastdbApi")
async def test_store_sample(mock_vastdb_api, vdb_driver, data):
    # Test storing a sample and interacting with the VastdbApi

    insert_mock = MagicMock()
    mock_vastdb_api.return_value.insert = insert_mock
    mock_vastdb_api.return_value.list_schemas.return_value = True
    mock_vastdb_api.return_value.list_tables.return_value = ("", "", ["test-table"])
    mock_vastdb_api.return_value.list_columns.return_value = [
        [
            ("PID", pa.int32()),
            ("OPEN_COUNT", pa.int32()),
            ("ENV_JOB", pa.string()),
        ]
    ]
    await vdb_driver.setup(namespace=COMMON_ARGS)

    await vdb_driver.store_sample(data)
    assert insert_mock.call_count == 1
    assert insert_mock.call_args.kwargs == {
        "bucket": "test-bucket",
        "schema": "test-schema",
        "table": "test-table",
        "rows": {
            "PID": [1, 1, 2, 2],
            "OPEN_COUNT": [2, 2, 1, 1],
            "ENV_JOB": ["1", "", "", "2"],
        },
    }


@pytest.mark.asyncio
@patch("vastdb.api.VastdbApi")
async def test_store_sample_fail(mock_vastdb_api, vdb_driver, data):
    # Test store_sample failure and retry mechanism
    mock_vastdb_api.return_value.insert.side_effect = ValueError("Insert failed")
    mock_vastdb_api.return_value.list_schemas.return_value = True
    mock_vastdb_api.return_value.list_tables.return_value = ("", "", ["test-table"])
    mock_vastdb_api.return_value.list_columns.return_value = [
        [
            ("PID", pa.int32()),
            ("OPEN_COUNT", pa.int32()),
            ("ENV_JOB", pa.string()),
            ("ENV_JOB1", pa.string()),
        ]
    ]
    await vdb_driver.setup(namespace=COMMON_ARGS)

    assert vdb_driver.common_args.envs == ["JOB", "JOB1"]
    mock_vastdb_api.return_value.list_columns.return_value = [
        [
            ("PID", pa.int32()),
            ("OPEN_COUNT", pa.int32()),
            ("ENV_JOB", pa.string()),
            ("ENV_JOB1", pa.string()),
            ("ENV_JOB2", pa.string()),
            ("ENV_JOB3", pa.string()),
        ]
    ]
    with pytest.raises(ValueError, match="Insert failed"):
        with patch.object(vdb_driver.logger, "warning") as mock_warning:
            await vdb_driver.store_sample(data, fail_on_error=False)
            mock_warning.assert_called_once_with("Insert failed")

    assert mock_vastdb_api.return_value.list_tables.call_count == 2
    # After the failure, the schema should be refreshed and the new columns should be fetched
    assert vdb_driver.common_args.envs == ["JOB", "JOB1", "JOB2", "JOB3"]
