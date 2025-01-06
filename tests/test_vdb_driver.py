import pytest
from unittest.mock import patch, MagicMock
import argparse
from vast_client_tools.drivers import VdbDriver
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
mock_schema = pa.schema(mock_cols)


@pytest.mark.asyncio
@patch.object(VdbDriver, "_get_vdb_schema", MagicMock(return_value=mock_schema))
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

    assert vdb_driver.db_endpoint == "https://test-db-endpoint.com"
    assert vdb_driver.db_access_key == "access-key"
    assert vdb_driver.db_secret_key == "secret-key"
    assert vdb_driver.db_bucket == "test-bucket"
    assert vdb_driver.db_schema == "test-schema"
    assert vdb_driver.db_table == "test-table"
    assert vdb_driver.db_ssl_verify is True


mock_schema = pa.schema(
    [
        ("ENV_VAR_1", pa.string()),
        ("ENV_VAR_2", pa.string()),
        ("column_1", pa.int32()),
    ]
)


@pytest.mark.asyncio
@patch.object(VdbDriver, "_get_vdb_schema", MagicMock(return_value=mock_schema))
async def test_refresh_vdb_schema(vdb_driver):
    vdb_driver.common_args.envs = ["VAR_1", "VAR_2"]
    await vdb_driver.setup(namespace=COMMON_ARGS)
    assert vdb_driver.arrow_schema == mock_schema
    assert vdb_driver.common_args.envs == ["VAR_1", "VAR_2"]


@pytest.mark.asyncio
@patch("vastdb.connect")  # Mock the vastdb.connect function
async def test_store_sample(mock_vastdb_connect, vdb_driver, data):
    # Test storing a sample and interacting with the VastdbApi
    mock_session = MagicMock()
    mock_transaction = MagicMock()
    mock_table = MagicMock()

    # Setting up the mock chain
    mock_vastdb_connect.return_value = mock_session
    mock_session.transaction.return_value.__enter__.return_value = mock_transaction
    mock_transaction.bucket.return_value.schema.return_value.table.return_value = (
        mock_table
    )

    insert_mock = MagicMock()
    mock_table.insert = insert_mock

    vdb_driver.db_endpoint = "test-endpoint"
    vdb_driver.db_access_key = "test-access-key"
    vdb_driver.db_secret_key = "test-secret-key"
    vdb_driver.db_ssl_verify = False
    vdb_driver.db_bucket = "test-bucket"
    vdb_driver.db_schema = "test-schema"

    vdb_driver._refresh_vdb_schema = MagicMock()
    vdb_driver.arrow_schema = pa.schema(
        [
            pa.field("PID", pa.int32()),
            pa.field("OPEN_COUNT", pa.int32()),
            pa.field("ENV_JOB", pa.string()),
        ]
    )
    await vdb_driver.setup(namespace=COMMON_ARGS)
    await vdb_driver.store_sample(data)
    rows = insert_mock.call_args.kwargs["rows"]
    rows_dict = {
        "PID": rows["PID"].to_pandas().tolist(),
        "OPEN_COUNT": rows["OPEN_COUNT"].to_pandas().tolist(),
        "ENV_JOB": rows["ENV_JOB"].to_pandas().tolist(),
    }
    # Assertions
    assert insert_mock.call_count == 1
    assert rows_dict == {
        "PID": [1, 1, 2, 2],
        "OPEN_COUNT": [2, 2, 1, 1],
        "ENV_JOB": ["1", "", "", "2"],
    }

    mock_vastdb_connect.assert_called_once_with(
        endpoint="https://test-db-endpoint.com",
        access="access-key",
        secret="secret-key",
        ssl_verify=True,
    )
    mock_session.transaction.assert_called_once()
    mock_transaction.bucket.assert_called_once_with("test-bucket")
    mock_transaction.bucket().schema.assert_called_once_with("test-schema")
    mock_transaction.bucket().schema().table.assert_called_once_with("test-table")
