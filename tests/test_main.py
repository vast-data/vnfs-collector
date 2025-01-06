from pathlib import Path
import pyarrow as pa

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from vast_client_tools.main import _exec

ROOT = Path(__file__).parent.resolve()

mock_schema = pa.schema(
    [
        ("ENV_VAR_1", pa.string()),
        ("ENV_VAR_2", pa.string()),
        ("column_1", pa.int32()),
    ]
)

@pytest.mark.asyncio
@patch(
    "vast_client_tools.main.await_until_event_or_timeout", AsyncMock(return_value=True)
)
@patch("vast_client_tools.main.StatsCollector", MagicMock())
class TestMainSuite:

    async def test_no_driver(self, capfd):
        with pytest.raises(SystemExit) as e:
            await _exec()
            assert e.value.code == 2
        out, err = capfd.readouterr()
        assert "No driver specified" in err

    @pytest.mark.parametrize("from_config", [True, False])
    @pytest.mark.parametrize(
        "cmd, expected_error",
        [
            ("-d=screen", None),
            ("-d=screen --table-format", None),
            (
                f"-d=screen -d=file --samples-path={ROOT} --max-backups=5 --max-size-mb=200",
                "is directory",
            ),
            (
                "-d=kafka --bootstrap-servers=foobar",
                "the following arguments are required:",
            ),
            ("-d=kafka --topic=my-topic", "the following arguments are required:"),
            (
                "-d=kafka --bootstrap-servers=foobar:abc --topic=foobar",
                "invalid literal for int",
            ),
            (
                "-d=kafka --bootstrap-servers=foobar:10 --topic=foobar --security-protocol=PLAINTEXT "
                "--sasl-username=1 --sasl-password=1",
                "SASL credentials provided but security_protocol is PLAINTEXT",
            ),
            (
                "-d=kafka --bootstrap-servers=foobar:10 --topic=foobar --security-protocol=PLAINTEXT "
                "--ssl-ca-cert=/ca --ssl-cert=/cert --ssl-key=/key",
                "SSL context provided but security_protocol is PLAINTEXT",
            ),
            # Invalid integer value
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic --max-request-size=notanumber",
                "invalid int value",
            ),
            # Invalid choice for compression-type
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic --compression-type=invalid",
                "invalid choice",
            ),
            # Invalid choice for sasl-mechanism
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic --sasl-mechanism=invalid",
                "invalid choice",
            ),
            # # Missing SASL credentials for SASL_PLAINTEXT
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic --security-protocol=SASL_PLAINTEXT",
                "SASL_PLAINTEXT requires SASL credentials.",
            ),
            # # Missing SASL credentials and SSL context for SASL_SSL
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic --security-protocol=SASL_SSL",
                "SASL_SSL requires SASL credentials.",
            ),
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic"
                " --security-protocol=SASL_SSL --sasl-username=1 --sasl-password=1",
                "SASL_SSL requires both client certificate and key.",
            ),
            # Missing SSL context for SSL
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic --security-protocol=SSL",
                "SSL requires both client certificate and key.",
            ),
            # SASL credentials provided but security_protocol is PLAINTEXT
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic "
                "--security-protocol=PLAINTEXT --sasl-username=user --sasl-password=pass",
                "SASL credentials provided but security_protocol is PLAINTEXT.",
            ),
            # SSL context provided but security_protocol is PLAINTEXT
            (
                "-d=kafka --bootstrap-servers=broker1:9092 --topic=my-topic"
                " --security-protocol=PLAINTEXT --ssl-ca-cert=/path/to/ca"
                " --ssl-cert=/path/to/cert --ssl-key=/path/to/key",
                "SSL context provided but security_protocol is PLAINTEXT.",
            ),
            (
                "-d=vdb --bootstrap-servers=broker1:9092 --topic=my-topic",
                "the following arguments are required",
            ),
            (
                "-d=vdb --db-endpoint=http://test --db-access-key=1 --db-schema=1",
                "the following arguments are required",
            ),
            (
                "-d=vdb --db-tenant=my-tenant --db-bucket=my-bucket"
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                "Cannot specify both --db-tenant and --db-bucket, --db-schema, or --db-table together.",
            ),
            (
                "-d=vdb --db-tenant=my-tenant --db-schema=my-schema"
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                "Cannot specify both --db-tenant and --db-bucket, --db-schema, or --db-table together.",
            ),
            (
                "-d=vdb --db-tenant=my-tenant --db-table=my-table "
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                "Cannot specify both --db-tenant and --db-bucket, --db-schema, or --db-table together.",
            ),
            (
                "-d=vdb --db-tenant=my-tenant --db-bucket=my-bucket --db-schema=my-schema"
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                "Cannot specify both --db-tenant and --db-bucket, --db-schema, or --db-table together.",
            ),
            (
                "-d=vdb --db-tenant=my-tenant --db-bucket=my-bucket --db-table=my-table"
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                "Cannot specify both --db-tenant and --db-bucket, --db-schema, or --db-table together.",
            ),
            (
                "-d=vdb --db-bucket=my-bucket --db-schema=my-schema --db-table=my-table"
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                None,
            ),
    #         # Valid case without db-tenant
            (
                "-d=vdb --db-tenant=my-tenant"
                " --db-endpoint=http://test --db-access-key=test --db-secret-key=test",
                None,
            ),
        ],
    )
    @patch("vast_client_tools.drivers.vdb_driver.VdbDriver._get_vdb_schema", MagicMock(return_value=mock_schema))
    async def test_parsed_arguments(
        self, capfd, cli_factory, config_factory, cmd, expected_error, from_config
    ):
        """Test errors propagation and consistency between CLI arguments and arguments passed as configuration."""
        if from_config:
            config_file = config_factory(cmd=cmd)
            cmd = f"-C={config_file}"

        cli_factory(cmd=cmd)
        if expected_error:
            with patch("vast_client_tools.main.logger.error") as m_logger:
                await _exec()
            _, err = capfd.readouterr()
            error_log = m_logger.call_args[0][0]
            assert expected_error in err or expected_error in error_log
        else:
            await _exec()


    @pytest.mark.parametrize("from_config", [True, False])
    @pytest.mark.parametrize(
        "cmd, unknown_option",
        [

            ("-d=screen --ttable-format", "--ttable-format"),
            ("-d=screen --ttable=format", "--ttable"),
            ("-d=screen --table-fofrmat=abc", "--table-fofrmat"),
            ("-d=file --vacuum-interval=600", "--vacuum-interval"),
            ("-d=file --vacuum-interval 600", "--vacuum-interval"),
            (
                "-d=kafka --bootstrap-serverss=foobar",
                "--bootstrap-serverss",
            ),
            (
                "-d=kafka --bootstrap-servers=foobar --topic=foobar --security-protocol=PLAINTEXT "
                "--sasl-username=1 --sasl-passwordd=1",
                "--sasl-passwordd",
            ),
            (
                "-d=vdb --db-endpoint=http://test --vdb-access-key=1 --db-schema=1",
                "--vdb-access-key",
            ),
        ],
    )
    async def test_invalid_parsed_arguments(
        self, capfd, cli_factory, config_factory, cmd, unknown_option, from_config
    ):
        if from_config:
            config_file = config_factory(cmd=cmd)
            cmd = f"-C={config_file}"

        cli_factory(cmd=cmd)
        with patch("vast_client_tools.main.logger.error") as m_logger:
            try:
                await _exec()
            except SystemExit:
                pass
        _, err = capfd.readouterr()
        if from_config:
            unknown_option = unknown_option.lstrip("-").replace("-", "_")
        assert f"Unknown option '{unknown_option}"  in err
