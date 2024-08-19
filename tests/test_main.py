from pathlib import Path

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from vast_client_tools.main import _exec

ROOT = Path(__file__).parent.resolve()


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
                "-d=vdb --db-endpoint=1 --vdb-access-key=1 --vdb-schema=1",
                "the following arguments are required",
            ),
        ],
    )
    async def test_parsed_arguments(
        self, capfd, cli_factory, config_factory, cmd, expected_error, from_config
    ):
        """Test errors propagation and consistency between CLI arguments and arguments passed as configuration."""
        if from_config:
            config_file = config_factory(cmd=cmd)
            cmd = f"-C={config_file}"

        cli_factory(cmd=cmd)
        if expected_error:
            with pytest.raises(Exception) as ctx:
                await _exec()
            _, err = capfd.readouterr()
            assert expected_error in err or expected_error in str(ctx.value)
        else:
            await _exec()
