import pytest
import argparse
import json
from unittest.mock import AsyncMock, MagicMock
from vast_client_tools.drivers import KafkaDriver


@pytest.mark.asyncio
async def test_kafka_driver(data):
    # We should expect also 'JOB' env in sample headers.
    driver = KafkaDriver(common_args=argparse.Namespace(envs=["JOB"]))
    driver.producer = MagicMock()
    driver.topic = "my-topic"
    send = AsyncMock(return_value=AsyncMock())
    driver.producer.send = send

    await driver.store_sample(data)
    assert send.await_count == len(data)

    for exec, (_, raw) in zip(send.await_args_list, data.iterrows()):
        kwargs = exec.kwargs
        assert kwargs["topic"] == driver.topic
        restored_data = json.loads(kwargs["value"])
        assert restored_data == raw.to_dict()
        headers = kwargs["headers"]
        assert headers[0][1].decode() == raw.HOSTNAME
        assert headers[1][1].decode() == str(raw.UID)
        assert headers[2][1].decode() == raw.COMM
        assert headers[3][1].decode() == raw.MOUNT
        assert headers[4][1].decode() == raw.REMOTE_PATH
        if "JOB" in raw.TAGS:
            assert headers[5][0] == "JOB"
        assert (
            kwargs["key"].decode() == f"{raw.HOSTNAME}:{raw.COMM}:{raw.UID}:{raw.PID}"
        )
