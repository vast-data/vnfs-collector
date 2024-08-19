import asyncio
import signal

import pandas as pd


# Serializer for ISO 8601 format
def iso_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError(f"Type {type(obj)} not serializable")


# Serializer for Unix timestamp (u64)
def unix_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        # Convert Timestamp to Unix timestamp (seconds since epoch)
        return int(obj.timestamp())
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError(f"Type {type(obj)} not serializable")


def set_signal_handler(handler):
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


async def await_until_event_or_timeout(timeout: int, stop_event: asyncio.Event) -> bool:
    """
    Waits for either a specified timeout or an external event, whichever comes first.
    Args:
        timeout (int): The number of seconds to wait before timing out.
        stop_event (asyncio.Event): An asyncio Event object to wait for.

    Returns:
        bool: True if the timeout completed before the event, False otherwise.
    """
    sleep_task = asyncio.create_task(asyncio.sleep(timeout))
    cancel_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait({sleep_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()

    return cancel_task in done
