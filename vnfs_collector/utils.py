# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import asyncio
import signal
import argparse
import inspect

import pandas as pd


class InvalidArgument(Exception):
    pass


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


def set_signal_handler(handler, loop):
    loop.add_signal_handler(signal.SIGINT, handler)
    loop.add_signal_handler(signal.SIGTERM, handler)


async def await_until_event_or_timeout(timeout: int, stop_event: asyncio.Event) -> bool:
    """
    Waits for either a specified timeout or an external event, whichever comes first.
    Args:
        timeout (int): The number of seconds to wait before timing out.
        stop_event (asyncio.Event): An asyncio Event object to wait for.

    Returns:
        bool: True if the timeout completed before the event, False otherwise.
    """
    sleep_task = asyncio.ensure_future(asyncio.sleep(timeout))
    cancel_task = asyncio.ensure_future(stop_event.wait())
    # Wait for either the sleep_task or cancel_task to complete
    done, pending = await asyncio.wait({sleep_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()

    return cancel_task in done


def parse_args_options_from_namespace(namespace, parser):
    """
    Parse the arguments and options from the namespace object.
    Primarily used to parse the arguments in kebab-case format and snake_case format uniformly.
    Args:
        namespace (dict): The namespace dict object representation to parse the arguments and options from.
        parser (argparse.ArgumentParser): The parser object to parse the arguments and options with.

    Returns:
        Modified namespace object with the parsed arguments and options.
    """
    namespace = argparse.Namespace(**namespace)
    # Create a new namespace with default values and update it from action defaults.
    for action in parser._actions:
        dest = action.dest
        dest_kebab_case = dest.replace("_", "-")
        if hasattr(namespace, dest_kebab_case):
            setattr(namespace, dest, getattr(namespace, dest_kebab_case))
        if not hasattr(namespace, dest):
            if action.default is not argparse.SUPPRESS:
                setattr(namespace, dest, action.default)
            if action.required:
                get_val_or_raise(namespace, dest)
        # Validate choices if available
        if action.choices:
            value = getattr(namespace, dest, argparse.SUPPRESS)
            if value != argparse.SUPPRESS and value not in set(action.choices) | {action.default}:
                raise InvalidArgument(
                    f"invalid choice '{value}' for argument '--{dest}'. Must be one of {action.choices}."
                )
        # Validate integer types
        if action.type == int:
            value = getattr(namespace, dest, argparse.SUPPRESS)
            if value != argparse.SUPPRESS:
                try:
                    # Ensure value can be converted to int and fits the integer type
                    int_value = int(value)
                    setattr(namespace, dest, int_value)
                except ValueError:
                    raise InvalidArgument(
                        f"invalid int value '{value}' for argument '--{dest}'."
                    )
        elif inspect.isfunction(action.type):
            value = getattr(namespace, dest, argparse.SUPPRESS)
            if value != argparse.SUPPRESS:
                setattr(namespace, dest, action.type(value))
    return namespace


def get_val_or_raise(args, key):
    val = getattr(args, key)
    if val is None:
        raise InvalidArgument(f"the following arguments are required: --{key}")
    return val


def maybe_list_parse(maybe_list):
    if isinstance(maybe_list, str):
        return list(map(str.strip, maybe_list.split(',')))
    return maybe_list


def maybe_bool_parse(maybe_bool):
    return str(maybe_bool).lower() in ("true", "1", "yes")



def flatten_keys(d):
    """Recursively flattens dictionary keys, including nested dictionaries."""
    keys = []
    for k, v in d.items():
        keys.append(k)
        if isinstance(v, dict):
            keys.extend(flatten_keys(v))
    return keys
