import pytest
import argparse
import asyncio
import pandas as pd

from vnfs_collector.utils import (
    InvalidArgument,
    parse_args_options_from_namespace,
    iso_serializer,
    unix_serializer,
    maybe_list_parse,
    await_until_event_or_timeout,
    get_val_or_raise,
    flatten_keys,
)


def test_get_val_or_raise_with_value():
    class Args:
        def __init__(self, value):
            self.key = value

    args = Args("value")
    result = get_val_or_raise(args, "key")
    assert result == "value"


def test_get_val_or_raise_with_none_value():
    class Args:
        def __init__(self):
            self.key = None

    args = Args()
    with pytest.raises(InvalidArgument):
        get_val_or_raise(args, "key")


def test_iso_serializer_with_timestamp():
    timestamp = pd.Timestamp("2024-01-01T00:00:00")
    result = iso_serializer(timestamp)
    assert result == "2024-01-01T00:00:00"


def test_iso_serializer_with_bytes():
    result = iso_serializer(b"some bytes")
    assert result == "some bytes"


def test_iso_serializer_with_unsupported_type():
    with pytest.raises(TypeError):
        iso_serializer(123)


def test_unix_serializer_with_timestamp():
    timestamp = pd.Timestamp("2024-01-01T00:00:00")
    result = unix_serializer(timestamp)
    assert result == int(timestamp.timestamp())


def test_unix_serializer_with_bytes():
    result = unix_serializer(b"some bytes")
    assert result == "some bytes"


def test_unix_serializer_with_unsupported_type():
    with pytest.raises(TypeError):
        unix_serializer(123)


def test_parse_args_with_kebab_case():
    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", type=int, default=10)

    namespace = {"example-arg": "20"}
    result = parse_args_options_from_namespace(namespace, parser)

    assert result.example_arg == 20


def test_parse_args_with_default_value():
    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", type=int, default=10)

    namespace = {}
    result = parse_args_options_from_namespace(namespace, parser)

    assert result.example_arg == 10


def test_parse_args_with_choices():
    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", choices=["a", "b", "c"])

    namespace = {"example-arg": "b"}
    result = parse_args_options_from_namespace(namespace, parser)

    assert result.example_arg == "b"


def test_parse_args_with_default_choice():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--example-arg", dest="example_arg", choices=["a", "b", "c"], default="a"
    )

    namespace = {"example-arg": "a"}
    result = parse_args_options_from_namespace(namespace, parser)

    assert result.example_arg == "a"


def test_parse_args_with_invalid_choice():
    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", choices=["a", "b", "c"])

    namespace = {"example-arg": "d"}

    with pytest.raises(InvalidArgument):
        parse_args_options_from_namespace(namespace, parser)


def test_parse_args_with_integer_type():
    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", type=int)

    namespace = {"example-arg": "42"}
    result = parse_args_options_from_namespace(namespace, parser)

    assert result.example_arg == 42


def test_parse_args_with_invalid_integer_type():
    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", type=int)

    namespace = {"example-arg": "invalid"}

    with pytest.raises(InvalidArgument):
        parse_args_options_from_namespace(namespace, parser)


def test_parse_args_with_custom_type():
    def custom_type(value):
        return value.upper()

    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", type=custom_type)

    namespace = {"example-arg": "lowercase"}
    result = parse_args_options_from_namespace(namespace, parser)

    assert result.example_arg == "LOWERCASE"


def test_parse_args_with_required_argument():
    def mock_get_val_or_raise(namespace, dest):
        # Mock behavior for required argument
        if dest not in namespace:
            raise InvalidArgument(f"Required argument '{dest}' is missing.")

    global get_val_or_raise
    get_val_or_raise = mock_get_val_or_raise

    parser = argparse.ArgumentParser()
    parser.add_argument("--example-arg", dest="example_arg", required=True)

    namespace = {}

    with pytest.raises(InvalidArgument):
        parse_args_options_from_namespace(namespace, parser)


def test_maybe_list_parse_with_string():
    result = maybe_list_parse("a, b, c")
    assert result == ["a", "b", "c"]


def test_maybe_list_parse_with_list():
    result = maybe_list_parse(["a", "b", "c"])
    assert result == ["a", "b", "c"]


def test_maybe_list_parse_with_empty_string():
    result = maybe_list_parse("")
    assert result == [""]


@pytest.mark.asyncio
async def test_await_until_event_or_timeout_with_timeout():
    stop_event = asyncio.Event()
    canceled = await await_until_event_or_timeout(1, stop_event)
    assert canceled is False


def test_empty_dict():
    """Test flatten_keys with an empty dictionary."""
    assert flatten_keys({}) == []


def test_flat_dict():
    """Test flatten_keys with a flat dictionary."""
    data = {"a": 1, "b": 2, "c": 3}
    assert set(flatten_keys(data)) == {"a", "b", "c"}


def test_nested_dict():
    """Test flatten_keys with a nested dictionary."""
    data = {"a": 1, "b": {"c": 2, "d": 3}}
    assert set(flatten_keys(data)) == {"a", "b", "c", "d"}


def test_deeply_nested_dict():
    """Test flatten_keys with a deeply nested dictionary."""
    data = {"a": {"b": {"c": {"d": 1}}}}
    assert set(flatten_keys(data)) == {"a", "b", "c", "d"}


def test_mixed_types():
    """Test flatten_keys with mixed value types."""
    data = {"a": 1, "b": [1, 2, 3], "c": {"d": {"e": "value"}}}
    assert set(flatten_keys(data)) == {"a", "b", "c", "d", "e"}
