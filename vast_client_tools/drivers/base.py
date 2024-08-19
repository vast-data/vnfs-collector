import abc
import argparse

from vast_client_tools.logger import get_logger, COLORS


class InvalidArgument(Exception):
    pass


def get_val_or_raise(args, key):
    val = getattr(args, key)
    if val is None:
        raise InvalidArgument(f"the following arguments are required: --{key}")
    return val


class DriverBase(abc.ABC):
    parser = NotImplemented

    def __init__(self, common_args: argparse.Namespace):
        self.name = self.__class__.__name__.lower().replace("driver", "")
        self.common_args = common_args
        self.logger = get_logger(self.name, COLORS.blue)

    def __str__(self):
        raise NotImplementedError()

    __repr__ = __str__

    @abc.abstractmethod
    async def store_sample(self, data):
        pass

    async def setup(self, args=(), namespace=None):
        self.logger.info("Setting up driver.")
        if namespace:
            namespace = argparse.Namespace(**namespace)
            # Create a new namespace with default values and update it from action defaults.
            for action in self.parser._actions:
                dest = action.dest
                if not hasattr(namespace, dest):
                    if action.default is not argparse.SUPPRESS:
                        setattr(namespace, dest, action.default)
                    if action.required:
                        get_val_or_raise(namespace, dest)
                # Validate choices if available
                if action.choices:
                    value = getattr(namespace, dest, argparse.SUPPRESS)
                    if value != argparse.SUPPRESS and value not in action.choices:
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
            return namespace
        try:
            args, _ = self.parser.parse_known_args(args)
            return args
        except SystemExit as e:
            raise InvalidArgument() from e

    async def teardown(self):
        pass
