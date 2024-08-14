import abc
import argparse
from vast_client_tools.logger import get_logger, COLORS


class InvalidArgument(Exception):
    pass


def get_val_or_raise(args, key):
    val = getattr(args, key)
    if val is None:
        raise InvalidArgument(f"Missing required argument: {key}")
    return val


class DriverBase(abc.ABC):
    parser = NotImplemented

    def __init__(self, envs: list):
        self.name = self.__class__.__name__.lower().replace("driver", "")
        self.envs = envs
        self.logger = get_logger(self.name, COLORS.blue)

    def __str__(self):
        raise NotImplementedError()

    __repr__ = __str__

    @abc.abstractmethod
    async def store_sample(self, data):
        pass

    async def setup(self, args=(), namespace=None):
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
            return namespace
        try:
            args, _ = self.parser.parse_known_args(args)
            return args
        except SystemExit as e:
            raise InvalidArgument() from e

    def teardown(self):
        pass
