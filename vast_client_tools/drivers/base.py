import abc
import argparse

from vast_client_tools.logger import get_logger, COLORS
from vast_client_tools.utils import InvalidArgument, parse_args_options_from_namespace


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
            if not isinstance(namespace, dict):
                raise InvalidArgument(
                    f"Invalid argument '{namespace}'."
                    f" Check available arguments for {self.__class__.__name__} driver."
                )
            return parse_args_options_from_namespace(namespace=namespace, parser=self.parser)
        try:
            args, _ = self.parser.parse_known_args(args)
            return args
        except SystemExit as e:
            raise InvalidArgument() from e

    async def teardown(self):
        pass
