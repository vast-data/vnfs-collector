import argparse

from vast_client_tools.nfsops import group_stats
from vast_client_tools.drivers.base import DriverBase


class ScreenDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--table-format', action="store_true", help='Tabulated output.')

    def __str__(self):
        return f"{self.__class__.__name__}(table_format={self.table_format})"

    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        self.table_format = args.table_format
        self.logger.info(f"{self} has been initialized.")

    async def store_sample(self, data):
        if self.common_args.squash_pid:
            data = group_stats(data, ["MOUNT", "COMM"])
        else:
            data = group_stats(data, ["PID", "MOUNT", "COMM"])
        if self.table_format:
            output = data.T.to_string(index=False)
        else:
            output = "\n".join(str(d.to_dict()) for _, d in data.iterrows())
        self.logger.info(f">>>\n{output}")
