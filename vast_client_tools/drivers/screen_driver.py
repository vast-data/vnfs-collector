# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import json
import argparse

from vast_client_tools.drivers.base import DriverBase
from vast_client_tools.utils import iso_serializer


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
        if self.table_format:
            output = data.T.to_string(index=True, header=False)
        else:
            output = "\n".join(json.dumps(d.to_dict(), default=iso_serializer) for _, d in data.iterrows())
        self.logger.info(f">>>\n{output}")
