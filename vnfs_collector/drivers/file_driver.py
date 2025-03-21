# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import json
import logging
import argparse
from pathlib import Path
from logging.handlers import RotatingFileHandler

from vnfs_collector.drivers.base import DriverBase, InvalidArgument
from vnfs_collector.utils import iso_serializer

class FileDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '--samples-path',
        help='Absolute or relative path to file where samples will be stored.',
        default="/opt/vnfs-collector/vnfs-collector.log"
    )
    parser.add_argument(
        '--max-backups',
        help='Maximum number of backup files to keep before overwriting the oldest.',
        type=int,
        default=5
    )
    parser.add_argument(
        '--max-size-mb',
        help=' Maximum size (in megabytes) per log file before rotation occurs.',
        type=int,
        default=200
    )

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(path={self.path.as_posix()}, "
            f"max_size_mb={self.max_size_mb}, "
            f"max_backups={self.max_backups})"
        )

    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        self.samples_logger = logging.getLogger("samples_logger")
        self.samples_logger.setLevel(logging.DEBUG)
        self.samples_logger.propagate = False

        self.path = Path(args.samples_path)
        self.path.parent.mkdir(exist_ok=True)
        if self.path.is_dir():
            raise InvalidArgument(f"{self.path} is directory.")

        self.max_size_mb = args.max_size_mb
        self.max_backups = args.max_backups
        self.samples_logger.handlers = [
            RotatingFileHandler(
                filename=self.path,
                maxBytes=args.max_size_mb * 1024 * 1024,
                backupCount=self.max_backups
            )
        ]
        self.logger.info(f"{self} has been initialized.")

    async def store_sample(self, data):
        for _, entry in data.iterrows():
            self.samples_logger.debug(json.dumps(entry.to_dict(), default=iso_serializer))
