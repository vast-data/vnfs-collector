# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import argparse
from datetime import datetime, timedelta

import vastdb
from vastdb.errors import NotFound
import pyarrow as pa

from vnfs_collector.drivers.base import DriverBase
from vnfs_collector.utils import InvalidArgument

ENV_VAR_PREFIX = "ENV_"

class VDBValidationError(Exception):
    pass

class VdbDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--db-endpoint', type=str, required=True, help='Database endpoint.')
    parser.add_argument('--db-access-key', type=str, required=True, help='Database access key.')
    parser.add_argument('--db-secret-key', type=str, required=True, help='Database secret key.')
    parser.add_argument(
        '--db-bucket',
        type=str,
        help=(
            'Specify the database bucket name.\n'
        )
    )
    parser.add_argument(
        '--db-schema',
        type=str,
        default="vast_client_metrics_schema",
        help=(
            'Specify the database schema name.\n'
        )
    )
    parser.add_argument(
        '--db-table',
        type=str,
        default="vast_client_metrics_table",
        help=(
            'Specify the database table name.\n'
        )
    )
    parser.add_argument('--db-ssl-verify', type=bool, default=True, help='Verify https connection.')

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(endpoint={self.db_endpoint}, "
            f"bucket={self.db_bucket}, "
            f"schema={self.db_schema}, "
            f"table={self.db_table}, "
            f"ssl_verify={self.db_ssl_verify})"
        )

    @property
    def should_read_envs(self):
        """Determine if common environment variables can be read from the schema."""
        if not self.envs_from_vdb_schema:
            return False
        elif datetime.now() - self.read_db_schema_ts < self.vdb_schema_refresh_interval:
            return False
        return True

    def _get_vdb_schema(self):
        """Fetch the column definitions for the configured database table."""
        with vastdb.connect(
            endpoint=self.db_endpoint,
            access=self.db_access_key,
            secret=self.db_secret_key,
            ssl_verify=self.db_ssl_verify,
            timeout=15,
        ).transaction() as tx:
            table = tx.bucket(self.db_bucket).schema(self.db_schema).table(self.db_table)
            return table.arrow_schema

    def _refresh_vdb_schema(self):
        """Refresh the schema of the database table and update common environment variables if needed."""
        self.arrow_schema = self._get_vdb_schema()
        envs = set()
        common_envs = set(self.common_args.envs or [])
        for col in self.arrow_schema:
            if col.name.startswith(ENV_VAR_PREFIX):
                original_name = col.name[len(ENV_VAR_PREFIX):]
                envs.add(original_name)
                if self.envs_from_vdb_schema and col.type != pa.string():
                    raise VDBValidationError(
                        f"Wrong type of {col.name!r}. "
                        f"Only 'string' type is acceptable for ENV_ columns."
                    )

        if self.envs_from_vdb_schema:
            added_columns = envs - common_envs
            removed_columns = common_envs - envs
            if added_columns:
                self.logger.info(f"ENV columns added: " + ",".join(added_columns))
            if removed_columns:
                self.logger.info(f"ENV columns removed: " + ",".join(removed_columns))
            self.common_args.envs = sorted(envs)
        self.read_db_schema_ts = datetime.now()


    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        if not args.db_endpoint.startswith(("http", "https")):
            raise InvalidArgument("Database endpoint must start with 'http' or 'https'.")

        self.db_endpoint = args.db_endpoint
        self.read_db_schema_ts = datetime(1970, 1, 1)
        self.vdb_schema_refresh_interval = timedelta(seconds=self.common_args.vdb_schema_refresh_interval)
        self.envs_from_vdb_schema = self.common_args.envs_from_vdb_schema
        self.db_access_key = args.db_access_key
        self.db_secret_key = args.db_secret_key
        self.db_bucket = args.db_bucket
        self.db_schema = args.db_schema
        self.db_table = args.db_table
        self.db_ssl_verify = args.db_ssl_verify
        self._refresh_vdb_schema()
        self.logger.info(f"{self} has been initialized.")

    async def store_sample(self, data, fail_on_error=False):
        if self.should_read_envs:
            self._refresh_vdb_schema()

        rows = {}
        tags = data.TAGS.to_list()
        for col in self.arrow_schema:
            if col.name.startswith(ENV_VAR_PREFIX):
                original_name = col.name[len(ENV_VAR_PREFIX):]
                rows[col.name] = [t.get(original_name, "") for t in tags]
            else:
                rows[col.name] = data[col.name].to_list()

        session = vastdb.connect(
            endpoint=self.db_endpoint,
            access=self.db_access_key,
            secret=self.db_secret_key,
            ssl_verify=self.db_ssl_verify,
        )
        with session.transaction() as tx:
            table = tx.bucket(self.db_bucket).schema(self.db_schema).table(self.db_table)
            try:
                table.insert(rows=pa.Table.from_pydict(rows, schema=self.arrow_schema))
            except (ValueError, NotFound) as exc:
                if self.envs_from_vdb_schema and not fail_on_error:
                    self.read_db_schema_ts = datetime(1970, 1, 1)
                    self._refresh_vdb_schema()
                    await self.store_sample(data, fail_on_error=True)
                else:
                    raise exc
            finally:
                del session
