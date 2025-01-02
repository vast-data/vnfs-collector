import argparse
from datetime import datetime, timedelta

import pyarrow as pa

from vast_client_tools.drivers.base import DriverBase
from vast_client_tools.utils import InvalidArgument

ENV_VAR_PREFIX = "ENV_"

class VDBValidationError(Exception):
    pass

class VdbDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--db-endpoint', type=str, required=True, help='Database endpoint.')
    parser.add_argument('--db-access-key', type=str, required=True, help='Database access key.')
    parser.add_argument('--db-secret-key', type=str, required=True, help='Database secret key.')
    parser.add_argument(
        '--db-tenant',
        type=str,
        default="default",
        help=(
            'Specify the tenant name. When provided, it is combined with --db-bucket to form the actual bucket name, '
            'e.g., "default-vast-client-metrics-bucket".\n'
            ' Note: If --db-tenant is used, --db-bucket, --db-schema, and --db-table cannot be specified.\n'
        )
    )
    parser.add_argument(
        '--db-bucket',
        type=str,
        default="vast-client-metrics-bucket",
        help=(
            'Specify the database bucket name. It is combined with the tenant (if provided) to form the actual bucket name.\n'
            ' Note: If --db-tenant is specified, this argument should not be used.\n'
        )
    )
    parser.add_argument(
        '--db-schema',
        type=str,
        default="vast_client_metrics_schema",
        help=(
            'Specify the database schema name.\n'
            ' Note: This argument should not be used when --db-tenant is specified.\n'
        )
    )
    parser.add_argument(
        '--db-table',
        type=str,
        default="vast_client_metrics_table",
        help=(
            'Specify the database table name.\n'
            ' Note: This argument should not be used when --db-tenant is specified.\n'
        )
    )
    parser.add_argument('--db-ssl-verify', type=bool, default=True, help='Verify https connection.')

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(endpoint={self.db_endpoint}, "
            f"tenant={self.db_tenant}, "
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

    def _get_vdb_columns(self):
        """Fetch the column definitions for the configured database table."""
        from vastdb.api import VastdbApi

        vastapi = VastdbApi(host=self.db_endpoint, access_key=self.db_access_key,
                            secret_key=self.db_secret_key, secure=self.db_ssl_verify)
        if not vastapi.list_schemas(bucket=self.db_bucket, schema=self.db_schema):
            raise VDBValidationError(f"Schema {self.db_schema} does not exist.")

        _, _, tables, *_ = vastapi.list_tables(
            bucket=self.db_bucket, schema=self.db_schema, name_prefix=self.db_table, exact_match=True
        )
        if not tables:
            raise VDBValidationError(f"Table {self.db_table} does not exist.")

        columns, *_ = vastapi.list_columns(bucket=self.db_bucket, schema=self.db_schema, table=self.db_table)
        return columns

    def _refresh_vdb_schema(self):
        """Refresh the schema of the database table and update common environment variables if needed."""
        columns = self._get_vdb_columns()
        fields = []
        envs = set()
        common_envs = set(self.common_args.envs or [])
        for name, dtype, *_ in columns:
            if name.startswith(ENV_VAR_PREFIX):
                original_name = name[len(ENV_VAR_PREFIX):]
                envs.add(original_name)
                if self.envs_from_vdb_schema and dtype != pa.string():
                    raise VDBValidationError(
                        f"Wrong type of {name!r}. "
                        f"Only 'string' type is acceptable for ENV_ columns."
                    )
            fields.append(pa.field(name, dtype))

        if self.envs_from_vdb_schema:
            added_columns = envs - common_envs
            removed_columns = common_envs - envs
            if added_columns:
                self.logger.info(f"ENV columns added: " + ",".join(added_columns))
            if removed_columns:
                self.logger.info(f"ENV columns removed: " + ",".join(removed_columns))
            self.common_args.envs = sorted(envs)

        self.arrow_schema = pa.schema(fields)
        self.read_db_schema_ts = datetime.now()


    async def setup(self, args=(), namespace=None):
        args = await super().setup(args, namespace)
        if args.db_endpoint.startswith("http://"):
            self.db_endpoint = args.db_endpoint[len("http://"):]
        elif args.db_endpoint.startswith("https://"):
            self.db_endpoint = args.db_endpoint[len("https://"):]
        else:
            self.db_endpoint = args.db_endpoint

        # Check if the user has provided a db_tenant and make sure no other db args are provided if db_tenant is used
        if args.db_tenant != self.parser.get_default("db_tenant") and (
                args.db_bucket != self.parser.get_default("db_bucket") or
                args.db_schema != self.parser.get_default("db_schema") or
                args.db_table != self.parser.get_default("db_table")
        ):
            raise InvalidArgument(
                "Cannot specify both --db-tenant and --db-bucket, --db-schema, or --db-table together."
            )
        self.read_db_schema_ts = datetime(1970, 1, 1)
        self.vdb_schema_refresh_interval = timedelta(seconds=self.common_args.vdb_schema_refresh_interval)
        self.envs_from_vdb_schema = self.common_args.envs_from_vdb_schema
        self.db_access_key = args.db_access_key
        self.db_secret_key = args.db_secret_key
        self.db_tenant = args.db_tenant

        if args.db_bucket == self.parser.get_default("db_bucket"):
            self.db_bucket = f"{self.db_tenant}-{args.db_bucket}"
        else:
            self.db_bucket = args.db_bucket

        self.db_schema = args.db_schema
        self.db_table = args.db_table
        self.db_ssl_verify = args.db_ssl_verify
        self._refresh_vdb_schema()
        self.logger.info(f"{self} has been initialized.")

    async def store_sample(self, data, fail_on_error=False):
        from vastdb.api import VastdbApi

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

        vastapi = VastdbApi(
            host=self.db_endpoint,
            access_key=self.db_access_key,
            secret_key=self.db_secret_key,
            secure=self.db_ssl_verify
        )
        try:
            vastapi.insert(
                bucket=self.db_bucket,
                schema=self.db_schema,
                table=self.db_table,
                rows=rows
            )
        except ValueError as exc:
            exc_str = str(exc)
            if self.envs_from_vdb_schema and not fail_on_error:
                self.logger.warning(exc_str)
                self.read_db_schema_ts = datetime(1970, 1, 1)
                self._refresh_vdb_schema()
                await self.store_sample(data, fail_on_error=True)
            else:
                raise exc
        finally:
            del vastapi
