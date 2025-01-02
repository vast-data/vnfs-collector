import argparse

import pyarrow as pa

from vast_client_tools.drivers.base import DriverBase
from vast_client_tools.utils import InvalidArgument


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

    async def setup(self, args=(), namespace=None):
        from vastdb.api import VastdbApi

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
        vastapi = VastdbApi(host=self.db_endpoint, access_key=self.db_access_key,
                            secret_key=self.db_secret_key, secure=self.db_ssl_verify)
        if not vastapi.list_schemas(bucket=self.db_bucket, schema=self.db_schema):
            raise ValueError(f"Schema {self.db_schema} does not exist.")

        _, _, tables, *_ = vastapi.list_tables(
            bucket=self.db_bucket, schema=self.db_schema, name_prefix=self.db_table, exact_match=True
        )
        if not tables:
            raise ValueError(f"Table {self.db_table} does not exist.")

        columns, *_ = vastapi.list_columns(bucket=self.db_bucket, schema=self.db_schema, table=self.db_table)
        fields = [pa.field(name, dtype) for name, dtype, *_ in columns]
        self.arrow_schema = pa.schema(fields)
        self.logger.info(f"{self} has been initialized.")
        del vastapi

    async def store_sample(self, data):
        from vastdb.api import VastdbApi

        df_copy = data.copy()
        df_copy['TAGS'] = df_copy['TAGS'].apply(lambda d: list(d.items()))
        df_copy['TIMEDELTA'] = self.common_args.interval
        record_batch = pa.RecordBatch.from_pandas(df=df_copy, schema=self.arrow_schema)
        vastapi = VastdbApi(
            host=self.db_endpoint,
            access_key=self.db_access_key,
            secret_key=self.db_secret_key,
            secure=self.db_ssl_verify
        )
        vastapi.insert(
            bucket=self.db_bucket,
            schema=self.db_schema,
            table=self.db_table,
            record_batch=record_batch,
        )
        del vastapi
