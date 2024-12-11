import argparse

import pyarrow as pa

from vast_client_tools.drivers.base import DriverBase


class VdbDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--db-endpoint', type=str, required=True, help='Database endpoint.')
    parser.add_argument('--db-access-key', type=str, required=True, help='Database access key.')
    parser.add_argument('--db-secret-key', type=str, required=True, help='Database secret key.')
    parser.add_argument('--db-bucket', type=str, required=True, help='Database bucket.')
    parser.add_argument('--db-schema', type=str, required=True, help='Database schema.')
    parser.add_argument('--db-table', type=str, default='nfsops_metrics', help='Database table.')
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

    async def setup(self, args=(), namespace=None):
        from vastdb.api import VastdbApi

        args = await super().setup(args, namespace)
        self.db_endpoint = args.db_endpoint.strip("http://").strip("https://")
        self.db_access_key = args.db_access_key
        self.db_secret_key = args.db_secret_key
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
        df_copy = data.copy()
        df_copy['TAGS'] = df_copy['TAGS'].apply(lambda d: list(d.items()))
        record_batch = pa.RecordBatch.from_pandas(df=df_copy, schema=self.arrow_schema)
        vastapi = VastdbApi(host=self.db_endpoint, access_key=self.db_access_key,
                            secret_key=self.db_secret_key, secure=self.db_ssl_verify)
        vastapi.insert(bucket=self.db_bucket, schema=self.db_schema, table=self.db_table,
                       record_batch=record_batch)
        del vastapi
