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
        db_access_key = args.db_access_key
        db_secret_key = args.db_secret_key
        self.db_bucket = args.db_bucket
        self.db_schema = args.db_schema
        self.db_table = args.db_table
        self.db_ssl_verify = args.db_ssl_verify
        self.vastapi = VastdbApi(
            host=self.db_endpoint, access_key=db_access_key, secret_key=db_secret_key, secure=self.db_ssl_verify
        )
        if not self.vastapi.list_schemas(bucket=self.db_bucket, schema=self.db_schema):
            self.vastapi.create_schema(self.db_bucket, self.db_schema)
        if not self.vastapi.list_tables(
                bucket=self.db_bucket, schema=self.db_schema, name_prefix=self.db_table, exact_match=True
        ):
            self.vastapi.create_table(
                bucket=self.db_bucket, schema=self.db_schema, name=self.db_table, arrow_schema=self.get_columns()
            )
        self.logger.info(f"{self} has been initialized.")

    async def store_sample(self, data):
        record_batch = pa.RecordBatch.from_pandas(df=data, schema=self.get_columns())
        self.vastapi.insert(
            bucket=self.db_bucket, schema=self.db_schema, table=self.db_table, record_batch=record_batch
        )

    @classmethod
    def get_columns(cls):
        return pa.schema([
            ('TIMESTAMP', pa.timestamp('s')),
            ('HOSTNAME', pa.utf8()),
            ('PID', pa.uint32()),
            ('UID', pa.uint32()),
            ('COMM', pa.utf8()),
            ('OPEN_COUNT', pa.uint32()),
            ('OPEN_ERRORS', pa.uint32()),
            ('OPEN_DURATION', pa.float64()),
            ('CLOSE_COUNT', pa.uint32()),
            ('CLOSE_ERRORS', pa.uint32()),
            ('CLOSE_DURATION', pa.float64()),
            ('READ_COUNT', pa.uint32()),
            ('READ_ERRORS', pa.uint32()),
            ('READ_DURATION', pa.float64()),
            ('READ_BYTES', pa.uint32()),
            ('WRITE_COUNT', pa.uint32()),
            ('WRITE_ERRORS', pa.uint32()),
            ('WRITE_DURATION', pa.float64()),
            ('WRITE_BYTES', pa.uint32()),
            ('GETATTR_COUNT', pa.uint32()),
            ('GETATTR_ERRORS', pa.uint32()),
            ('GETATTR_DURATION', pa.float64()),
            ('SETATTR_COUNT', pa.uint32()),
            ('SETATTR_ERRORS', pa.uint32()),
            ('SETATTR_DURATION', pa.float64()),
            ('FLUSH_COUNT', pa.uint32()),
            ('FLUSH_ERRORS', pa.uint32()),
            ('FLUSH_DURATION', pa.float64()),
            ('FSYNC_COUNT', pa.uint32()),
            ('FSYNC_ERRORS', pa.uint32()),
            ('FSYNC_DURATION', pa.float64()),
            ('LOCK_COUNT', pa.uint32()),
            ('LOCK_ERRORS', pa.uint32()),
            ('LOCK_DURATION', pa.float64()),
            ('MMAP_COUNT', pa.uint32()),
            ('MMAP_ERRORS', pa.uint32()),
            ('MMAP_DURATION', pa.float64()),
            ('READDIR_COUNT', pa.uint32()),
            ('READDIR_ERRORS', pa.uint32()),
            ('READDIR_DURATION', pa.float64()),
            ('CREATE_COUNT', pa.uint32()),
            ('CREATE_ERRORS', pa.uint32()),
            ('CREATE_DURATION', pa.float64()),
            ('LINK_COUNT', pa.uint32()),
            ('LINK_ERRORS', pa.uint32()),
            ('LINK_DURATION', pa.float64()),
            ('UNLINK_COUNT', pa.uint32()),
            ('UNLINK_ERRORS', pa.uint32()),
            ('UNLINK_DURATION', pa.float64()),
            ('SYMLINK_COUNT', pa.uint32()),
            ('SYMLINK_ERRORS', pa.uint32()),
            ('SYMLINK_DURATION', pa.float64()),
            ('LOOKUP_COUNT', pa.uint32()),
            ('LOOKUP_ERRORS', pa.uint32()),
            ('LOOKUP_DURATION', pa.float64()),
            ('RENAME_COUNT', pa.uint32()),
            ('RENAME_ERRORS', pa.uint32()),
            ('RENAME_DURATION', pa.float64()),
            ('ACCESS_COUNT', pa.uint32()),
            ('ACCESS_ERRORS', pa.uint32()),
            ('ACCESS_DURATION', pa.float64()),
            ('LISTXATTR_COUNT', pa.uint32()),
            ('LISTXATTR_ERRORS', pa.uint32()),
            ('LISTXATTR_DURATION', pa.float64()),
            ('TAGS', pa.map_(pa.string(), pa.string())),  # The TAGS column may be manipulated by the client.
            ('MOUNT', pa.utf8())
        ])
