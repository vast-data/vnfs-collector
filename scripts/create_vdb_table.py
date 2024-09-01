"""
Usage:
python create_vdb_table.py \
  --db-endpoint vippool-1.vast217-az.vasteng.lab \
  --db-access-key UHDG11ZBT46LBVJU8483 \
  --db-secret-key CgJvlHYzsZEa/kYCnNpJok+lzCpO/+JC5pOkoNmP \
  --db-bucket nfsops-metrics \
  --db-schema nfsops \
  --db-table nfsops \
  --db-ssl-verify False
"""

import argparse
from vastdb.api import VastdbApi
import pyarrow as pa
from requests.exceptions import HTTPError


arrow_schema = pa.schema(
    [
        ("TIMESTAMP", pa.timestamp("s")),
        ("HOSTNAME", pa.utf8()),
        ("PID", pa.uint32()),
        ("UID", pa.uint32()),
        ("COMM", pa.utf8()),
        ("OPEN_COUNT", pa.uint32()),
        ("OPEN_ERRORS", pa.uint32()),
        ("OPEN_DURATION", pa.float64()),
        ("CLOSE_COUNT", pa.uint32()),
        ("CLOSE_ERRORS", pa.uint32()),
        ("CLOSE_DURATION", pa.float64()),
        ("READ_COUNT", pa.uint32()),
        ("READ_ERRORS", pa.uint32()),
        ("READ_DURATION", pa.float64()),
        ("READ_BYTES", pa.uint32()),
        ("WRITE_COUNT", pa.uint32()),
        ("WRITE_ERRORS", pa.uint32()),
        ("WRITE_DURATION", pa.float64()),
        ("WRITE_BYTES", pa.uint32()),
        ("GETATTR_COUNT", pa.uint32()),
        ("GETATTR_ERRORS", pa.uint32()),
        ("GETATTR_DURATION", pa.float64()),
        ("SETATTR_COUNT", pa.uint32()),
        ("SETATTR_ERRORS", pa.uint32()),
        ("SETATTR_DURATION", pa.float64()),
        ("FLUSH_COUNT", pa.uint32()),
        ("FLUSH_ERRORS", pa.uint32()),
        ("FLUSH_DURATION", pa.float64()),
        ("FSYNC_COUNT", pa.uint32()),
        ("FSYNC_ERRORS", pa.uint32()),
        ("FSYNC_DURATION", pa.float64()),
        ("LOCK_COUNT", pa.uint32()),
        ("LOCK_ERRORS", pa.uint32()),
        ("LOCK_DURATION", pa.float64()),
        ("MMAP_COUNT", pa.uint32()),
        ("MMAP_ERRORS", pa.uint32()),
        ("MMAP_DURATION", pa.float64()),
        ("READDIR_COUNT", pa.uint32()),
        ("READDIR_ERRORS", pa.uint32()),
        ("READDIR_DURATION", pa.float64()),
        ("CREATE_COUNT", pa.uint32()),
        ("CREATE_ERRORS", pa.uint32()),
        ("CREATE_DURATION", pa.float64()),
        ("LINK_COUNT", pa.uint32()),
        ("LINK_ERRORS", pa.uint32()),
        ("LINK_DURATION", pa.float64()),
        ("UNLINK_COUNT", pa.uint32()),
        ("UNLINK_ERRORS", pa.uint32()),
        ("UNLINK_DURATION", pa.float64()),
        ("SYMLINK_COUNT", pa.uint32()),
        ("SYMLINK_ERRORS", pa.uint32()),
        ("SYMLINK_DURATION", pa.float64()),
        ("LOOKUP_COUNT", pa.uint32()),
        ("LOOKUP_ERRORS", pa.uint32()),
        ("LOOKUP_DURATION", pa.float64()),
        ("RENAME_COUNT", pa.uint32()),
        ("RENAME_ERRORS", pa.uint32()),
        ("RENAME_DURATION", pa.float64()),
        ("ACCESS_COUNT", pa.uint32()),
        ("ACCESS_ERRORS", pa.uint32()),
        ("ACCESS_DURATION", pa.float64()),
        ("MKDIR_COUNT", pa.uint32()),
        ("MKDIR_ERRORS", pa.uint32()),
        ("MKDIR_DURATION", pa.float64()),
        ("RMDIR_COUNT", pa.uint32()),
        ("RMDIR_ERRORS", pa.uint32()),
        ("RMDIR_DURATION", pa.float64()),
        ("LISTXATTR_COUNT", pa.uint32()),
        ("LISTXATTR_ERRORS", pa.uint32()),
        ("LISTXATTR_DURATION", pa.float64()),
        ("MOUNT", pa.utf8()),
        ('TAGS', pa.map_(pa.string(), pa.string())),
    ]
)


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Initialize VastDB schema and table.")
    parser.add_argument(
        "--db-endpoint", type=str, required=True, help="Database endpoint."
    )
    parser.add_argument(
        "--db-access-key", type=str, required=True, help="Database access key."
    )
    parser.add_argument(
        "--db-secret-key", type=str, required=True, help="Database secret key."
    )
    parser.add_argument("--db-bucket", type=str, required=True, help="Database bucket.")
    parser.add_argument("--db-schema", type=str, required=True, help="Database schema.")
    parser.add_argument(
        "--db-table", type=str, default="nfsops_metrics", help="Database table."
    )
    parser.add_argument(
        "--db-ssl-verify", type=bool, default=True, help="Verify HTTPS connection."
    )

    # Parse arguments
    args = parser.parse_args()

    db_endpoint = args.db_endpoint
    db_access_key = args.db_access_key
    db_secret_key = args.db_secret_key
    db_bucket = args.db_bucket
    db_schema = args.db_schema
    db_table = args.db_table
    db_ssl_verify = False

    # Initialize VastDB API
    vastapi = VastdbApi(
        host=db_endpoint,
        access_key=db_access_key,
        secret_key=db_secret_key,
        secure=db_ssl_verify,
    )
    # Check if the schema exists, if not, create it
    try:
        vastapi.list_schemas(bucket=db_bucket, schema=db_schema)
    except HTTPError as e:
        if e.response.status_code == 404:
            vastapi.create_schema(bucket=db_bucket, name=db_schema)
        else:
            raise

    _, _, tables, *_ = vastapi.list_tables(
        bucket=db_bucket, schema=db_schema, name_prefix=db_table, exact_match=True
    )
    if not tables:
        vastapi.create_table(
            bucket=db_bucket, schema=db_schema, name=db_table, arrow_schema=arrow_schema
        )


if __name__ == "__main__":
    main()
