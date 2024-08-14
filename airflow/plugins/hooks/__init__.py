from minio import Minio, S3Error
from airflow.hooks.base import BaseHook
from pyiceberg.catalog import Catalog, load_catalog

__all__ = ["MinioHook", "IcebergHook"]


class MinioHook(BaseHook):
    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "minio"
    hook_name = "Minio"

    def __init__(self, minio_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.minio_conn_id = minio_conn_id
        self.connection = self.get_connection(self.minio_conn_id)
        self.extras = self.connection.extra_dejson.copy()
        self.client: Minio | None = None

    def get_conn(self) -> Minio:
        conn = self.connection

        if self.client is None:
            self.client = Minio(
                endpoint=conn.host,
                access_key=conn.login,
                secret_key=conn.password,
                region=conn.extra_dejson.get('region', None),
                secure=conn.extra_dejson.get('secure', False)
            )

        return self.client

    def object_exist(self, bucket_name: str, object_name: str) -> bool:
        client = self.get_conn()
        try:
            client.stat_object(bucket_name, object_name)
            return True
        except S3Error:
            return False

    def get_storage_options(self, fs: bool = False) -> dict:
        conn = self.connection

        if fs:
            storage_options = {
                "key": conn.login,
                "secret": conn.password,
                "client_kwargs": {
                    "region_name": self.extras.get('region', None),
                    "endpoint_url": "http://" + conn.host,
                    "verify": False
                }
            }
        else:
            storage_options = {
                "aws_access_key_id": conn.login,
                "aws_secret_access_key": conn.password,
                "aws_region": self.extras.get('region', None),
                "endpoint_url": "http://" + conn.host,
            }

        return storage_options


class IcebergHook(BaseHook):
    conn_name_attr = "iceberg_conn_id"
    default_conn_name = "iceberg_default"
    conn_type = "http"
    hook_name = "Iceberg"

    def __init__(self, iceberg_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.iceberg_conn_id = iceberg_conn_id
        self.connection = self.get_connection(self.iceberg_conn_id)
        self.extras = self.connection.extra_dejson.copy()
        self.catalog: Catalog | None = None

    def get_conn(self) -> Catalog:
        conn = self.connection

        if self.catalog is None:
            properties = {
                "uri": self.extras.get("uri"),
                "s3.access-key-id": conn.login,
                "s3.secret-access-key": conn.password,
                "s3.endpoint": self.extras.get("endpoint", None),
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }

            self.catalog = load_catalog("iceberg", **properties)

        return self.catalog
