import boto3
import psycopg2
import polars as pl

from minio import Minio
from typing import Optional
from pymongo import MongoClient
from pyspark.sql import SparkSession
from dagster import ConfigurableResource
from tempfile import NamedTemporaryFile

__all__ = [
    "GlobalResource",
    "LocalResource",
    "StorageResource",
    "DatabaseResource",
    "DocumentResource",
    "LakehouseResource",
    "IcebergResource",
    "PyDeequResource",
    "WarehouseResource",
]


class GlobalResource(ConfigurableResource):
    materialize: bool = False
    delete: bool = False
    drop: bool = False


class LocalResource(ConfigurableResource):
    version_id: Optional[str] = None


class StorageResource(ConfigurableResource):
    endpoint: str
    region_name: str
    bucket_name: str
    access_key: str
    secret_key: str
    secure: Optional[bool] = False

    def get_client(self) -> Minio:
        client = Minio(
            endpoint=self.endpoint,
            region=self.region_name,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        if not client.bucket_exists(self.bucket_name):
            client.make_bucket(self.bucket_name)

        return client

    def get_session(self):
        session = boto3.resource(
            "s3",
            verify=False,
            use_ssl=False,
            endpoint_url="http://" + self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=None,
        ).meta.client

        return session

    def get_storage_options(self) -> dict:
        options = {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_endpoint_url": "http://" + self.endpoint,
            "aws_region": self.region_name,
        }

        return options

    def read_csv(self, object_name: str):
        client = self.get_client()
        with NamedTemporaryFile() as tmpfile:
            client.fget_object(self.bucket_name, object_name, tmpfile.name)
            df = pl.read_csv(tmpfile.name)
            return df


class DatabaseResource(ConfigurableResource):
    host: str
    database: str
    username: str
    password: str
    port: int = 5432

    def get_connection(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_psycopg(self, autocommit: bool = False):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.database,
        )

        conn.autocommit = autocommit
        return conn

    def get_jdbc_connection(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


class DocumentResource(ConfigurableResource):
    host: str
    username: str
    password: str
    port: int = 27017

    def get_client(self) -> MongoClient:
        return MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            authSource="admin",
            authMechanism="SCRAM-SHA-256",
        )


class LakehouseResource(ConfigurableResource):
    endpoint: str
    region_name: str
    bucket_name: str
    access_key: str
    secret_key: str
    secure: Optional[bool] = False

    def get_client(self) -> Minio:
        client = Minio(
            endpoint=self.endpoint,
            region=self.region_name,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        if not client.bucket_exists(self.bucket_name):
            client.make_bucket(self.bucket_name)

        return client

    def get_session(self):
        session = boto3.resource(
            "s3",
            verify=False,
            use_ssl=False,
            endpoint_url="http://" + self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=None,
        ).meta.client

        return session

    def get_storage_options(self) -> dict:
        options = {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_endpoint_url": "http://" + self.endpoint,
            "aws_region": self.region_name,
        }

        return options


class WarehouseResource(ConfigurableResource):
    host: str
    database: str
    username: str
    password: str
    port: int = 5432

    def get_connection(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_psycopg(self, autocommit: bool = False):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.database,
        )

        conn.autocommit = autocommit
        return conn

    def get_jdbc_connection(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def get_jdbc_properties(self) -> dict:
        properties = dict(
            user=self.username,
            password=self.password,
            driver="org.postgresql.Driver",
            prepareThreshold="0",
        )

        return properties

    def get_jdbc(self):
        return self.get_jdbc_connection(), self.get_jdbc_properties()


class IcebergResource(ConfigurableResource):
    uri: str
    endpoint: str
    access_key: str
    secret_key: str
    warehouse: str
    secure: Optional[bool] = False
    region: Optional[str] = "us-east-1"
    impl: Optional[str] = "pyiceberg.io.pyarrow.PyArrowFileIO"

    def get_spark(
            self,
            app_name: str,
            connect_document: bool = False,
            connect_lakehouse: bool = False,
    ):
        session = SparkSession.builder.appName(app_name)

        session = (
            session
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.defaultCatalog", "iceberg")
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "rest")
            .config("spark.sql.catalog.iceberg.uri", self.uri)
            .config("spark.sql.catalog.iceberg.warehouse", self.warehouse)
            .config("spark.sql.catalog.iceberg.client.region", self.region)
            .config("spark.sql.catalog.iceberg.s3.access-key-id", self.access_key)
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", self.secret_key)
            .config("spark.sql.catalog.iceberg.s3.endpoint", self.endpoint)
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        )

        if connect_document:
            session = (
                session
                .config("spark.mongodb.read.connection.uri",
                        "mongodb://admin:password@document.io/ecommerce?authSource=admin")
            )

        if connect_lakehouse:
            session = (
                session
                .config("spark.hadoop.fs.s3a.access.key", self.access_key)
                .config("spark.hadoop.fs.s3a.secret.key", self.secret_key)
                .config("spark.hadoop.fs.s3a.endpoint", self.endpoint)
                .config("spark.hadoop.fs.s3a.endpoint.region", self.region)
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            )

        spark = session.getOrCreate()

        return spark


class PyDeequResource(ConfigurableResource):
    def get_repository(self, spark: SparkSession, task: str, table: str, partition: str = None):
        from pydeequ.repository import FileSystemMetricsRepository, ResultKey

        assert task in ("checks", "metrics", "profiles")
        path = f"s3a://ecommerce/pydeequ/{table}/{table}.{task}.json"
        if partition:
            path = f"s3a://ecommerce/pydeequ/{table}/{table}.{partition}.{task}.json"
        result_key = ResultKey(spark, ResultKey.current_milli_time(), {"tag": table})
        repository = FileSystemMetricsRepository(spark, path)
        return repository, result_key
