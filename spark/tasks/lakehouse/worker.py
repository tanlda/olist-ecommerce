from __future__ import annotations

import os
import argparse
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from exceptions import SparkSkip

os.environ["SPARK_VERSION"] = "3.5"  # pydeequ


class LakehouseWorker(ABC):
    def __init__(self, name: str, args: argparse.Namespace):
        self.dataframe: DataFrame | None = None
        self.name = name
        self.args = args

    def get_spark(self) -> SparkSession:
        spark = (
            SparkSession.builder.appName(self.args.table_name)
            # Iceberg REST catalog
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.defaultCatalog", "iceberg")
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "rest")
            .config("spark.sql.catalog.iceberg.uri", "http://iceberg.io")
            .config("spark.sql.catalog.iceberg.warehouse", "s3://ecommerce/iceberg/")
            .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password")
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://lakehouse.io")
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
            # Reading files from S3
            .config("spark.hadoop.fs.s3a.access.key", "admin")
            .config("spark.hadoop.fs.s3a.secret.key", "password")
            .config("spark.hadoop.fs.s3a.endpoint", "http://lakehouse.io")
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )

        return spark

    def get_object_name(self) -> str:
        table = self.args.table_name
        date = self.args.date_cursor.strip()
        object_name = f"s3a://ecommerce/landing/{table}/{table}.{date}.csv"
        return object_name

    def get_pydeequ_repository(self, task: str):
        from pydeequ.repository import FileSystemMetricsRepository, ResultKey

        assert task in ("checks", "metrics", "profiles")

        spark = self.get_spark()
        table = self.args.table_name
        date = self.args.date_cursor.strip()
        repository = FileSystemMetricsRepository(spark, f"s3a://ecommerce/landing/{table}/{table}.{date}.{task}.json")
        result_key = ResultKey(spark, ResultKey.current_milli_time(), {"tag": self.args.table_name})
        return repository, result_key

    def stop_spark(self):
        spark = self.get_spark()
        spark.sparkContext._gateway.shutdown_callback_server()  # noqa
        spark.stop()

    @abstractmethod
    def get_dataframe(self, force: bool = False, clean: bool = True, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def test(self):
        pass

    @abstractmethod
    def ingest(self):
        pass

    @abstractmethod
    def profile(self):
        pass

    def analyse(self):
        """ TODO: from pydeequ.analyzers import * """

    @staticmethod
    def factory(args: argparse.Namespace) -> LakehouseWorker:
        from orders import LakehouseOrdersWorker
        from order_items import LakehouseOrderItemsWorker
        from order_reviews import LakehouseOrderReviewsWorker
        from order_payments import LakehouseOrderPaymentsWorker

        mapping = {
            "orders": LakehouseOrdersWorker,
            "order_items": LakehouseOrderItemsWorker,
            "order_reviews": LakehouseOrderReviewsWorker,
            "order_payments": LakehouseOrderPaymentsWorker,
        }

        worker_class = mapping.get(args.table_name)
        if not worker_class:
            raise RuntimeError("FATAL")

        worker = worker_class(name=args.table_name, args=args)
        return worker


def main():
    parser = argparse.ArgumentParser()
    tables = ["orders", "order_items", "order_reviews", "order_payments"]
    parser.add_argument("--table-name", required=True, choices=tables)
    parser.add_argument("--date-cursor", required=True)
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--profile", action="store_true")

    args = parser.parse_args()
    worker = LakehouseWorker.factory(args)

    try:
        if args.test:
            worker.test()

        if args.ingest:
            worker.ingest()

        if args.profile:
            worker.profile()
    except SparkSkip:
        pass
    except Exception:
        raise
    finally:
        worker.stop_spark()


if __name__ == "__main__":
    main()
