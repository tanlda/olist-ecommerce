from __future__ import annotations

import argparse
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from exceptions import SparkSkip


class WarehouseWorker(ABC):
    def __init__(self, name: str, args: argparse.Namespace):
        self.name = name
        self.args = args

    def get_spark(self) -> SparkSession:
        spark = (
            SparkSession.builder.appName(self.args.table_name)
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
            .getOrCreate()
        )

        return spark

    def get_jdbc(self):
        properties = dict(
            user="admin",
            password="password",
            driver="org.postgresql.Driver",
            prepareThreshold="0",
        )

        url = "jdbc:postgresql://warehouse.io:5432/ecommerce"
        return url, properties

    def template_merge_query(self, where: str, template: str):
        merge_query_tpl = """
            DO $$
            DECLARE
                columns text;
                update_set text;
                insert_columns text;
                insert_values text;
            BEGIN
                SELECT 
                    string_agg(column_name, ', '),
                    string_agg(column_name || ' = s.' || column_name, ', '),
                    string_agg('t.' || column_name, ', '),
                    string_agg('s.' || column_name, ', ')
                INTO
                    columns,
                    update_set,
                    insert_columns,
                    insert_values
                FROM
                    information_schema.columns
                WHERE
                    {where}

                EXECUTE format(
                    '{template}',
                    update_set, columns, insert_values
                );
            END $$
        """
        return merge_query_tpl.format(where=where, template=template)

    def stop_spark(self):
        spark = self.get_spark()
        spark.sparkContext._gateway.shutdown_callback_server()  # noqa
        spark.stop()

    @abstractmethod
    def ingest(self):
        pass

    def test(self):
        """ TODO: dbt """

    @staticmethod
    def factory(args: argparse.Namespace) -> WarehouseWorker:
        from sellers import WarehouseSellersWorker
        from products import WarehouseProductsWorker
        from customers import WarehouseCustomersWorker
        from geolocation import WarehouseGeolocationWorker

        from orders import WarehouseOrdersWorker
        from order_items import WarehouseOrderItemsWorker
        from order_reviews import WarehouseOrderReviewsWorker
        from order_payments import WarehouseOrderPaymentsWorker

        mapping = {
            "sellers": WarehouseSellersWorker,
            "products": WarehouseProductsWorker,
            "customers": WarehouseCustomersWorker,
            "geolocation": WarehouseGeolocationWorker,

            "orders": WarehouseOrdersWorker,
            "order_items": WarehouseOrderItemsWorker,
            "order_reviews": WarehouseOrderReviewsWorker,
            "order_payments": WarehouseOrderPaymentsWorker,
        }

        worker_class = mapping.get(args.table_name)
        if not worker_class:
            raise RuntimeError("FATAL")

        worker = worker_class(name=args.table_name, args=args)
        return worker


def main():
    parser = argparse.ArgumentParser()
    tables = [
        "sellers",
        "products",
        "customers",
        "geolocation",
        "orders",
        "order_items",
        "order_reviews",
        "order_payments",
    ]
    parser.add_argument("--table-name", required=True, choices=tables)
    parser.add_argument("--date-cursor", required=True)
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--ingest", action="store_true")

    args = parser.parse_args()
    worker = WarehouseWorker.factory(args)

    try:
        if args.ingest:
            worker.ingest()
        if args.test:
            worker.test()
    except SparkSkip:
        pass
    except Exception:
        raise
    finally:
        worker.stop_spark()


if __name__ == "__main__":
    main()
