import psycopg2 as ps
from worker import WarehouseWorker
from pyspark.sql import DataFrame
from exceptions import SparkSkip

__all__ = ["WarehouseOrderItemsWorker"]


class WarehouseOrderItemsWorker(WarehouseWorker):
    def get_dataframe(self) -> DataFrame:
        spark = self.get_spark()
        year, month, day = self.args.date_cursor.split("-")

        if not spark.catalog.tableExists("iceberg.ecommerce.order_items"):
            return spark.createDataFrame([], "")

        query = f"""
            SELECT items.*
            FROM iceberg.ecommerce.orders orders
            LEFT JOIN iceberg.ecommerce.order_items items ON orders.order_id = items.order_id
            WHERE orders.year = '{year}' AND orders.month = '{month}' AND orders.day = '{day}'
        """

        df = spark.sql(query)
        return df

    def ingest(self):
        df = self.get_dataframe()
        url, properties = self.get_jdbc()

        if df.isEmpty():
            raise SparkSkip()

        df.write.jdbc(
            url=url,
            mode="ignore",
            table="order_items",
            properties=properties,
        )

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="order_items_staging",
            properties=properties,
        )

        conn = ps.connect(
            host="warehouse.io",
            port=5432,
            user="admin",
            password="password",
            dbname="ecommerce",
        )
        cursor = conn.cursor()

        merge_query = """
            MERGE INTO order_items t
            USING order_items_staging s
            ON
                t.order_id = s.order_id AND
                t.product_id = s.product_id AND
                t.seller_id = s.seller_id AND
                t.order_item_id = s.order_item_id AND
                t.shipping_limit_date = s.shipping_limit_date
            WHEN NOT MATCHED THEN
                INSERT (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
                VALUES (order_id, s.order_item_id, s.product_id, s.seller_id, s.shipping_limit_date, s.price, s.freight_value)
        """
        cursor.execute(merge_query)
        conn.commit()

        drop_query = "DROP TABLE IF EXISTS order_items_staging;"
        cursor.execute(drop_query)
        conn.commit()

        cursor.close()
        conn.close()
