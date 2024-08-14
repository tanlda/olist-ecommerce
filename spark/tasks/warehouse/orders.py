import psycopg2 as ps
from worker import WarehouseWorker
from pyspark.sql import DataFrame
from exceptions import SparkSkip

__all__ = ["WarehouseOrdersWorker"]


class WarehouseOrdersWorker(WarehouseWorker):
    def get_dataframe(self) -> DataFrame:
        spark = self.get_spark()
        year, month, day = self.args.date_cursor.split("-")

        if not spark.catalog.tableExists("iceberg.ecommerce.orders"):
            return spark.createDataFrame([], "")

        query = f"""
            SELECT * FROM iceberg.ecommerce.orders
            WHERE year = '{year}' AND month = '{month}' AND day = '{day}'
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
            table="orders",
            properties=properties,
        )

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="orders_staging",
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

        merge_query = self.template_merge_query(
            where="table_name = 'orders' AND column_name != 'order_id';",
            template="""
                MERGE INTO orders t
                USING orders_staging s
                ON t.order_id = s.order_id
                WHEN MATCHED THEN
                    UPDATE SET %s
                WHEN NOT MATCHED THEN
                    INSERT (order_id, %s)
                    VALUES (s.order_id, %s)
            """
        )
        cursor.execute(merge_query)
        conn.commit()

        drop_query = "DROP TABLE IF EXISTS orders_staging;"
        cursor.execute(drop_query)
        conn.commit()

        cursor.close()
        conn.close()
