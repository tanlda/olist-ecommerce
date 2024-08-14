import psycopg2 as ps
from worker import WarehouseWorker
from pyspark.sql import DataFrame
from exceptions import SparkSkip

__all__ = ["WarehouseOrderPaymentsWorker"]


class WarehouseOrderPaymentsWorker(WarehouseWorker):
    def get_dataframe(self) -> DataFrame:
        spark = self.get_spark()
        year, month, day = self.args.date_cursor.split("-")

        if not spark.catalog.tableExists("iceberg.ecommerce.order_payments"):
            return spark.createDataFrame([], "")

        query = f"""
            SELECT * FROM iceberg.ecommerce.order_payments
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
            table="order_payments",
            properties=properties,
        )

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="order_payments_staging",
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
            where="table_name = 'order_payments' AND column_name != 'order_id';",
            template="""
                MERGE INTO order_payments t
                USING order_payments_staging s
                ON 
                    t.order_id = s.order_id AND
                    t.payment_type = s.payment_type AND
                    t.payment_sequential = s.payment_sequential AND
                    t.payment_installments = s.payment_installments
                WHEN MATCHED THEN
                    UPDATE SET %s
                WHEN NOT MATCHED THEN
                    INSERT (order_id, %s)
                    VALUES (s.order_id, %s)
            """
        )
        cursor.execute(merge_query)
        conn.commit()

        drop_query = "DROP TABLE IF EXISTS order_payments_staging;"
        cursor.execute(drop_query)
        conn.commit()

        cursor.close()
        conn.close()
