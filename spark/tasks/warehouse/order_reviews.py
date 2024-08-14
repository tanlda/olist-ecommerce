import psycopg2 as ps
from worker import WarehouseWorker
from pyspark.sql import DataFrame
from exceptions import SparkSkip

__all__ = ["WarehouseOrderReviewsWorker"]


class WarehouseOrderReviewsWorker(WarehouseWorker):
    def get_dataframe(self) -> DataFrame:
        spark = self.get_spark()
        year, month, day = self.args.date_cursor.split("-")

        if not spark.catalog.tableExists("iceberg.ecommerce.order_reviews"):
            return spark.createDataFrame([], "")

        query = f"""
            SELECT * FROM iceberg.ecommerce.order_reviews
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
            table="order_reviews",
            properties=properties,
        )

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="order_reviews_staging",
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
            where="table_name = 'order_reviews' AND column_name != 'review_id' AND column_name != 'order_id';",
            template="""
                MERGE INTO order_reviews t
                USING order_reviews_staging s
                ON 
                    t.review_id = s.review_id AND
                    t.order_id = s.order_id
                WHEN MATCHED THEN
                    UPDATE SET %s
                WHEN NOT MATCHED THEN
                    INSERT (review_id, order_id, %s)
                    VALUES (s.review_id, s.order_id, %s)
            """
        )
        cursor.execute(merge_query)
        conn.commit()

        drop_query = "DROP TABLE IF EXISTS order_reviews_staging;"
        cursor.execute(drop_query)
        conn.commit()

        cursor.close()
        conn.close()
