from worker import WarehouseWorker
from pyspark.sql import DataFrame
from exceptions import SparkSkip

__all__ = ["WarehouseCustomersWorker"]


class WarehouseCustomersWorker(WarehouseWorker):
    def get_dataframe(self) -> DataFrame:
        spark = self.get_spark()

        if not spark.catalog.tableExists("iceberg.ecommerce.customers"):
            return spark.createDataFrame([], "")

        df = spark.sql("SELECT * FROM iceberg.ecommerce.customers")
        return df

    def ingest(self):
        df = self.get_dataframe()
        url, properties = self.get_jdbc()

        if df.isEmpty():
            raise SparkSkip()

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="customers",
            properties=properties,
        )
