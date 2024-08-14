from worker import WarehouseWorker
from pyspark.sql import DataFrame
from exceptions import SparkSkip

__all__ = ["WarehouseGeolocationWorker"]


class WarehouseGeolocationWorker(WarehouseWorker):
    def get_dataframe(self) -> DataFrame:
        spark = self.get_spark()

        if not spark.catalog.tableExists("iceberg.ecommerce.geolocation"):
            return spark.createDataFrame([], "")

        df = spark.sql("SELECT * FROM iceberg.ecommerce.geolocation")
        return df

    def ingest(self):
        df = self.get_dataframe()
        url, properties = self.get_jdbc()

        if df.isEmpty():
            raise SparkSkip()

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="geolocation",
            properties=properties,
        )
