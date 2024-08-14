from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from hooks import MinioHook, IcebergHook
from utils import pre_execute_skip_tasks

default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "trigger_rule": "all_done",
    "pre_execute": pre_execute_skip_tasks,
    "retry_delay": timedelta(seconds=30),
    "retries": 0,
}


@dag(
    catchup=False,
    schedule=None,
    start_date=datetime(2024, 8, 13),
    tags=["Onetime"]
)
def ecommerce_sync_customers_etl():
    @task(task_id="upload_landing")
    def upload_landing_customers():
        import polars as pl

        postgres = PostgresHook(postgres_conn_id="database")
        client = MinioHook(minio_conn_id="lakehouse").get_conn()

        with postgres.get_conn().cursor() as cursor:
            query = "SELECT * FROM customers"
            df = pl.read_database(query=query, connection=cursor)

        with NamedTemporaryFile() as tmpfile:
            df.write_csv(tmpfile.name)
            bucket_name = "ecommerce"
            object_name = f"landing/customers/customers.csv"
            client.fput_object(bucket_name, object_name, tmpfile.name)

    @task(task_id="sync_lakehouse")
    def sync_lakehouse_customers():
        import polars as pl
        import pyarrow as pa
        from pyiceberg.catalog import Catalog

        minio = MinioHook(minio_conn_id="lakehouse").get_conn()
        catalog: Catalog = IcebergHook(iceberg_conn_id="iceberg").get_conn()
        response = minio.get_object("ecommerce", "landing/customers/customers.csv")
        df: pa.Table = pl.read_csv(response.read()).to_arrow()

        if not df.num_rows:
            return

        table = catalog.create_table_if_not_exists(
            "ecommerce.customers",
            schema=df.schema
        )

        table.overwrite(df)

    def sync_warehouse_customers():
        return SparkSubmitOperator(
            conn_id="spark",
            task_id="sync_warehouse",
            deploy_mode="cluster",
            conf={
                "spark.executor.instances": "1",
                "spark.kubernetes.namespace": "spark",
                "spark.kubernetes.container.image": "registry.io/spark",
                "spark.kubernetes.container.image.pullPolicy": "Always",
                "spark.kubernetes.driver.podTemplateFile": "/opt/spark/templates/driver.yaml",
                "spark.kubernetes.executor.podTemplateFile": "/opt/spark/templates/executor.yaml",
            },
            application="local:///opt/spark/work-dir/tasks/warehouse/worker.py",
            application_args=[
                "--table-name", "customers",
                "--date-cursor", "",
                "--ingest"
            ]
        )

    upload_landing = upload_landing_customers()
    sync_lakehouse = sync_lakehouse_customers()
    sync_warehouse = sync_warehouse_customers()

    upload_landing >> sync_lakehouse >> sync_warehouse


ecommerce_sync_customers_etl()
