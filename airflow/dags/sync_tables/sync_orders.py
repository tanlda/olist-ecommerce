from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from hooks import MinioHook
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
    # schedule="@daily",
    schedule="*/3 * * * *",
    start_date=datetime(2024, 8, 13),
    tags=["Daily"]
)
def ecommerce_sync_orders_etl():
    @task(task_id="upload_landing")
    def upload_landing_orders():
        import polars as pl

        date_cursor = Variable.get("orders_date_cursor", default_var="2017-01-01")
        conn = PostgresHook(postgres_conn_id="database").get_conn()
        with conn.cursor() as cursor:
            query = rf"""
                SELECT *
                FROM orders
                WHERE date_trunc('day', order_purchase_timestamp) = date '{date_cursor}' 
            """
            df = pl.read_database(query=query, connection=cursor)

        client = MinioHook(minio_conn_id="lakehouse").get_conn()
        with NamedTemporaryFile() as tmpfile:
            df.write_csv(tmpfile.name)
            bucket_name = "ecommerce"
            object_name = f"landing/orders/orders.{date_cursor}.csv"
            client.fput_object(bucket_name, object_name, tmpfile.name)

        conn.close()

    def sync_lakehouse_orders():
        return SparkSubmitOperator(
            conn_id="spark",
            task_id="sync_lakehouse",
            deploy_mode="cluster",
            conf={
                "spark.executor.instances": "1",
                "spark.kubernetes.namespace": "spark",
                "spark.kubernetes.container.image": "registry.io/spark",
                "spark.kubernetes.container.image.pullPolicy": "Always",
                "spark.kubernetes.driver.podTemplateFile": "/opt/spark/templates/driver.yaml",
                "spark.kubernetes.executor.podTemplateFile": "/opt/spark/templates/executor.yaml",
            },
            application="local:///opt/spark/work-dir/tasks/lakehouse/worker.py",
            application_args=[
                "--table-name", "orders",
                "--date-cursor", "{{ var.value.get('orders_date_cursor', '2017-01-01') }}",
                "--test",
                "--ingest",
            ]
        )

    def sync_warehouse_orders():
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
                "--table-name", "orders",
                "--date-cursor", "{{ var.value.get('orders_date_cursor', '2017-01-01') }}",
                "--ingest",
            ]
        )

    @task(task_id="update_cursor")
    def update_cursor_orders():
        last_date_cursor = Variable.get("orders_date_cursor", default_var="2017-01-01")
        next_date_cursor = datetime.strptime(last_date_cursor, "%Y-%m-%d") + timedelta(days=1)
        Variable.set("orders_date_cursor", next_date_cursor.strftime("%Y-%m-%d"))

    upload_landing = upload_landing_orders()
    sync_lakehouse = sync_lakehouse_orders()
    sync_warehouse = sync_warehouse_orders()
    update_cursor = update_cursor_orders()

    upload_landing >> sync_lakehouse >> sync_warehouse >> update_cursor


ecommerce_sync_orders_etl()
