from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
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
def ecommerce_sync_order_reviews_etl():
    @task(task_id="upload_landing")
    def upload_landing_order_reviews():
        import polars as pl

        date_cursor = Variable.get("order_reviews_date_cursor", default_var="2017-01-01")

        mongo = MongoHook(mongo_conn_id="document").get_conn()
        minio = MinioHook(minio_conn_id="lakehouse").get_conn()

        database = mongo["ecommerce"]
        collection = database["order_reviews"]
        start = datetime.strptime(date_cursor, "%Y-%m-%d")
        records = collection.find({"review_creation_date": {
            "$gte": start,
            "$lt": start + timedelta(days=1)
        }}, {"_id": 0})

        df = pl.DataFrame(records)
        with NamedTemporaryFile() as tmpfile:
            df.write_csv(tmpfile.name)
            bucket_name = "ecommerce"
            object_name = f"landing/order_reviews/order_reviews.{date_cursor}.csv"
            minio.fput_object(bucket_name, object_name, tmpfile.name)

        mongo.close()

    def sync_lakehouse_order_reviews():
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
                "spark.kubernetes.executor.podTemplateFile": "/opt/spark/templates/executor.yaml"
            },
            application="local:///opt/spark/work-dir/tasks/lakehouse/worker.py",
            application_args=[
                "--table-name", "order_reviews",
                "--date-cursor", "{{ var.value.get('order_reviews_date_cursor', '2017-01-01') }}",
                "--test",
                "--ingest",
            ]
        )

    def sync_warehouse_order_reviews():
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
                "--table-name", "order_reviews",
                "--date-cursor", "{{ var.value.get('order_reviews_date_cursor', '2017-01-01') }}",
                "--ingest",
            ]
        )

    @task(task_id="update_cursor")
    def update_cursor_order_reviews():
        last_date_cursor = Variable.get("order_reviews_date_cursor", default_var="2017-01-01")
        next_date_cursor = datetime.strptime(last_date_cursor, "%Y-%m-%d") + timedelta(days=1)
        Variable.set("order_reviews_date_cursor", next_date_cursor.strftime("%Y-%m-%d"))

    upload_landing = upload_landing_order_reviews()
    sync_lakehouse = sync_lakehouse_order_reviews()
    sync_warehouse = sync_warehouse_order_reviews()
    update_cursor = update_cursor_order_reviews()

    upload_landing >> sync_lakehouse >> sync_warehouse >> update_cursor


ecommerce_sync_order_reviews_etl()
