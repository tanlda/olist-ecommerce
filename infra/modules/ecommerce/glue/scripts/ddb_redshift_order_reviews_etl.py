import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Relational DB
RelationalDB_node1722703634673 = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce",
    table_name="ecommerce_public_order_reviews",
    transformation_ctx="RelationalDB_node1722703634673")

# Script generated for node Amazon Redshift
AmazonRedshift_node1722703639583 = glueContext.write_dynamic_frame.from_options(
    frame=RelationalDB_node1722703634673,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "public.orders_temp_tysd3g",
        "connectionName": "ecommerce-redshift-connection",
        "redshiftTmpDir": "s3://olist-ecommerce-bucket/glue/staging/",
        "preactions": """
            DROP TABLE IF EXISTS public.orders_temp_tysd3g;
            CREATE TABLE IF NOT EXISTS public.orders (order_status VARCHAR, order_delivered_customer_date VARCHAR, order_purchase_timestamp VARCHAR, order_estimated_delivery_date VARCHAR, customer_id VARCHAR, order_id VARCHAR, order_delivered_carrier_date VARCHAR, order_approved_at VARCHAR);
            CREATE TABLE public.orders_temp_tysd3g (order_status VARCHAR, order_delivered_customer_date VARCHAR, order_purchase_timestamp VARCHAR, order_estimated_delivery_date VARCHAR, customer_id VARCHAR, order_id VARCHAR, order_delivered_carrier_date VARCHAR, order_approved_at VARCHAR);
        """,
        "postactions": """
            BEGIN;
            MERGE INTO public.orders USING public.orders_temp_tysd3g
            ON orders.order_id = orders_temp_tysd3g.order_id
            WHEN MATCHED THEN UPDATE SET order_status = orders_temp_tysd3g.order_status, order_delivered_customer_date = orders_temp_tysd3g.order_delivered_customer_date, order_purchase_timestamp = orders_temp_tysd3g.order_purchase_timestamp, order_estimated_delivery_date = orders_temp_tysd3g.order_estimated_delivery_date, customer_id = orders_temp_tysd3g.customer_id, order_id = orders_temp_tysd3g.order_id, order_delivered_carrier_date = orders_temp_tysd3g.order_delivered_carrier_date, order_approved_at = orders_temp_tysd3g.order_approved_at
            WHEN NOT MATCHED THEN INSERT VALUES (orders_temp_tysd3g.order_status, orders_temp_tysd3g.order_delivered_customer_date, orders_temp_tysd3g.order_purchase_timestamp, orders_temp_tysd3g.order_estimated_delivery_date, orders_temp_tysd3g.customer_id, orders_temp_tysd3g.order_id, orders_temp_tysd3g.order_delivered_carrier_date, orders_temp_tysd3g.order_approved_at);
            DROP TABLE public.orders_temp_tysd3g;
            END;
        """,
    },
    transformation_ctx="AmazonRedshift_node1722703639583")

job.commit()
