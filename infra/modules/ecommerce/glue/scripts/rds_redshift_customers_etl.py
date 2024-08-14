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

# Script generated for node PostgreSQL
PostgreSQL_node1722003710527 = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce",
    table_name="src_ecommerce_public_customers",
    transformation_ctx="PostgreSQL_node1722003710527",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1722003716162 = glueContext.write_dynamic_frame.from_options(
    frame=PostgreSQL_node1722003710527,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "ecommerce-redshift-connection",
        "redshiftTmpDir": "s3://olist-ecommerce-bucket/glue/temporary/",
        "dbtable": "public.customers_temp_3nbffb",
        "preactions": """
            DROP TABLE IF EXISTS public.customers_temp_3nbffb;
            CREATE TABLE IF NOT EXISTS public.customers (customer_unique_id VARCHAR, customer_state VARCHAR, customer_id VARCHAR, customer_zip_code_prefix BIGINT, customer_city VARCHAR);
            CREATE TABLE public.customers_temp_3nbffb (customer_unique_id VARCHAR, customer_state VARCHAR, customer_id VARCHAR, customer_zip_code_prefix BIGINT, customer_city VARCHAR);
        """,
        "postactions": """
            BEGIN;
            MERGE INTO public.customers USING public.customers_temp_3nbffb
            ON customers.customer_id = customers_temp_3nbffb.customer_id
            WHEN MATCHED THEN UPDATE SET customer_unique_id = customers_temp_3nbffb.customer_unique_id, customer_state = customers_temp_3nbffb.customer_state, customer_id = customers_temp_3nbffb.customer_id, customer_zip_code_prefix = customers_temp_3nbffb.customer_zip_code_prefix, customer_city = customers_temp_3nbffb.customer_city
            WHEN NOT MATCHED THEN INSERT VALUES (customers_temp_3nbffb.customer_unique_id, customers_temp_3nbffb.customer_state, customers_temp_3nbffb.customer_id, customers_temp_3nbffb.customer_zip_code_prefix, customers_temp_3nbffb.customer_city);
            DROP TABLE public.customers_temp_3nbffb;
            END;
        """,
    },
    transformation_ctx="AmazonRedshift_node1722003716162",
)

job.commit()
