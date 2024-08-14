import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    flink = StreamTableEnvironment.create(stream_execution_environment=env)
    stmt_set = flink.create_statement_set()

    def sync_orders():
        flink.execute_sql("""
            CREATE TABLE IF NOT EXISTS kafka_orders (
                order_id string,
                customer_id string,
                order_status string,
                order_approved_at bigint,
                order_purchase_timestamp bigint,
                order_delivered_carrier_date bigint,
                order_delivered_customer_date bigint,
                order_estimated_delivery_date bigint
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'orders.public.orders',
                'properties.bootstrap.servers' = 'cluster-kafka-brokers.kafka:9092',
                'properties.group.id' = 'opensearch-orders',
                'scan.startup.mode' = 'earliest-offset',
                'value.format' = 'avro-confluent',
                'value.avro-confluent.url' = 'http://schema-registry.kafka:8081'
            );
        """)

        flink.execute_sql("""
            CREATE TABLE IF NOT EXISTS opensearch_orders (
                order_id string,
                customer_id string,
                order_status string,
                order_approved_at timestamp,
                order_purchase_timestamp timestamp,
                order_delivered_carrier_date timestamp,
                order_delivered_customer_date timestamp,
                order_estimated_delivery_date timestamp
            ) WITH (
                'connector' = 'opensearch-2',
                'hosts' = 'http://opensearch.io:80',
                'allow-insecure' = 'true',
                'index' = 'orders',
                'format' = 'json'
            );
        """)

        stmt_set.add_insert_sql("""
            INSERT INTO opensearch_orders
            SELECT
                order_id,
                customer_id,
                order_status,
                to_timestamp_ltz(order_approved_at / 1e3, 3) as order_approved_at,
                to_timestamp_ltz(order_purchase_timestamp / 1e3, 3) as order_purchase_timestamp,
                to_timestamp_ltz(order_delivered_carrier_date / 1e3, 3) as order_delivered_carrier_date,
                to_timestamp_ltz(order_delivered_customer_date / 1e3, 3) as order_delivered_customer_date,
                to_timestamp_ltz(order_estimated_delivery_date / 1e3, 3) as order_estimated_delivery_date
            FROM kafka_orders
        """)

    def sync_order_items():
        flink.execute_sql("""
            CREATE TABLE IF NOT EXISTS kafka_order_items (
                order_id string,
                seller_id string,
                product_id string,
                order_item_id bigint,
                shipping_limit_date bigint,
                freight_value double,
                price double
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'order_items.public.order_items',
                'properties.bootstrap.servers' = 'cluster-kafka-brokers.kafka:9092',
                'properties.group.id' = 'opensearch-order-items',
                'scan.startup.mode' = 'earliest-offset',
                'value.format' = 'avro-confluent',
                'value.avro-confluent.url' = 'http://schema-registry.kafka:8081'
            );
        """)

        flink.execute_sql("""
            CREATE TABLE IF NOT EXISTS opensearch_order_items (
                order_id string,
                seller_id string,
                product_id string,
                order_item_id bigint,
                shipping_limit_date timestamp(3),
                freight_value decimal,
                price decimal
            ) WITH (
                'connector' = 'opensearch-2',
                'hosts' = 'http://opensearch.io:80',
                'allow-insecure' = 'true',
                'index' = 'order_items',
                'format' = 'json'
            );
        """)

        stmt_set.add_insert_sql("""
            INSERT INTO opensearch_order_items
            SELECT
                order_id,
                seller_id,
                product_id,
                order_item_id,
                to_timestamp_ltz(shipping_limit_date / 1e3, 3) as shipping_limit_date,
                freight_value,
                price
            FROM kafka_order_items;
        """)

    def sync_order_reviews():
        flink.execute_sql(r"""
            CREATE TABLE IF NOT EXISTS kafka_order_reviews (
                payload string
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'order_reviews.ecommerce.order_reviews',
                'properties.bootstrap.servers' = 'cluster-kafka-brokers.kafka:9092',
                'properties.group.id' = 'opensearch-order-reviews',
                'scan.startup.mode' = 'earliest-offset',
                'value.format' = 'json'
            );
        """)

        flink.execute_sql(r"""
            CREATE TABLE IF NOT EXISTS opensearch_order_reviews (
                order_id string,
                review_id string,
                review_score integer,
                review_comment_title string,
                review_comment_message string,
                review_creation_date timestamp(3),
                review_answer_timestamp timestamp(3)
            ) WITH (
                'connector' = 'opensearch-2',
                'hosts' = 'http://opensearch.io:80',
                'allow-insecure' = 'true',
                'index' = 'order_reviews',
                'format' = 'json'
            );
        """)

        stmt_set.add_insert_sql(r"""
            INSERT INTO opensearch_order_reviews
            SELECT
                order_id,
                review_id,
                cast(review_score as integer) as review_score,
                review_comment_title,
                review_comment_message,
                to_timestamp(review_creation_date) as review_creation_date,
                to_timestamp(review_answer_timestamp) as review_answer_timestamp
            FROM (
                SELECT
                    json_value(payload, '$.order_id') as order_id,
                    json_value(payload, '$.review_id') as review_id,
                    json_value(payload, '$.review_score') as review_score,
                    json_value(payload, '$.review_comment_title') as review_comment_title,
                    json_value(payload, '$.review_comment_message') as review_comment_message,
                    replace(replace(json_value(payload, '$.review_creation_date'), 'T', ' '), 'Z', '') as review_creation_date,
                    replace(replace(json_value(payload, '$.review_answer_timestamp'), 'T', ' '), 'Z', '') as review_answer_timestamp
                FROM kafka_order_reviews
            );
        """)

    def sync_order_payments():
        flink.execute_sql(r"""
            CREATE TABLE IF NOT EXISTS kafka_order_payments (
                order_id string,
                payment_type string,
                payment_sequential bigint,
                payment_installments bigint,
                payment_value double
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'order_payments.public.order_payments',
                'properties.bootstrap.servers' = 'cluster-kafka-brokers.kafka:9092',
                'properties.group.id' = 'opensearch-order-payments',
                'scan.startup.mode' = 'earliest-offset',
                'value.format' = 'avro-confluent',
                'value.avro-confluent.url' = 'http://schema-registry.kafka:8081'
            );
        """)

        flink.execute_sql(r"""
            CREATE TABLE IF NOT EXISTS opensearch_order_payments (
                order_id string,
                payment_type string,
                payment_sequential bigint,
                payment_installments bigint,
                payment_value double
            ) WITH (
                'connector' = 'opensearch-2',
                'hosts' = 'http://opensearch.io:80',
                'allow-insecure' = 'true',
                'index' = 'order_payments',
                'format' = 'json'
            );
        """)

        stmt_set.add_insert_sql(r"""
            INSERT INTO opensearch_order_payments
            SELECT * FROM kafka_order_payments;
        """)

    sync_orders()
    sync_order_items()
    sync_order_reviews()
    sync_order_payments()

    stmt_set.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
