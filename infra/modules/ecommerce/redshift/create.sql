CREATE TABLE IF NOT EXISTS customers
(
    customer_id              VARCHAR(255) NOT NULL,
    customer_unique_id       VARCHAR(255) NOT NULL,
    customer_zip_code_prefix INTEGER,
    customer_city            VARCHAR(255),
    customer_state           VARCHAR(10)
)
    DISTSTYLE key
    DISTKEY (customer_id)
    SORTKEY (customer_id); -- sort merge join (https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html)


CREATE TABLE IF NOT EXISTS geolocation
(
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            VARCHAR(255),
    geolocation_state           VARCHAR(10)
)
    DISTSTYLE key
    DISTKEY (geolocation_zip_code_prefix);


CREATE TABLE IF NOT EXISTS order_items
(
    order_id            VARCHAR(255) NOT NULL,
    order_item_id       INTEGER      NOT NULL,
    product_id          VARCHAR(255) NOT NULL,
    seller_id           VARCHAR(255) NOT NULL,
    shipping_limit_date TIMESTAMP,
    price               DOUBLE PRECISION,
    freight_value       DOUBLE PRECISION
)
    DISTSTYLE key
    DISTKEY (order_id)
    SORTKEY (shipping_limit_date);


CREATE TABLE IF NOT EXISTS order_payments
(
    order_id             VARCHAR(255) NOT NULL,
    payment_type         VARCHAR(255) NOT NULL,
    payment_sequential   INTEGER,
    payment_installments INTEGER,
    payment_value        DOUBLE PRECISION
)
    DISTSTYLE key
    DISTKEY (order_id)
    SORTKEY (order_id);


CREATE TABLE IF NOT EXISTS order_reviews
(
    order_id                VARCHAR(255) NOT NULL,
    review_id               VARCHAR(255) NOT NULL,
    review_score            INTEGER,
    review_comment_title    VARCHAR,
    review_comment_message  VARCHAR,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
)
    DISTSTYLE key
    DISTKEY (order_id)
    SORTKEY (order_id);


CREATE TABLE IF NOT EXISTS orders
(
    order_id                      VARCHAR(255) NOT NULL,
    customer_id                   VARCHAR(255) NOT NULL,
    order_status                  VARCHAR(50),
    order_purchase_timestamp      TIMESTAMP    NOT NULL,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
)
    DISTSTYLE key
    DISTKEY (order_id)
    SORTKEY (order_purchase_timestamp);


CREATE TABLE products
(
    product_id                 VARCHAR(255) NOT NULL,
    product_category_name      VARCHAR(255),
    product_name_lenght        BIGINT,
    product_description_lenght BIGINT,
    product_photos_qty         BIGINT,
    product_weight_g           BIGINT,
    product_length_cm          BIGINT,
    product_height_cm          BIGINT,
    product_width_cm           BIGINT
)
    DISTSTYLE key
    DISTKEY (product_id)
    SORTKEY (product_id);

CREATE TABLE sellers
(
    seller_id              VARCHAR(255) NOT NULL,
    seller_zip_code_prefix BIGINT,
    seller_city            VARCHAR(255),
    seller_state           VARCHAR(10)
)
    -- DISTSTYLE auto
    DISTKEY (seller_id)
    SORTKEY (seller_id);


--
SELECT "column", type, encoding, distkey, sortkey
FROM pg_table_def
WHERE tablename = 'orders';

SELECT slice, col, num_values as rows, minvalue, maxvalue
FROM svv_diskusage
WHERE name = 'orders'
  AND col = 0
  AND rows > 0
ORDER BY slice, col;
