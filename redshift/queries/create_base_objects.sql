CREATE SCHEMA analytics;

CREATE TABLE analytics.fact_orders (
    order_id VARCHAR(100),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    order_timestamp TIMESTAMP,
    processed_timestamp TIMESTAMP,
    year VARCHAR(4),
    month VARCHAR(2),
    day VARCHAR(2)
);

CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG
DATABASE 'retail_analytics'
IAM_ROLE 'iam-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

INSERT INTO analytics.fact_orders
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    order_status,
    CAST(order_timestamp AS TIMESTAMP),
    CAST(processed_timestamp AS TIMESTAMP),
    year,
    month,
    day
FROM spectrum.silver_orders;

DROP TABLE analytics.fact_orders;
