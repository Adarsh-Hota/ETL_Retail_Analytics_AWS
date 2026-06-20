CREATE SCHEMA analytics;

CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG
DATABASE 'retail_analytics'
IAM_ROLE 'iam-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE TABLE analytics.dim_customer AS
(
    SELECT DISTINCT
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        loyalty_tier,
        signup_date,
        processed_timestamp
    FROM spectrum.silver_customers
);

CREATE TABLE analytics.dim_product AS
(
    SELECT DISTINCT
        product_id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        is_active,
        processed_timestamp
    FROM spectrum.silver_products
);

CREATE TABLE analytics.dim_date AS
(
    SELECT DISTINCT
        CAST(order_timestamp AS DATE) AS full_date,
        EXTRACT(YEAR FROM CAST(order_timestamp AS TIMESTAMP)) AS year,
        EXTRACT(MONTH FROM CAST(order_timestamp AS TIMESTAMP)) AS month,
        EXTRACT(DAY FROM CAST(order_timestamp AS TIMESTAMP)) AS day,
        EXTRACT(QUARTER FROM CAST(order_timestamp AS TIMESTAMP)) AS quarter,
        TO_CHAR(
            CAST(order_timestamp AS TIMESTAMP),
            'Month'
        ) AS month_name,
        TO_CHAR(
            CAST(order_timestamp AS TIMESTAMP),
            'Day'
        ) AS weekday_name
    FROM analytics.fact_orders
);

CREATE TABLE analytics.fact_orders AS
(
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
        CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
        payment_method,
        order_status,
        CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
        CAST(processed_timestamp AS TIMESTAMP) AS processed_timestamp
    FROM spectrum.silver_orders
);
