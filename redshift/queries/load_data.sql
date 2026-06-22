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
