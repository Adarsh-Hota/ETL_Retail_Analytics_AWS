-- These read native serving views only.

-- Daily revenue trend
SELECT
    sales_date,
    SUM(gross_revenue) AS gross_revenue,
    SUM(completed_orders) AS completed_orders
FROM retail_serving.vw_sales_by_category
GROUP BY sales_date
ORDER BY sales_date;

-- Revenue by category over time
SELECT
    sales_date,
    category,
    SUM(gross_revenue) AS gross_revenue
FROM retail_serving.vw_sales_by_category
GROUP BY sales_date, category
ORDER BY sales_date, category;

-- Top products by revenue
SELECT
    product_name,
    category,
    gross_revenue,
    units_sold,
    estimated_gross_profit
FROM retail_serving.vw_top_products
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM retail_serving.vw_top_products
)
ORDER BY gross_revenue DESC
LIMIT 10;

-- Conversion funnel
SELECT
    funnel_stage,
    SUM(sessions_reaching_stage) AS sessions_reaching_stage,
    MIN(stage_conversion_rate) AS stage_conversion_rate
FROM retail_serving.vw_device_conversion_funnel
WHERE funnel_date = (
    SELECT MAX(funnel_date)
    FROM retail_serving.vw_device_conversion_funnel
)
GROUP BY funnel_stage
ORDER BY CASE funnel_stage
    WHEN 'PRODUCT_VIEW' THEN 1
    WHEN 'ADD_TO_CART' THEN 2
    WHEN 'CHECKOUT' THEN 3
    WHEN 'PURCHASE' THEN 4
END;

-- Customer health distribution
SELECT
    customer_status,
    COUNT(*) AS customer_count,
    SUM(gross_revenue) AS gross_revenue
FROM retail_serving.vw_customer_segments
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM retail_serving.vw_customer_segments
)
GROUP BY customer_status
ORDER BY customer_count DESC;

-- Customer lifetime value
SELECT
    customer_id,
    loyalty_tier,
    observed_lifetime_value,
    lifetime_gross_revenue
FROM retail_serving.vw_top_customer_value
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM retail_serving.vw_top_customer_value
)
ORDER BY observed_lifetime_value DESC, customer_id
LIMIT 10;

-- Inventory risk
SELECT
    stock_status,
    COUNT(*) AS inventory_positions
FROM retail_serving.vw_low_stock_products
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM retail_serving.vw_low_stock_products
)
GROUP BY stock_status
ORDER BY stock_status;
