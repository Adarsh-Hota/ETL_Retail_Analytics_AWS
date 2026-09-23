-- Bootstrap the Redshift-local Gold serving layer.

CREATE EXTERNAL SCHEMA IF NOT EXISTS gold_lakehouse
FROM DATA CATALOG
DATABASE 'retail_gold'
IAM_ROLE default;

CREATE SCHEMA IF NOT EXISTS retail_serving;

CREATE TABLE IF NOT EXISTS retail_serving.daily_sales (
    sales_date DATE, category VARCHAR(256), total_orders BIGINT,
    completed_orders BIGINT, cancelled_orders BIGINT,
    unique_purchasing_customers BIGINT, units_sold BIGINT,
    gross_revenue DOUBLE PRECISION, average_order_value DOUBLE PRECISION,
    payment_attempts BIGINT, captured_payments BIGINT, failed_payments BIGINT,
    refunded_payments BIGINT, captured_amount DOUBLE PRECISION,
    refunded_amount DOUBLE PRECISION, payment_capture_rate DOUBLE PRECISION,
    processed_timestamp TIMESTAMP
) DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS retail_serving.product_performance (
    snapshot_date DATE, product_id VARCHAR(256), product_name VARCHAR(65535),
    category VARCHAR(256), subcategory VARCHAR(256), brand VARCHAR(256),
    is_active BOOLEAN, cost_price DOUBLE PRECISION, list_price DOUBLE PRECISION,
    completed_orders BIGINT, units_sold BIGINT, gross_revenue DOUBLE PRECISION,
    average_selling_price DOUBLE PRECISION,
    estimated_cost_of_goods_sold DOUBLE PRECISION,
    estimated_gross_profit DOUBLE PRECISION, gross_margin_pct DOUBLE PRECISION,
    cancelled_orders BIGINT, product_views BIGINT, add_to_cart_events BIGINT,
    purchase_events BIGINT, view_to_cart_rate DOUBLE PRECISION,
    view_to_purchase_rate DOUBLE PRECISION, units_restocked BIGINT,
    units_sold_from_inventory_events BIGINT, net_inventory_change BIGINT,
    processed_timestamp TIMESTAMP
) DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS retail_serving.inventory_health (
    snapshot_date DATE, product_id VARCHAR(256), warehouse_id VARCHAR(256),
    product_name VARCHAR(65535), category VARCHAR(256), subcategory VARCHAR(256),
    brand VARCHAR(256), is_active BOOLEAN, on_hand_units BIGINT,
    stock_in_units BIGINT, stock_out_units BIGINT, sale_units BIGINT,
    adjustment_units BIGINT, inventory_event_count BIGINT,
    last_inventory_event_timestamp TIMESTAMP, low_stock_threshold BIGINT,
    stock_status VARCHAR(64), processed_timestamp TIMESTAMP
) DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS retail_serving.conversion_funnel (
    funnel_date DATE, device_type VARCHAR(64), funnel_stage VARCHAR(64),
    total_sessions BIGINT, sessions_reaching_stage BIGINT,
    previous_stage_sessions BIGINT, stage_conversion_rate DOUBLE PRECISION,
    stage_dropoff_sessions BIGINT,
    overall_purchase_conversion_rate DOUBLE PRECISION,
    processed_timestamp TIMESTAMP
) DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS retail_serving.customer_metrics (
    snapshot_date DATE, customer_id VARCHAR(256), city VARCHAR(256),
    state VARCHAR(256), signup_date DATE, loyalty_tier VARCHAR(256),
    preferred_category VARCHAR(256), customer_tenure_days BIGINT,
    completed_orders BIGINT, units_purchased BIGINT,
    gross_revenue DOUBLE PRECISION, average_order_value DOUBLE PRECISION,
    first_purchase_timestamp TIMESTAMP, last_purchase_timestamp TIMESTAMP,
    days_since_last_purchase BIGINT, favorite_category VARCHAR(256),
    favorite_brand VARCHAR(256), most_recent_category VARCHAR(256),
    most_recent_brand VARCHAR(256), payment_attempts BIGINT,
    captured_payments BIGINT, failed_payments BIGINT, refunded_payments BIGINT,
    captured_amount DOUBLE PRECISION, refunded_amount DOUBLE PRECISION,
    payment_capture_rate DOUBLE PRECISION, customer_status VARCHAR(64),
    processed_timestamp TIMESTAMP
) DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS retail_serving.customer_lifetime_value (
    snapshot_date DATE, customer_id VARCHAR(256), city VARCHAR(256),
    state VARCHAR(256), signup_date DATE, loyalty_tier VARCHAR(256),
    preferred_category VARCHAR(256), customer_tenure_days BIGINT,
    lifetime_completed_orders BIGINT, lifetime_units_purchased BIGINT,
    lifetime_gross_revenue DOUBLE PRECISION,
    first_purchase_timestamp TIMESTAMP, last_purchase_timestamp TIMESTAMP,
    average_order_value DOUBLE PRECISION, days_since_last_purchase BIGINT,
    purchase_frequency_per_30_days DOUBLE PRECISION,
    observed_lifetime_value DOUBLE PRECISION, payment_attempts BIGINT,
    captured_payments BIGINT, failed_payments BIGINT, refunded_payments BIGINT,
    lifetime_captured_amount DOUBLE PRECISION,
    lifetime_refunded_amount DOUBLE PRECISION,
    payment_capture_rate DOUBLE PRECISION, processed_timestamp TIMESTAMP
) DISTSTYLE AUTO;

-- Staging tables intentionally match their corresponding native targets.
CREATE TABLE IF NOT EXISTS retail_serving.daily_sales_stage
    (LIKE retail_serving.daily_sales);
CREATE TABLE IF NOT EXISTS retail_serving.product_performance_stage
    (LIKE retail_serving.product_performance);
CREATE TABLE IF NOT EXISTS retail_serving.inventory_health_stage
    (LIKE retail_serving.inventory_health);
CREATE TABLE IF NOT EXISTS retail_serving.conversion_funnel_stage
    (LIKE retail_serving.conversion_funnel);
CREATE TABLE IF NOT EXISTS retail_serving.customer_metrics_stage
    (LIKE retail_serving.customer_metrics);
CREATE TABLE IF NOT EXISTS retail_serving.customer_lifetime_value_stage
    (LIKE retail_serving.customer_lifetime_value);

CREATE TABLE IF NOT EXISTS retail_serving.refresh_audit (
    refresh_timestamp TIMESTAMP NOT NULL,
    target_table_name VARCHAR(256) NOT NULL,
    source_table_name VARCHAR(256) NOT NULL,
    source_row_count BIGINT NOT NULL,
    target_row_count BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL
) DISTSTYLE AUTO;

-- Views expose native serving tables only; no Gold transformations are recreated.
CREATE OR REPLACE VIEW retail_serving.vw_sales_by_category AS
SELECT sales_date, category, total_orders, completed_orders, cancelled_orders,
       unique_purchasing_customers, units_sold, gross_revenue, average_order_value,
       payment_attempts, captured_payments, failed_payments, refunded_payments,
       captured_amount, refunded_amount, payment_capture_rate, processed_timestamp
FROM retail_serving.daily_sales;

CREATE OR REPLACE VIEW retail_serving.vw_top_products AS
SELECT snapshot_date, product_id, product_name, category, subcategory, brand,
       is_active, cost_price, list_price, completed_orders, units_sold,
       gross_revenue, average_selling_price, estimated_cost_of_goods_sold,
       estimated_gross_profit, gross_margin_pct, cancelled_orders, product_views,
       add_to_cart_events, purchase_events, view_to_cart_rate,
       view_to_purchase_rate, units_restocked, units_sold_from_inventory_events,
       net_inventory_change, processed_timestamp
FROM retail_serving.product_performance;

CREATE OR REPLACE VIEW retail_serving.vw_low_stock_products AS
SELECT snapshot_date, product_id, warehouse_id, product_name, category,
       subcategory, brand, is_active, on_hand_units, stock_in_units,
       stock_out_units, sale_units, adjustment_units, inventory_event_count,
       last_inventory_event_timestamp, low_stock_threshold, stock_status,
       processed_timestamp
FROM retail_serving.inventory_health;

CREATE OR REPLACE VIEW retail_serving.vw_device_conversion_funnel AS
SELECT funnel_date, device_type, funnel_stage, total_sessions,
       sessions_reaching_stage, previous_stage_sessions, stage_conversion_rate,
       stage_dropoff_sessions, overall_purchase_conversion_rate,
       processed_timestamp
FROM retail_serving.conversion_funnel;

CREATE OR REPLACE VIEW retail_serving.vw_customer_segments AS
SELECT snapshot_date, customer_id, city, state, signup_date, loyalty_tier,
       preferred_category, customer_tenure_days, completed_orders, units_purchased,
       gross_revenue, average_order_value, first_purchase_timestamp,
       last_purchase_timestamp, days_since_last_purchase, favorite_category,
       favorite_brand, most_recent_category, most_recent_brand, payment_attempts,
       captured_payments, failed_payments, refunded_payments, captured_amount,
       refunded_amount, payment_capture_rate, customer_status, processed_timestamp
FROM retail_serving.customer_metrics;

CREATE OR REPLACE VIEW retail_serving.vw_top_customer_value AS
SELECT snapshot_date, customer_id, city, state, signup_date, loyalty_tier,
       preferred_category, customer_tenure_days, lifetime_completed_orders,
       lifetime_units_purchased, lifetime_gross_revenue, first_purchase_timestamp,
       last_purchase_timestamp, average_order_value, days_since_last_purchase,
       purchase_frequency_per_30_days, observed_lifetime_value, payment_attempts,
       captured_payments, failed_payments, refunded_payments,
       lifetime_captured_amount, lifetime_refunded_amount, payment_capture_rate,
       processed_timestamp
FROM retail_serving.customer_lifetime_value;

-- Atomic stored procedure: Redshift commits the CALL only if every statement
-- succeeds. Do not add BEGIN, COMMIT, ROLLBACK, or TRUNCATE inside this procedure.
CREATE OR REPLACE PROCEDURE retail_serving.refresh_gold_serving()
AS $$
BEGIN
    DELETE FROM retail_serving.daily_sales_stage;
    INSERT INTO retail_serving.daily_sales_stage (sales_date, category, total_orders, completed_orders, cancelled_orders, unique_purchasing_customers, units_sold, gross_revenue, average_order_value, payment_attempts, captured_payments, failed_payments, refunded_payments, captured_amount, refunded_amount, payment_capture_rate, processed_timestamp)
    SELECT sales_date, category, total_orders, completed_orders, cancelled_orders, unique_purchasing_customers, units_sold, gross_revenue, average_order_value, payment_attempts, captured_payments, failed_payments, refunded_payments, captured_amount, refunded_amount, payment_capture_rate, processed_timestamp FROM gold_lakehouse.gold_daily_sales;
    DELETE FROM retail_serving.daily_sales;
    INSERT INTO retail_serving.daily_sales (sales_date, category, total_orders, completed_orders, cancelled_orders, unique_purchasing_customers, units_sold, gross_revenue, average_order_value, payment_attempts, captured_payments, failed_payments, refunded_payments, captured_amount, refunded_amount, payment_capture_rate, processed_timestamp)
    SELECT sales_date, category, total_orders, completed_orders, cancelled_orders, unique_purchasing_customers, units_sold, gross_revenue, average_order_value, payment_attempts, captured_payments, failed_payments, refunded_payments, captured_amount, refunded_amount, payment_capture_rate, processed_timestamp FROM retail_serving.daily_sales_stage;
    INSERT INTO retail_serving.refresh_audit SELECT GETDATE(), 'retail_serving.daily_sales', 'gold_lakehouse.gold_daily_sales', (SELECT COUNT(*) FROM gold_lakehouse.gold_daily_sales), (SELECT COUNT(*) FROM retail_serving.daily_sales), 'SUCCESS';

    DELETE FROM retail_serving.product_performance_stage;
    INSERT INTO retail_serving.product_performance_stage (snapshot_date, product_id, product_name, category, subcategory, brand, is_active, cost_price, list_price, completed_orders, units_sold, gross_revenue, average_selling_price, estimated_cost_of_goods_sold, estimated_gross_profit, gross_margin_pct, cancelled_orders, product_views, add_to_cart_events, purchase_events, view_to_cart_rate, view_to_purchase_rate, units_restocked, units_sold_from_inventory_events, net_inventory_change, processed_timestamp)
    SELECT snapshot_date, product_id, product_name, category, subcategory, brand, is_active, cost_price, list_price, completed_orders, units_sold, gross_revenue, average_selling_price, estimated_cost_of_goods_sold, estimated_gross_profit, gross_margin_pct, cancelled_orders, product_views, add_to_cart_events, purchase_events, view_to_cart_rate, view_to_purchase_rate, units_restocked, units_sold_from_inventory_events, net_inventory_change, processed_timestamp FROM gold_lakehouse.gold_product_performance;
    DELETE FROM retail_serving.product_performance;
    INSERT INTO retail_serving.product_performance SELECT * FROM retail_serving.product_performance_stage;
    INSERT INTO retail_serving.refresh_audit SELECT GETDATE(), 'retail_serving.product_performance', 'gold_lakehouse.gold_product_performance', (SELECT COUNT(*) FROM gold_lakehouse.gold_product_performance), (SELECT COUNT(*) FROM retail_serving.product_performance), 'SUCCESS';

    DELETE FROM retail_serving.inventory_health_stage;
    INSERT INTO retail_serving.inventory_health_stage (snapshot_date, product_id, warehouse_id, product_name, category, subcategory, brand, is_active, on_hand_units, stock_in_units, stock_out_units, sale_units, adjustment_units, inventory_event_count, last_inventory_event_timestamp, low_stock_threshold, stock_status, processed_timestamp)
    SELECT snapshot_date, product_id, warehouse_id, product_name, category, subcategory, brand, is_active, on_hand_units, stock_in_units, stock_out_units, sale_units, adjustment_units, inventory_event_count, last_inventory_event_timestamp, low_stock_threshold, stock_status, processed_timestamp FROM gold_lakehouse.gold_inventory_health;
    DELETE FROM retail_serving.inventory_health;
    INSERT INTO retail_serving.inventory_health SELECT * FROM retail_serving.inventory_health_stage;
    INSERT INTO retail_serving.refresh_audit SELECT GETDATE(), 'retail_serving.inventory_health', 'gold_lakehouse.gold_inventory_health', (SELECT COUNT(*) FROM gold_lakehouse.gold_inventory_health), (SELECT COUNT(*) FROM retail_serving.inventory_health), 'SUCCESS';

    DELETE FROM retail_serving.conversion_funnel_stage;
    INSERT INTO retail_serving.conversion_funnel_stage (funnel_date, device_type, funnel_stage, total_sessions, sessions_reaching_stage, previous_stage_sessions, stage_conversion_rate, stage_dropoff_sessions, overall_purchase_conversion_rate, processed_timestamp)
    SELECT funnel_date, device_type, funnel_stage, total_sessions, sessions_reaching_stage, previous_stage_sessions, stage_conversion_rate, stage_dropoff_sessions, overall_purchase_conversion_rate, processed_timestamp FROM gold_lakehouse.gold_conversion_funnel;
    DELETE FROM retail_serving.conversion_funnel;
    INSERT INTO retail_serving.conversion_funnel SELECT * FROM retail_serving.conversion_funnel_stage;
    INSERT INTO retail_serving.refresh_audit SELECT GETDATE(), 'retail_serving.conversion_funnel', 'gold_lakehouse.gold_conversion_funnel', (SELECT COUNT(*) FROM gold_lakehouse.gold_conversion_funnel), (SELECT COUNT(*) FROM retail_serving.conversion_funnel), 'SUCCESS';

    DELETE FROM retail_serving.customer_metrics_stage;
    INSERT INTO retail_serving.customer_metrics_stage (snapshot_date, customer_id, city, state, signup_date, loyalty_tier, preferred_category, customer_tenure_days, completed_orders, units_purchased, gross_revenue, average_order_value, first_purchase_timestamp, last_purchase_timestamp, days_since_last_purchase, favorite_category, favorite_brand, most_recent_category, most_recent_brand, payment_attempts, captured_payments, failed_payments, refunded_payments, captured_amount, refunded_amount, payment_capture_rate, customer_status, processed_timestamp)
    SELECT snapshot_date, customer_id, city, state, signup_date, loyalty_tier, preferred_category, customer_tenure_days, completed_orders, units_purchased, gross_revenue, average_order_value, first_purchase_timestamp, last_purchase_timestamp, days_since_last_purchase, favorite_category, favorite_brand, most_recent_category, most_recent_brand, payment_attempts, captured_payments, failed_payments, refunded_payments, captured_amount, refunded_amount, payment_capture_rate, customer_status, processed_timestamp FROM gold_lakehouse.gold_customer_metrics;
    DELETE FROM retail_serving.customer_metrics;
    INSERT INTO retail_serving.customer_metrics SELECT * FROM retail_serving.customer_metrics_stage;
    INSERT INTO retail_serving.refresh_audit SELECT GETDATE(), 'retail_serving.customer_metrics', 'gold_lakehouse.gold_customer_metrics', (SELECT COUNT(*) FROM gold_lakehouse.gold_customer_metrics), (SELECT COUNT(*) FROM retail_serving.customer_metrics), 'SUCCESS';

    DELETE FROM retail_serving.customer_lifetime_value_stage;
    INSERT INTO retail_serving.customer_lifetime_value_stage (snapshot_date, customer_id, city, state, signup_date, loyalty_tier, preferred_category, customer_tenure_days, lifetime_completed_orders, lifetime_units_purchased, lifetime_gross_revenue, first_purchase_timestamp, last_purchase_timestamp, average_order_value, days_since_last_purchase, purchase_frequency_per_30_days, observed_lifetime_value, payment_attempts, captured_payments, failed_payments, refunded_payments, lifetime_captured_amount, lifetime_refunded_amount, payment_capture_rate, processed_timestamp)
    SELECT snapshot_date, customer_id, city, state, signup_date, loyalty_tier, preferred_category, customer_tenure_days, lifetime_completed_orders, lifetime_units_purchased, lifetime_gross_revenue, first_purchase_timestamp, last_purchase_timestamp, average_order_value, days_since_last_purchase, purchase_frequency_per_30_days, observed_lifetime_value, payment_attempts, captured_payments, failed_payments, refunded_payments, lifetime_captured_amount, lifetime_refunded_amount, payment_capture_rate, processed_timestamp FROM gold_lakehouse.gold_customer_lifetime_value;
    DELETE FROM retail_serving.customer_lifetime_value;
    INSERT INTO retail_serving.customer_lifetime_value SELECT * FROM retail_serving.customer_lifetime_value_stage;
    INSERT INTO retail_serving.refresh_audit SELECT GETDATE(), 'retail_serving.customer_lifetime_value', 'gold_lakehouse.gold_customer_lifetime_value', (SELECT COUNT(*) FROM gold_lakehouse.gold_customer_lifetime_value), (SELECT COUNT(*) FROM retail_serving.customer_lifetime_value), 'SUCCESS';
END;
$$ LANGUAGE plpgsql;
