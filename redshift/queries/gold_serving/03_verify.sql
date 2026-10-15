-- Read-only checks after bootstrap/refresh.
-- External Gold source visibility. 
-- gold_clickstream_session_analytics is excluded from native serving materialization because it is an intermediate Gold mart.

SELECT schemaname, tablename, location
FROM svv_external_tables
WHERE schemaname = 'gold_lakehouse'
ORDER BY tablename;

-- Source-to-native row counts for all six serving marts.
SELECT 'gold_daily_sales' AS source_table,
       (SELECT COUNT(*) FROM gold_lakehouse.gold_daily_sales) AS source_rows,
       (SELECT COUNT(*) FROM retail_serving.daily_sales) AS target_rows
UNION ALL
SELECT 'gold_product_performance',
       (SELECT COUNT(*) FROM gold_lakehouse.gold_product_performance),
       (SELECT COUNT(*) FROM retail_serving.product_performance)
UNION ALL
SELECT 'gold_inventory_health',
       (SELECT COUNT(*) FROM gold_lakehouse.gold_inventory_health),
       (SELECT COUNT(*) FROM retail_serving.inventory_health)
UNION ALL
SELECT 'gold_conversion_funnel',
       (SELECT COUNT(*) FROM gold_lakehouse.gold_conversion_funnel),
       (SELECT COUNT(*) FROM retail_serving.conversion_funnel)
UNION ALL
SELECT 'gold_customer_metrics',
       (SELECT COUNT(*) FROM gold_lakehouse.gold_customer_metrics),
       (SELECT COUNT(*) FROM retail_serving.customer_metrics)
UNION ALL
SELECT 'gold_customer_lifetime_value',
       (SELECT COUNT(*) FROM gold_lakehouse.gold_customer_lifetime_value),
       (SELECT COUNT(*) FROM retail_serving.customer_lifetime_value)
ORDER BY source_table;

SELECT refresh_timestamp, target_table_name, source_table_name,
       source_row_count, target_row_count, status
FROM retail_serving.refresh_audit
ORDER BY refresh_timestamp DESC, target_table_name
LIMIT 60;

SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema = 'retail_serving'
  AND table_name IN (
      'vw_sales_by_category', 'vw_top_products', 'vw_low_stock_products',
      'vw_device_conversion_funnel', 'vw_customer_segments',
      'vw_top_customer_value'
  )
ORDER BY table_name;

-- PII guard: expected result is zero rows for each customer-facing native table.
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'retail_serving'
  AND table_name IN ('customer_metrics', 'customer_lifetime_value')
  AND LOWER(column_name) IN ('first_name', 'last_name', 'email')
ORDER BY table_name, column_name;