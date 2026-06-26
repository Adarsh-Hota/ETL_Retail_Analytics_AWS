SELECT *
FROM analytics.dim_customer
LIMIT 10;

SELECT *
FROM analytics.dim_product
LIMIT 10;

SELECT *
FROM analytics.dim_date
LIMIT 10;

SELECT *
FROM analytics.fact_orders
LIMIT 10;

(
    SELECT
        p.category,
        SUM(f.total_amount) AS revenue
    FROM analytics.fact_orders f
    JOIN analytics.dim_product p
        ON f.product_id = p.product_id
    GROUP BY p.category
    ORDER BY revenue DESC
);

(
    SELECT
        d.year,
        d.month_name,
        p.category,
        SUM(f.total_amount) AS revenue
    FROM analytics.fact_orders f
    JOIN analytics.dim_product p
        ON f.product_id = p.product_id
    JOIN analytics.dim_date d
        ON CAST(f.order_timestamp AS DATE) = d.full_date
    GROUP BY
        d.year,
        d.month_name,
        p.category
    ORDER BY revenue DESC
);
