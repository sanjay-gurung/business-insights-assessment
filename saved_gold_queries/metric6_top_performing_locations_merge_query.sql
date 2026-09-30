MERGE INTO business_insights_gold_db.metric6_top_performing_locations AS target

USING (

    WITH items_revenue AS (
        SELECT
            restaurant_id,
            order_id,
            creation_time_utc AS order_date,
            SUM(item_price * item_quantity) AS total_item_revenue
        FROM business_insights_silver_db.order_items_transformed
        GROUP BY
            restaurant_id,
            order_id,
            creation_time_utc
    ),

    options_revenue AS (
        SELECT
            order_id,
            SUM(option_price * option_quantity) AS total_option_revenue
        FROM business_insights_silver_db.order_item_options_transformed
        GROUP BY order_id
    ),

    total_order_value AS (
        SELECT
            i.restaurant_id,
            i.order_id,
            i.order_date,
            i.total_item_revenue
                + COALESCE(o.total_option_revenue, 0) AS total_order_value
        FROM items_revenue i
        LEFT JOIN options_revenue o
            ON i.order_id = o.order_id
    ),

    avg_order_value AS (
        SELECT
            restaurant_id,
            AVG(total_order_value) AS avg_order_value
        FROM total_order_value
        GROUP BY restaurant_id
    ),

    orders_per_day AS (
        SELECT
            restaurant_id,
            CAST(DATE_TRUNC('day', order_date) AS DATE) AS order_day,
            COUNT(DISTINCT order_id) AS orders_per_day
        FROM total_order_value
        GROUP BY
            restaurant_id,
            CAST(DATE_TRUNC('day', order_date) AS DATE)
    ),

    avg_orders_per_day AS (
        SELECT
            restaurant_id,
            AVG(orders_per_day) AS avg_orders_per_day
        FROM orders_per_day
        GROUP BY restaurant_id
    ),

    orders_per_week AS (
        SELECT
            restaurant_id,
            CAST(DATE_TRUNC('week', order_date) AS DATE) AS order_week,
            COUNT(DISTINCT order_id) AS orders_per_week
        FROM total_order_value
        GROUP BY
            restaurant_id,
            CAST(DATE_TRUNC('week', order_date) AS DATE)
    ),

    avg_orders_per_week AS (
        SELECT
            restaurant_id,
            AVG(orders_per_week) AS avg_orders_per_week
        FROM orders_per_week
        GROUP BY restaurant_id
    ),

    restaurant_revenue AS (
        SELECT
            restaurant_id,
            SUM(total_order_value) AS total_restaurant_revenue,
            DENSE_RANK() OVER (
                ORDER BY SUM(total_order_value) DESC
            ) AS revenue_rank
        FROM total_order_value
        GROUP BY restaurant_id
    )

    SELECT
        r.restaurant_id,
        ROUND(r.total_restaurant_revenue, 2) AS total_revenue,
        ROUND(a.avg_order_value, 2) AS avg_order_value,
        ROUND(d.avg_orders_per_day, 2) AS avg_orders_per_day,
        ROUND(w.avg_orders_per_week, 2) AS avg_orders_per_week,
        r.revenue_rank,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS gold_updated_at

    FROM restaurant_revenue r

    LEFT JOIN avg_order_value a
        ON r.restaurant_id = a.restaurant_id

    LEFT JOIN avg_orders_per_day d
        ON r.restaurant_id = d.restaurant_id

    LEFT JOIN avg_orders_per_week w
        ON r.restaurant_id = w.restaurant_id

) AS source


ON target.restaurant_id = source.restaurant_id


WHEN MATCHED AND (

       target.total_revenue
           IS DISTINCT FROM source.total_revenue

    OR target.avg_order_value
           IS DISTINCT FROM source.avg_order_value

    OR target.avg_orders_per_day
           IS DISTINCT FROM source.avg_orders_per_day

    OR target.avg_orders_per_week
           IS DISTINCT FROM source.avg_orders_per_week

    OR target.revenue_rank
           IS DISTINCT FROM source.revenue_rank
)

THEN UPDATE SET

    total_revenue = source.total_revenue,
    avg_order_value = source.avg_order_value,
    avg_orders_per_day = source.avg_orders_per_day,
    avg_orders_per_week = source.avg_orders_per_week,
    revenue_rank = source.revenue_rank,
    gold_updated_at = source.gold_updated_at


WHEN NOT MATCHED THEN

    INSERT (
        restaurant_id,
        total_revenue,
        avg_order_value,
        avg_orders_per_day,
        avg_orders_per_week,
        revenue_rank,
        gold_updated_at
    )

    VALUES (
        source.restaurant_id,
        source.total_revenue,
        source.avg_order_value,
        source.avg_orders_per_day,
        source.avg_orders_per_week,
        source.revenue_rank,
        source.gold_updated_at
    );