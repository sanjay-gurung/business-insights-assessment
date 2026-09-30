MERGE INTO business_insights_gold_db.metric4_sales_trends_monitoring AS target

USING (

    WITH daily_revenue AS (
        SELECT
            CAST(DATE_TRUNC('day', creation_time_utc) AS DATE) AS period_date,
            'Daily' AS period_type,
            restaurant_id,
            item_category,
            SUM(item_price * item_quantity) AS revenue
        FROM business_insights_silver_db.order_items_transformed
        GROUP BY
            CAST(DATE_TRUNC('day', creation_time_utc) AS DATE),
            restaurant_id,
            item_category
    ),

    weekly_revenue AS (
        SELECT
            CAST(DATE_TRUNC('week', creation_time_utc) AS DATE) AS period_date,
            'Weekly' AS period_type,
            restaurant_id,
            item_category,
            SUM(item_price * item_quantity) AS revenue
        FROM business_insights_silver_db.order_items_transformed
        GROUP BY
            CAST(DATE_TRUNC('week', creation_time_utc) AS DATE),
            restaurant_id,
            item_category
    ),

    monthly_revenue AS (
        SELECT
            CAST(DATE_TRUNC('month', creation_time_utc) AS DATE) AS period_date,
            'Monthly' AS period_type,
            restaurant_id,
            item_category,
            SUM(item_price * item_quantity) AS revenue
        FROM business_insights_silver_db.order_items_transformed
        GROUP BY
            CAST(DATE_TRUNC('month', creation_time_utc) AS DATE),
            restaurant_id,
            item_category
    ),

    sales_trends AS (
        SELECT * FROM daily_revenue

        UNION ALL

        SELECT * FROM weekly_revenue

        UNION ALL

        SELECT * FROM monthly_revenue
    )

    SELECT
        restaurant_id,
        item_category,
        period_date,
        period_type,
        revenue,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS gold_updated_at
    FROM sales_trends

) AS source

ON  target.restaurant_id = source.restaurant_id
AND target.item_category = source.item_category
AND target.period_date = source.period_date
AND target.period_type = source.period_type


WHEN MATCHED AND (
    target.revenue IS DISTINCT FROM source.revenue
)

THEN UPDATE SET
    revenue = source.revenue,
    gold_updated_at = source.gold_updated_at


WHEN NOT MATCHED THEN

    INSERT (
        restaurant_id,
        item_category,
        period_date,
        period_type,
        revenue,
        gold_updated_at
    )

    VALUES (
        source.restaurant_id,
        source.item_category,
        source.period_date,
        source.period_type,
        source.revenue,
        source.gold_updated_at
    );