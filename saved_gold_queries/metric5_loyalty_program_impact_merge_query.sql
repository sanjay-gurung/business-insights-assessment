MERGE INTO business_insights_gold_db.metric5_loyalty_program_impact AS target

USING (

    WITH item_revenue_by_order AS (
        SELECT
            user_id,
            is_loyalty,
            order_id,
            SUM(item_price * item_quantity) AS item_revenue
        FROM business_insights_silver_db.order_items_transformed
        WHERE TRIM(user_id) <> ''
        GROUP BY
            user_id,
            is_loyalty,
            order_id
    ),

    option_revenue_by_order AS (
        SELECT
            order_id,
            SUM(option_price * option_quantity) AS option_revenue
        FROM business_insights_silver_db.order_item_options_transformed
        GROUP BY order_id
    ),

    order_revenue AS (
        SELECT
            i.user_id,
            i.is_loyalty,
            i.order_id,
            i.item_revenue + COALESCE(o.option_revenue, 0)
                AS total_order_revenue
        FROM item_revenue_by_order i
        LEFT JOIN option_revenue_by_order o
            ON i.order_id = o.order_id
    )

    SELECT
        user_id,
        is_loyalty,
        ROUND(AVG(total_order_revenue), 2) AS avg_spend,
        GREATEST(COUNT(DISTINCT order_id) - 1, 0) AS repeat_orders,
        ROUND(SUM(total_order_revenue), 2) AS lifetime_value,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS gold_updated_at

    FROM order_revenue

    GROUP BY
        user_id,
        is_loyalty

) AS source


ON  target.user_id = source.user_id
AND target.is_loyalty = source.is_loyalty


WHEN MATCHED AND (

       target.avg_spend
           IS DISTINCT FROM source.avg_spend

    OR target.repeat_orders
           IS DISTINCT FROM source.repeat_orders

    OR target.lifetime_value
           IS DISTINCT FROM source.lifetime_value

)

THEN UPDATE SET

    avg_spend = source.avg_spend,
    repeat_orders = source.repeat_orders,
    lifetime_value = source.lifetime_value,
    gold_updated_at = source.gold_updated_at


WHEN NOT MATCHED THEN

    INSERT (
        user_id,
        is_loyalty,
        avg_spend,
        repeat_orders,
        lifetime_value,
        gold_updated_at
    )

    VALUES (
        source.user_id,
        source.is_loyalty,
        source.avg_spend,
        source.repeat_orders,
        source.lifetime_value,
        source.gold_updated_at
    );