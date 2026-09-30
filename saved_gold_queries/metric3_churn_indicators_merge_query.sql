MERGE INTO business_insights_gold_db.metric3_churn_indicators AS target

USING (

    WITH analysis_date AS (
        SELECT
            MAX(creation_time_utc) AS max_order_date
        FROM business_insights_silver_db.order_items_transformed
    ),

    last_order_days AS (
        SELECT
            oi.user_id,
            DATE_DIFF(
                'day',
                MAX(oi.creation_time_utc),
                MAX(a.max_order_date)
            ) AS days_since_last_order
        FROM business_insights_silver_db.order_items_transformed oi
        CROSS JOIN analysis_date a
        WHERE TRIM(oi.user_id) <> ''
        GROUP BY oi.user_id
    ),

    order_dates AS (
        SELECT
            user_id,
            order_id,
            creation_time_utc,
            LAG(creation_time_utc) OVER (
                PARTITION BY user_id
                ORDER BY creation_time_utc
            ) AS previous_order_date
        FROM business_insights_silver_db.order_items_transformed
        WHERE TRIM(user_id) <> ''
        GROUP BY
            user_id,
            order_id,
            creation_time_utc
    ),

    avg_gaps AS (
        SELECT
            user_id,
            AVG(
                DATE_DIFF(
                    'day',
                    previous_order_date,
                    creation_time_utc
                )
            ) AS avg_days_between_orders
        FROM order_dates
        GROUP BY user_id
    ),

    item_revenue_by_order AS (
        SELECT
            oi.user_id,
            oi.order_id,
            oi.creation_time_utc,
            SUM(oi.item_price * oi.item_quantity) AS item_revenue
        FROM business_insights_silver_db.order_items_transformed oi
        CROSS JOIN analysis_date a
        WHERE oi.creation_time_utc BETWEEN
                DATE_ADD('month', -24, a.max_order_date)
                AND a.max_order_date
          AND TRIM(oi.user_id) <> ''
        GROUP BY
            oi.user_id,
            oi.order_id,
            oi.creation_time_utc
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
            i.order_id,
            i.creation_time_utc,
            i.item_revenue,
            COALESCE(o.option_revenue, 0) AS option_revenue,
            i.item_revenue + COALESCE(o.option_revenue, 0)
                AS total_order_revenue
        FROM item_revenue_by_order i
        LEFT JOIN option_revenue_by_order o
            ON i.order_id = o.order_id
    ),

    period_spend AS (
        SELECT
            o.user_id,

            SUM(
                CASE
                    WHEN o.creation_time_utc >=
                        DATE_ADD('month', -12, a.max_order_date)
                    THEN o.total_order_revenue
                    ELSE 0
                END
            ) AS current_period_spend,

            SUM(
                CASE
                    WHEN o.creation_time_utc >=
                            DATE_ADD('month', -24, a.max_order_date)
                     AND o.creation_time_utc <
                            DATE_ADD('month', -12, a.max_order_date)
                    THEN o.total_order_revenue
                    ELSE 0
                END
            ) AS previous_period_spend

        FROM order_revenue o
        CROSS JOIN analysis_date a
        GROUP BY o.user_id
    ),

    spend_change AS (
        SELECT
            user_id,
            current_period_spend,
            previous_period_spend,

            (
                (current_period_spend - previous_period_spend)
                / NULLIF(previous_period_spend, 0)
            ) * 100 AS percent_change_in_spend

        FROM period_spend
    )

    SELECT
        l.user_id,
        l.days_since_last_order,

        ROUND(
            a.avg_days_between_orders,
            2
        ) AS avg_days_between_orders,

        ROUND(
            COALESCE(s.current_period_spend, 0),
            2
        ) AS current_period_spend,

        ROUND(
            COALESCE(s.previous_period_spend, 0),
            2
        ) AS previous_period_spend,

        ROUND(
            s.percent_change_in_spend,
            2
        ) AS percent_change_in_spend,

        CASE
            WHEN l.days_since_last_order > 45
                THEN 'At Risk'
            ELSE 'Low Risk'
        END AS churn_indicator,

        CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
            AS gold_updated_at

    FROM last_order_days l

    LEFT JOIN avg_gaps a
        ON l.user_id = a.user_id

    LEFT JOIN spend_change s
        ON l.user_id = s.user_id

) AS source


ON target.user_id = source.user_id


WHEN MATCHED AND (

       target.days_since_last_order
           IS DISTINCT FROM source.days_since_last_order

    OR target.avg_days_between_orders
           IS DISTINCT FROM source.avg_days_between_orders

    OR target.current_period_spend
           IS DISTINCT FROM source.current_period_spend

    OR target.previous_period_spend
           IS DISTINCT FROM source.previous_period_spend

    OR target.percent_change_in_spend
           IS DISTINCT FROM source.percent_change_in_spend

    OR target.churn_indicator
           IS DISTINCT FROM source.churn_indicator
)

THEN UPDATE SET

    days_since_last_order =
        source.days_since_last_order,

    avg_days_between_orders =
        source.avg_days_between_orders,

    current_period_spend =
        source.current_period_spend,

    previous_period_spend =
        source.previous_period_spend,

    percent_change_in_spend =
        source.percent_change_in_spend,

    churn_indicator =
        source.churn_indicator,

    gold_updated_at =
        source.gold_updated_at


WHEN NOT MATCHED THEN

    INSERT (
        user_id,
        days_since_last_order,
        avg_days_between_orders,
        current_period_spend,
        previous_period_spend,
        percent_change_in_spend,
        churn_indicator,
        gold_updated_at
    )

    VALUES (
        source.user_id,
        source.days_since_last_order,
        source.avg_days_between_orders,
        source.current_period_spend,
        source.previous_period_spend,
        source.percent_change_in_spend,
        source.churn_indicator,
        source.gold_updated_at
    );