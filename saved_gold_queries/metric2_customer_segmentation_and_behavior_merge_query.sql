MERGE INTO business_insights_gold_db.metric2_customer_segmentation_and_behavior AS target

USING (

    WITH analysis_date AS (
        SELECT
            MAX(creation_time_utc) AS analysis_date
        FROM business_insights_silver_db.order_items_transformed
    ),

    recency AS (
        SELECT
            user_id,
            MAX(creation_time_utc) AS last_purchase_date,
            DATE_DIFF(
                'day',
                MAX(creation_time_utc),
                (SELECT analysis_date FROM analysis_date)
            ) AS days_since_last_purchase
        FROM business_insights_silver_db.order_items_transformed
        WHERE TRIM(user_id) <> ''
        GROUP BY user_id
    ),

    frequency AS (
        SELECT
            user_id,
            COUNT(DISTINCT order_id) AS purchase_frequency
        FROM business_insights_silver_db.order_items_transformed
        WHERE creation_time_utc BETWEEN
            DATE_ADD(
                'month',
                -36,
                (SELECT analysis_date FROM analysis_date)
            )
            AND (SELECT analysis_date FROM analysis_date)
          AND TRIM(user_id) <> ''
        GROUP BY user_id
    ),

    item_revenue_by_order AS (
        SELECT
            user_id,
            order_id,
            SUM(item_price * item_quantity) AS item_revenue
        FROM business_insights_silver_db.order_items_transformed
        WHERE creation_time_utc BETWEEN
            DATE_ADD(
                'month',
                -36,
                (SELECT analysis_date FROM analysis_date)
            )
            AND (SELECT analysis_date FROM analysis_date)
          AND TRIM(user_id) <> ''
        GROUP BY
            user_id,
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
            i.order_id,
            i.item_revenue,
            COALESCE(o.option_revenue, 0) AS option_revenue,
            i.item_revenue + COALESCE(o.option_revenue, 0)
                AS total_order_revenue
        FROM item_revenue_by_order i
        LEFT JOIN option_revenue_by_order o
            ON i.order_id = o.order_id
    ),

    monetary AS (
        SELECT
            user_id,
            SUM(total_order_revenue) AS total_spend
        FROM order_revenue
        GROUP BY user_id
    ),

    rfm AS (
        SELECT
            r.user_id,
            r.last_purchase_date,
            r.days_since_last_purchase,
            COALESCE(f.purchase_frequency, 0) AS purchase_frequency,
            ROUND(COALESCE(m.total_spend, 0), 2) AS total_spend
        FROM recency r
        LEFT JOIN frequency f
            ON r.user_id = f.user_id
        LEFT JOIN monetary m
            ON r.user_id = m.user_id
        ORDER BY r.days_since_last_purchase
    ),

    segmented_rfm AS (
        SELECT
            user_id,
            last_purchase_date,
            days_since_last_purchase,
            purchase_frequency,
            total_spend,

            NTILE(5) OVER (
                ORDER BY days_since_last_purchase DESC, user_id
            ) AS recency_score,

            NTILE(5) OVER (
                ORDER BY purchase_frequency ASC, user_id
            ) AS frequency_score,

            NTILE(5) OVER (
                ORDER BY total_spend ASC, user_id
            ) AS monetary_score

        FROM rfm
    )

    SELECT
        user_id,
        last_purchase_date,
        days_since_last_purchase,
        purchase_frequency,
        total_spend,

        CASE
            WHEN recency_score >= 4
                 AND frequency_score >= 4
                 AND monetary_score >= 4
                THEN 'VIP'

            WHEN recency_score >= 4
                 AND frequency_score <= 2
                THEN 'New Customer'

            WHEN recency_score <= 2
                 AND frequency_score <= 2
                THEN 'Churn Risk'

            ELSE 'Regular Customer'
        END AS segment,

        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS gold_updated_at

    FROM segmented_rfm

) AS source

ON target.user_id = source.user_id


WHEN MATCHED AND (

       target.last_purchase_date
           IS DISTINCT FROM source.last_purchase_date

    OR target.days_since_last_purchase
           IS DISTINCT FROM source.days_since_last_purchase

    OR target.purchase_frequency
           IS DISTINCT FROM source.purchase_frequency

    OR target.total_spend
           IS DISTINCT FROM source.total_spend

    OR target.segment
           IS DISTINCT FROM source.segment
)

THEN UPDATE SET
    last_purchase_date = source.last_purchase_date,
    days_since_last_purchase = source.days_since_last_purchase,
    purchase_frequency = source.purchase_frequency,
    total_spend = source.total_spend,
    segment = source.segment,
    gold_updated_at = source.gold_updated_at


WHEN NOT MATCHED THEN

    INSERT (
        user_id,
        last_purchase_date,
        days_since_last_purchase,
        purchase_frequency,
        total_spend,
        segment,
        gold_updated_at
    )

    VALUES (
        source.user_id,
        source.last_purchase_date,
        source.days_since_last_purchase,
        source.purchase_frequency,
        source.total_spend,
        source.segment,
        source.gold_updated_at
    );