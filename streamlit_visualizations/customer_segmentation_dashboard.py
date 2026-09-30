import streamlit as st
import pandas as pd
import plotly.express as px
from pyathena import connect

st.set_page_config(
    page_title="Business Insights Dashboard",
    layout="wide"
)

conn = connect(
    region_name="us-east-2",
    s3_staging_dir="s3://atherna-query-output-storage-v1-sanjay/"
)

######## Customer Segmentation ###########

st.title("Business Insights Dashboard")
st.header("Customer Segmentation Dashboard")

query = """
SELECT
    g.user_id,
    g.days_since_last_purchase,
    g.purchase_frequency,
    g.total_spend,
    g.segment,
    MAX(CASE WHEN s.is_loyalty THEN 1 ELSE 0 END) AS is_loyalty
FROM business_insights_gold_db.metric2_customer_segmentation_and_behavior g
LEFT JOIN business_insights_silver_db.order_items_transformed s
    ON g.user_id = s.user_id
GROUP BY
    g.user_id,
    g.days_since_last_purchase,
    g.purchase_frequency,
    g.total_spend,
    g.segment
"""

df = pd.read_sql(query, conn)

df["loyalty_status"] = df["is_loyalty"].map({
    1: "Loyalty Member",
    0: "Non-Loyalty Member"
})

loyalty_filter = st.selectbox(
    "Loyalty Status",
    ["All", "Loyalty Member", "Non-Loyalty Member"]
)

if loyalty_filter != "All":
    filtered_df = df[df["loyalty_status"] == loyalty_filter]
else:
    filtered_df = df

segment_counts = (
    filtered_df
    .groupby("segment")
    .size()
    .reset_index(name="customer_count")
)

fig = px.bar(
    segment_counts,
    x="segment",
    y="customer_count",
    text="customer_count",
    title="Customers by RFM Segment"
)

fig.update_layout(
    width=900,
    height=500,
    xaxis_title="Customer Segment",
    yaxis_title="Number of Customers"
)

st.plotly_chart(fig, use_container_width=False)


######## Churn Risk Indicators Dashboard ########

st.header("Churn Risk Indicators Dashboard")


query_churn = """
SELECT
    user_id,
    days_since_last_order,
    avg_days_between_orders,
    percent_change_in_spend,
    churn_indicator
FROM business_insights_gold_db.metric3_churn_indicators
"""

df_churn = pd.read_sql(query_churn, conn)


# Calculate average indicators by customer status
churn_summary = (
    df_churn
    .groupby("churn_indicator")
    .agg(
        avg_days_since_last_order=(
            "days_since_last_order",
            "mean"
        ),
        avg_days_between_orders=(
            "avg_days_between_orders",
            "mean"
        ),
        avg_spend_change=(
            "percent_change_in_spend",
            "mean"
        )
    )
    .reset_index()
)


# Create three columns
col1, col2, col3 = st.columns(3)


# Chart 1 - Days Since Last Order
fig1 = px.bar(
    churn_summary,
    x="churn_indicator",
    y="avg_days_since_last_order",
    text_auto=".1f",
    title="Days Since Last Order",
    labels={
        "churn_indicator": "Customer Status",
        "avg_days_since_last_order": "Average Days"
    }
)

fig1.update_layout(
    height=450,
    showlegend=False
)

with col1:
    st.plotly_chart(
        fig1,
        use_container_width=True
    )


# Chart 2 - Average Days Between Orders
fig2 = px.bar(
    churn_summary,
    x="churn_indicator",
    y="avg_days_between_orders",
    text_auto=".1f",
    title="Average Days Between Orders",
    labels={
        "churn_indicator": "Customer Status",
        "avg_days_between_orders": "Average Days"
    }
)

fig2.update_layout(
    height=450,
    showlegend=False
)

with col2:
    st.plotly_chart(
        fig2,
        use_container_width=True
    )


# Chart 3 - Average Spend Change
fig3 = px.bar(
    churn_summary,
    x="churn_indicator",
    y="avg_spend_change",
    text_auto=".1f",
    title="Average Spend Change",
    labels={
        "churn_indicator": "Customer Status",
        "avg_spend_change": "Average Spend Change (%)"
    }
)

fig3.update_layout(
    height=450,
    showlegend=False
)

with col3:
    st.plotly_chart(
        fig3,
        use_container_width=True
    )



######## Sales Trends and Seasonality Dashboard ########

st.header("Sales Trends and Seasonality Dashboard")

query_sales = """
SELECT
    period_date,
    period_type,
    restaurant_id,
    item_category,
    revenue
FROM business_insights_gold_db.metric4_sales_trends_monitoring
"""

df_sales = pd.read_sql(query_sales, conn)

# Convert period_date to datetime
df_sales["period_date"] = pd.to_datetime(df_sales["period_date"])


# -------------------------
# Filters
# -------------------------

col1, col2, col3 = st.columns(3)

with col1:
    period_filter = st.selectbox(
        "Time Period",
        ["Monthly", "Weekly"]
    )

with col2:
    location_options = sorted(
        df_sales["restaurant_id"].dropna().unique()
    )

    location_filter = st.selectbox(
        "Location",
        ["All"] + location_options
    )

with col3:
    category_options = sorted(
        df_sales["item_category"].dropna().unique()
    )

    category_filter = st.selectbox(
        "Product Category",
        ["All"] + category_options
    )


# -------------------------
# Apply Filters
# -------------------------

filtered_sales = df_sales[
    df_sales["period_type"] == period_filter
].copy()

if location_filter != "All":
    filtered_sales = filtered_sales[
        filtered_sales["restaurant_id"] == location_filter
    ]

if category_filter != "All":
    filtered_sales = filtered_sales[
        filtered_sales["item_category"] == category_filter
    ]


# -------------------------
# Aggregate Revenue
# -------------------------

sales_trend = (
    filtered_sales
    .groupby("period_date", as_index=False)["revenue"]
    .sum()
    .sort_values("period_date")
)


# -------------------------
# Line Chart
# -------------------------

fig_sales = px.line(
    sales_trend,
    x="period_date",
    y="revenue",
    markers=True,
    title=f"{period_filter} Sales Trend",
    labels={
        "period_date": "Date",
        "revenue": "Revenue ($)"
    }
)

fig_sales.update_layout(
    height=500,
    xaxis_title="Date",
    yaxis_title="Revenue ($)",
    hovermode="x unified"
)


# Center Chart
left, center, right = st.columns([1, 5, 1])

with center:
    st.plotly_chart(
        fig_sales,
        use_container_width=True
    )


######## Loyalty Program Impact Dashboard ########

import plotly.graph_objects as go

st.header("Loyalty Program Impact Dashboard")

query_loyalty = """
SELECT
    user_id,
    is_loyalty,
    avg_spend,
    repeat_orders,
    lifetime_value
FROM business_insights_gold_db.metric5_loyalty_program_impact
"""

df_loyalty = pd.read_sql(query_loyalty, conn)

df_loyalty["loyalty_status"] = df_loyalty["is_loyalty"].map({
    True: "Loyalty Member",
    False: "Non-Loyalty Member"
})


# Use median because the data contains extreme outliers
summary = (
    df_loyalty
    .groupby("loyalty_status")
    .agg(
        avg_spend=("avg_spend", "median"),
        repeat_orders=("repeat_orders", "median"),
        lifetime_value=("lifetime_value", "median")
    )
)


metrics = [
    "Average Spend",
    "Repeat Orders",
    "Lifetime Value"
]

columns = [
    "avg_spend",
    "repeat_orders",
    "lifetime_value"
]


# Normalize each metric to 0-100
normalized = summary.copy()

for col in columns:
    max_value = summary[col].max()

    if max_value > 0:
        normalized[col] = summary[col] / max_value * 100


fig = go.Figure()


# Loyalty Member
fig.add_trace(
    go.Scatter(
        x=metrics,

        y=[
            normalized.loc["Loyalty Member", "avg_spend"],
            normalized.loc["Loyalty Member", "repeat_orders"],
            normalized.loc["Loyalty Member", "lifetime_value"]
        ],

        mode="lines+markers",

        marker=dict(size=14),

        line=dict(width=3),

        name="Loyalty Member",

        text=[
            f"${summary.loc['Loyalty Member', 'avg_spend']:,.2f}",
            f"{summary.loc['Loyalty Member', 'repeat_orders']:,.1f}",
            f"${summary.loc['Loyalty Member', 'lifetime_value']:,.2f}"
        ],

        hovertemplate=(
            "<b>%{x}</b><br>"
            "Loyalty Member<br>"
            "Actual Value: %{text}"
            "<extra></extra>"
        )
    )
)


# Non-Loyalty Member
fig.add_trace(
    go.Scatter(
        x=metrics,

        y=[
            normalized.loc["Non-Loyalty Member", "avg_spend"],
            normalized.loc["Non-Loyalty Member", "repeat_orders"],
            normalized.loc["Non-Loyalty Member", "lifetime_value"]
        ],

        mode="lines+markers",

        marker=dict(size=14),

        line=dict(width=3),

        name="Non-Loyalty Member",

        text=[
            f"${summary.loc['Non-Loyalty Member', 'avg_spend']:,.2f}",
            f"{summary.loc['Non-Loyalty Member', 'repeat_orders']:,.1f}",
            f"${summary.loc['Non-Loyalty Member', 'lifetime_value']:,.2f}"
        ],

        hovertemplate=(
            "<b>%{x}</b><br>"
            "Non-Loyalty Member<br>"
            "Actual Value: %{text}"
            "<extra></extra>"
        )
    )
)


fig.update_layout(
    title="Loyalty vs Non-Loyalty Customer Behavior",

    height=450,

    xaxis_title="Customer Behavior Metric",

    yaxis=dict(
        title="Relative Performance (%)",
        range=[0, 110]
    ),

    legend_title="Customer Type",

    hovermode="x unified"
)


left, center, right = st.columns([1, 5, 1])

with center:
    st.plotly_chart(
        fig,
        use_container_width=True
    )


######## Location Performance Dashboard ########

st.header("Location Performance Dashboard")

query_location = """
SELECT
    restaurant_id,
    total_revenue,
    avg_order_value,
    avg_orders_per_day,
    avg_orders_per_week,
    revenue_rank
FROM business_insights_gold_db.metric6_top_performing_locations
"""

df_location = pd.read_sql(query_location, conn)


# Sort locations by revenue
df_location = df_location.sort_values(
    "total_revenue",
    ascending=True
)


# -------------------------
# Summary Metrics
# -------------------------

top_location = df_location.loc[
    df_location["total_revenue"].idxmax()
]

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Top Location",
        top_location["restaurant_id"]
    )

with col2:
    st.metric(
        "Top Location Revenue",
        f"${top_location['total_revenue']:,.2f}"
    )

with col3:
    st.metric(
        "Average Order Value",
        f"${top_location['avg_order_value']:,.2f}"
    )


# -------------------------
# Revenue Ranking Chart
# -------------------------

fig_location = px.bar(
    df_location,
    x="total_revenue",
    y="restaurant_id",
    orientation="h",

    hover_data={
        "restaurant_id": True,
        "total_revenue": ":$,.2f",
        "avg_order_value": ":$,.2f",
        "avg_orders_per_day": ":.1f",
        "avg_orders_per_week": ":.1f",
        "revenue_rank": True
    },

    labels={
        "restaurant_id": "Restaurant Location",
        "total_revenue": "Total Revenue ($)",
        "avg_order_value": "Average Order Value",
        "avg_orders_per_day": "Avg Orders / Day",
        "avg_orders_per_week": "Avg Orders / Week",
        "revenue_rank": "Revenue Rank"
    },

    title="Restaurant Locations Ranked by Total Revenue"
)


fig_location.update_layout(
    height=600,
    xaxis_title="Total Revenue ($)",
    yaxis_title="Restaurant Location"
)


# Center chart
left, center, right = st.columns([1, 5, 1])

with center:
    st.plotly_chart(
        fig_location,
        use_container_width=True
    )

