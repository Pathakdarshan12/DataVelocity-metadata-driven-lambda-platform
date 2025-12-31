import os
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Page configuration
st.set_page_config(
    page_title="Food Delivery Analytics",
    page_icon="🍕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        margin-top: 2rem;
        margin-bottom: 1rem;
        color: #262730;
    }
</style>
""", unsafe_allow_html=True)

load_dotenv()

# Snowflake connection
@st.cache_resource
def init_connection():
    """Initialize Snowflake connection"""
    return snowflake.connector.connect(
        user = os.getenv('SNOWFLAKE_USER'),
        password = os.getenv('SNOWFLAKE_PASSWORD'),
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database = os.getenv('SNOWFLAKE_DATABASE'),
        schema = os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

@st.cache_data(ttl=600)
def run_query(query):
    """Run query and return results as DataFrame"""
    conn = init_connection()
    return pd.read_sql(query, conn)


# Sidebar Navigation
st.sidebar.title("🍕 Navigation")
page = st.sidebar.radio(
    "Select Dashboard",
    [
        "📊 Executive Overview",
        "👥 Customer Analytics",
        "🍽️ Restaurant Performance",
        "🚗 Delivery Analytics",
        "📦 Order Analytics",
        "💰 Revenue Analytics"
    ]
)

# Date filter in sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Date Filter")
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=30), datetime.now()),
    max_value=datetime.now()
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = date_range[0]
    end_date = datetime.now().date()

# ============================================================================
# PAGE 1: EXECUTIVE OVERVIEW
# ============================================================================
if page == "📊 Executive Overview":
    st.markdown('<div class="main-header">🎯 Executive Overview Dashboard</div>', unsafe_allow_html=True)

    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)

    # Total Orders
    query_orders = f"""
    SELECT COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS,
           SUM(TOTAL_AMOUNT) as TOTAL_REVENUE,
           AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE,
           COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS
    FROM ANALYTICS.MART_ORDER_SUMMARY
    WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
    """
    metrics = run_query(query_orders)

    with col1:
        st.metric("Total Orders", f"{metrics['TOTAL_ORDERS'][0]:,}")
    with col2:
        st.metric("Total Revenue", f"₹{metrics['TOTAL_REVENUE'][0]:,.2f}")
    with col3:
        st.metric("Avg Order Value", f"₹{metrics['AVG_ORDER_VALUE'][0]:,.2f}")
    with col4:
        st.metric("Unique Customers", f"{metrics['UNIQUE_CUSTOMERS'][0]:,}")

    # Row 2: Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Daily Revenue Trend")
        query_daily = f"""
        SELECT DATE(ORDER_DATE) as ORDER_DATE,
               COUNT(DISTINCT ORDER_ID) as ORDERS,
               SUM(TOTAL_AMOUNT) as REVENUE
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY DATE(ORDER_DATE)
        ORDER BY ORDER_DATE
        """
        daily_data = run_query(query_daily)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_data['ORDER_DATE'],
            y=daily_data['REVENUE'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color='#1f77b4', width=2)
        ))
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🕐 Orders by Time of Day")
        query_time = f"""
        SELECT TIME_OF_DAY,
               COUNT(DISTINCT ORDER_ID) as ORDERS
        FROM ANALYTICS.MART_ORDER_TIME_ANALYSIS
        WHERE ORDER_DATE_KEY BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY TIME_OF_DAY
        ORDER BY 
            CASE TIME_OF_DAY
                WHEN 'MORNING' THEN 1
                WHEN 'AFTERNOON' THEN 2
                WHEN 'EVENING' THEN 3
                WHEN 'NIGHT' THEN 4
            END
        """
        time_data = run_query(query_time)
        fig = px.bar(time_data, x='TIME_OF_DAY', y='ORDERS',
                     color='TIME_OF_DAY',
                     color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Row 3: More insights
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Top 5 Restaurants")
        query_top_rest = f"""
        SELECT RESTAURANT_NAME,
               TOTAL_ORDERS,
               TOTAL_REVENUE,
               AVG_ORDER_VALUE
        FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
        ORDER BY TOTAL_ORDERS DESC
        LIMIT 5
        """
        top_restaurants = run_query(query_top_rest)
        st.dataframe(
            top_restaurants.style.format({
                'TOTAL_ORDERS': '{:,.0f}',
                'TOTAL_REVENUE': '₹{:,.2f}',
                'AVG_ORDER_VALUE': '₹{:,.2f}'
            }),
            use_container_width=True,
            hide_index=True
        )

    with col2:
        st.subheader("🍽️ Cuisine Performance")
        query_cuisine = f"""
        SELECT CUISINE_TYPE,
               TOTAL_ORDERS,
               TOTAL_REVENUE
        FROM ANALYTICS.MART_CUISINE_PERFORMANCE
        ORDER BY TOTAL_ORDERS DESC
        LIMIT 5
        """
        cuisine_data = run_query(query_cuisine)
        fig = px.pie(cuisine_data, values='TOTAL_ORDERS', names='CUISINE_TYPE',
                     title='Orders by Cuisine Type')
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PAGE 2: CUSTOMER ANALYTICS
# ============================================================================
elif page == "👥 Customer Analytics":
    st.markdown('<div class="main-header">👥 Customer Analytics Dashboard</div>', unsafe_allow_html=True)

    # Customer Metrics
    col1, col2, col3, col4 = st.columns(4)

    query_customer_metrics = """
    SELECT 
        COUNT(DISTINCT CUSTOMER_ID) as TOTAL_CUSTOMERS,
        AVG(LIFETIME_VALUE) as AVG_LTV,
        AVG(TOTAL_ORDERS) as AVG_ORDERS_PER_CUSTOMER,
        SUM(CASE WHEN CUSTOMER_STATUS = 'ACTIVE' THEN 1 ELSE 0 END) as ACTIVE_CUSTOMERS
    FROM ANALYTICS.MART_CUSTOMER_ORDER_SUMMARY
    """
    cust_metrics = run_query(query_customer_metrics)

    with col1:
        st.metric("Total Customers", f"{cust_metrics['TOTAL_CUSTOMERS'][0]:,}")
    with col2:
        st.metric("Avg Lifetime Value", f"₹{cust_metrics['AVG_LTV'][0]:,.2f}")
    with col3:
        st.metric("Avg Orders/Customer", f"{cust_metrics['AVG_ORDERS_PER_CUSTOMER'][0]:.1f}")
    with col4:
        st.metric("Active Customers", f"{cust_metrics['ACTIVE_CUSTOMERS'][0]:,}")

    # Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🎯 Customer Segmentation")
        query_segments = """
        SELECT CUSTOMER_SEGMENT,
               COUNT(DISTINCT CUSTOMER_ID) as CUSTOMERS,
               AVG(LIFETIME_VALUE) as AVG_LTV
        FROM ANALYTICS.MART_CUSTOMER_ORDER_SUMMARY
        GROUP BY CUSTOMER_SEGMENT
        ORDER BY 
            CASE CUSTOMER_SEGMENT
                WHEN 'VIP' THEN 1
                WHEN 'LOYAL' THEN 2
                WHEN 'REGULAR' THEN 3
                WHEN 'REPEAT' THEN 4
                WHEN 'NEW' THEN 5
            END
        """
        segments = run_query(query_segments)
        fig = px.bar(segments, x='CUSTOMER_SEGMENT', y='CUSTOMERS',
                     color='AVG_LTV',
                     color_continuous_scale='Blues',
                     labels={'CUSTOMERS': 'Number of Customers', 'AVG_LTV': 'Avg LTV'})
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("📊 Customer Status Distribution")
        query_status = """
        SELECT CUSTOMER_STATUS,
               COUNT(DISTINCT CUSTOMER_ID) as CUSTOMERS
        FROM ANALYTICS.MART_CUSTOMER_ORDER_SUMMARY
        GROUP BY CUSTOMER_STATUS
        """
        status_data = run_query(query_status)
        fig = px.pie(status_data, values='CUSTOMERS', names='CUSTOMER_STATUS',
                     color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c'])
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Row 3
    st.subheader("📈 Customer Cohort Retention")
    query_cohort = """
    SELECT COHORT_MONTH,
           MONTHS_SINCE_FIRST_ORDER,
           RETENTION_RATE
    FROM ANALYTICS.MART_CUSTOMER_COHORT_ANALYSIS
    WHERE COHORT_MONTH >= DATEADD(MONTH, -6, CURRENT_DATE())
    ORDER BY COHORT_MONTH, MONTHS_SINCE_FIRST_ORDER
    """
    cohort_data = run_query(query_cohort)

    if not cohort_data.empty:
        pivot_cohort = cohort_data.pivot(
            index='COHORT_MONTH',
            columns='MONTHS_SINCE_FIRST_ORDER',
            values='RETENTION_RATE'
        )
        fig = px.imshow(pivot_cohort,
                        labels=dict(x="Months Since First Order", y="Cohort Month", color="Retention %"),
                        color_continuous_scale='RdYlGn',
                        aspect='auto')
        fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PAGE 3: RESTAURANT PERFORMANCE
# ============================================================================
elif page == "🍽️ Restaurant Performance":
    st.markdown('<div class="main-header">🍽️ Restaurant Performance Dashboard</div>', unsafe_allow_html=True)

    # Restaurant Metrics
    col1, col2, col3, col4 = st.columns(4)

    query_rest_metrics = """
    SELECT 
        COUNT(DISTINCT RESTAURANT_ID) as TOTAL_RESTAURANTS,
        AVG(TOTAL_ORDERS) as AVG_ORDERS,
        AVG(AVG_ORDER_VALUE) as AVG_ORDER_VALUE,
        AVG(CANCELLATION_RATE) as AVG_CANCEL_RATE
    FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
    """
    rest_metrics = run_query(query_rest_metrics)

    with col1:
        st.metric("Total Restaurants", f"{rest_metrics['TOTAL_RESTAURANTS'][0]:,}")
    with col2:
        st.metric("Avg Orders/Restaurant", f"{rest_metrics['AVG_ORDERS'][0]:.0f}")
    with col3:
        st.metric("Avg Order Value", f"₹{rest_metrics['AVG_ORDER_VALUE'][0]:,.2f}")
    with col4:
        st.metric("Avg Cancellation Rate", f"{rest_metrics['AVG_CANCEL_RATE'][0]:.1f}%")

    # Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Restaurant Tier Distribution")
        query_tiers = """
        SELECT RESTAURANT_TIER,
               COUNT(DISTINCT RESTAURANT_ID) as RESTAURANTS,
               AVG(TOTAL_REVENUE) as AVG_REVENUE
        FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
        GROUP BY RESTAURANT_TIER
        ORDER BY 
            CASE RESTAURANT_TIER
                WHEN 'PLATINUM' THEN 1
                WHEN 'GOLD' THEN 2
                WHEN 'SILVER' THEN 3
                WHEN 'BRONZE' THEN 4
            END
        """
        tiers = run_query(query_tiers)
        fig = px.bar(tiers, x='RESTAURANT_TIER', y='RESTAURANTS',
                     color='AVG_REVENUE',
                     color_continuous_scale='Viridis')
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("📊 Top Cuisines by Revenue")
        query_cuisine_rev = """
        SELECT CUISINE_TYPE,
               TOTAL_REVENUE
        FROM ANALYTICS.MART_CUISINE_PERFORMANCE
        ORDER BY TOTAL_REVENUE DESC
        LIMIT 8
        """
        cuisine_rev = run_query(query_cuisine_rev)
        fig = px.bar(cuisine_rev, y='CUISINE_TYPE', x='TOTAL_REVENUE',
                     orientation='h',
                     color='TOTAL_REVENUE',
                     color_continuous_scale='Blues')
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Row 3: Restaurant Performance Table
    st.subheader("📋 Top Performing Restaurants")

    # City filter
    query_cities = "SELECT DISTINCT CITY FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE ORDER BY CITY"
    cities = run_query(query_cities)
    selected_city = st.selectbox("Filter by City", ["All"] + cities['CITY'].tolist())

    city_filter = "" if selected_city == "All" else f"WHERE CITY = '{selected_city}'"

    query_rest_table = f"""
    SELECT RESTAURANT_NAME,
           CUISINE_TYPE,
           CITY,
           TOTAL_ORDERS,
           TOTAL_REVENUE,
           AVG_ORDER_VALUE,
           CANCELLATION_RATE,
           RESTAURANT_TIER
    FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
    {city_filter}
    ORDER BY TOTAL_REVENUE DESC
    LIMIT 20
    """
    rest_table = run_query(query_rest_table)
    st.dataframe(
        rest_table.style.format({
            'TOTAL_ORDERS': '{:,.0f}',
            'TOTAL_REVENUE': '₹{:,.2f}',
            'AVG_ORDER_VALUE': '₹{:,.2f}',
            'CANCELLATION_RATE': '{:.1f}%'
        }),
        use_container_width=True,
        hide_index=True
    )

# ============================================================================
# PAGE 4: DELIVERY ANALYTICS
# ============================================================================
elif page == "🚗 Delivery Analytics":
    st.markdown('<div class="main-header">🚗 Delivery Analytics Dashboard</div>', unsafe_allow_html=True)

    # Delivery Metrics
    col1, col2, col3, col4 = st.columns(4)

    query_delivery_metrics = f"""
    SELECT 
        COUNT(DISTINCT DELIVERY_ID) as TOTAL_DELIVERIES,
        AVG(CASE WHEN DELIVERY_TIMELINESS = 'ON_TIME' THEN 1 ELSE 0 END) * 100 as ON_TIME_PCT,
        AVG(ACTUAL_DELIVERY_TIME_MINS) as AVG_DELIVERY_TIME,
        AVG(TIME_VARIANCE_MINS) as AVG_VARIANCE
    FROM ANALYTICS.MART_DELIVERY_TIME_ANALYSIS
    WHERE DELIVERY_DATE BETWEEN '{start_date}' AND '{end_date}'
    """
    del_metrics = run_query(query_delivery_metrics)

    with col1:
        st.metric("Total Deliveries", f"{del_metrics['TOTAL_DELIVERIES'][0]:,}")
    with col2:
        st.metric("On-Time Delivery %", f"{del_metrics['ON_TIME_PCT'][0]:.1f}%")
    with col3:
        st.metric("Avg Delivery Time", f"{del_metrics['AVG_DELIVERY_TIME'][0]:.0f} mins")
    with col4:
        variance = del_metrics['AVG_VARIANCE'][0]
        st.metric("Avg Time Variance", f"{variance:+.0f} mins")

    # Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Delivery Timeliness")
        query_timeliness = f"""
        SELECT DELIVERY_TIMELINESS,
               COUNT(DISTINCT DELIVERY_ID) as DELIVERIES
        FROM ANALYTICS.MART_DELIVERY_TIME_ANALYSIS
        WHERE DELIVERY_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY DELIVERY_TIMELINESS
        """
        timeliness = run_query(query_timeliness)
        fig = px.pie(timeliness, values='DELIVERIES', names='DELIVERY_TIMELINESS',
                     color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c'])
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🚴 Agent Performance Tiers")
        query_agent_tiers = """
        SELECT PERFORMANCE_TIER,
               COUNT(DISTINCT DELIVERY_AGENT_ID) as AGENTS,
               AVG(SUCCESS_RATE) as AVG_SUCCESS_RATE
        FROM ANALYTICS.MART_DELIVERY_AGENT_PERFORMANCE
        GROUP BY PERFORMANCE_TIER
        ORDER BY 
            CASE PERFORMANCE_TIER
                WHEN 'EXCELLENT' THEN 1
                WHEN 'GOOD' THEN 2
                WHEN 'AVERAGE' THEN 3
                WHEN 'NEEDS_IMPROVEMENT' THEN 4
            END
        """
        agent_tiers = run_query(query_agent_tiers)
        fig = px.bar(agent_tiers, x='PERFORMANCE_TIER', y='AGENTS',
                     color='AVG_SUCCESS_RATE',
                     color_continuous_scale='RdYlGn')
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Row 3: Top Delivery Agents
    st.subheader("🏆 Top Delivery Agents")
    query_top_agents = """
    SELECT DELIVERY_AGENT_NAME,
           CITY,
           TOTAL_DELIVERIES,
           SUCCESSFUL_DELIVERIES,
           SUCCESS_RATE,
           AVG_ESTIMATED_TIME_MINS,
           PERFORMANCE_TIER
    FROM ANALYTICS.MART_DELIVERY_AGENT_PERFORMANCE
    ORDER BY SUCCESSFUL_DELIVERIES DESC
    LIMIT 15
    """
    top_agents = run_query(query_top_agents)
    st.dataframe(
        top_agents.style.format({
            'TOTAL_DELIVERIES': '{:,.0f}',
            'SUCCESSFUL_DELIVERIES': '{:,.0f}',
            'SUCCESS_RATE': '{:.1f}%',
            'AVG_ESTIMATED_TIME_MINS': '{:.0f} mins'
        }),
        use_container_width=True,
        hide_index=True
    )

# ============================================================================
# PAGE 5: ORDER ANALYTICS
# ============================================================================
elif page == "📦 Order Analytics":
    st.markdown('<div class="main-header">📦 Order Analytics Dashboard</div>', unsafe_allow_html=True)

    # Order Metrics
    col1, col2, col3, col4 = st.columns(4)

    query_order_metrics = f"""
    SELECT 
        COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS,
        SUM(IS_COMPLETED) as COMPLETED_ORDERS,
        SUM(IS_CANCELLED) as CANCELLED_ORDERS,
        AVG(ORDER_PROCESSING_TIME_MINS) as AVG_PROCESS_TIME
    FROM ANALYTICS.MART_ORDER_SUMMARY
    WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
    """
    order_metrics = run_query(query_order_metrics)

    total_orders = order_metrics['TOTAL_ORDERS'][0]
    completed = order_metrics['COMPLETED_ORDERS'][0]
    cancelled = order_metrics['CANCELLED_ORDERS'][0]

    with col1:
        st.metric("Total Orders", f"{total_orders:,}")
    with col2:
        st.metric("Completed Orders", f"{completed:,}",
                  delta=f"{(completed / total_orders * 100):.1f}%")
    with col3:
        st.metric("Cancelled Orders", f"{cancelled:,}",
                  delta=f"{(cancelled / total_orders * 100):.1f}%", delta_color="inverse")
    with col4:
        st.metric("Avg Process Time", f"{order_metrics['AVG_PROCESS_TIME'][0]:.0f} mins")

    # Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Order Value Distribution")
        query_value_dist = f"""
        SELECT ORDER_VALUE_SEGMENT,
               COUNT(DISTINCT ORDER_ID) as ORDERS,
               SUM(TOTAL_AMOUNT) as REVENUE
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ORDER_VALUE_SEGMENT
        ORDER BY 
            CASE ORDER_VALUE_SEGMENT
                WHEN 'PREMIUM' THEN 1
                WHEN 'HIGH_VALUE' THEN 2
                WHEN 'MEDIUM_VALUE' THEN 3
                WHEN 'LOW_VALUE' THEN 4
            END
        """
        value_dist = run_query(query_value_dist)
        fig = px.bar(value_dist, x='ORDER_VALUE_SEGMENT', y='ORDERS',
                     color='REVENUE',
                     color_continuous_scale='Greens')
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🕐 Peak Ordering Hours")
        query_peak_hours = f"""
        SELECT ORDER_HOUR,
               COUNT(DISTINCT ORDER_ID) as ORDERS
        FROM ANALYTICS.MART_ORDER_TIME_ANALYSIS
        WHERE ORDER_DATE_KEY BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ORDER_HOUR
        ORDER BY ORDER_HOUR
        """
        peak_hours = run_query(query_peak_hours)
        fig = px.line(peak_hours, x='ORDER_HOUR', y='ORDERS',
                      markers=True,
                      line_shape='spline')
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Row 3
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📅 Weekday vs Weekend Orders")
        query_day_type = f"""
        SELECT DAY_TYPE,
               COUNT(DISTINCT ORDER_ID) as ORDERS,
               AVG(TOTAL_AMOUNT) as AVG_VALUE
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY DAY_TYPE
        """
        day_type = run_query(query_day_type)
        fig = px.bar(day_type, x='DAY_TYPE', y='ORDERS',
                     color='AVG_VALUE',
                     text='ORDERS')
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🍽️ Meal Time Distribution")
        query_meal = f"""
        SELECT MEAL_TIME,
               COUNT(DISTINCT ORDER_ID) as ORDERS
        FROM ANALYTICS.MART_ORDER_TIME_ANALYSIS
        WHERE ORDER_DATE_KEY BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY MEAL_TIME
        ORDER BY 
            CASE MEAL_TIME
                WHEN 'BREAKFAST' THEN 1
                WHEN 'LUNCH' THEN 2
                WHEN 'SNACK' THEN 3
                WHEN 'DINNER' THEN 4
                WHEN 'LATE_NIGHT' THEN 5
            END
        """
        meal_data = run_query(query_meal)
        fig = px.pie(meal_data, values='ORDERS', names='MEAL_TIME',
                     hole=0.4)
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

# # ============================================================================
# # PAGE 6: REVENUE ANALYTICS
# # ============================================================================
# elif page == "💰 Revenue Analytics":
#     st.markdown('<div class="main-header">💰 Revenue Analytics Dashboard</div>', unsafe_allow_html=True)
#
#     # Revenue Metrics
#     col1, col2, col3, col4 = st.columns(4)
#
#     query_revenue = f"""
#     SELECT
#         SUM(TOTAL_AMOUNT) as TOTAL_REVENUE,
#         AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE,
#         SUM(TOTAL_AMOUNT) / COUNT(DISTINCT CUSTOMER_ID) as REVENUE_PER_CUSTOMER,
#         COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS
#     FROM ANALYTICS.MART_ORDER_SUMMARY
#     WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
#     """
#     rev_metrics = run_query(query_revenue)
#
#     with col1:
#         st.metric("Total Revenue", f"₹{rev_metrics['TOTAL_REVENUE'][0]:,.2f}")
#     with col2:
#         st.metric("Avg Order Value", f"₹{rev_metrics['AVG_ORDER_VALUE'][0]:,.2f}")
#     with col3:
#         st.metric("Revenue/Customer", f"₹{rev_metrics['REVENUE_PER_CUSTOMER'][0]:,.2f}")
#     with col4:
#         st.metric("Total Orders", f"{rev_metrics['TOTAL_ORDERS'][0]:,}")
#
#     # Row 2
#     st.subheader("📈 Revenue Trend Over Time")
#     query_rev_trend = f"""
#     SELECT DATE(ORDER_DATE) as DATE,
#            SUM(TOTAL_AMOUNT) as DAILY_REVENUE,
#            COUNT(DISTINCT ORDER_ID) as DAILY_ORDERS,
#            AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE
#     FROM ANALYTICS.MART_ORDER_SUMMARY
#     WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
#     GROUP BY DATE(ORDER_DATE)
#     ORDER BY DATE
#     """
#     rev_trend = run_query(query_rev_trend)