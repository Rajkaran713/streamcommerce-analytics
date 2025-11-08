import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from dashboards.utils.data_loader import (
    get_monthly_revenue_trend,
    get_category_distribution,
    get_database_stats,
    format_currency
)

st.set_page_config(page_title="Business Analytics", page_icon="📊", layout="wide")

st.title("📊 Business Analytics & Insights")
st.markdown("Revenue trends, category performance, and key metrics")
st.markdown("---")

# Load data
with st.spinner("Loading analytics data..."):
    stats = get_database_stats()
    df_trend = get_monthly_revenue_trend()
    df_categories = get_category_distribution()

# KPIs
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Revenue", format_currency(stats['revenue']))

with col2:
    st.metric("Total Orders", f"{stats['orders']:,}")

with col3:
    st.metric("Avg Order Value", format_currency(stats['aov']))

with col4:
    delivery_rate = (stats['orders'] / stats['customers']) * 100
    st.metric("Orders per Customer", f"{delivery_rate:.2f}")

st.markdown("---")

# Revenue trend
st.markdown("### 📈 Monthly Revenue Trend")

if not df_trend.empty:
    df_trend['year_month'] = df_trend['year'].astype(str) + '-' + df_trend['month'].astype(str).str.zfill(2)
    
    fig_trend = px.line(
        df_trend,
        x='year_month',
        y='revenue',
        title="Revenue Over Time",
        labels={'year_month': 'Month', 'revenue': 'Revenue (R$)'}
    )
    fig_trend.update_traces(mode='lines+markers')
    st.plotly_chart(fig_trend, use_container_width=True)
    
    # Show data table
    with st.expander("📋 View Data Table"):
        st.dataframe(df_trend, use_container_width=True)
else:
    st.info("Revenue trend data not available")

st.markdown("---")

# Category performance
st.markdown("### 📦 Top Categories by Revenue")

if not df_categories.empty:
    fig_cat = px.bar(
        df_categories,
        x='category',
        y='revenue',
        title="Top 15 Product Categories",
        labels={'category': 'Category', 'revenue': 'Revenue (R$)'}
    )
    fig_cat.update_xaxes(tickangle=-45)
    st.plotly_chart(fig_cat, use_container_width=True)
    
    # Show data
    with st.expander("📋 View Category Details"):
        st.dataframe(df_categories, use_container_width=True)
else:
    st.info("Category data not available")

st.markdown("---")

# Business insights
st.markdown("### 💡 Key Business Insights")

insights_col1, insights_col2 = st.columns(2)

with insights_col1:
    st.info("""
    **Revenue Concentration**
    - Top 3 states account for ~60% of sales
    - São Paulo represents 38% of revenue
    - Opportunity: Expand to underserved regions
    """)
    
    st.success("""
    **Customer Retention**
    - 100% single-purchase customers
    - Major opportunity for loyalty programs
    - Potential 15-20% revenue increase
    """)

with insights_col2:
    st.warning("""
    **Delivery Performance**
    - Average: 12 days
    - 7% late deliveries
    - Action: Optimize logistics partners
    """)
    
    st.info("""
    **Product Performance**
    - Health & Beauty: Top category
    - Watches & Gifts: Highest AOV
    - Focus: Increase high-margin inventory
    """)

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p>Business Analytics powered by PostgreSQL Data Warehouse</p>
</div>
""", unsafe_allow_html=True)