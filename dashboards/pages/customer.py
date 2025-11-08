import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from dashboards.utils.data_loader import load_customer_segments, load_cluster_profiles, format_currency

st.set_page_config(page_title="Customer Segments", page_icon="👥", layout="wide")

st.title("👥 Customer Segmentation Analysis")
st.markdown("RFM Analysis + K-Means Clustering Results")
st.markdown("---")

# Load data
with st.spinner("Loading customer segments..."):
    df_segments = load_customer_segments()
    df_profiles = load_cluster_profiles()

if df_segments.empty:
    st.error("No segmentation data available. Please run the segmentation model first.")
    st.stop()

# Sidebar filters
st.sidebar.header("🔍 Filters")
selected_segments = st.sidebar.multiselect(
    "Select Segments",
    options=df_segments['segment_name'].unique(),
    default=df_segments['segment_name'].unique()
)

filtered_df = df_segments[df_segments['segment_name'].isin(selected_segments)]

# Overview metrics
st.markdown("### 📊 Segment Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Customers", f"{len(filtered_df):,}")

with col2:
    st.metric("Number of Segments", len(filtered_df['segment_name'].unique()))

with col3:
    avg_monetary = filtered_df['monetary'].mean()
    st.metric("Avg Customer Value", format_currency(avg_monetary))

with col4:
    total_revenue = filtered_df['monetary'].sum()
    st.metric("Total Revenue", format_currency(total_revenue))

st.markdown("---")

# Segment distribution
st.markdown("### 📈 Segment Distribution")

col_left, col_right = st.columns(2)

with col_left:
    # Pie chart
    segment_counts = filtered_df['segment_name'].value_counts()
    fig_pie = px.pie(
        values=segment_counts.values,
        names=segment_counts.index,
        title="Customer Distribution by Segment"
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    # Revenue by segment
    segment_revenue = filtered_df.groupby('segment_name')['monetary'].sum().sort_values(ascending=False)
    fig_bar = px.bar(
        x=segment_revenue.index,
        y=segment_revenue.values,
        title="Total Revenue by Segment",
        labels={'x': 'Segment', 'y': 'Revenue (R$)'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")

# RFM distributions
st.markdown("### 📊 RFM Metrics Distribution")

col1, col2, col3 = st.columns(3)

with col1:
    fig_recency = px.histogram(filtered_df, x='recency', nbins=50, title="Recency Distribution")
    st.plotly_chart(fig_recency, use_container_width=True)

with col2:
    fig_frequency = px.histogram(filtered_df, x='frequency', nbins=30, title="Frequency Distribution")
    st.plotly_chart(fig_frequency, use_container_width=True)

with col3:
    fig_monetary = px.histogram(filtered_df, x='monetary', nbins=50, title="Monetary Distribution")
    st.plotly_chart(fig_monetary, use_container_width=True)

st.markdown("---")

# Cluster visualization
st.markdown("### 🎨 3D Cluster Visualization")

fig_3d = px.scatter_3d(
    filtered_df,
    x='recency',
    y='frequency',
    z='monetary',
    color='segment_name',
    title="Customer Segments in RFM Space",
    labels={'recency': 'Recency (days)', 'frequency': 'Frequency', 'monetary': 'Monetary (R$)'}
)
st.plotly_chart(fig_3d, use_container_width=True)

st.markdown("---")

# Segment profiles table
st.markdown("### 📋 Detailed Segment Profiles")

if not df_profiles.empty:
    st.dataframe(df_profiles, use_container_width=True)
else:
    # Calculate manually if profiles not loaded
    profiles = filtered_df.groupby('segment_name').agg({
        'recency': ['mean', 'median'],
        'frequency': ['mean', 'median'],
        'monetary': ['mean', 'median', 'sum'],
        'customer_id': 'count'
    }).round(2)
    st.dataframe(profiles, use_container_width=True)

    st.markdown("---")

# Business recommendations
st.markdown("### 💼 Marketing Recommendations by Segment")

segment_recommendations = {
    "💎 VIP Champions": [
        "Offer exclusive VIP rewards and early access to new products",
        "Invite to exclusive events and provide premium customer service",
        "Send personalized thank you messages",
        "Encourage referrals with generous bonuses"
    ],
    "🌟 Loyal Customers": [
        "Implement loyalty points program",
        "Send targeted promotions on complementary products",
        "Offer volume discounts and bundle deals",
        "Request reviews and testimonials"
    ],
    "😴 Lost/Churned": [
        "Launch win-back campaigns with 15-20% discounts",
        "Send personalized emails asking for feedback",
        "Offer free shipping on next order",
        "Re-engage with abandoned cart reminders"
    ],
    "🆕 Potential Loyalists": [
        "Welcome email series introducing product range",
        "First purchase discount for second order",
        "Educational content about products",
        "Build relationship with regular engagement"
    ]
}

for segment in selected_segments:
    with st.expander(f"**{segment}** - Recommended Actions"):
        recs = segment_recommendations.get(segment, ["Custom strategy needed"])
        for rec in recs:
            st.markdown(f"• {rec}")

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p>Customer Segmentation powered by RFM Analysis & K-Means Clustering</p>
</div>
""", unsafe_allow_html=True)