"""
StreamCommerce Analytics Dashboard
Main application entry point
"""

import streamlit as st
import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Page config
st.set_page_config(
    page_title="StreamCommerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .subtitle {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Main page
st.markdown('<p class="main-header">🛒 StreamCommerce Analytics Platform</p>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Enterprise-Grade E-Commerce Intelligence & ML-Powered Insights</p>', unsafe_allow_html=True)

# Welcome section
st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🎯 **What This Is**")
    st.markdown("""
    A complete end-to-end data analytics and machine learning platform for e-commerce:
    
    - **96K+ Customers** analyzed
    - **32K+ Products** tracked
    - **110K+ Purchases** processed
    - **2 ML Models** deployed
    """)

with col2:
    st.markdown("### 🤖 **ML Models**")
    st.markdown("""
    Production-ready machine learning:
    
    1. **Customer Segmentation**
       - RFM Analysis + K-Means
       - 4 distinct segments
    
    2. **Product Recommendations**
       - Collaborative Filtering
       - Association Rules Mining
    """)

with col3:
    st.markdown("### 🚀 **Technology Stack**")
    st.markdown("""
    Modern data engineering:
    
    - **Python** (Pandas, Scikit-learn)
    - **PostgreSQL** (Data Warehouse)
    - **Apache Kafka** (Streaming)
    - **Apache Spark** (Processing)
    - **Docker** (Containerization)
    - **Streamlit** (Dashboards)
    """)

st.markdown("---")

# Navigation guide
st.markdown("### 📖 **Navigation Guide**")

st.info("""
Use the **sidebar** to navigate between pages:

- **🏠 Home**: Overview and key metrics
- **👥 Customers**: Customer segmentation and RFM analysis
- **🛒 Products**: Product recommendations and similar items
- **📊 Analytics**: Business insights and trends
- **🤖 ML Models**: Model performance and evaluation
""")

# Quick stats
st.markdown("### 📊 **Quick Stats**")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Customers",
        value="96.5K",
        delta="Active in dataset"
    )

with col2:
    st.metric(
        label="Total Products",
        value="32.2K",
        delta="Across 72 categories"
    )

with col3:
    st.metric(
        label="ML Models",
        value="2",
        delta="Production-ready"
    )

with col4:
    st.metric(
        label="Data Warehouse",
        value="10M+",
        delta="Records processed"
    )

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem 0;'>
    <p><strong>StreamCommerce Analytics Platform</strong></p>
    <p>Built with ❤️ using Streamlit, PostgreSQL, Kafka, Spark & Docker</p>
    <p style='font-size: 0.9rem;'>🎓 Data Engineer Portfolio Project</p>
</div>
""", unsafe_allow_html=True)
