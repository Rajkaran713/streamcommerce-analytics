import streamlit as st
import pandas as pd
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from dashboards.utils.data_loader import load_recommendations, load_association_rules, load_popular_products

st.set_page_config(page_title="Product Recommendations", page_icon="🛒", layout="wide")

st.title("🛒 Product Recommendation System")
st.markdown("Collaborative Filtering + Association Rules")
st.markdown("---")

# Load data
with st.spinner("Loading recommendations..."):
    df_recs = load_recommendations()
    df_rules = load_association_rules()
    df_popular = load_popular_products()

# Sidebar - Customer search
st.sidebar.header("🔍 Get Recommendations")
customer_search = st.sidebar.text_input("Enter Customer ID")

if customer_search:
    customer_recs = df_recs[df_recs['customer_id'] == customer_search]
    
    if not customer_recs.empty:
        st.markdown(f"### 🎯 Recommendations for Customer: `{customer_search}`")
        st.dataframe(customer_recs[['product_id', 'category', 'score', 'rank']], use_container_width=True)
    else:
        st.warning(f"No recommendations found for customer: {customer_search}")
        st.info("Showing popular products as fallback:")
        st.dataframe(df_popular.head(10), use_container_width=True)

# Overview metrics
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Recommendations", f"{len(df_recs):,}")

with col2:
    st.metric("Association Rules", f"{len(df_rules):,}")

with col3:
    st.metric("Customers Covered", f"{df_recs['customer_id'].nunique():,}")

st.markdown("---")

# Association rules
st.markdown("### 🔗 Top Product Associations")
st.markdown("Products frequently bought together")

if not df_rules.empty:
    top_rules = df_rules.head(20)
    st.dataframe(
        top_rules[['category_a', 'category_b', 'support', 'confidence_a_to_b', 'lift', 'pair_count']],
        use_container_width=True
    )
else:
    st.info("No association rules data available")

st.markdown("---")

# Popular products
st.markdown("### 🔥 Most Popular Products")
st.markdown("Top products for cold start recommendations")

if not df_popular.empty:
    st.dataframe(df_popular.head(20), use_container_width=True)

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p>Product Recommendations powered by Collaborative Filtering & Market Basket Analysis</p>
</div>
""", unsafe_allow_html=True)