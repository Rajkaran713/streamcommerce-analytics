"""
Data Loader Utilities for Streamlit Dashboard
Handles loading ML models, cached data, and database connections
"""

import pandas as pd
import pickle
import streamlit as st
import os
import sys

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from src.utils.db_connection import DatabaseConnection

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_customer_segments():
    """Load customer segmentation results"""
    try:
        df = pd.read_csv('outputs/customer_segments.csv')
        return df
    except FileNotFoundError:
        st.error("Customer segments file not found. Please run the segmentation model first.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_cluster_profiles():
    """Load cluster profile summaries"""
    try:
        df = pd.read_csv('outputs/cluster_profiles.csv')
        return df
    except FileNotFoundError:
        st.warning("Cluster profiles not found.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_recommendations():
    """Load pre-generated product recommendations"""
    try:
        df = pd.read_csv('outputs/recommendations/customer_recommendations.csv')
        return df
    except FileNotFoundError:
        st.warning("Recommendations file not found.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_association_rules():
    """Load product association rules"""
    try:
        df = pd.read_csv('outputs/recommendations/association_rules.csv')
        return df
    except FileNotFoundError:
        st.warning("Association rules not found.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_popular_products():
    """Load popular products for cold start"""
    try:
        df = pd.read_csv('outputs/recommendations/popular_products.csv')
        return df
    except FileNotFoundError:
        st.warning("Popular products not found.")
        return pd.DataFrame()

@st.cache_resource
def load_segmentation_model():
    """Load trained customer segmentation model"""
    try:
        with open('models/saved_models/customer_segmentation_model.pkl', 'rb') as f:
            model_artifacts = pickle.load(f)
        return model_artifacts
    except FileNotFoundError:
        st.error("Segmentation model not found.")
        return None

@st.cache_data(ttl=3600)
def load_similarity_matrix():
    """Load product similarity matrix"""
    try:
        df = pd.read_csv('models/saved_models/product_similarity_matrix.csv', index_col=0)
        return df
    except FileNotFoundError:
        st.warning("Similarity matrix not found.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_database_stats():
    """Get key statistics from database"""
    try:
        with DatabaseConnection() as db:
            # Total customers
            customers = db.fetch_one("SELECT COUNT(DISTINCT customer_id) FROM dim_customers")[0]
            
            # Total products
            products = db.fetch_one("SELECT COUNT(DISTINCT product_id) FROM dim_products")[0]
            
            # Total orders
            orders = db.fetch_one("SELECT COUNT(*) FROM fact_orders WHERE order_status = 'delivered'")[0]
            
            # Total revenue
            revenue = db.fetch_one("""
                SELECT ROUND(SUM(price)::NUMERIC, 2) 
                FROM fact_order_items oi
                JOIN fact_orders o ON oi.order_key = o.order_key
                WHERE o.order_status = 'delivered'
            """)[0]
            
            # Average order value
            aov = db.fetch_one("""
                SELECT ROUND(AVG(order_total)::NUMERIC, 2)
                FROM (
                    SELECT order_key, SUM(price) as order_total
                    FROM fact_order_items
                    GROUP BY order_key
                ) subq
            """)[0]
            
            return {
                'customers': customers,
                'products': products,
                'orders': orders,
                'revenue': float(revenue) if revenue else 0,
                'aov': float(aov) if aov else 0
            }
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return {
            'customers': 96478,
            'products': 32216,
            'orders': 96478,
            'revenue': 13591643.70,
            'aov': 137.75
        }

@st.cache_data(ttl=3600)
def get_category_distribution():
    """Get product category distribution"""
    try:
        with DatabaseConnection() as db:
            df = pd.read_sql("""
                SELECT 
                    p.product_category_name_english as category,
                    COUNT(DISTINCT oi.order_id) as orders,
                    ROUND(SUM(oi.price)::NUMERIC, 2) as revenue
                FROM fact_order_items oi
                JOIN dim_products p ON oi.product_key = p.product_key
                JOIN fact_orders o ON oi.order_key = o.order_key
                WHERE o.order_status = 'delivered'
                  AND p.product_category_name_english != 'unknown'
                GROUP BY p.product_category_name_english
                ORDER BY revenue DESC
                LIMIT 15
            """, db.conn)
            return df
    except Exception as e:
        st.error(f"Error loading category data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_monthly_revenue_trend():
    """Get monthly revenue trends"""
    try:
        with DatabaseConnection() as db:
            df = pd.read_sql("""
                SELECT 
                    d.year,
                    d.month,
                    d.month_name,
                    COUNT(DISTINCT o.order_id) as orders,
                    ROUND(SUM(oi.price)::NUMERIC, 2) as revenue
                FROM fact_orders o
                JOIN dim_date d ON o.purchase_date_key = d.date_key
                JOIN fact_order_items oi ON o.order_key = oi.order_key
                WHERE o.order_status = 'delivered'
                GROUP BY d.year, d.month, d.month_name
                ORDER BY d.year, d.month
            """, db.conn)
            return df
    except Exception as e:
        st.error(f"Error loading trend data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_top_customers(limit=100):
    """Get top customers by spending"""
    try:
        with DatabaseConnection() as db:
            df = pd.read_sql(f"""
                SELECT 
                    c.customer_id,
                    c.customer_state,
                    c.customer_city,
                    COUNT(DISTINCT o.order_id) as total_orders,
                    ROUND(SUM(oi.price)::NUMERIC, 2) as total_spent
                FROM dim_customers c
                JOIN fact_orders o ON c.customer_key = o.customer_key
                JOIN fact_order_items oi ON o.order_key = oi.order_key
                WHERE o.order_status = 'delivered'
                GROUP BY c.customer_id, c.customer_state, c.customer_city
                ORDER BY total_spent DESC
                LIMIT {limit}
            """, db.conn)
            return df
    except Exception as e:
        st.error(f"Error loading customer data: {e}")
        return pd.DataFrame()

def format_currency(value):
    """Format value as Brazilian Real currency"""
    return f"R$ {value:,.2f}"

def format_number(value):
    """Format large numbers with K/M suffix"""
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    else:
        return f"{value:.0f}"
