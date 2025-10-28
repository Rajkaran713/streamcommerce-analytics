"""
Transformation Script: Load Fact Tables from Staging
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import DatabaseConnection
import pandas as pd

class FactLoader:
    """Loads fact tables from staging data with calculated metrics"""
    
    def __init__(self):
        self.db = DatabaseConnection()
    
    def load_fact_orders(self):
        """Load orders fact table with calculated metrics"""
        print("\n" + "="*80)
        print("📊 Loading fact_orders...")
        print("="*80)
        
        query = """
        INSERT INTO fact_orders (
            order_id, customer_key, order_status,
            order_purchase_timestamp, order_approved_at,
            order_delivered_carrier_date, order_delivered_customer_date,
            order_estimated_delivery_date, purchase_date_key,
            delivery_time_days, estimated_delivery_time_days, delivery_delay_days
        )
        SELECT 
            o.order_id,
            c.customer_key,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INTEGER as purchase_date_key,
            
            -- Calculated metrics
            CASE 
                WHEN o.order_delivered_customer_date IS NOT NULL 
                THEN EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))::INTEGER
                ELSE NULL
            END as delivery_time_days,
            
            EXTRACT(DAY FROM (o.order_estimated_delivery_date - o.order_purchase_timestamp))::INTEGER as estimated_delivery_time_days,
            
            CASE 
                WHEN o.order_delivered_customer_date IS NOT NULL 
                THEN EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date))::INTEGER
                ELSE NULL
            END as delivery_delay_days
            
        FROM staging_orders o
        INNER JOIN dim_customers c ON o.customer_id = c.customer_id
        ON CONFLICT (order_id) DO NOTHING;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM fact_orders")
        print(f"✅ Loaded {result[0]:,} orders into fact_orders")
        
        # Show some stats
        stats = self.db.fetch_one("""
            SELECT 
                COUNT(*) as total_orders,
                COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) as delivered_orders,
                ROUND(AVG(delivery_time_days), 2) as avg_delivery_days
            FROM fact_orders
            WHERE delivery_time_days IS NOT NULL
        """)
        
        print(f"   📈 {stats[1]:,} delivered orders")
        print(f"   📈 {stats[2]} days average delivery time")
    
    def load_fact_order_items(self):
        """Load order items fact table"""
        print("\n" + "="*80)
        print("📊 Loading fact_order_items...")
        print("="*80)
        
        query = """
        INSERT INTO fact_order_items (
            order_key, order_id, order_item_id, product_key, seller_key,
            shipping_limit_date, price, freight_value
        )
        SELECT 
            fo.order_key,
            oi.order_id,
            oi.order_item_id,
            p.product_key,
            s.seller_key,
            oi.shipping_limit_date,
            oi.price,
            oi.freight_value
        FROM staging_order_items oi
        INNER JOIN fact_orders fo ON oi.order_id = fo.order_id
        INNER JOIN dim_products p ON oi.product_id = p.product_id
        INNER JOIN dim_sellers s ON oi.seller_id = s.seller_id;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM fact_order_items")
        print(f"✅ Loaded {result[0]:,} order items into fact_order_items")
        
        # Show revenue stats
        stats = self.db.fetch_one("""
            SELECT 
                COUNT(*) as total_items,
                ROUND(SUM(price)::NUMERIC, 2) as total_revenue,
                ROUND(AVG(price)::NUMERIC, 2) as avg_item_price
            FROM fact_order_items
        """)
        
        print(f"   💰 R$ {stats[1]:,} total revenue")
        print(f"   💰 R$ {stats[2]} average item price")
    
    def load_fact_payments(self):
        """Load payments fact table"""
        print("\n" + "="*80)
        print("📊 Loading fact_payments...")
        print("="*80)
        
        query = """
        INSERT INTO fact_payments (
            order_key, order_id, payment_sequential, payment_type,
            payment_installments, payment_value
        )
        SELECT 
            fo.order_key,
            p.order_id,
            p.payment_sequential,
            p.payment_type,
            p.payment_installments,
            p.payment_value
        FROM staging_payments p
        INNER JOIN fact_orders fo ON p.order_id = fo.order_id;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM fact_payments")
        print(f"✅ Loaded {result[0]:,} payments into fact_payments")
        
        # Show payment method breakdown
        print("\n   💳 Payment Methods:")
        methods = self.db.fetch_all("""
            SELECT 
                payment_type,
                COUNT(*) as count,
                ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::NUMERIC, 2) as percentage
            FROM fact_payments
            GROUP BY payment_type
            ORDER BY count DESC
        """)
        
        for method in methods:
            print(f"      {method[0]:15s}: {method[1]:6,} ({method[2]}%)")
    
    def load_fact_reviews(self):
        """Load reviews fact table"""
        print("\n" + "="*80)
        print("📊 Loading fact_reviews...")
        print("="*80)
        
        query = """
        INSERT INTO fact_reviews (
            review_id, order_key, order_id, review_score,
            review_comment_title, review_comment_message,
            review_creation_date, review_answer_timestamp
        )
        SELECT 
            r.review_id,
            fo.order_key,
            r.order_id,
            r.review_score,
            r.review_comment_title,
            r.review_comment_message,
            r.review_creation_date,
            r.review_answer_timestamp
        FROM staging_reviews r
        INNER JOIN fact_orders fo ON r.order_id = fo.order_id
        ON CONFLICT (review_id) DO NOTHING;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM fact_reviews")
        print(f"✅ Loaded {result[0]:,} reviews into fact_reviews")
        
        # Show review score distribution
        print("\n   ⭐ Review Score Distribution:")
        scores = self.db.fetch_all("""
            SELECT 
                review_score,
                COUNT(*) as count,
                ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::NUMERIC, 2) as percentage
            FROM fact_reviews
            GROUP BY review_score
            ORDER BY review_score DESC
        """)
        
        for score in scores:
            stars = "⭐" * int(score[0])
            print(f"      {stars:10s} ({score[0]}): {score[1]:6,} ({score[2]}%)")
    
    def run_all(self):
        """Load all fact tables"""
        try:
            self.db.connect()
            
            print("\n" + "🔥"*40)
            print("STARTING FACT TABLE TRANSFORMATION")
            print("🔥"*40)
            
            self.load_fact_orders()
            self.load_fact_order_items()
            self.load_fact_payments()
            self.load_fact_reviews()
            
            print("\n" + "🎉"*40)
            print("ALL FACT TABLES LOADED SUCCESSFULLY!")
            print("🎉"*40)
            
            # Final summary
            print("\n" + "="*80)
            print("📊 DATA WAREHOUSE SUMMARY")
            print("="*80)
            
            summary = self.db.fetch_all("""
                SELECT 
                    'dim_customers' as table_name, COUNT(*) as records FROM dim_customers
                UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
                UNION ALL SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
                UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
                UNION ALL SELECT 'dim_geolocation', COUNT(*) FROM dim_geolocation
                UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders
                UNION ALL SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
                UNION ALL SELECT 'fact_payments', COUNT(*) FROM fact_payments
                UNION ALL SELECT 'fact_reviews', COUNT(*) FROM fact_reviews
            """)
            
            total = 0
            for table in summary:
                print(f"   {table[0]:20s}: {table[1]:>10,} rows")
                total += table[1]
            
            print(f"\n   {'TOTAL RECORDS':20s}: {total:>10,} rows")
            print("="*80)
            
        except Exception as e:
            print(f"\n❌ Fact loading failed: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.db.disconnect()


if __name__ == "__main__":
    loader = FactLoader()
    loader.run_all()
