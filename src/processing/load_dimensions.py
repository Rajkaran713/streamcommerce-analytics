"""
Transformation Script: Load Dimension Tables from Staging
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import DatabaseConnection

class DimensionLoader:
    """Loads dimension tables from staging data"""
    
    def __init__(self):
        self.db = DatabaseConnection()
    
    def load_dim_customers(self):
        """Load customer dimension"""
        print("\n" + "="*80)
        print("📊 Loading dim_customers...")
        print("="*80)
        
        query = """
        INSERT INTO dim_customers (
            customer_id, customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state
        )
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM staging_customers
        ON CONFLICT (customer_id) DO NOTHING;
        """
        
        self.db.execute_query(query)
        
        # Get count
        result = self.db.fetch_one("SELECT COUNT(*) FROM dim_customers")
        print(f"✅ Loaded {result[0]:,} customers into dim_customers")
    
    def load_dim_products(self):
        """Load product dimension with category translation"""
        print("\n" + "="*80)
        print("📊 Loading dim_products...")
        print("="*80)
        
        # First, load category translation into a temp table
        print("📋 Loading category translations...")
        
        import pandas as pd
        cat_df = pd.read_csv('data/raw/product_category_name_translation.csv')
        
        # Create temp table for translations
        self.db.execute_query("""
            CREATE TEMP TABLE temp_category_translation (
                product_category_name VARCHAR(100),
                product_category_name_english VARCHAR(100)
            );
        """)
        
        # Insert translations
        data = [(row['product_category_name'], row['product_category_name_english']) 
                for _, row in cat_df.iterrows()]
        
        self.db.execute_many(
            "INSERT INTO temp_category_translation VALUES (%s, %s)",
            data
        )
        
        print(f"✅ Loaded {len(data)} category translations")
        
        # Now load products with English categories
        query = """
        INSERT INTO dim_products (
            product_id, product_category_name, product_category_name_english,
            product_name_length, product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm
        )
        SELECT DISTINCT
            p.product_id,
            p.product_category_name,
            COALESCE(t.product_category_name_english, 'unknown') as product_category_name_english,
            p.product_name_length,
            p.product_description_length,
            p.product_photos_qty,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm
        FROM staging_products p
        LEFT JOIN temp_category_translation t 
            ON p.product_category_name = t.product_category_name
        ON CONFLICT (product_id) DO NOTHING;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM dim_products")
        print(f"✅ Loaded {result[0]:,} products into dim_products")
    
    def load_dim_sellers(self):
        """Load seller dimension"""
        print("\n" + "="*80)
        print("📊 Loading dim_sellers...")
        print("="*80)
        
        query = """
        INSERT INTO dim_sellers (
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        )
        SELECT DISTINCT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM staging_sellers
        ON CONFLICT (seller_id) DO NOTHING;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM dim_sellers")
        print(f"✅ Loaded {result[0]:,} sellers into dim_sellers")
    
    def load_dim_date(self):
        """Generate date dimension for all dates in orders"""
        print("\n" + "="*80)
        print("📊 Loading dim_date...")
        print("="*80)
        
        query = """
        INSERT INTO dim_date (
            date_key, date, year, quarter, month, month_name,
            week, day, day_of_week, day_name, is_weekend, is_holiday
        )
        SELECT DISTINCT
            TO_CHAR(order_purchase_timestamp, 'YYYYMMDD')::INTEGER as date_key,
            DATE(order_purchase_timestamp) as date,
            EXTRACT(YEAR FROM order_purchase_timestamp)::INTEGER as year,
            EXTRACT(QUARTER FROM order_purchase_timestamp)::INTEGER as quarter,
            EXTRACT(MONTH FROM order_purchase_timestamp)::INTEGER as month,
            TO_CHAR(order_purchase_timestamp, 'Month') as month_name,
            EXTRACT(WEEK FROM order_purchase_timestamp)::INTEGER as week,
            EXTRACT(DAY FROM order_purchase_timestamp)::INTEGER as day,
            EXTRACT(DOW FROM order_purchase_timestamp)::INTEGER as day_of_week,
            TO_CHAR(order_purchase_timestamp, 'Day') as day_name,
            CASE WHEN EXTRACT(DOW FROM order_purchase_timestamp) IN (0, 6) 
                 THEN TRUE ELSE FALSE END as is_weekend,
            FALSE as is_holiday
        FROM staging_orders
        WHERE order_purchase_timestamp IS NOT NULL
        ON CONFLICT (date_key) DO NOTHING;
        """
        
        self.db.execute_query(query)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM dim_date")
        print(f"✅ Loaded {result[0]:,} dates into dim_date")
    
    def load_dim_geolocation(self):
        """Load geolocation dimension"""
        print("\n" + "="*80)
        print("📊 Loading dim_geolocation...")
        print("="*80)
        
        # Load from CSV
        import pandas as pd
        geo_df = pd.read_csv('data/raw/olist_geolocation_dataset.csv')
        
        print(f"📋 Processing {len(geo_df):,} geolocation records...")
        
        # Get unique zip codes (since there are many duplicates)
        # Take the first occurrence of each zip code
        geo_df = geo_df.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')
        
        print(f"📋 Found {len(geo_df):,} unique zip codes")
        
        data = [
            (
                row['geolocation_zip_code_prefix'],
                float(row['geolocation_lat']),
                float(row['geolocation_lng']),
                row['geolocation_city'],
                row['geolocation_state']
            )
            for _, row in geo_df.iterrows()
        ]
        
        query = """
        INSERT INTO dim_geolocation (
            zip_code_prefix, latitude, longitude, city, state
        )
        VALUES (%s, %s, %s, %s, %s)
        """
        
        self.db.execute_many(query, data)
        
        result = self.db.fetch_one("SELECT COUNT(*) FROM dim_geolocation")
        print(f"✅ Loaded {result[0]:,} geolocations into dim_geolocation")
    
    def run_all(self):
        """Load all dimension tables"""
        try:
            self.db.connect()
            
            print("\n" + "🌟"*40)
            print("STARTING DIMENSION TABLE TRANSFORMATION")
            print("🌟"*40)
            
            self.load_dim_customers()
            self.load_dim_products()
            self.load_dim_sellers()
            self.load_dim_date()
            self.load_dim_geolocation()
            
            print("\n" + "🎉"*40)
            print("ALL DIMENSION TABLES LOADED SUCCESSFULLY!")
            print("🎉"*40)
            
        except Exception as e:
            print(f"\n❌ Dimension loading failed: {e}")
            raise
        finally:
            self.db.disconnect()


if __name__ == "__main__":
    loader = DimensionLoader()
    loader.run_all()
