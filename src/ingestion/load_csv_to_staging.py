"""
ETL Script: Load CSV files into PostgreSQL staging tables
"""
import pandas as pd
from pathlib import Path
import sys
import numpy as np
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import DatabaseConnection

class CSVLoader:
    """Loads CSV files into PostgreSQL staging tables"""
    
    def __init__(self, data_path='data/raw'):
        self.data_path = Path(data_path)
        self.db = DatabaseConnection()
        
    def load_customers(self):
        """Load customers CSV to staging"""
        print("\n" + "="*80)
        print("📦 Loading CUSTOMERS data...")
        print("="*80)
        
        df = pd.read_csv(self.data_path / 'olist_customers_dataset.csv')
        print(f"📊 Loaded {len(df):,} rows from CSV")
        
        # Prepare data for insertion
        data = [
            (
                row['customer_id'],
                row['customer_unique_id'],
                row['customer_zip_code_prefix'],
                row['customer_city'],
                row['customer_state']
            )
            for _, row in df.iterrows()
        ]
        
        # Insert into staging
        query = """
            INSERT INTO staging_customers 
            (customer_id, customer_unique_id, customer_zip_code_prefix, 
             customer_city, customer_state)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        self.db.execute_many(query, data)
        print(f"✅ Loaded {len(data):,} customers into staging_customers")
        
    def load_products(self):
        """Load products CSV to staging"""
        print("\n" + "="*80)
        print("📦 Loading PRODUCTS data...")
        print("="*80)
        
        df = pd.read_csv(self.data_path / 'olist_products_dataset.csv')
        print(f"📊 Loaded {len(df):,} rows from CSV")
        
        # Fill NaN values - using actual column names
        df = df.fillna({
            'product_category_name': 'unknown',
            'product_name_lenght': 0,  # Note: 'lenght' not 'length' in CSV
            'product_description_lenght': 0,  # Note: 'lenght' not 'length'
            'product_photos_qty': 0,
            'product_weight_g': 0,
            'product_length_cm': 0,
            'product_height_cm': 0,
            'product_width_cm': 0
        })
        
        data = [
            (
                row['product_id'],
                row['product_category_name'],
                int(row['product_name_lenght']) if pd.notna(row['product_name_lenght']) else 0,
                int(row['product_description_lenght']) if pd.notna(row['product_description_lenght']) else 0,
                int(row['product_photos_qty']) if pd.notna(row['product_photos_qty']) else 0,
                int(row['product_weight_g']) if pd.notna(row['product_weight_g']) else 0,
                int(row['product_length_cm']) if pd.notna(row['product_length_cm']) else 0,
                int(row['product_height_cm']) if pd.notna(row['product_height_cm']) else 0,
                int(row['product_width_cm']) if pd.notna(row['product_width_cm']) else 0
            )
            for _, row in df.iterrows()
        ]
        
        query = """
            INSERT INTO staging_products 
            (product_id, product_category_name, product_name_length,
             product_description_length, product_photos_qty, product_weight_g,
             product_length_cm, product_height_cm, product_width_cm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        self.db.execute_many(query, data)
        print(f"✅ Loaded {len(data):,} products into staging_products")
        
    def load_sellers(self):
        """Load sellers CSV to staging"""
        print("\n" + "="*80)
        print("📦 Loading SELLERS data...")
        print("="*80)
        
        df = pd.read_csv(self.data_path / 'olist_sellers_dataset.csv')
        print(f"📊 Loaded {len(df):,} rows from CSV")
        
        data = [
            (
                row['seller_id'],
                row['seller_zip_code_prefix'],
                row['seller_city'],
                row['seller_state']
            )
            for _, row in df.iterrows()
        ]
        
        query = """
            INSERT INTO staging_sellers 
            (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            VALUES (%s, %s, %s, %s)
        """
        
        self.db.execute_many(query, data)
        print(f"✅ Loaded {len(data):,} sellers into staging_sellers")
    
    def load_orders(self):
        """Load orders CSV to staging"""
        print("\n" + "="*80)
        print("📦 Loading ORDERS data...")
        print("="*80)
        
        df = pd.read_csv(self.data_path / 'olist_orders_dataset.csv')
        print(f"📊 Loaded {len(df):,} rows from CSV")
        
        # Convert dates
        date_columns = [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
        
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Replace NaT and NaN with None for SQL NULL
        df = df.replace({pd.NaT: None, np.nan: None})
        
        data = []
        for _, row in df.iterrows():
            data.append((
                row['order_id'],
                row['customer_id'],
                row['order_status'],
                row['order_purchase_timestamp'],
                row['order_approved_at'],
                row['order_delivered_carrier_date'],
                row['order_delivered_customer_date'],
                row['order_estimated_delivery_date']
            ))
        
        query = """
            INSERT INTO staging_orders 
            (order_id, customer_id, order_status, order_purchase_timestamp,
             order_approved_at, order_delivered_carrier_date,
             order_delivered_customer_date, order_estimated_delivery_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        self.db.execute_many(query, data)
        print(f"✅ Loaded {len(data):,} orders into staging_orders")
    
    def run_all(self):
        """Run all data loads"""
        try:
            self.db.connect()
            
            print("\n" + "🚀"*40)
            print("STARTING ETL PROCESS: CSV → PostgreSQL Staging Tables")
            print("🚀"*40)
            
            self.load_customers()
            self.load_products()
            self.load_sellers()
            self.load_orders()
            
            print("\n" + "🎉"*40)
            print("ETL PROCESS COMPLETED SUCCESSFULLY!")
            print("🎉"*40)
            
        except Exception as e:
            print(f"\n❌ ETL process failed: {e}")
            raise
        finally:
            self.db.disconnect()


if __name__ == "__main__":
    loader = CSVLoader()
    loader.run_all()