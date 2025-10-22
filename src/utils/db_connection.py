"""
Database connection utilities for StreamCommerce Analytics
"""
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConnection:
    """Manages PostgreSQL database connections"""
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = os.getenv('POSTGRES_PORT', '5433')
        self.database = os.getenv('POSTGRES_DB', 'ecommerce_db')
        self.user = os.getenv('POSTGRES_USER', 'streamcommerce')
        self.password = os.getenv('POSTGRES_PASSWORD', 'streamcommerce123')
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            print(f"✅ Connected to database: {self.database}")
            return self.conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("✅ Database connection closed")
    
    def execute_query(self, query, params=None):
        """Execute a single query"""
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            return self.cursor
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Query execution failed: {e}")
            raise
    
    def execute_many(self, query, data):
        """Execute batch insert"""
        try:
            execute_batch(self.cursor, query, data)
            self.conn.commit()
            print(f"✅ Batch insert completed: {len(data)} rows")
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Batch insert failed: {e}")
            raise
    
    def fetch_one(self, query, params=None):
        """Fetch single row"""
        self.cursor.execute(query, params)
        return self.cursor.fetchone()
    
    def fetch_all(self, query, params=None):
        """Fetch all rows"""
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


if __name__ == "__main__":
    # Test the connection
    with DatabaseConnection() as db:
        result = db.fetch_one("SELECT version();")
        print(f"PostgreSQL version: {result[0]}")