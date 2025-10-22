-- ============================================================================
-- STREAMCOMMERCE ANALYTICS - DATABASE SCHEMA
-- Star Schema Design for Data Warehouse
-- ============================================================================

-- Drop existing tables if they exist (for fresh start)
DROP TABLE IF EXISTS fact_order_items CASCADE;
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS fact_reviews CASCADE;
DROP TABLE IF EXISTS fact_payments CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_sellers CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_geolocation CASCADE;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension: Customers
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Products
CREATE TABLE dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Sellers
CREATE TABLE dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date (for time-based analysis)
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Dimension: Geolocation
CREATE TABLE dim_geolocation (
    geo_key SERIAL PRIMARY KEY,
    zip_code_prefix VARCHAR(10) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    city VARCHAR(100),
    state VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Fact: Orders (main fact table)
CREATE TABLE fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    
    -- Date keys for time dimensions
    purchase_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Calculated metrics
    delivery_time_days INTEGER,
    estimated_delivery_time_days INTEGER,
    delivery_delay_days INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Order Items (detail level)
CREATE TABLE fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_key INTEGER REFERENCES fact_orders(order_key),
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER,
    product_key INTEGER REFERENCES dim_products(product_key),
    seller_key INTEGER REFERENCES dim_sellers(seller_key),
    
    -- Metrics
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Payments
CREATE TABLE fact_payments (
    payment_key SERIAL PRIMARY KEY,
    order_key INTEGER REFERENCES fact_orders(order_key),
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INTEGER,
    payment_type VARCHAR(20),
    payment_installments INTEGER,
    payment_value DECIMAL(10, 2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Reviews
CREATE TABLE fact_reviews (
    review_key SERIAL PRIMARY KEY,
    review_id VARCHAR(50) UNIQUE NOT NULL,
    order_key INTEGER REFERENCES fact_orders(order_key),
    order_id VARCHAR(50) NOT NULL,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Dimension table indexes
CREATE INDEX idx_customers_id ON dim_customers(customer_id);
CREATE INDEX idx_customers_state ON dim_customers(customer_state);
CREATE INDEX idx_products_id ON dim_products(product_id);
CREATE INDEX idx_products_category ON dim_products(product_category_name_english);
CREATE INDEX idx_sellers_id ON dim_sellers(seller_id);
CREATE INDEX idx_sellers_state ON dim_sellers(seller_state);
CREATE INDEX idx_date_date ON dim_date(date);
CREATE INDEX idx_geo_zip ON dim_geolocation(zip_code_prefix);

-- Fact table indexes
CREATE INDEX idx_orders_customer ON fact_orders(customer_key);
CREATE INDEX idx_orders_purchase_date ON fact_orders(order_purchase_timestamp);
CREATE INDEX idx_orders_status ON fact_orders(order_status);
CREATE INDEX idx_order_items_order ON fact_order_items(order_key);
CREATE INDEX idx_order_items_product ON fact_order_items(product_key);
CREATE INDEX idx_order_items_seller ON fact_order_items(seller_key);
CREATE INDEX idx_payments_order ON fact_payments(order_key);
CREATE INDEX idx_reviews_order ON fact_reviews(order_key);

-- ============================================================================
-- STAGING TABLES (for raw data loading)
-- ============================================================================

CREATE TABLE staging_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE staging_order_items (
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);

CREATE TABLE staging_products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE TABLE staging_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2)
);

CREATE TABLE staging_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

CREATE TABLE staging_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(20),
    payment_installments INTEGER,
    payment_value DECIMAL(10, 2)
);

CREATE TABLE staging_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE dim_customers IS 'Customer dimension with location information';
COMMENT ON TABLE dim_products IS 'Product dimension with category and physical attributes';
COMMENT ON TABLE dim_sellers IS 'Seller dimension with location information';
COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis';
COMMENT ON TABLE fact_orders IS 'Main fact table containing order-level metrics';
COMMENT ON TABLE fact_order_items IS 'Order item detail fact table';
COMMENT ON TABLE fact_payments IS 'Payment transaction fact table';
COMMENT ON TABLE fact_reviews IS 'Customer review fact table';

-- ============================================================================
-- SCHEMA CREATION COMPLETE
-- ============================================================================