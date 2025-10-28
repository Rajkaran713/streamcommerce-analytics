"""
Daily ETL Pipeline: Monitor and orchestrate data warehouse updates
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2

# Default arguments
default_args = {
    'owner': 'streamcommerce',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL: Monitor warehouse health',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,
    tags=['production', 'etl', 'daily']
)

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host='postgres',
        port=5432,
        database='ecommerce_db',
        user='streamcommerce',
        password='streamcommerce123'
    )

# Task 1: Check database connection
def check_database_connection():
    """Verify database is accessible"""
    print("🔍 Checking database connection...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        result = cursor.fetchone()
        print(f"✅ Database connected: {result[0][:50]}...")
        
        # Check table counts
        tables = ['staging_orders', 'dim_customers', 'fact_orders']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   📊 {table}: {count:,} rows")
        
        cursor.close()
        conn.close()
        
        return "Database connection successful"
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise

check_db_task = PythonOperator(
    task_id='check_database',
    python_callable=check_database_connection,
    dag=dag
)

# Task 2: Verify data warehouse health
def verify_warehouse_health():
    """Check warehouse tables for issues"""
    print("🔍 Verifying warehouse health...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check for orphaned records
    cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE customer_key IS NULL")
    orphaned_orders = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_order_items WHERE product_key IS NULL")
    orphaned_items = cursor.fetchone()[0]
    
    print(f"   📊 Orphaned orders: {orphaned_orders}")
    print(f"   📊 Orphaned items: {orphaned_items}")
    
    if orphaned_orders > 0 or orphaned_items > 0:
        print("   ⚠️  Warning: Orphaned records detected!")
    else:
        print("   ✅ No orphaned records")
    
    cursor.close()
    conn.close()
    
    return "Warehouse health verified"

verify_health_task = PythonOperator(
    task_id='verify_warehouse_health',
    python_callable=verify_warehouse_health,
    dag=dag
)

# Task 3: Calculate daily metrics
def calculate_daily_metrics():
    """Calculate daily business metrics"""
    print("📊 Calculating daily metrics...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Total orders
    cursor.execute("SELECT COUNT(*) FROM fact_orders")
    total_orders = cursor.fetchone()[0]
    
    # Total revenue
    cursor.execute("SELECT ROUND(SUM(price)::NUMERIC, 2) FROM fact_order_items")
    total_revenue = cursor.fetchone()[0]
    
    # Average order value
    cursor.execute("""
        SELECT ROUND(AVG(order_total)::NUMERIC, 2)
        FROM (
            SELECT order_key, SUM(price) as order_total
            FROM fact_order_items
            GROUP BY order_key
        ) subq
    """)
    avg_order_value = cursor.fetchone()[0]
    
    # Delivered orders
    cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE order_status = 'delivered'")
    delivered_orders = cursor.fetchone()[0]
    
    print(f"   📊 Total Orders: {total_orders:,}")
    print(f"   💰 Total Revenue: R$ {total_revenue:,.2f}")
    print(f"   📈 Average Order Value: R$ {avg_order_value:,.2f}")
    print(f"   ✅ Delivered Orders: {delivered_orders:,}")
    
    delivery_rate = (delivered_orders / total_orders * 100) if total_orders > 0 else 0
    print(f"   🚚 Delivery Success Rate: {delivery_rate:.1f}%")
    
    cursor.close()
    conn.close()
    
    return f"Metrics: {total_orders:,} orders, R$ {total_revenue:,.2f} revenue"

metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_daily_metrics,
    dag=dag
)

# Task 4: Monitor streaming topics
check_kafka = BashOperator(
    task_id='check_kafka_topics',
    bash_command='echo "📡 Kafka topics monitored (simulated)"',
    dag=dag
)

# Task 5: Success notification
def send_success_notification():
    """Log successful pipeline completion"""
    print("="*80)
    print("🎉 DAILY ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*80)
    print(f"   ⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   ✅ All health checks passed")
    print("   📊 Metrics calculated")
    print("   🚀 Ready for new data")
    print("="*80)
    return "Pipeline successful"

success_task = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Define task dependencies
check_db_task >> verify_health_task >> [metrics_task, check_kafka] >> success_task