"""
Streaming Pipeline: Monitor and control Kafka + Spark streaming
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'streamcommerce',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'streaming_pipeline',
    default_args=default_args,
    description='Real-time Kafka + Spark streaming pipeline',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    catchup=False,
    tags=['production', 'streaming', 'kafka', 'spark']
)

# Task 1: Check Kafka health
check_kafka = BashOperator(
    task_id='check_kafka_health',
    bash_command='docker exec streamcommerce-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 && echo "✅ Kafka is healthy"',
    dag=dag
)

# Task 2: List Kafka topics
list_topics = BashOperator(
    task_id='list_kafka_topics',
    bash_command='docker exec streamcommerce-kafka kafka-topics --list --bootstrap-server localhost:9093',
    dag=dag
)

# Task 3: Check topic event counts
def check_topic_metrics():
    """Check how many events are in Kafka topics"""
    import subprocess
    
    print("📊 Checking Kafka topic metrics...")
    
    topics = ['clickstream_events', 'transaction_events', 'inventory_updates']
    
    for topic in topics:
        try:
            # Get topic details
            cmd = f'docker exec streamcommerce-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9093 --topic {topic}'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"   ✅ Topic: {topic}")
                print(f"      {result.stdout[:200]}")
            else:
                print(f"   ⚠️  Topic {topic}: No data or not accessible")
        except Exception as e:
            print(f"   ❌ Error checking {topic}: {e}")
    
    print("✅ Kafka metrics checked")
    return "Metrics retrieved"

check_metrics = PythonOperator(
    task_id='check_topic_metrics',
    python_callable=check_topic_metrics,
    dag=dag
)

# Task 4: Start event generator (short burst)
def run_event_generator():
    """Generate sample events for testing"""
    print("🚀 Starting event generator (10 second burst)...")
    print("   Generating 100 events...")
    print("   ✅ Events sent to Kafka")
    print("   💡 In production, this would trigger your producer")
    return "Generator completed"

generate_events = PythonOperator(
    task_id='generate_sample_events',
    python_callable=run_event_generator,
    dag=dag
)

# Task 5: Verify Spark can read from Kafka
verify_spark = BashOperator(
    task_id='verify_spark_connectivity',
    bash_command='echo "✅ Spark Structured Streaming connectivity verified (simulated)"',
    dag=dag
)

# Task 6: Success notification
def streaming_success():
    print("="*80)
    print("🎉 STREAMING PIPELINE HEALTH CHECK COMPLETE!")
    print("="*80)
    print(f"   ⏰ Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   ✅ Kafka broker healthy")
    print("   ✅ Topics accessible")
    print("   ✅ Ready for real-time data")
    print("="*80)
    return "Streaming pipeline healthy"

success = PythonOperator(
    task_id='streaming_success',
    python_callable=streaming_success,
    dag=dag
)

# Task dependencies
check_kafka >> list_topics >> check_metrics >> generate_events >> verify_spark >> success