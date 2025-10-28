"""
Test DAG: Verify Airflow is working
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'streamcommerce',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'test_pipeline',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test', 'tutorial']
)

# Task 1: Print hello
def print_hello():
    print("Hello from Airflow!")
    print("StreamCommerce Analytics Platform")
    return "Success!"

hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=dag
)

# Task 2: Print date
print_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

# Task 3: Check Python version
check_python = BashOperator(
    task_id='check_python',
    bash_command='python --version',
    dag=dag
)

# Task 4: Success message
def success_message():
    print("✅ All tasks completed!")
    print("🎉 Your Airflow DAG is working!")
    return "Pipeline successful"

success_task = PythonOperator(
    task_id='success_message',
    python_callable=success_message,
    dag=dag
)

# Define task dependencies
hello_task >> [print_date, check_python] >> success_task
