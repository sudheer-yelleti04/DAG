from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# -----------------------------
# Default arguments (applied to all tasks unless overridden)
# -----------------------------
default_args = {
    'owner': 'airflow',                      # DAG owner name
    'depends_on_past': False,                # Task depends on previous run of same task?
    'email': ['alerts@company.com'],         # Alert email recipients
    'email_on_failure': True,                # Send email if a task fails
    'email_on_retry': False,                 # Send email when retry happens
    'retries': 2,                            # Number of retries before marking task as failed
    'retry_delay': timedelta(minutes=5),     # Delay between retries
    'start_date': datetime(2024, 1, 1),      # Start date for DAG scheduling
    'end_date': None,                        # End date (optional)
    'execution_timeout': timedelta(hours=1), # Max time task can run before failing
    'retry_exponential_backoff': True,       # Progressive retry delay
    'max_retry_delay': timedelta(minutes=30),# Max delay for exponential backoff
    'priority_weight': 5,                    # Priority for scheduler
    'sla': timedelta(hours=2),               # SLA (service level agreement) time for task
    'trigger_rule': 'all_success'            # When downstream tasks should trigger
}

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id='basic_dag_template',             # Unique DAG ID
    description='A basic Airflow DAG template with all parameters',
    default_args=default_args,               # Apply defaults to all tasks
    schedule_interval='@daily',              # Cron or preset like '@daily', '@hourly'
    catchup=False,                           # Run missed DAGs since start_date? False → no
    max_active_runs=1,                       # Prevent overlapping DAG runs
    concurrency=5,                           # Max number of parallel tasks allowed
    dagrun_timeout=timedelta(hours=2),       # Timeout for entire DAG run
    tags=['example', 'template', 'demo'],    # Tag your DAGs for organization
    doc_md="""
    ### DAG Documentation
    This DAG demonstrates all major Airflow DAG parameters:
    - Task retries
    - Scheduling
    - SLA monitoring
    - Email alerts
    - DAG timeout
    """
) as dag:

    # -----------------------------
    # Python Task Definition
    # -----------------------------
    def extract(**context):
        print("Extracting data...")
        return "raw_data_path"

    def transform(**context):
        print("Transforming data...")
        return "transformed_data_path"

    def load(**context):
        print("Loading data to target...")
        return "success"

    extract_task = PythonOperator(
        task_id='extract_task',               # Unique task name
        python_callable=extract,              # Function to call
        provide_context=True,                 # Pass context (like ds, execution_date)
        retries=1,                            # Override retries for this task
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20),
        depends_on_past=False,
        sla=timedelta(minutes=30),
        pool='default_pool',                  # Optional: use specific task pool
        queue='default',                      # Optional: target worker queue
        priority_weight=10,
        doc_md="#### Extract Task: Fetch data from source"
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True,
        retries=2,
        doc_md="#### Transform Task: Clean and process data"
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True,
        trigger_rule='all_success',           # Run only if all upstream succeed
        doc_md="#### Load Task: Load data into destination"
    )

    bash_notify = BashOperator(
        task_id='notify_success',
        bash_command='echo "Pipeline Completed Successfully!"',
        trigger_rule='all_done'               # Runs even if upstream fails
    )

    # -----------------------------
    # Task Dependencies
    # -----------------------------
    extract_task >> transform_task >> load_task >> bash_notify
