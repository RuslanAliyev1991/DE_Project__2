from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="my_dag",
    schedule_interval = None,
    default_args = {
        "owner": "Ruslan",
        "start_date": datetime(2026,3,2),
        "catchup": False
        #"retries": 5,
        #"retry_delay": timedelta(seconds=30),
        #"depends_on_past": True
    },
    tags = ["RealTime"],
    
) as mydag:
    
    # 1. API Taskı (Bunu arxa fona atırıq və terminal anında Success qaytarır)
    run_api = BashOperator(
        task_id='run_api_stream',
        bash_command='bash /opt/airflow/dags/test.sh '
    )

