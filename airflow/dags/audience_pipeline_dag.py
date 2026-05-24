from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="audience_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python app/spark_jobs/bronze_to_silver.py"
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python app/spark_jobs/silver_to_gold.py"
    )

    bronze_to_silver >> silver_to_gold