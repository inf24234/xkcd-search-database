from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

args = {
    "owner": "airflow",
    "env": {
        "HADOOP_USER_NAME": "hadoop",
    },
}

with DAG(
    dag_id="xkcd_etl",
    description="ETL pipeline for XKCD comics",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=args,
) as dag:

    extract_task = BashOperator(
        task_id="extract_xkcd",
        bash_command="python3 /home/airflow/airflow/etl/extract_xkcd.py 100 ",
    )

    transform_task = BashOperator(
        task_id="transform_final",
        bash_command="python3 /home/airflow/airflow/etl/transform_final.py ",
    )

    load_task = BashOperator(
        task_id="load_mysql",
        bash_command="python3 /home/airflow/airflow/etl/load_mysql.py ",
    )

    extract_task >> transform_task >> load_task