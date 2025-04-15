from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'dbt_transforms',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps',
        dag=dag
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts+',
        dag=dag
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --select marts+',
        dag=dag
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command='cd /opt/airflow/dbt && dbt docs generate',
        dag=dag
    )

    dbt_deps >> dbt_run >> dbt_test >> dbt_docs