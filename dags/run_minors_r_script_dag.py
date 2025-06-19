from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

default_args = {
    'owner': 'nathan',
}

dag = DAG(
    'run_MINORS_baseballr_etl_pipeline_daily',
    default_args=default_args,
    description='Run an R script every day at 10AM',
    schedule_interval='0 14 * * *', #None
    start_date=datetime(2025, 6, 17),
    catchup=False,
    tags=['rscript'],
)


# --- CONFIG ---
BACKFILL_FLAG = 1

START_DATE = datetime.strptime("2025-04-02", "%Y-%m-%d") # 03-20 then 03-28 then #07-19
END_DATE = datetime.strptime("2025-04-05", "%Y-%m-%d") # 03-21 then 07-14 then #09-30

date_range = [
    (START_DATE + timedelta(days=i)).strftime("%Y-%m-%d")
    for i in range((END_DATE - START_DATE).days + 1)
]


if BACKFILL_FLAG == 0:
    date_range = [(datetime.today() - timedelta(days=1)).date()] #yesterday, not sure why I need to subtract 2 days

previous_task = None  # Keep track of the end of the last date's chain
for date in date_range:
    bash_cmd = f'Rscript {AIRFLOW_HOME}/r_scripts/MINORS_baseballr_pbp_scheduler.R {date}'

    run_r_script = BashOperator(
        task_id=f"run_r_script_{date}",
        bash_command=bash_cmd,
        dag=dag,
    )

    run_r_script 

    if previous_task:
        previous_task >> run_r_script

    previous_task = run_r_script


