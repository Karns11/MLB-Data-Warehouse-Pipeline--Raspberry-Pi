from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine

from sql_python_functions.mlb_sql_python_functions import add_people_data_to_staging
from sql_python_functions.mlb_sql_python_functions import update_people_dim
from sql_python_functions.mlb_sql_python_functions import insert_people_dim_data
from sql_python_functions.mlb_sql_python_functions import add_team_data_to_staging
from sql_python_functions.mlb_sql_python_functions import update_team_dim
from sql_python_functions.mlb_sql_python_functions import insert_team_dim_data
from sql_python_functions.mlb_sql_python_functions import add_surrogate_keys_to_fact
from sql_python_functions.mlb_sql_python_functions import append_stats
from sql_python_functions.mlb_sql_python_functions import add_surrogate_keys_to_team_games_fact
from sql_python_functions.mlb_sql_python_functions import append_team_game_stats
from sql_python_functions.mlb_sql_python_functions import update_is_currently_qualified_field




AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

default_args = {
    'owner': 'nathan',
}

dag = DAG(
    'run_baseballr_etl_pipeline_daily',
    default_args=default_args,
    description='Run an R script every day at 10AM',
    schedule_interval='0 14 * * *',  #None, #
    start_date=datetime(2025, 6, 14),
    catchup=False,
    tags=['rscript'],
)


# --- CONFIG ---
BACKFILL_FLAG = 0

START_DATE = datetime.strptime("2025-05-02", "%Y-%m-%d") # 03-20 then 03-28 then #07-19
END_DATE = datetime.strptime("2025-06-21", "%Y-%m-%d") # 03-21 then 07-14 then #09-30

date_range = [
    (START_DATE + timedelta(days=i)).strftime("%Y-%m-%d")
    for i in range((END_DATE - START_DATE).days + 1)
]


if BACKFILL_FLAG == 0:
    date_range = [(datetime.today() - timedelta(days=1)).date()] #yesterday, not sure why I need to subtract 2 days

previous_task = None  # Keep track of the end of the last date's chain
for date in date_range:
    bash_cmd = f'Rscript {AIRFLOW_HOME}/r_scripts/baseballr_pbp_scheduler.R {date}'

    run_r_script = BashOperator(
        task_id=f"run_r_script_{date}",
        bash_command=bash_cmd,
        dag=dag,
    )

    run_add_people_to_staging= PythonOperator(
        task_id=f"add_people_data_to_staging_{date}",
        python_callable=add_people_data_to_staging,
        op_args=[date],
        dag=dag,
    )

    run_update_people_dim = PythonOperator(
        task_id=f"updated_people_dim_{date}",
        python_callable=update_people_dim,
        op_args=[date],
        dag=dag,
    )

    run_insert_people_dim_data = PythonOperator(
        task_id=f"insert_people_dim_data_{date}",
        python_callable=insert_people_dim_data,
        op_args=[date],
        dag=dag,
    )

    run_add_team_to_staging= PythonOperator(
        task_id=f"add_team_data_to_staging_{date}",
        python_callable=add_team_data_to_staging,
        op_args=[date],
        dag=dag,
    )

    run_update_team_dim = PythonOperator(
        task_id=f"update_team_dim_{date}",
        python_callable=update_team_dim,
        op_args=[date],
        dag=dag,
    )

    run_insert_team_dim_data = PythonOperator(
        task_id=f"insert_team_dim_data{date}",
        python_callable=insert_team_dim_data,
        op_args=[date],
        dag=dag,
    )

    run_dbt = BashOperator(
        task_id=f"run_dbt_build_{date}",
        # bash_command="cd /opt/airflow/dbt && dbt build --full-refresh -d",
        bash_command=(
        f"cd /opt/airflow/dbt && dbt deps && "
        f"dbt build --full-refresh -d --vars '{{\"date_variable\": \"{date}\"}}'"
    ),
        dag=dag,
    )

    run_add_on_surrogate_keys_to_fact = PythonOperator(
        task_id=f"add_on_surrogate_key_to_fact_{date}",
        python_callable=add_surrogate_keys_to_fact,
        op_args=[date],
        dag=dag,
    )

    run_append_stats = PythonOperator(
        task_id=f"append_player_stats_{date}",
        python_callable=append_stats,
        op_args=[date],
        dag=dag,
    )

    run_add_surrogate_keys_to_team_games_fact = PythonOperator(
        task_id=f"add_surrogate_keys_to_team_games_fact_{date}",
        python_callable=add_surrogate_keys_to_team_games_fact,
        op_args=[date],
        dag=dag,
    )

    run_append_team_game_stats = PythonOperator(
        task_id=f"append_team_game_stats_{date}",
        python_callable=append_team_game_stats,
        op_args=[date],
        dag=dag,
    )

    run_update_is_currently_qualified_field = PythonOperator(
        task_id=f"update_is_currently_qualified_field_{date}",
        python_callable=update_is_currently_qualified_field,
        op_args=[date],
        dag=dag,
    )
    
    

    # Run dbt after R script for the same date
    run_r_script >> run_add_people_to_staging >> run_update_people_dim >> run_insert_people_dim_data >> run_add_team_to_staging >> run_update_team_dim >> run_insert_team_dim_data >> run_dbt >> run_add_on_surrogate_keys_to_fact >> run_append_stats >> run_add_surrogate_keys_to_team_games_fact >> run_append_team_game_stats >> run_update_is_currently_qualified_field


    if previous_task:
        previous_task >> run_r_script

    previous_task = run_update_is_currently_qualified_field


