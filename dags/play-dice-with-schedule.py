from airflow import DAG

from datetime import datetime

from airflow.operators.python import PythonOperator

import random

dag = DAG(
    dag_id='playing_dice_with_schedule',
    start_date=datetime(2024, 4, 6),
    end_date=datetime(2024, 4, 9),
    # schedule_interval=timedelta(hours=6)
    schedule_interval='@daily'

    # """
    # '@once', '@daily', '@hourly', '@weekly', '@monthly', '@yearly'
    #
    # schedule with CRON "*****"
    #     minute (0-59) / hour (0-23) /day of month (1-31) / month (1-12)/ day of week (0-6) (sunday is 0)
    # """
)

def play(ti):
    sum = 0
    for i in range(5):
        for j in range(3):
            sum+= random.randint(1, 6)

    ti.xcom_push(key='who_play', value=sum)

def result(ti):
    total = sum([val for val in ti.xcom_pull(key='who_play', task_ids=['player_hoang', 'player_duy'])])
    return total

anh_hoang_play = PythonOperator(
    task_id='player_hoang',
    python_callable=play,
    dag=dag
)

anh_duy_play = PythonOperator(
    task_id='player_duy',
    python_callable=play,
    dag=dag
)

ket_qua = PythonOperator(
    task_id='player_result',
    python_callable=result,
    dag=dag
)

[anh_hoang_play, anh_duy_play] >> ket_qua