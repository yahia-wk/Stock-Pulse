from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}
with DAG(
    "retrain_lstm",
    schedule_interval="0 4 * * SUN",   # every Sunday 04:00
    start_date=datetime(2025, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["ml"],
):
    fetch = BashOperator(
        task_id="dump_last_week",
        bash_command="python /scripts/dump_ticks.py --week",
    )
    train = BashOperator(
        task_id="train_lstm",
        bash_command="python /scripts/train.py",
    )
    publish = BashOperator(
        task_id="push_prediction",
        bash_command="python /scripts/publish_signals.py",
    )
    fetch >> train >> publish
