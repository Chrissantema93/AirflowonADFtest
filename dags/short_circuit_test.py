from airflow import DAG
from datetime import datetime
import os
import time
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator

environment = os.getenv("AIRFLOW_VAR_ENVIRONMENT", "")

default_args = {"owner": "philips poc test", "retries": 1}

with DAG(
    default_args=default_args,
    dag_id="short_circuit_test1",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    doc_md="Testing the ShortCircuitOperator",
    tags=["testing"],
) as dag:
    dummy_task_1 = DummyOperator(
        task_id="dummy_task_1", doc="dummy to test shortcircuit"
    )
    dummy_task_2 = DummyOperator(
        task_id="dummy_task_2", doc="dummy to test shortcircuit"
    )
    python_sleep_task = PythonOperator(
        task_id="python_sleep_task", python_callable=lambda: time.sleep(10)
    )
    dummy_task_3_run_in_env = ShortCircuitOperator(
        task_id="dummy_task_3_run_in_env",
        doc="ShortCircuit to make sure dummy_task_3 only runs in Development, QA",
        python_callable=lambda: environment in "Development, QA",
        ignore_downstream_trigger_rules=False,
    )
    dummy_task_3 = DummyOperator(
        task_id="dummy_task_3", doc="dummy to test shortcircuit"
    )
    dummy_task_4 = DummyOperator(
        task_id="dummy_task_4", doc="dummy to test shortcircuit"
    )

dummy_task_1 >> [dummy_task_2, dummy_task_3_run_in_env]
dummy_task_2 >> [python_sleep_task]
dummy_task_3_run_in_env >> [dummy_task_3]
dummy_task_3 >> [dummy_task_4]
