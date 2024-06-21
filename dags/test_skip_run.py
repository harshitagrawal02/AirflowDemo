from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.state import State
from airflow.models import DagRun
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
}

def _check_if_previous_dag_is_running(**kwargs):
    dag_id = kwargs['dag'].dag_id
    current_execution_date = kwargs['data_interval_start']
    
    # Find the previous DAG run
    previous_dag_run = DagRun.find(
        dag_id=dag_id,
        execution_date=current_execution_date - timedelta(minutes=1)
    )
    
#    print(previous_dag_run)
    
    if previous_dag_run:
        return previous_dag_run[0].state != State.RUNNING
    return True



with DAG(
        'test_dag',
        default_args=default_args,
        schedule_interval='* * * * *',
#        max_active_runs=1,
        start_date=datetime(2024, 6, 5),
        catchup=False
) as dag:

    check_if_running = ShortCircuitOperator(
        task_id='check_if_running',
        python_callable=_check_if_previous_dag_is_running,
        provide_context=True
    )
    
    wait_task = BashOperator(
    task_id="wait_100_seconds",
    bash_command="sleep 100")



check_if_running >> wait_task







