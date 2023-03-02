from datetime import datetime, timedelta

#
# @author: anhlt
#
from airflow.operators.python import PythonOperator

from common.rv_dag import RvDAG

with RvDAG(
        'common_test_failure_dag',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='A common dag to test failure',
        default_args={
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=1),
            'depends_on_past': False,
            'wait_for_downstream': False,
        },
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        end_date=None,
        catchup=False,
        tags=['Test', 'Common'],
) as dag:
    def throw_ex():
        raise Exception("Ann error from python")


    ex_operator = PythonOperator(
        task_id='throw_an_exception',
        python_callable=throw_ex
    )

    ex_operator
