from datetime import datetime, timedelta

#
# @author: anhlt
#
from airflow.operators.python import PythonOperator

from common.rv_dag import RvDAG

with RvDAG(
        'common_gen_fernet_key_dag',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='A common dag to gen fernet key',
        default_args={
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=1),
            'depends_on_past': False,
            'wait_for_downstream': False,
            # 'queue': 'bash_queue',
            'pool': 'default_pool',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),

            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
            # 'trigger_rule': 'all_success'
        },
        schedule_interval='0,15,30,45 * * * *',
        start_date=datetime(2022, 1, 1),
        end_date=None,
        catchup=False,
        tags=['Test', 'Common'],
) as dag:
    def gen_fernet_key():
        from cryptography.fernet import Fernet
        fernet_key = Fernet.generate_key()
        print(f'New generated FERNET key: {fernet_key.decode()}')


    ex_operator = PythonOperator(
        task_id='gen_fernet_key',
        python_callable=gen_fernet_key
    )

    ex_operator
