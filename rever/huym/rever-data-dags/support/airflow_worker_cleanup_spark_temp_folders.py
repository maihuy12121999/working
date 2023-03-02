from datetime import datetime

#
# @author: anhlt
#
from airflow.operators.bash import BashOperator

from common.rv_dag import RvDAG

with RvDAG(
        'airflow_worker_cleanup_spark_temp_folders',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Cleanup temporarily folders in Airflow Worker',
        default_args={
            'depends_on_past': False,
            'wait_for_downstream': False
        },
        schedule_interval='@weekly',
        start_date=datetime(2022, 10, 1),
        catchup=False,
        tags=['Cleanup Tool', 'Airflow Worker'],
) as dag:
    cleanup_spark_folder_blockmgr = BashOperator(
        task_id="cleanup_spark_folder_blockmgr",
        bash_command="rm -rf /tmp/blockmgr-*"
    )

    cleanup_spark_folder_blockmgr
