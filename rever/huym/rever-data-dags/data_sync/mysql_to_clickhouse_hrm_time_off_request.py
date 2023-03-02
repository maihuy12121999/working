from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'mysql_to_clickhouse_hrm_time_off_request',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Sync HRM TimeOff Request To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='*/5 * * * *',
        # schedule_interval='05 * * * *',
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'MySQL -> ClickHouse', 'HRM TimeOff Request'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_time_off_request_operator = RLocalSparkOperator(
        task_id="sync_hrm_time_off_request_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.TimeOffRequestToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "MYSQL_DRIVER": Variable.get('MYSQL_DRIVER'),
            "MYSQL_HOST": Variable.get('MYSQL_HOST'),
            "MYSQL_PORT": Variable.get('MYSQL_PORT'),
            "MYSQL_USER_NAME": Variable.get('MYSQL_USER_NAME'),
            "MYSQL_PASSWORD": Variable.get('MYSQL_PASSWORD'),
            "MYSQL_DB": 'rever-time-off',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "source_table": 'booking',
            "target_table": 'hrm_time_off_request',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_time_off_request_operator
