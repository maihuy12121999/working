from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'mysql_to_clickhouse_rever_user',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Sync Rever User To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='*/5 * * * *',
        # schedule_interval='01 * * * *',
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'MySQL -> ClickHouse', 'Rever User'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_user_operator = RLocalSparkOperator(
        task_id="sync_sys_rever_user_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.ReverUserToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),

            "MYSQL_DRIVER": Variable.get('MYSQL_DRIVER'),
            "MYSQL_HOST": Variable.get('MYSQL_HOST'),
            "MYSQL_PORT": Variable.get('MYSQL_PORT'),
            "MYSQL_USER_NAME": Variable.get('MYSQL_USER_NAME'),
            "MYSQL_PASSWORD": Variable.get('MYSQL_PASSWORD'),
            "MYSQL_DB": 'rever-profile',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "source_table": 'user',
            "target_table": 'rever_profile_user',
            'rva_job_titles': Variable.get('AGENT_JOB_TITLES'),
            'sm_job_titles': Variable.get('SM_JOB_TITLES'),
            'sd_job_titles': Variable.get('SD_JOB_TITLES'),
            "SYSTEM_USER_EMAILS": Variable.get('SYSTEM_USER_EMAILS'),
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_user_operator
