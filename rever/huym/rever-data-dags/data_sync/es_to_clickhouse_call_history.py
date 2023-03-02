from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'sync_es_call_history_to_clickhouse',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Sync Call History To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='*/5 * * * *',
        # schedule_interval='20 * * * *',
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'Call History', 'ES -> ClickHouse'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_call_operator = RLocalSparkOperator(
        task_id="sync_es_call_history_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.CallHistoryToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'call_history',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "target_call_history_table": 'call_history',
            'source_batch_size': '500',
            'target_batch_size': '800',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_call_operator
