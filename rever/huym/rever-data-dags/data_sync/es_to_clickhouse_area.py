from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'es_to_clickhouse_area',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Area To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='04 * * * *',
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'Rever Area', 'ES -> ClickHouse'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_area_operator = RLocalSparkOperator(
        task_id="sync_rever_area_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.AreaToClickhouseRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'rever-search',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "target_area_table": 'rever_search_area',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_area_operator
