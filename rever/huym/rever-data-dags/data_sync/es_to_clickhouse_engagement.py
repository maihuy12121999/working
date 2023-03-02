from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'es_to_clickhouse_rever_engagement',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Rever Engagement To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='*/5 * * * *',
        # schedule_interval='01 * * * *',
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'Rever Engagement', 'ES -> ClickHouse'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_call_operator = RLocalSparkOperator(
        task_id="sync_call_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.EngagementToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'rever-engagement',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "engagement_type": 'call',
            "target_engagement_table": 'engagement_call',
            "source_batch_size": '200',
            "target_batch_size": '500',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_email_operator = RLocalSparkOperator(
        task_id="sync_email_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.EngagementToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'rever-engagement',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "engagement_type": 'email',
            "target_engagement_table": 'engagement_email',
            "source_batch_size": '200',
            "target_batch_size": '500',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_meeting_operator = RLocalSparkOperator(
        task_id="sync_meeting_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.EngagementToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'rever-engagement',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "engagement_type": 'meeting',
            "target_engagement_table": 'engagement_meeting',
            "source_batch_size": '200',
            "target_batch_size": '500',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_note_operator = RLocalSparkOperator(
        task_id="sync_note_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.EngagementToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'rever-engagement',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "engagement_type": 'note',
            "target_engagement_table": 'engagement_note',
            "source_batch_size": '200',
            "target_batch_size": '500',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    sync_survey_operator = RLocalSparkOperator(
        task_id="sync_survey_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.EngagementToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_ES_SERVERS": Variable.get('RV_ES_SERVERS'),
            "RV_ES_CLUSTER_NAME": Variable.get('RV_ES_CLUSTER_NAME'),
            "RV_ES_INDEX_NAME": 'rever-engagement',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "engagement_type": 'survey',
            "target_engagement_table": 'engagement_survey',
            "source_batch_size": '200',
            "target_batch_size": '500',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf["is_sync_all"] if dag_run.conf else "false" }}',
        }
    )

    [sync_meeting_operator, sync_note_operator, sync_survey_operator] >> sync_call_operator >> sync_email_operator
