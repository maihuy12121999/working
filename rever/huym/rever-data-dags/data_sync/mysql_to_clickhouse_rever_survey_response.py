from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

from common.utils import base64_encode

with RvDAG(
        'mysql_to_clickhouse_rever_survey',
        owner='vylnt1',
        emails=['vylnt1@rever.vn'],
        description='Sync Rever Survey To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='10 * * * *',
        start_date=datetime(2022, 12, 28),
        end_date=None,
        catchup=False,
        tags=['Data Sync', 'MySQL -> ClickHouse', 'Rever Survey Response'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_survey_response_operator = RLocalSparkOperator(
        task_id="sync_sys_rever_survey_response_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.EnjoyedSurveyResponseToCHRunner',
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
            "MYSQL_DB": 'rever-enjoyed',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "primary_pipeline_ids": Variable.get('OPPO_PRIMARY_SALE_PIPELINE_IDS'),
            "secondary_pipeline_ids": Variable.get('OPPO_SECONDARY_SALE_PIPELINE_IDS'),
            "phase_standardized_mapping": base64_encode(Variable.get('PHASE_STANDARDIZED_MAPPING')),
            "phase_to_stage_mapping": base64_encode(Variable.get('PHASE_TO_STAGE_MAPPING')),
            "oppo_stage_to_phase_id_mapping": base64_encode(Variable.get('OPPO_STAGE_TO_PHASE_ID_MAPPING')),
            "business_mapping": base64_encode(Variable.get('SURVEY_BUSINESS_MAPPING')),
            "source_table": 'survey_person',
            "target_table": 'survey_response',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf.get("is_sync_all", "false") if dag_run.conf else "false" }}'
        }
    )

    sync_survey_response_operator
