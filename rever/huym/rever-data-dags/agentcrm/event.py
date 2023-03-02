from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'agentcrm_event',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='AgentCRM - Event',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='5 * * * *',
        # start_date=datetime(2022, 8, 24),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['AgentCRM', 'Event'],
) as dag:
    from spark_job_synchronizer_factory import AGENTCRM_REPORT_PROJECT

    version = 'v1.8'

    spark_job_operator = RLocalSparkOperator(
        task_id="run_event_job",
        project=AGENTCRM_REPORT_PROJECT,
        version=version,
        main_clazz='rever.etl.agentcrm.EventAnalysisRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "event_topic": 'rap.analytics.agentcrm_event_actions',
            "tech_team_ids": ",".join(['1170', '1164', '1114', '1172', '1166', '1168', '1191']),
            "tracking_segment_project_ids": ",".join(['9rTFbmMAfAc6pXvoUKXYqo']),
            "merge_after_write": 'true'
        }
    )

    spark_job_operator
