from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'web_user_agents',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='User Agent Info analysis',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='15 0 * * *',
        # start_date=datetime(2020, 6, 14),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['User Agent', 'Web'],
) as dag:
    from spark_job_synchronizer_factory import USER_AGENT_ANALYSIS_PROJECT

    version = 'v1.2'

    spark_job_operator = RLocalSparkOperator(
        task_id="run_user_agent_job",
        project=USER_AGENT_ANALYSIS_PROJECT,
        version=version,
        main_clazz='rever.etl.user_agent.ParseUserAgentRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "RV_S3_ACCESS_KEY": Variable.get('RV_S3_ACCESS_KEY', ''),
            "RV_S3_SECRET_KEY": Variable.get('RV_S3_SECRET_KEY', ''),
            "RV_S3_REGION": Variable.get('RV_S3_REGION', ''),
            "RV_S3_BUCKET": Variable.get('RV_S3_BUCKET', ''),
            "RV_S3_PARENT_PATH": Variable.get('RV_S3_PARENT_PATH').strip(),
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "user_agent_topic": 'rap.analytics.web_user_agents',
            "merge_after_write": 'true'
        }
    )

    spark_job_operator
