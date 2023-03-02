from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'call_non_call_users',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Call - Non Call User',
        default_args={
            'wait_for_downstream': False,
            'retries': 1
        },
        schedule_interval='45 2 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 12, 1),
        end_date=None,
        catchup=True,
        tags=['Call', 'Non Call Users'],
) as dag:
    from spark_job_synchronizer_factory import CALL_REPORT

    version = 'v1.1'

    # Wait for historical/user_historical.py to complete first.
    user_historical_dag_sensor = ExternalTaskSensor(
        task_id='wait_for_user_historical',
        external_dag_id="user_historical",
        execution_delta=timedelta(days=2, hours=2, minutes=40),
        check_existence=True
    )

    non_call_user_operator = RLocalSparkOperator(
        task_id="exec_non_call_users",
        project=CALL_REPORT,
        version=version,
        main_clazz='rever.etl.call.NonCallAgentRunner',
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
            "RV_S3_PARENT_PATH": '',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            'rva_job_titles': Variable.get('AGENT_JOB_TITLES'),
            'sm_job_titles': Variable.get('SM_JOB_TITLES'),
            'sd_job_titles': Variable.get('SD_JOB_TITLES'),
            "non_call_user_topic": 'rap.analytics.call_non_call_users',
            'user_historical_job_id': 'user_historical',
            'merge_after_write': 'true',
            'forcerun.enabled': '{{ dag_run.conf["forcerun.enabled"] if dag_run.conf else "false" }}',
            'forcerun.from_date': '{{ dag_run.conf["forcerun.from_date"] if dag_run.conf else "" }}',
            'forcerun.to_date': '{{ dag_run.conf["forcerun.to_date"] if dag_run.conf else "" }}'
        }
    )

    user_historical_dag_sensor >> non_call_user_operator
