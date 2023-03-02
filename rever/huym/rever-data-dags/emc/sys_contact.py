from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'sys_contact',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='EMC Sys Contact',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='5 1 * * *',
        # start_date=datetime(2020, 3, 20),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['EMC', 'Sys Contact'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_REPORT

    version = 'v2.8'

    # Wait for historical/user_historical.py to complete first.
    user_historical_dag_sensor = ExternalTaskSensor(
        task_id='wait_for_user_historical',
        external_dag_id="user_historical",
        execution_delta=timedelta(hours=1),
        check_existence=True
    )

    spark_job_operator = RLocalSparkOperator(
        task_id="new_and_total_sys_contact",
        project=CONTACT_REPORT,
        version=version,
        main_clazz='rever.etl.emc.SysContactRunner',
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
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "new_sys_contact_topic": 'rap.analytics.new_sys_contact',
            "total_sys_contact_topic": 'rap.analytics.total_sys_contact',
            "user_historical_job_id": "user_historical",
            'merge_after_write': 'true',
            'forcerun.enabled': '{{ dag_run.conf["forcerun.enabled"] if dag_run.conf else "false" }}',
            'forcerun.from_date': '{{ dag_run.conf["forcerun.from_date"] if dag_run.conf else "" }}',
            'forcerun.to_date': '{{ dag_run.conf["forcerun.to_date"] if dag_run.conf else "" }}'
        }
    )

    user_historical_dag_sensor >> spark_job_operator
