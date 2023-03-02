from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'vision_tracker_hr',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Vision Tracker HR',
        default_args={
            'wait_for_downstream': False,
            'retries': 1
        },
        schedule_interval='45 2 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 12, 4),
        end_date=None,
        catchup=True,
        tags=['HR', 'Users', 'Vision Tracker'],
) as dag:
    from spark_job_synchronizer_factory import HR_REPORT

    version = 'v2.0'

    # Wait for historical/user_historical.py to complete first.
    user_historical_dag_sensor = ExternalTaskSensor(
        task_id='wait_for_user_historical',
        external_dag_id="user_historical",
        execution_delta=timedelta(days=2, hours=2, minutes=40),
        check_existence=True
    )

    hr_lifecycle_operator = RLocalSparkOperator(
        task_id="exec_total_hr",
        project=HR_REPORT,
        version=version,
        main_clazz='rever.etl.hr.HrLifecycleRunner',
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
            'lc_job_titles': Variable.get('LC_JOB_TITLES'),
            'rva_job_titles': Variable.get('AGENT_JOB_TITLES'),
            'sm_job_titles': Variable.get('SM_JOB_TITLES'),
            'sd_job_titles': Variable.get('SD_JOB_TITLES'),
            "hr_hired_quit_topic": 'rap.analytics.hr_hired_quit',
            'user_historical_job_id': 'user_historical',
            'merge_after_write': 'true',
            "run_rebuild": 'false'
        }
    )

    active_user_operator = RLocalSparkOperator(
        task_id="exec_total_active_agent",
        project=HR_REPORT,
        version=version,
        main_clazz='rever.etl.hr.TotalActiveUserRunner',
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
            'lc_job_titles': Variable.get('LC_JOB_TITLES'),
            'rva_job_titles': Variable.get('AGENT_JOB_TITLES'),
            'sm_job_titles': Variable.get('SM_JOB_TITLES'),
            'sd_job_titles': Variable.get('SD_JOB_TITLES'),
            "total_active_user_topic": 'rap.analytics.hr_total_staff',
            'user_historical_job_id': 'user_historical',
            'merge_after_write': 'true'
        }
    )

    user_historical_dag_sensor >> [hr_lifecycle_operator, active_user_operator]
