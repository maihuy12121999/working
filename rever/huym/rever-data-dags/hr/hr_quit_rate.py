from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'hr_quit_rate',
        owner='vylnt1',
        emails=['vylnt1@rever.vn', 'anhlt@rever.vn'],
        description='HR Quit Rate',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='50 2 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['HR', 'Quit Rate'],
) as dag:
    from spark_job_synchronizer_factory import HR_REPORT

    version = 'v1.9'

    # Wait for vision_tracker/call_non_call_users.py to complete first.
    vision_tracker_hr_dag_sensor = ExternalTaskSensor(
        task_id='wait_for_vision_tracker_hr',
        external_dag_id="vision_tracker_hr",
        execution_delta=timedelta(minutes=5),
        check_existence=True
    )

    spark_job_operator = RLocalSparkOperator(
        task_id="exec_hr_quit_rate",
        project=HR_REPORT,
        version=version,
        main_clazz='rever.etl.hr.QuitRateRunner',
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
            "hr_hired_quit_table": 'analytics.hr_hired_quit_1',
            "total_rv_agent_table": 'analytics.hr_total_staff_1',
            "hr_quit_rate_topic": 'rap.analytics.hr_quit_rate',
            "merge_after_write": 'true'
        }
    )

    vision_tracker_hr_dag_sensor >> spark_job_operator
