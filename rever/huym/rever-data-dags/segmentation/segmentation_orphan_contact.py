from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'segmentation_orphan_contact',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Orphan Contact Segmentation',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='20 3 * * *',
        # start_date=datetime(2022, 10, 20),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Segmentation', 'Orphan Contact'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_STAGE_SEGMENTATION_PROJECT

    version = 'v3.4'

    # Wait for historical/sys_contact_historical to complete first.
    sys_contact_historical_sensor = ExternalTaskSensor(
        task_id='wait_for_sys_contact_historical',
        external_dag_id="sys_contact_historical",
        execution_delta=timedelta(hours=3, minutes=10),
        check_existence=True
    )

    # Wait for historical/sys_contact_historical to complete first.
    personal_contact_historical_sensor = ExternalTaskSensor(
        task_id='wait_for_personal_contact_historical',
        external_dag_id="personal_contact_historical",
        execution_delta=timedelta(hours=3, minutes=7),
        check_existence=True
    )

    spark_job_operator = RLocalSparkOperator(
        task_id="segmentation_orphan_contact",
        project=CONTACT_STAGE_SEGMENTATION_PROJECT,
        version=version,
        main_clazz='rever.segmentation.OrphanContactRunner',
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
            "RV_PUSH_KAFKA_HOST": Variable.get('RV_PUSH_KAFKA_HOST'),
            "RV_PUSH_KAFKA_SK": Variable.get('RV_PUSH_KAFKA_SK'),
            'personal_contact_historical_job_id': 'personal_contact_historical',
            'sys_contact_historical_job_id': 'sys_contact_historical',
            "segmentation_sys_contact_tagging_topic": "segmentation.sys_contact_tagging"
        }
    )

    [sys_contact_historical_sensor, personal_contact_historical_sensor] >> spark_job_operator
