from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'vision_tracker_total_customer',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Vision Tracker Customer',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='30 1 * * *',
        # start_date=datetime(2020, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Vision Tracker', 'Customer'],
) as dag:
    from spark_job_synchronizer_factory import VISION_TRACKER_REPORT_JOB_PROJECT

    version = 'v6.7'

    spark_job_operator = RLocalSparkOperator(
        task_id="exec_total_customer",
        project=VISION_TRACKER_REPORT_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.vision_tracker.TotalCustomerEntryPoint',
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
            "primary_sale_pipeline_ids": '1034,1042',
            "secondary_sale_pipeline_ids": '1002,1003,1043,1044',
            "primary_success_stage_ids": '1317,1318,1319,1320,1321,1216,1221',
            "total_customer_topic": 'rap.analytics.vision_tracker_total_customer',
            'merge_after_write': 'true'
        }
    )

    spark_job_operator
