from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'new_and_total_inquiry',
        owner='vylnt1',
        emails=['vylnt1@rever.vn'],
        description='EMC Inquiry',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='45 0 * * *',
        # start_date=datetime(2021, 9, 14),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['EMC', 'Inquiry'],
) as dag:
    from spark_job_synchronizer_factory import INQUIRY_REPORT

    version = 'v1.6'

    spark_job_operator = RLocalSparkOperator(
        task_id="new_and_total_inquiry",
        project=INQUIRY_REPORT,
        version=version,
        main_clazz='rever.etl.emc.InquiryRunner',
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
            "RV_PUSH_KAFKA_HOST": Variable.get('RV_PUSH_KAFKA_HOST'),
            "RV_PUSH_KAFKA_SK": Variable.get('RV_PUSH_KAFKA_SK'),
            "new_inquiry_topic": 'rap.analytics.new_inquiry',
            "total_inquiry_topic": 'rap.analytics.total_inquiry',
            'merge_after_write': 'true'
        }
    )

    spark_job_operator
