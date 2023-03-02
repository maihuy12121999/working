from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'publish_listings',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Publish Listing',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='45 0 * * *',
        # start_date=datetime(2016, 6, 7),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['EMC', 'Publish Listing'],
) as dag:
    from spark_job_synchronizer_factory import LISTING_REPORT

    version = 'v2.8'

    # Wait for historical/user_historical.py to complete first.
    user_historical_dag_sensor = ExternalTaskSensor(
        task_id='wait_for_user_historical',
        external_dag_id="user_historical",
        execution_delta=timedelta(minutes=40),
        check_existence=True
    )

    spark_job_operator = RLocalSparkOperator(
        task_id="exec_publish_listings",
        project=LISTING_REPORT,
        version=version,
        main_clazz='rever.etl.listing.PublishedListingRunner',
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
            "published_listing_topic": 'rap.analytics.publish_listings',
            'user_historical_job_id': 'user_historical',
            'republish_gap_duration': f'{30 * 24 * 60 * 60 * 1000}'
        }
    )

    spark_job_operator
