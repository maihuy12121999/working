from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'listing_view_impression',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Listing View Impression',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='30 1 * * *',
        # start_date=datetime(2020, 6, 14),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Listing', 'View Impression'],
) as dag:
    from spark_job_synchronizer_factory import LISTING_REPORT

    version = 'v2.8'

    spark_job_operator = RLocalSparkOperator(
        task_id="exec_listing_view_impression",
        project=LISTING_REPORT,
        version=version,
        main_clazz='rever.etl.listing.ListingImpressionRunner',
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
            "listing_view_impressions_topic": 'rap.analytics.listing_view_impressions'
        }
    )

    spark_job_operator
