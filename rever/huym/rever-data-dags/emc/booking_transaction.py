from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'booking_transactions',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='EMC Transactions',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='0 3 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['EMC', 'Booking Transaction'],
) as dag:
    from spark_job_synchronizer_factory import TRANSACTION_REPORT

    version = 'v6.6'

    booked_transaction_operator = RLocalSparkOperator(
        task_id="booking_transaction",
        project=TRANSACTION_REPORT,
        version=version,
        main_clazz='rever.etl.emc.BookingTransactionRunner',
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
            "primary_sale_pipeline_ids": Variable.get('OPPO_PRIMARY_SALE_PIPELINE_IDS'),
            "secondary_sale_pipeline_ids": Variable.get('OPPO_SECONDARY_SALE_PIPELINE_IDS'),
            "won_phase_ids": Variable.get('OPPO_WON_PHASE_IDS'),
            "won_stage_ids": Variable.get('OPPO_WON_STAGE_IDS'),
            "booking_transaction_topic": 'rap.analytics.booking_transaction',
            "f2_agent_ids": Variable.get('F2_AGENT_IDS'),
            "ctv_agent_ids": Variable.get('CTV_AGENT_IDS'),
            "f2_team_ids": Variable.get('F2_TEAM_IDS'),
            "ctv_team_ids": Variable.get('CTV_TEAM_IDS'),
            "oppo_historical_job_id": 'oppo_historical',
            'merge_after_write': 'true'
        }
    )

    booked_transaction_operator
