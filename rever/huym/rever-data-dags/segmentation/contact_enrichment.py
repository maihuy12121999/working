from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'contact_enrichment',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Contact Enrichment',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='0 * * * *',
        start_date=datetime(2022, 12, 15),
        end_date=None,
        catchup=True,
        tags=['Enrichment', 'Personal Contact'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_REPORT

    version = 'v2.8'

    p_contact_enrichment_operator = RLocalSparkOperator(
        task_id="p_contact_enrichment",
        project=CONTACT_REPORT,
        version=version,
        main_clazz='rever.etl.emc.PContactFulfillmentRunner',
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
            "data_delivery_personal_contact_fulfillment_topic": "data_delivery.enrichment.personal_contact",
            'push_to_kafka': 'true',
            'merge_after_write': 'true',
            'is_sync_all': '{{ dag_run.conf.get("is_sync_all", "false") if dag_run.conf else "false" }}',
            'forcerun.enabled': '{{ dag_run.conf.get("forcerun.enabled", "false") if dag_run.conf else "false" }}',
            'forcerun.from_date': '{{ dag_run.conf.get("forcerun.from_date", "") if dag_run.conf else "" }}',
            'forcerun.to_date': '{{ dag_run.conf.get("forcerun.to_date", "") if dag_run.conf else "" }}'
        }
    )

    p_contact_enrichment_operator
