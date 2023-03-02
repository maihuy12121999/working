from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'segmentation_contact_8stage',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Contact 8 Stage',
        default_args={
            'wait_for_downstream': False,
        },
        # schedule_interval='0 1 * * *',
        # start_date=datetime(2022, 9, 20),
        schedule_interval='0,15,30,45 * * * *',
        # start_date=datetime(2022, 11, 7),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Segmentation', 'Contact 8 Stage'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_STAGE_SEGMENTATION_PROJECT

    version = 'v3.4'

    contact_8stage_operator = RLocalSparkOperator(
        task_id="contact_8stage",
        project=CONTACT_STAGE_SEGMENTATION_PROJECT,
        version=version,
        main_clazz='rever.segmentation.StageSegmentationRunner',
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
            "crm_pipeline_ids": ",".join(["1054", "1055", "1056", "1057", "1058"]),
            "for_sale_pipeline_ids": ",".join(["1057"]),
            "for_lease_pipeline_ids": ",".join(["1058"]),
            "primary_pipeline_ids": ",".join(["1056"]),
            "secondary_pipeline_ids": ",".join(["1054", "1055"]),
            "won_phase_ids": Variable.get('OPPO_WON_PHASE_IDS'),
            "won_stage_ids": Variable.get('OPPO_WON_STAGE_IDS'),
            "segmentation_p_contact_topic": "segmentation.p_contact_8_stage",
            "segmentation_c_contact_topic": "segmentation.c_contact_8_stage",
            "p_contact_stage_historical_topic": "rap.analytics.p_contact_stage_historical",
            'push_to_kafka': 'true',
            'merge_after_write': 'true'
        }
    )

    contact_8stage_operator
