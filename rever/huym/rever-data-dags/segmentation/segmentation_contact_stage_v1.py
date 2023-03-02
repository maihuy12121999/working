from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'contact_stage_segmentation',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Contact 8 Stage Segmentation',
        default_args={
            'wait_for_downstream': False,
        },
        # schedule_interval='0 * * * *',
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        end_date=None,
        catchup=False,
        tags=['Segmentation', 'Contact 8 Stage'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_STAGE_SEGMENTATION_PROJECT

    version = 'v3.1'

    spark_job_operator = RLocalSparkOperator(
        task_id="contact_stage_segmentation",
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
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "RV_PUSH_KAFKA_HOST": Variable.get('RV_PUSH_KAFKA_HOST'),
            "RV_PUSH_KAFKA_SK": Variable.get('RV_PUSH_KAFKA_SK'),
            "crm_pipeline_ids": ",".join(["1054", "1055", "1056", "1057", "1058"]),
            "for_sale_pipeline_ids": ",".join(["1000", "1057"]),
            "for_lease_pipeline_ids": ",".join(["1033", "1058"]),
            "primary_pipeline_ids": ",".join(["1034", "1042", "1045", "1056"]),
            "secondary_pipeline_ids": ",".join(["1002", "1003", "1043", "1044", "1054", "1055"]),
            "decision_stage_ids": ",".join(["1020", "1187", "1215", "1174", "1058", "1312", "1328", "1343"]),
            "under_contract_phase_ids": ",".join(["1010", "1089", "1077", "1140", "1021", "1145",
                                                  "1102", "1150", "1135", "1170", "1173",
                                                  "1176", "1177", "1180", "1183"]),
            "won_phase_ids": ",".join(["1171", "1174", "1178", "1181", "1184"]),
            "won_stage_ids": ",".join(["1331", "1346", "1317", "1243", "1244"]),

            "segmentation_p_contact_topic": "segmentation.p_contact_8_stage",
            "segmentation_c_contact_topic": "segmentation.c_contact_8_stage",
            'push_to_kafka': 'true'
        }
    )

    spark_job_operator
