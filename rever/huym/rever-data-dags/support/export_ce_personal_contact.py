from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'export_tool_hot_lead_distribution',
        owner='huym',
        emails=['huym@rever.vn'],
        description='Export Tool - Hot Lead',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval=None,
        # start_date=datetime(2020, 3, 20),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=False,
        tags=['Export Tool', 'Hot Lead'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_REPORT

    version = 'v3.0'

    export_hot_lead_operator = RLocalSparkOperator(
        task_id="export_tool_hot_lead_distribution",
        project=CONTACT_REPORT,
        version=version,
        main_clazz='rever.etl.emc.HotLeadRunner',
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
            "system_tags": "hot_lead",
            'merge_after_write': 'true'
        }
    )

    export_hot_lead_operator
