from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'export',
        owner='vylnt1',
        emails=['vylnt1@rever.vn'],
        description='Export Inquiry Description From DM',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval=None,
        start_date=datetime(2022, 11, 30),
        end_date=None,
        catchup=False,
        tags=['Export', 'Inquiry Description'],
) as dag:
    from spark_job_synchronizer_factory import SUPPORT_REPORT

    version = 'v2.0'

    export_inquiry_description_operator = RLocalSparkOperator(
        task_id="export_inquiry_description",
        project=SUPPORT_REPORT,
        version=version,
        main_clazz='rever.etl.support.InquiryDescriptionRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "RV_S3_ACCESS_KEY": Variable.get('RV_S3_ACCESS_KEY', ''),
            "RV_S3_SECRET_KEY": Variable.get('RV_S3_SECRET_KEY', ''),
            "RV_S3_REGION": Variable.get('RV_S3_REGION', ''),
            "RV_S3_BUCKET": Variable.get('RV_S3_BUCKET', ''),
            "RV_S3_PARENT_PATH": Variable.get('RV_S3_PARENT_PATH').strip(),
            "source_table": 'inquiry',
            'merge_after_write': 'true'
        }

    )

    export_inquiry_description_operator
