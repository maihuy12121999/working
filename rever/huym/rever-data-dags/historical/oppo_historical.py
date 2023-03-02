from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'oppo_historical',
        owner='vyltn1',
        emails=['vyltn1@rever.vn'],
        description='Oppo Historical',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='30 0 * * *',
        # start_date=datetime(2019, 6, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Oppo', 'Historical'],
) as dag:
    from spark_job_synchronizer_factory import ISA_REPORT_JOB_PROJECT

    version = 'v4.8'

    oppo_report_operator = RLocalSparkOperator(
        task_id="exec_oppo_historical",
        project=ISA_REPORT_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.oppo_historical.OppoHistoricalEntryPoint',
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
            "RV_S3_PARENT_PATH": Variable.get('RV_S3_PARENT_PATH').strip()
        }
    )

    oppo_report_operator
