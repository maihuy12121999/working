from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'sys_contact_historical',
        owner='huym',
        emails=['huym@rever.vn', 'anhlt@rever.vn'],
        description='Sys Contact Historical',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='10 0 * * *',
        # start_date=datetime(2020, 3, 20),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Historical', 'Sys Contact'],
) as dag:
    from spark_job_synchronizer_factory import CONTACT_REPORT

    version = 'v2.8'

    spark_job_operator = RLocalSparkOperator(
        task_id="sys_contact_historical",
        project=CONTACT_REPORT,
        version=version,
        main_clazz='rever.etl.emc.SysContactHistoricalRunner',
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
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD')
        }
    )

    spark_job_operator
