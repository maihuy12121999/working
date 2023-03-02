from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'isa_report_oppo_phase_funnel',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Oppo phase funnel',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='45 1 * * *',
        # start_date=datetime(2021, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['ISA Dashboard', 'Oppo Phase Funnel'],
) as dag:
    from spark_job_synchronizer_factory import ISA_REPORT_JOB_PROJECT

    version = 'v4.5'

    oppo_phase_funnel_operator = RLocalSparkOperator(
        task_id="exec_oppo_phase_funnel",
        project=ISA_REPORT_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.isa.OppoPhaseFunnelEntryPoint',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "oppo_phase_funnel_topic": 'rap.analytics.isa_dashboard_oppo_phase_funnel',
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
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST')
        }
    )

    oppo_phase_funnel_operator
