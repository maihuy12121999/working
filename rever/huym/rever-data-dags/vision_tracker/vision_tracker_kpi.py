from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'vision_tracker_kpi',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Vision Tracker KPI',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='30 0 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=False,
        tags=['Vision Tracker', 'KPI'],
) as dag:
    from spark_job_synchronizer_factory import VISION_TRACKER_REPORT_JOB_PROJECT

    version = 'v6.7'

    spark_job_operator = RLocalSparkOperator(
        task_id="exec_kpi",
        project=VISION_TRACKER_REPORT_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.vision_tracker.KpiEntryPoint',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "kpi_topic": 'rap.analytics.vision_tracker_kpi',
            "causative_kpi_topic": 'rap.analytics.vision_tracker_kpi_causative',
            "agent_kpi_topic": 'rap.analytics.vision_tracker_kpi_agent',
            "customer_kpi_topic": 'rap.analytics.vision_tracker_kpi_customer',
            "nps_kpi_topic": 'rap.analytics.vision_tracker_kpi_nps',
            "ohi_topic": 'rap.analytics.vision_tracker_ohi',
            "primary_unit_topic": 'rap.analytics.vision_tracker_primary_unit',
            "primary_unit_kpi_topic": 'rap.analytics.vision_tracker_kpi_primary_unit',
            "merge_after_write": 'true'
        }
    )

    spark_job_operator
