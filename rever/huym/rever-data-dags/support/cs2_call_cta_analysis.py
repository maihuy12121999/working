from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'cs2_call_cta_analysis',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='CS2: Export Call CTA Analysis',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='0 3 * * *',
        # start_date=datetime(2022, 6, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Export', 'CS2', 'Call CTA'],
) as dag:
    from spark_job_synchronizer_factory import SUPPORT_REPORT

    version = 'v1.8'

    events = ["AgentDetailV2_ClickAgentPhone",
              "ContactProperty_ClickHiddenHotlineInForm",
              "MLSContact_ClickHiddenHotline",
              "ProjectDetailV3_ClickRVA3CX",
              "PropertyDetail_ClickRVA3CX",
              "ContactProject_ClickFixedSidebarHotline",
              "ContactProject_ClickSidebarHotline",
              "ContactProperty_ClickHiddenHotline",
              "ContactProperty_ClickHiddenHotlineInForm",
              "hotline"]

    spark_job_operator = RLocalSparkOperator(
        task_id="cs2_export_call_cta_event",
        project=SUPPORT_REPORT,
        version=version,
        main_clazz='rever.etl.support.CallCtaAnalysisEntryPoint',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "MYSQL_DRIVER": Variable.get('MYSQL_DRIVER'),
            "MYSQL_HOST": Variable.get('MYSQL_HOST'),
            "MYSQL_PORT": Variable.get('MYSQL_PORT'),
            "MYSQL_USER_NAME": Variable.get('MYSQL_USER_NAME'),
            "MYSQL_PASSWORD": Variable.get('MYSQL_PASSWORD'),
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "call_cta_events": ",".join(events),
            "cs2_call_cta_event_topic": "rap.analytics.cs2_call_cta_events",
            "cs2_call_cta_analysis_topic": "rap.analytics.cs2_call_cta_analysis",
            "max_duration_after_clicked": f"{5 * 60 * 1000}",
            "max_duration_after_user_clicked": f"{10 * 60 * 1000}",
            'merge_after_write': 'true'
        }
    )

    spark_job_operator
